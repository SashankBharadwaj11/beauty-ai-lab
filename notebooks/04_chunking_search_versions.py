# Databricks notebook source
# MAGIC %md
# MAGIC # Day 4: Chunking, search, versions and access over time
# MAGIC **Synthetic training data only. Not Estée Lauder or C5i material.**
# MAGIC
# MAGIC ### Business requirement
# MAGIC Brand managers ask "what wording is approved?" The answer must come from the version **in effect on the date asked about**, never from an outdated FAQ, a superseded policy or a future policy, and never from a document the user may not see. Kamal's question: *"What happens to the old embeddings when a policy is updated?"* This notebook answers it with evidence.
# MAGIC
# MAGIC ### Architecture
# MAGIC ```
# MAGIC  doc_registry (all versions, arrival batch, access group, authority, effective dates)
# MAGIC        │  replayed batch by batch: B0 Jul 1 → B1 Jul 15 → B2 Nov 1 → B3 Nov 12 → B4 Nov 20
# MAGIC        ▼
# MAGIC  parse  (PDF: ai_parse_document, or pypdf fallback · PPTX: python-pptx · XLSX: routed to tables, Day 5)
# MAGIC        ▼  headers/footers stripped, headings detected
# MAGIC  chunk  (structure-aware: sections, whole tables, speaker notes; title + section prefixed)
# MAGIC        ▼  MERGE (insert new, update changed) + supersession rule closes the previous version
# MAGIC  doc_chunks (Delta, change data feed ON)  ──Delta Sync──▶  AI Search index (embeddings)
# MAGIC        ▼
# MAGIC  search(query, user_groups, as_of)  =  filters INSIDE the query: access_group, effective_from_int <= d <= effective_to_int
# MAGIC        ▼  rerank by source authority (policy > reference > FAQ/draft)
# MAGIC  probes after every batch (hard rules ASSERTED) · retrieval eval on 21 questions · experiments E1–E6
# MAGIC ```
# MAGIC **Design decisions to defend:** (1) route by file type, not one parser for everything; (2) strip repeated headers so unchanged pages hash the same; (3) chunk by structure so a table row never splits and an update changes few chunks; (4) metadata filters inside the query, not after it; (5) integer dates for reliable range filters; (6) versions are never deleted. They are closed with `effective_to`, so history stays answerable.
# MAGIC
# MAGIC **Prerequisites:** Day 2 run; `BCIA_FULL.zip` in the `lab_files` volume; the `beauty-ai-lab` Git folder (this notebook imports `../src`).

# COMMAND ----------

# MAGIC %pip install -q pypdf python-pptx scikit-learn databricks-vectorsearch

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys, json, hashlib, time
from datetime import date
import pandas as pd
from pyspark.sql import Row, functions as F

sys.path.insert(0, os.path.abspath("../src"))
from bcia.config import CATALOG, SCHEMA, VOL_ROOT, DATA_ROOT, EXT_ROOT, VECTOR_ENDPOINT, EMBEDDING_ENDPOINT, DOC_TITLES, AUTHORITY, fq
from bcia.extension import ensure_extension, verify_hashes
from bcia.parsing import parse_pdf_pypdf, parse_pptx, normalize_native
from bcia.chunking import structure_chunks, fixed_chunks
from bcia.versions import effective_windows, date_to_int
from bcia.retrieval import is_visible, to_index_filters, rerank_by_authority
from bcia.evaluation import expected_docs, doc_hit

spark.sql(f"USE CATALOG {CATALOG}"); spark.sql(f"USE SCHEMA {SCHEMA}")
dbutils.widgets.dropdown("FORCE_LOCAL_SEARCH", "false", ["false", "true"])
dbutils.widgets.dropdown("RESET", "true", ["true", "false"])
FORCE_LOCAL = dbutils.widgets.get("FORCE_LOCAL_SEARCH") == "true"
RESET = dbutils.widgets.get("RESET") == "true"

BATCHES = {"B0": "2026-07-01", "B1": "2026-07-15", "B2": "2026-11-01", "B3": "2026-11-12", "B4": "2026-11-20"}
MARKETING = {"marketing"}
INSIGHTS = {"customer_insights", "analytics"}
FINANCE = {"restricted_finance", "marketing"}
ALL_STANDARD = {"marketing", "customer_insights", "analytics", "operations", "science"}   # everything except restricted_*
assert spark.catalog.tableExists(fq("silver_fact_orders")), "Run Day 2 first"
print("ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Unpack the extension and verify every file against its manifest
# MAGIC Same principle as Day 3: nothing is processed until its bytes match the hash its owner registered.
# MAGIC **Expected:** `hash mismatches: []`, 11 files in `extra_docs`, 2 in `docs_updates`, the trend CSV present.

# COMMAND ----------

ensure_extension(VOL_ROOT, EXT_ROOT)
bad = verify_hashes(EXT_ROOT)
print("hash mismatches:", bad)
print("extra_docs:", sorted(os.listdir(f"{EXT_ROOT}/extra_docs")))
print("docs_updates:", sorted(os.listdir(f"{EXT_ROOT}/docs_updates")))
assert not bad and len(os.listdir(f"{EXT_ROOT}/extra_docs")) == 11 and os.path.exists(f"{EXT_ROOT}/raw_extra/fact_trend_signals.csv")
print("PASS: extension verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. One registry of every document version, with its simulated arrival
# MAGIC Real sources change over time; we replay that history. **Arrival** (when the file reached us) is not **effective date** (when its content applies). DOC013 arrives Nov 1 but has applied since Jan 1. DOC001 v3 arrives Nov 20 but applies only from Dec 1.
# MAGIC
# MAGIC **Expected:** 26 rows. B0 = 12 (baseline minus DOC001 v2), B1 = 1, B2 = 11, B3 = 1 (DOC013 v2), B4 = 1 (DOC001 v3). DOC018 has `route = table`.

# COMMAND ----------

def load_manifest(path, root, batch_fn):
    rows = []
    for m in json.load(open(path)):
        rows.append({"doc_id": m["doc_id"], "version": int(m["version"]), "doc_version_id": f"{m['doc_id']}_v{m['version']}",
                     "title": DOC_TITLES[m["doc_id"]], "file_type": m["file_type"], "path": f"{root}/{m['source_path']}",
                     "access_group": m["access_group"], "authority": AUTHORITY[m["doc_id"]],
                     "effective_from": date.fromisoformat(m["effective_from"]),
                     "declared_effective_to": date.fromisoformat(m["effective_to"]) if m["effective_to"] else None,
                     "batch": batch_fn(m), "route": "table" if m["file_type"] == "xlsx" else "index"})
    return rows

reg_rows = (load_manifest(f"{DATA_ROOT}/document_manifest.json", DATA_ROOT,
                          lambda m: "B1" if (m["doc_id"], int(m["version"])) == ("DOC001", 2) else "B0")
            + load_manifest(f"{EXT_ROOT}/document_manifest_extra.json", EXT_ROOT, lambda m: "B2")
            + load_manifest(f"{EXT_ROOT}/document_manifest_updates.json", EXT_ROOT,
                            lambda m: "B3" if m["doc_id"] == "DOC013" else "B4"))
registry = pd.DataFrame(reg_rows).astype({"version": object, "authority": object})
registry["declared_effective_to"] = registry["declared_effective_to"].astype(object).where(registry["declared_effective_to"].notna(), None)
registry["arrival_date"] = registry["batch"].map(lambda b: date.fromisoformat(BATCHES[b]))
spark.createDataFrame(registry).writeTo(fq("doc_registry")).createOrReplace()
display(spark.sql("SELECT batch, count(*) docs, collect_list(doc_version_id) versions FROM doc_registry GROUP BY 1 ORDER BY 1"))
assert len(registry) == 26 and registry.groupby("batch").size().to_dict() == {"B0": 12, "B1": 1, "B2": 11, "B3": 1, "B4": 1}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Probe the platform before choosing the design
# MAGIC Two capabilities decide the path: the managed parser, and AI Search (Vector Search) with a hosted embedding model. Without them we use **labelled substitutes** behind the same functions, so later code does not change.
# MAGIC **Record both results in your evidence log.**

# COMMAND ----------

NATIVE_PARSE, NATIVE_ERR = False, None
try:
    j = spark.sql(f"SELECT to_json(ai_parse_document(content, map('version','2.0'))) j "
                  f"FROM read_files('{DATA_ROOT}/docs/DOC002_v1.pdf', format => 'binaryFile')").first().j
    NATIVE_PARSE = '"elements"' in j
except Exception as e:
    NATIVE_ERR = str(e)[:300]

VECTOR_OK, VECTOR_ERR, EMBED_EP = False, None, None
if not FORCE_LOCAL:
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.vector_search.client import VectorSearchClient
        w = WorkspaceClient()
        for cand in [EMBEDDING_ENDPOINT, "databricks-bge-large-en"]:
            try:
                w.serving_endpoints.get(cand); EMBED_EP = cand; break
            except Exception:
                pass
        vsc = VectorSearchClient(disable_notice=True)
        vsc.list_endpoints()
        VECTOR_OK = EMBED_EP is not None
        if not VECTOR_OK:
            VECTOR_ERR = "no hosted embedding endpoint found"
    except Exception as e:
        VECTOR_ERR = str(e)[:300]

BACKEND = "ai_search" if VECTOR_OK else "local_tfidf_SUBSTITUTE"
print(f"NATIVE_PARSE={NATIVE_PARSE} {NATIVE_ERR or ''}\nVECTOR_OK={VECTOR_OK} embed={EMBED_EP} {VECTOR_ERR or ''}\nBACKEND={BACKEND}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Parse: route by file type
# MAGIC | File | Parser | Why |
# MAGIC |---|---|---|
# MAGIC | PDF | `ai_parse_document` if available, else pypdf | Managed parser does layout, tables and OCR; pypdf reads only the text layer |
# MAGIC | PPTX | python-pptx | Reads slide tables and **speaker notes** directly (Q032 lives in the notes) |
# MAGIC | XLSX | not chunked: becomes tables on Day 5 | A spreadsheet is data. Chunking it invites summing repeated sample sizes |
# MAGIC
# MAGIC A document is `needs_ocr` when it yields under 50 characters. It is recorded, **not** silently skipped.

# COMMAND ----------

def parse_native(path):
    j = spark.sql(f"SELECT to_json(ai_parse_document(content, map('version','2.0'))) j "
                  f"FROM read_files('{path}', format => 'binaryFile')").first().j
    doc = json.loads(j)
    raw = []
    for e in (doc.get("document") or {}).get("elements") or []:
        bbox = e.get("bbox") or []
        pid = bbox[0].get("page_id") if bbox and isinstance(bbox[0], dict) else None
        raw.append({"page": pid if pid is not None else 0, "type": e.get("type"), "content": e.get("content")})
    if raw and min(r["page"] for r in raw) == 0:
        for r in raw:
            r["page"] += 1
    return normalize_native(raw)

def parse_doc(r):
    """returns (elements, status, parser, error)"""
    if r["route"] == "table":
        return [], "routed_to_tables", "none", None
    parser = "python-pptx" if r["file_type"] == "pptx" else ("ai_parse_document_v2" if NATIVE_PARSE else "pypdf_fallback")
    try:
        if r["file_type"] == "pptx":
            els = parse_pptx(r["path"])
        elif NATIVE_PARSE:
            els = parse_native(r["path"])
        else:
            els = parse_pdf_pypdf(r["path"])
    except Exception as e:
        return [], "failed", parser, f"{type(e).__name__}: {e}"[:300]
    chars = sum(len(e["content"]) for e in els)
    return els, ("parsed" if chars >= 50 else "needs_ocr"), parser, None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Chunk table and ingestion with MERGE and the supersession rule
# MAGIC * `chunk_id` = `DOC013_v2::0007::structure`: stable, so re-runs MERGE instead of duplicating.
# MAGIC * `chunk_hash` = hash of the chunk text without the version: unchanged content hashes the same across versions.
# MAGIC * Change data feed is ON. AI Search Delta Sync reads it to know exactly which rows changed.
# MAGIC * After each batch, `effective_windows()` recomputes `effective_to` for every version, and a MERGE writes only the rows that changed.

# COMMAND ----------

CHUNK_COLS = """chunk_id STRING NOT NULL, doc_id STRING, version INT, doc_version_id STRING, title STRING,
  access_group STRING, authority INT, effective_from DATE, effective_to DATE, effective_from_int INT, effective_to_int INT,
  page_start INT, page_end INT, section STRING, chunk_type STRING, text STRING, chunk_hash STRING,
  parser STRING, strategy STRING, batch STRING"""

def create_chunk_table(name):
    spark.sql(f"CREATE TABLE IF NOT EXISTS {fq(name)} ({CHUNK_COLS}, ingested_at TIMESTAMP) "
              f"TBLPROPERTIES (delta.enableChangeDataFeed = true)")

if RESET:
    for t in ["doc_chunks", "doc_chunks_fixed"]:
        if spark.catalog.tableExists(fq(t)):
            spark.sql(f"DELETE FROM {fq(t)}")          # keep table + index; DELETE propagates through CDF
    spark.sql(f"DROP TABLE IF EXISTS {fq('doc_ingestion_status')}")
    spark.sql(f"DROP TABLE IF EXISTS {fq('retrieval_probe_results')}")
create_chunk_table("doc_chunks"); create_chunk_table("doc_chunks_fixed")

def current_windows(upto_batch):
    seen = registry[registry["batch"] <= upto_batch]
    recs = [{"doc_id": r["doc_id"], "version": int(r["version"]), "effective_from": r["effective_from"],
             "declared_effective_to": r["declared_effective_to"] if isinstance(r["declared_effective_to"], date) else None}
            for r in seen.to_dict("records")]
    return effective_windows(recs)

def apply_windows(upto_batch, table="doc_chunks"):
    win = current_windows(upto_batch)
    wdf = spark.createDataFrame([Row(doc_id=k[0], version=k[1], effective_to=v, effective_to_int=date_to_int(v))
                                 for k, v in win.items()], "doc_id STRING, version INT, effective_to DATE, effective_to_int INT")
    wdf.createOrReplaceTempView("windows")
    spark.sql(f"""MERGE INTO {fq(table)} t USING windows w ON t.doc_id = w.doc_id AND t.version = w.version
                  WHEN MATCHED AND t.effective_to_int <> w.effective_to_int
                  THEN UPDATE SET effective_to = w.effective_to, effective_to_int = w.effective_to_int""")

def ingest_batch(b, strategy="structure", table="doc_chunks"):
    win = current_windows(b)
    rows, status = [], []
    for r in registry[registry["batch"] == b].to_dict("records"):
        els, st, parser, err = parse_doc(r)
        cs = (structure_chunks(els) if strategy == "structure" else fixed_chunks(els)) if st == "parsed" else []
        r["version"], r["authority"] = int(r["version"]), int(r["authority"])
        end = win[(r["doc_id"], r["version"])]
        for c in cs:
            rows.append(Row(chunk_id=f"{r['doc_version_id']}::{c['chunk_index']:04d}::{strategy}", doc_id=r["doc_id"],
                            version=r["version"], doc_version_id=r["doc_version_id"], title=r["title"],
                            access_group=r["access_group"], authority=r["authority"],
                            effective_from=r["effective_from"], effective_to=end,
                            effective_from_int=date_to_int(r["effective_from"]), effective_to_int=date_to_int(end),
                            page_start=c["page_start"], page_end=c["page_end"], section=c["section"],
                            chunk_type=c["chunk_type"], text=f"{r['title']}\n{c['text']}",
                            chunk_hash=hashlib.sha256(c["text"].encode()).hexdigest(),
                            parser=parser, strategy=strategy, batch=b))
        status.append(Row(doc_version_id=r["doc_version_id"], batch=b, strategy=strategy, parser=parser, status=st,
                          n_elements=len(els), n_chunks=len(cs), error=err))
    if rows:
        spark.createDataFrame(rows, CHUNK_COLS.replace(" NOT NULL", "")).withColumn("ingested_at", F.current_timestamp()) \
             .createOrReplaceTempView("new_chunks")
        spark.sql(f"""MERGE INTO {fq(table)} t USING new_chunks s ON t.chunk_id = s.chunk_id
                      WHEN MATCHED AND t.chunk_hash <> s.chunk_hash THEN UPDATE SET *
                      WHEN NOT MATCHED THEN INSERT *""")
    apply_windows(b, table)
    if strategy == "structure":
        spark.createDataFrame(status, "doc_version_id STRING, batch STRING, strategy STRING, parser STRING, status STRING, "
                                      "n_elements INT, n_chunks INT, error STRING") \
             .withColumn("ingested_at", F.current_timestamp()).write.mode("append").saveAsTable(fq("doc_ingestion_status"))
    return len(rows), status

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Search: one interface, two backends
# MAGIC `search(query, groups, as_of, k, rerank)` is all the agent will ever call. Behind it:
# MAGIC * **AI Search** (Delta Sync index, managed embeddings). Filters go **into** the query, so restricted, stale and future chunks never come back.
# MAGIC * **Local TF-IDF substitute**, used only if AI Search is unavailable. It applies the same `is_visible` rule *before* ranking. It is lexical, not semantic: a weaker retriever, clearly labelled.
# MAGIC
# MAGIC First index creation can take 5–15 minutes. Each sync after a batch takes a minute or two.

# COMMAND ----------

INDEX_COLS = ["chunk_id", "doc_id", "version", "doc_version_id", "access_group", "authority",
              "effective_from_int", "effective_to_int", "page_start", "page_end", "chunk_type", "text"]

def wait_until(check, what, timeout_s=1800, every=20):
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        ok, info = check()
        if ok:
            print(f"{what}: ready after {int(time.time()-t0)}s"); return
        time.sleep(every)
    raise TimeoutError(f"{what} not ready after {timeout_s}s: {info}")

class AISearch:
    def __init__(self, table):
        self.table, self.name = fq(table), fq(f"{table}_index")
        names = [e["name"] for e in vsc.list_endpoints().get("endpoints", [])]
        if VECTOR_ENDPOINT not in names:
            vsc.create_endpoint(name=VECTOR_ENDPOINT, endpoint_type="STANDARD")
        wait_until(lambda: (vsc.get_endpoint(VECTOR_ENDPOINT).get("endpoint_status", {}).get("state") == "ONLINE", None),
                   "endpoint")
        try:
            self.idx = vsc.get_index(VECTOR_ENDPOINT, self.name)
        except Exception:
            self.idx = vsc.create_delta_sync_index(
                endpoint_name=VECTOR_ENDPOINT, index_name=self.name, source_table_name=self.table,
                pipeline_type="TRIGGERED", primary_key="chunk_id", embedding_source_column="text",
                embedding_model_endpoint_name=EMBED_EP, columns_to_sync=INDEX_COLS)
    def _status(self):
        s = self.idx.describe().get("status", {})
        return s.get("ready", False) and "PROVISIONING" not in s.get("detailed_state", ""), s
    def sync(self):
        wait_until(self._status, "index (pre-sync)")
        self.idx.sync()
        time.sleep(10)
        expected = spark.table(self.table).count()
        def caught_up():
            s = self.idx.describe().get("status", {})
            return (s.get("detailed_state", "") in ("ONLINE_NO_PENDING_UPDATE", "ONLINE")
                    and s.get("indexed_row_count", -1) == expected), s
        wait_until(caught_up, f"index sync ({expected} rows)")
    def search(self, query, groups, as_of, k=5, rerank=True, query_type="ANN"):
        extra = {} if query_type == "ANN" else {"query_type": query_type}     # older clients lack this argument
        res = self.idx.similarity_search(query_text=query, columns=INDEX_COLS, filters=to_index_filters(groups, as_of),
                                         num_results=max(k * 4, 20), **extra)
        cols = [c["name"] for c in res["manifest"]["columns"]]
        rows = [dict(zip(cols, r)) for r in res.get("result", {}).get("data_array", []) or []]
        rows = rerank_by_authority(rows) if rerank else rows
        return rows[:k]

class LocalSearch:
    """SUBSTITUTE for AI Search: TF-IDF, same visibility rule applied before ranking."""
    def __init__(self, table):
        self.table = fq(table)
    def sync(self):
        pass
    def search(self, query, groups, as_of, k=5, rerank=True, query_type="ANN"):
        from sklearn.feature_extraction.text import TfidfVectorizer
        pdf = spark.table(self.table).toPandas()
        def as_date(x):
            if x is None or (not isinstance(x, date) and pd.isna(x)):
                return None
            return x.date() if hasattr(x, "date") and not type(x) is date else x
        vis = [r for r in pdf.to_dict("records")
               if is_visible({**r, "effective_from": as_date(r["effective_from"]), "effective_to": as_date(r["effective_to"])},
                             groups, as_of)]
        if not vis:
            return []
        vec = TfidfVectorizer(stop_words="english", sublinear_tf=True).fit([r["text"] for r in vis])
        scores = (vec.transform([r["text"] for r in vis]) @ vec.transform([query]).T).toarray().ravel()
        rows = sorted([{**r, "score": float(s)} for r, s in zip(vis, scores)], key=lambda x: x["score"], reverse=True)[:max(k * 4, 20)]
        rows = rerank_by_authority(rows) if rerank else rows
        return rows[:k]

backend = AISearch("doc_chunks") if VECTOR_OK else LocalSearch("doc_chunks")
print("backend:", type(backend).__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Replay history: ingest → sync → probe, batch by batch
# MAGIC Each probe has **hard rules** (`must_exclude`: security and versioning, **asserted**) and **quality checks** (`must_include`: did search find it in the top 5, **reported**). A quality miss is a retrieval problem to improve. A hard-rule miss is a defect that stops everything.
# MAGIC
# MAGIC **Expected:** every `hard_rule_pass = true`. With the local substitute, every `quality_pass` was also true when tested offline on these exact files.

# COMMAND ----------

PROBES = {
  "B0": [("P0a", "hydration wording approved for the Aurora serum", MARKETING, "2026-07-01", ["DOC001_v1"], ["DOC001_v2"]),
         ("P0b", "innovation roadmap plans", MARKETING | {"operations"}, "2026-07-01", [], ["DOC007"])],
  "B1": [("P1a", "hydration wording approved for the Aurora serum", MARKETING, "2026-07-20", ["DOC001_v2"], ["DOC001_v1"]),
         ("P1b", "hydration wording approved for the Aurora serum", MARKETING, "2026-03-01", ["DOC001_v1"], ["DOC001_v2"])],
  "B2": [("P2a", "2027 pricing strategy", MARKETING, "2026-11-05", [], ["DOC021"]),
         ("P2b", "2027 pricing strategy", FINANCE, "2026-11-05", ["DOC021_v1"], []),
         ("P2c", "New York barrier comfort interest H2 panel", INSIGHTS, "2026-11-05", ["DOC017_v1"], []),
         ("P2d", "localized copy escalation path regional owner", MARKETING, "2026-11-05", ["DOC013_v1"], [])],
  "B3": [("P3a", "approvals logged against the final immutable text before release", MARKETING, "2026-11-13",
          ["DOC013_v2"], ["DOC013_v1"])],
  "B4": [("P4a", "hydration wording approved for the Aurora serum", MARKETING, "2026-11-25", ["DOC001_v2"], ["DOC001_v3"]),
         ("P4b", "hydration wording approved for the Aurora serum", MARKETING, "2026-12-01", ["DOC001_v3"], ["DOC001_v2"])],
}

def run_probe(pid, q, groups, as_of, inc, exc, label, k=5):
    top = backend.search(q, groups, date.fromisoformat(as_of), k=k)
    ids = [t["doc_version_id"] for t in top]
    hard = not any(x in (t["doc_version_id"], t["doc_id"]) for x in exc for t in top)
    quality = all(i in ids for i in inc)
    return Row(label=label, probe=pid, query=q, groups=",".join(sorted(groups)), as_of=as_of,
               top_docs=",".join(ids), must_include=",".join(inc), must_exclude=",".join(exc),
               hard_rule_pass=hard, quality_pass=quality)

probe_rows = []
for b in BATCHES:
    n, status = ingest_batch(b)
    backend.sync()
    print(f"{b}: {n} chunks written; statuses: {sorted({s.status for s in status})}")
    probe_rows += [run_probe(*p, label=f"replay_{b}") for p in PROBES[b]]

probes = spark.createDataFrame(probe_rows)
probes.write.mode("append").saveAsTable(fq("retrieval_probe_results"))
display(probes)
assert all(r.hard_rule_pass for r in probe_rows), "A stale, future or restricted document came back. STOP: this is a defect."
print("PASS: no stale, future or restricted document returned at any point in history")

# COMMAND ----------

# MAGIC %md
# MAGIC **Check the ingestion status.**
# MAGIC **Expected, fallback parser:** `routed_to_tables` DOC018; `needs_ocr` DOC012 and DOC022 (scans); everything else `parsed`.
# MAGIC **Expected, managed parser:** the scans should be `parsed` (OCR). If not, record it.

# COMMAND ----------

display(spark.sql(f"""SELECT status, parser, count(*) n, collect_list(doc_version_id) docs
                      FROM {fq('doc_ingestion_status')} GROUP BY ALL ORDER BY status"""))
display(spark.sql(f"""SELECT doc_version_id, effective_from, effective_to, count(*) chunks
                      FROM {fq('doc_chunks')} WHERE doc_id IN ('DOC001','DOC013') GROUP BY ALL ORDER BY 1"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Expected windows:** DOC001_v1 to 2026-07-14 · DOC001_v2 to **2026-11-30** (closed by v3) · DOC001_v3 open · DOC013_v1 to **2026-11-11** (closed by v2) · DOC013_v2 open. Nothing was deleted. History is still answerable.
# MAGIC
# MAGIC ### 🎙️ Drill 1: Kamal's exact questions from the client round
# MAGIC **Verbatim:** *"You have a policy which is processed, and tomorrow that policy changes. What happens to embedding? … Embedding is already generated. What happens to the previous embedding?"* Then: *"There's 20,000 documents here. How are you identifying which one is the incremental?"*
# MAGIC
# MAGIC **What happened in the real round:** he asked the first question three times, because the answers described the whole pipeline instead of the fate of the old embeddings. **Open with one sentence:**
# MAGIC *"Nothing happens to them automatically. They stay in the index until we act. We find them by the source document ID, close or remove them, and add the new version's chunks."*
# MAGIC For the second question: *"We never compare embeddings. The source system tells us what changed (delta query or modified timestamp); the source ID maps to our registry row, which lists that document's chunk IDs; the content hash tells us whether it really changed."*
# MAGIC
# MAGIC **Then the trade-off he'll probe:** at PNC you deleted old chunks from the active index (clean, simple). Here you keep them with an end date (history questions and rollback, as E5 shows). Know why you'd pick each.
# MAGIC
# MAGIC Answer with this notebook: the old chunks are **kept** (audit and history questions) but **closed** with an end date by the supersession rule; search filters by effective date inside the query; Delta Sync propagates only the changed rows through change data feed; and a probe after every update **proves** the old version can't come back. Then name the failure mode (E1 below) and how it's detected.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Experiments
# MAGIC ### E1. The supersession bug: one wrong cell leaks a stale claim
# MAGIC **Story:** a code change accidentally skips closing old versions. **Predict:** what does a marketing user see today?
# MAGIC 1. Re-open DOC001 v1 (as the bug would). 2. Search **before** sync. 3. Sync, search again. 4. A data-quality rule catches it without any search. 5. Fix and verify.

# COMMAND ----------

TODAY = "2026-11-13"
OVERLAP_SQL = f"""WITH v AS (SELECT DISTINCT doc_id, version, effective_from_int f, effective_to_int t FROM {fq('doc_chunks')})
  SELECT a.doc_id, a.version AS older, b.version AS newer FROM v a JOIN v b
    ON a.doc_id = b.doc_id AND a.version < b.version AND a.t >= b.f"""
Q_HYD = "hydration wording approved for the Aurora serum"

spark.sql(f"UPDATE {fq('doc_chunks')} SET effective_to = NULL, effective_to_int = 99991231 WHERE doc_version_id = 'DOC001_v1'")
before_sync = run_probe("E1-before-sync", Q_HYD, MARKETING, TODAY, ["DOC001_v2"], ["DOC001_v1"], "E1", k=10)
display(spark.sql(OVERLAP_SQL))                    # the data-quality rule: no search needed
backend.sync()
after_sync = run_probe("E1-after-sync", Q_HYD, MARKETING, TODAY, ["DOC001_v2"], ["DOC001_v1"], "E1", k=10)
print("hard rule before sync:", before_sync.hard_rule_pass, "| after sync:", after_sync.hard_rule_pass, "|", after_sync.top_docs)

# COMMAND ----------

# MAGIC %md
# MAGIC **Expected:** the overlap rule returns **DOC001 1→2 and 1→3** immediately, without any search. With AI Search, *before sync* still passes (the index hasn't seen the bad row: it is **eventually consistent**); *after sync* DOC001_v1 can be returned for today (`False`). With the local substitute there is no index, so the leak shows at once. Whether v1 lands in the top 10 also depends on ranking. The overlap rule doesn't, which is why it is the primary guard.
# MAGIC **Lesson:** guard versioning in two places: a data-quality rule on the chunk table (cheap, catches it before users can), and probes after sync (proves what users would actually see). Now fix and verify:

# COMMAND ----------

apply_windows("B4")
backend.sync()
fixed = run_probe("E1-fixed", Q_HYD, MARKETING, TODAY, ["DOC001_v2"], ["DOC001_v1"], "E1", k=10)
n_overlaps = spark.sql(OVERLAP_SQL).count()
print("after fix: hard rule", fixed.hard_rule_pass, "| overlaps", n_overlaps, "|", fixed.top_docs)
assert fixed.hard_rule_pass and n_overlaps == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ### E2. Source authority: does the FAQ outrank the policy?
# MAGIC Q014 asks whether an outdated phrase may be used. DOC023 (FAQ) repeats it; DOC013 page 7 says an FAQ must never override the current register.
# MAGIC **Expected (offline, local substitute):** without reranking, DOC023 ranks above DOC014; with reranking, DOC014 moves above it.

# COMMAND ----------

q14 = "May I use supports a hydrated appearance for P001 today?"
for rr in [False, True]:
    print("rerank", rr, [(t["doc_version_id"], round(t["score"], 3)) for t in backend.search(q14, MARKETING, date(2026, 11, 13), k=3, rerank=rr)])

# COMMAND ----------

# MAGIC %md
# MAGIC ### E3. Semantic vs keyword search (AI Search only)
# MAGIC Evidence IDs like `EV-AU-101` are exact tokens. Embeddings can blur them; hybrid search adds keyword matching.
# MAGIC **Predict**, then compare where DOC014 lands.

# COMMAND ----------

if VECTOR_OK:
    for qt in ["ANN", "HYBRID"]:
        try:
            print(qt, [t["doc_version_id"] for t in backend.search("Which claim uses evidence EV-AU-101?", ALL_STANDARD, date(2026, 11, 11), k=3, query_type=qt)])
        except Exception as e:
            print(qt, "not supported here:", str(e)[:150])
else:
    print("Skipped: the local substitute is keyword-only. Run this on the trial workspace.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### E4. Chunking strategy: structure vs fixed windows
# MAGIC Two measures, because they disagree: **does search find the right document** (recall), and **what does an update cost** (chunks that changed between DOC013 v1 and v2, which must be re-embedded).
# MAGIC **Expected (offline, local substitute):** recall@5 structure 18/21, fixed 19/21; changed chunks for the one-paragraph update: structure **1 of 25**, fixed **7 of 10**.

# COMMAND ----------

for b in BATCHES:
    ingest_batch(b, strategy="fixed", table="doc_chunks_fixed")
fixed_backend = LocalSearch("doc_chunks_fixed")
if VECTOR_OK:
    try:
        fixed_backend = AISearch("doc_chunks_fixed"); fixed_backend.sync()
    except Exception as e:
        print("Second index not allowed here, comparing fixed chunks with the local substitute:", str(e)[:150])

def changed_chunks(table):
    return spark.sql(f"""SELECT count_if(v1.chunk_hash IS NULL) changed, count(*) total
        FROM (SELECT chunk_hash FROM {fq(table)} WHERE doc_version_id='DOC013_v2') v2
        LEFT JOIN (SELECT DISTINCT chunk_hash FROM {fq(table)} WHERE doc_version_id='DOC013_v1') v1 USING (chunk_hash)""").first()

for t in ["doc_chunks", "doc_chunks_fixed"]:
    c = changed_chunks(t); print(f"{t}: {c.changed} of {c.total} DOC013 v2 chunks changed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Retrieval evaluation: 21 document questions, recall@k
# MAGIC Questions whose answer lives in an indexable document (spreadsheet questions go to Day 5). Run as of **2026-11-11**, the eval set's own reference date. After the DOC013 update, questions written against v1 would otherwise "fail" for the right reason. Evaluation sets go stale too and need versioning.
# MAGIC **Expected misses (offline, fallback parsers):** Q049 and Q050 (scanned, need OCR), and possibly Q017 (table question).

# COMMAND ----------

ev = pd.read_csv(f"{EXT_ROOT}/eval_questions_extra.csv").fillna("")
SUBSET = ["Q013","Q014","Q015","Q016","Q017","Q018","Q019","Q020","Q021","Q023","Q028","Q029","Q030","Q031","Q032",
          "Q036","Q046","Q047","Q048","Q049","Q050"]
EVAL_AS_OF = date(2026, 11, 11)
results = []
for name, be in [("structure", backend), ("fixed", fixed_backend)]:
    for k in [3, 5, 8]:
        for r in ev[ev.question_id.isin(SUBSET)].to_dict("records"):
            top = be.search(r["question"], ALL_STANDARD, EVAL_AS_OF, k=k)
            results.append(Row(strategy=name, backend=type(be).__name__, parser="ai_parse_document_v2" if NATIVE_PARSE else "pypdf_fallback",
                               k=k, question_id=r["question_id"], hit=doc_hit(top, expected_docs(r["expected_doc_page"])),
                               expected=r["expected_doc_page"], top_docs=",".join(t["doc_version_id"] for t in top)))
rdf = spark.createDataFrame(results)
rdf.withColumn("run_at", F.current_timestamp()).write.mode("append").saveAsTable(fq("retrieval_eval_results"))
display(rdf.groupBy("strategy", "k").agg(F.sum(F.col("hit").cast("int")).alias("hits"), F.count("*").alias("questions"),
                                         F.collect_list(F.when(~F.col("hit"), F.col("question_id"))).alias("misses")).orderBy("strategy", "k"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### E5. Rollback: DOC013 v2 was released by mistake
# MAGIC **Story:** legal says v2 was published too early. Roll back, verify, then re-apply.
# MAGIC Rollback is an **explicit, CDF-friendly change** (delete v2 chunks, re-open v1), not `RESTORE TABLE`. Whether an index sync handles a RESTORE correctly is something you would test before relying on it.

# COMMAND ----------

spark.sql(f"DELETE FROM {fq('doc_chunks')} WHERE doc_version_id = 'DOC013_v2'")
spark.sql(f"UPDATE {fq('doc_chunks')} SET effective_to = NULL, effective_to_int = 99991231 WHERE doc_version_id = 'DOC013_v1'")
backend.sync()
rb = run_probe("E5-rolled-back", "approvals logged against the final immutable text before release", MARKETING, TODAY, ["DOC013_v1"], ["DOC013_v2"], "E5")
print("rolled back:", rb.hard_rule_pass, rb.top_docs)
ingest_batch("B3"); ingest_batch("B4"); backend.sync()       # re-apply the release
ra = run_probe("E5-reapplied", "approvals logged against the final immutable text before release", MARKETING, TODAY, ["DOC013_v2"], ["DOC013_v1"], "E5")
print("re-applied:", ra.hard_rule_pass, ra.top_docs)
assert rb.hard_rule_pass and ra.hard_rule_pass

# COMMAND ----------

# MAGIC %md
# MAGIC ### E6. Index lag (AI Search): the gap between "table updated" and "users see it"
# MAGIC Already observed in E1 (before vs after sync). In production the ingestion job **ends with sync + probes**, and is not "done" until both pass. That makes freshness a measured number, not an assumption.
# MAGIC
# MAGIC ## 10. Evidence
# MAGIC **Expected:** all hard-rule probes PASS; recall numbers recorded for both strategies; E1–E5 outcomes noted.

# COMMAND ----------

dq = [("documents", "retrieval", "hard_rules_all_batches", "ERROR", float(sum(r.hard_rule_pass for r in probe_rows)),
       f"{len(probe_rows)} of {len(probe_rows)}", "PASS" if all(r.hard_rule_pass for r in probe_rows) else "FAIL"),
      ("documents", "retrieval", "backend", "INFO", None, BACKEND, "INFO"),
      ("documents", "retrieval", "parser", "INFO", None, "ai_parse_document_v2" if NATIVE_PARSE else "pypdf_fallback", "INFO")]
(spark.createDataFrame(dq, "layer STRING, table_name STRING, check_name STRING, severity STRING, metric_value DOUBLE, expected STRING, status STRING")
      .withColumn("batch_id", F.lit("day4")).withColumn("checked_at", F.current_timestamp())
      .write.mode("append").saveAsTable(fq("dq_results")))
print("logged")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎙️ Drill 2: the stakeholder version
# MAGIC **Priya:** *"Legal updated the guideline yesterday. Is the assistant already using it, and could it still show the old one?"*
# MAGIC Cover: when it became effective, when it was ingested, the probe that proves only v2 comes back, and the rule that would catch it if not. No tool names.
# MAGIC
# MAGIC ### 🎙️ Drill 3: senior engineer
# MAGIC *"Fixed chunks scored slightly higher recall here. Why not use them?"*
# MAGIC Strong answer: doc-level recall barely moved. Structure keeps table rows and speaker notes whole, carries section context, and made the update re-embed **1 chunk instead of 7**. At 20,000 documents that's cost and freshness. Then name what would change your mind (page-level recall, answer accuracy on table questions).
# MAGIC
# MAGIC ### ✅ Day 4 exit gate
# MAGIC - [ ] Extension verified; registry 26 rows
# MAGIC - [ ] Parser and search capability recorded (native or substitute)
# MAGIC - [ ] All hard-rule probes pass at every batch
# MAGIC - [ ] E1 leak reproduced, detected by the overlap rule, fixed
# MAGIC - [ ] E2, E4, E5 results recorded; recall table saved
# MAGIC - [ ] Commit: `git pull`, notes in `docs/evidence_log.md` and `docs/failure_log.md`, push
