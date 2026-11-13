# Databricks notebook source
# MAGIC %md
# MAGIC # Day 3: Documents in a Volume, a source-of-truth manifest, and parsing you can prove
# MAGIC **Synthetic training documents only. These are not Estée Lauder policies.**
# MAGIC
# MAGIC ### Why the business cares
# MAGIC In the client round, Kamal didn't accept "we use OCR". He asked **how you know every document was processed correctly before it is embedded.** A RAG answer is only as good as the worst document that silently failed to parse. Common examples: a scanned memo with no text layer, or a claims table flattened so that the "Texture" row loses its evidence column.
# MAGIC
# MAGIC Today's deliverable is a document pipeline that can answer, with numbers: **what should exist, what does exist, what was parsed, what failed, and why.**
# MAGIC
# MAGIC ### Architecture
# MAGIC ```
# MAGIC  document_manifest.json ─────────▶ doc_manifest          WHAT SHOULD EXIST (owner, version, hash, effective dates, access group)
# MAGIC  Volume …/docs/*.pdf ─binaryFile─▶ doc_file_inventory    WHAT DOES EXIST  (path, size, sha256)
# MAGIC                       FULL OUTER JOIN on file name  ──▶  doc_reconciliation   OK | HASH_MISMATCH | MISSING_FILE | UNREGISTERED_FILE
# MAGIC                                      ▼ only OK files
# MAGIC  ai_parse_document (managed)  ──▶ doc_parsed_raw (VARIANT) ─▶ doc_elements (one row per text/table/figure element)
# MAGIC  pypdf (labelled FALLBACK)    ──────────────────────────────▶ doc_elements (parser = 'pypdf_fallback')
# MAGIC                                      ▼
# MAGIC  doc_processing_status        WHAT WE DID: parsed | needs_ocr | failed | pending   →   ready_for_indexing (Day 4)
# MAGIC ```
# MAGIC **Design choice:** the manifest is **never updated by the pipeline.** It describes the source system. Pipeline state lives in `doc_processing_status`. If both were in one table, a bug in the pipeline could corrupt your record of what the source contains.
# MAGIC
# MAGIC **Prerequisite:** the Day 2 exit gate passed, and `lab_files/beauty_ai_lab_data/docs/` holds 13 PDFs.
# MAGIC
# MAGIC The next cell installs `pypdf`. It is used only for the labelled fallback parser and the parser comparison.

# COMMAND ----------

# MAGIC %pip install -q pypdf

# COMMAND ----------

import datetime, os, shutil
from pyspark.sql import functions as F, Row

CATALOG, SCHEMA, VOLUME = "workspace", "ai_architect_lab", "lab_files"
DATA_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/beauty_ai_lab_data"
DOCS      = f"{DATA_ROOT}/docs"
SCRATCH   = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/scratch_docs_day3"
RUN_ID    = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
fq = lambda n: f"{CATALOG}.{SCHEMA}.{n}"
spark.sql(f"USE CATALOG {CATALOG}"); spark.sql(f"USE SCHEMA {SCHEMA}")

assert spark.catalog.tableExists(fq("gold_sales_lines")), "Day 2 tables not found. Finish Day 2 first."
assert len([f for f in os.listdir(DOCS) if f.endswith(".pdf")]) == 13, "Expected 13 PDFs in the docs folder"
print("Prerequisites OK. Run", RUN_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The manifest becomes a governed Delta table
# MAGIC The JSON file is a stand-in for what a real connector would give you: a SharePoint or Confluence listing with item IDs, versions and modified dates. Loading it into Delta makes it queryable, versioned and joinable.
# MAGIC
# MAGIC **Expected:** 13 rows (12 documents, because DOC001 has v1 and v2). Exactly one active version per document. DOC001 v1 runs to 2026-07-14 and v2 starts 2026-07-15, with no gap or overlap.

# COMMAND ----------

manifest = (spark.read.option("multiLine", True).json(f"{DATA_ROOT}/document_manifest.json")
    .select("doc_id",
            F.col("version").cast("int").alias("version"),
            F.concat("doc_id", F.lit("_v"), F.col("version").cast("string")).alias("doc_version_id"),
            F.element_at(F.split("source_path", "/"), -1).alias("file_name"),
            "source_path", "source_system", "file_type",
            F.col("page_count").cast("int").alias("page_count"),
            F.col("content_hash").alias("expected_hash"),
            "access_group",
            F.col("is_active").cast("boolean").alias("is_active"),
            F.to_date("effective_from").alias("effective_from"),
            F.expr("to_date(nullif(effective_to, ''))").alias("effective_to")))
manifest.writeTo(fq("doc_manifest")).createOrReplace()
spark.sql(f"COMMENT ON TABLE {fq('doc_manifest')} IS 'Source-of-truth list of documents that SHOULD exist. Never updated by the pipeline. SYNTHETIC.'")

# Small extra: put the evaluation questions in Delta now, because Days 5-11 use them
(spark.read.option("header", True).csv(f"{DATA_ROOT}/eval_questions.csv")
      .writeTo(fq("eval_questions")).createOrReplace())

checks = spark.sql("""
SELECT
  (SELECT count(*) FROM doc_manifest)                                   AS manifest_rows,
  (SELECT count(DISTINCT doc_id) FROM doc_manifest)                     AS documents,
  (SELECT count(*) FROM (SELECT doc_id FROM doc_manifest GROUP BY doc_id
                         HAVING count_if(is_active) <> 1))              AS docs_without_exactly_one_active,
  (SELECT count(*) FROM doc_manifest a JOIN doc_manifest b
     ON a.doc_id = b.doc_id AND a.version < b.version
    WHERE coalesce(a.effective_to, DATE'9999-12-31') >= b.effective_from) AS overlapping_version_ranges,
  (SELECT count(*) FROM eval_questions)                                 AS eval_questions
""").first()
print(checks.asDict())
assert checks.asDict() == {"manifest_rows": 13, "documents": 12, "docs_without_exactly_one_active": 0,
                           "overlapping_version_ranges": 0, "eval_questions": 12}
print("PASS: manifest is internally consistent")

# COMMAND ----------

# MAGIC %md
# MAGIC **Effective dating in practice** (this supports eval Q002 and Q003): *which Aurora Serum policy was in force on a given date?*
# MAGIC **Expected:** 2026-03-01 → DOC001_v1, 2026-08-01 → DOC001_v2, today → DOC001_v2.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT d.as_of, m.doc_version_id, m.effective_from, m.effective_to
# MAGIC FROM (VALUES (DATE'2026-03-01'), (DATE'2026-08-01'), (current_date())) AS d(as_of)
# MAGIC JOIN doc_manifest m
# MAGIC   ON m.doc_id = 'DOC001'
# MAGIC  AND d.as_of >= m.effective_from
# MAGIC  AND d.as_of <= coalesce(m.effective_to, DATE'9999-12-31')
# MAGIC ORDER BY d.as_of

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. What actually exists: file inventory and hash reconciliation
# MAGIC `sha256(file bytes)` is the file's fingerprint. If the hash matches the manifest, this is exactly the version the owner registered. If it doesn't, someone changed the file and the manifest is stale, or the file is corrupt.
# MAGIC Both functions take a folder, so the tamper experiment in section 3 reuses them unchanged.
# MAGIC
# MAGIC **Expected:** 13 rows, all `OK`.

# COMMAND ----------

def inventory(folder):
    return (spark.read.format("binaryFile").load(folder)
            .select(F.regexp_replace("path", "^dbfs:", "").alias("path"),
                    F.element_at(F.split("path", "/"), -1).alias("file_name"),
                    F.col("length").alias("size_bytes"),
                    F.col("modificationTime").alias("modified_at"),
                    F.sha2("content", 256).alias("actual_hash")))

def reconcile(inv_df):
    man = spark.table(fq("doc_manifest")).select("doc_version_id", "file_name", "expected_hash")
    return (man.join(inv_df, "file_name", "full_outer")
               .withColumn("reconcile_status",
                   F.when(F.col("doc_version_id").isNull(), "UNREGISTERED_FILE")
                    .when(F.col("actual_hash").isNull(), "MISSING_FILE")
                    .when(F.col("actual_hash") != F.col("expected_hash"), "HASH_MISMATCH")
                    .otherwise("OK"))
               .select("file_name", "doc_version_id", "reconcile_status", "size_bytes", "expected_hash", "actual_hash", "path"))

inv = inventory(DOCS)
inv.writeTo(fq("doc_file_inventory")).createOrReplace()
rec = reconcile(spark.table(fq("doc_file_inventory")))
rec.writeTo(fq("doc_reconciliation")).createOrReplace()
display(spark.table(fq("doc_reconciliation")).orderBy("file_name"))

summary = {r.reconcile_status: r.n for r in spark.sql(
    "SELECT reconcile_status, count(*) n FROM doc_reconciliation GROUP BY 1").collect()}
print(summary)
assert summary == {"OK": 13}, "Fix reconciliation before parsing. Only OK files may be parsed."
print("PASS: 13/13 files match the manifest by name and hash")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Experiment 1: tamper, delete, and add a stray file (scratch copy only)
# MAGIC We copy the docs to a scratch folder and break it in three realistic ways:
# MAGIC * **DOC004_v1.pdf edited after registration**: bytes appended, so the hash changes
# MAGIC * **DOC011_v1.pdf deleted** from the source, but still listed in the manifest
# MAGIC * **DOC013_v1.pdf** dropped into the folder: not in the manifest, and not even a real PDF
# MAGIC
# MAGIC **Expected:** OK 11 · HASH_MISMATCH 1 (DOC004) · MISSING_FILE 1 (DOC011) · UNREGISTERED_FILE 1 (DOC013) = 14 rows.

# COMMAND ----------

dbutils.fs.rm(SCRATCH, True)
os.makedirs(SCRATCH, exist_ok=True)
for f in os.listdir(DOCS):
    if f != "DOC011_v1.pdf":
        shutil.copyfile(f"{DOCS}/{f}", f"{SCRATCH}/{f}")
with open(f"{DOCS}/DOC004_v1.pdf", "rb") as src, open(f"{SCRATCH}/DOC004_v1.pdf", "wb") as dst:
    dst.write(src.read() + b"\n% edited after registration\n")
with open(f"{SCRATCH}/DOC013_v1.pdf", "wb") as dst:
    dst.write(b"this is not a pdf - simulated corrupt upload")

scratch_rec = reconcile(inventory(SCRATCH))
display(scratch_rec.filter("reconcile_status <> 'OK'"))
scratch_summary = {r.reconcile_status: r.n for r in scratch_rec.groupBy("reconcile_status").count().withColumnRenamed("count", "n").collect()}
print(scratch_summary)
assert scratch_summary == {"OK": 11, "HASH_MISMATCH": 1, "MISSING_FILE": 1, "UNREGISTERED_FILE": 1}
print("PASS: every discrepancy detected and named")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎙️ Drill 1: Kamal's exact questions from the client round, answered with today's evidence (2 minutes)
# MAGIC **Kamal (verbatim):** *"Before you go to embedding, how are you making sure all your documents are processed, right? What is the way for verifying that everything has been processed correctly?"*
# MAGIC **Follow-up (verbatim):** *"My simple question is: your document which is coming with different formats, were you dealing with the standard format every time, or is it like you are not aware of what the format of document can be? … How are you making sure all the tables and images are processed?"*
# MAGIC
# MAGIC **What happened in the real round:** the first answer was a long pipeline description, so he had to ask the "simple question". **Rule: answer the literal question in one sentence first**, then two or three points, then stop.
# MAGIC * Q1 opener: *"We reconcile against a manifest: expected = processed + failed + pending, by name and content hash, before anything is embedded."*
# MAGIC * Follow-up opener: *"Unknown formats. We don't assume templates, so we route by file type and check each output for text, tables and pages."*
# MAGIC
# MAGIC Points to cover after the opener, in your own words:
# MAGIC * Start from **the expected set**: the manifest from the source system, not "whatever is in the folder".
# MAGIC * **Name and hash** reconciliation tells you missing, changed and unknown files apart, and each gets a different action.
# MAGIC * Only `OK` files go on to parsing. Everything else goes to an exception list with an owner.
# MAGIC * After parsing comes a **second** check (section 6): did each parsed file produce usable text, tables and pages?
# MAGIC * Close with the number: *"expected 13 = parsed + needs OCR + failed + pending, and here is the table that proves it."*
# MAGIC
# MAGIC **Senior architect:** *"Delta Sync keeps the vector index in step with a table. Why do you need any of this?"*
# MAGIC Delta Sync propagates **table** changes. It knows nothing about a PDF that was edited, deleted or never registered. Someone still has to turn files into a trustworthy chunk table. That is today's work, and it is the input Delta Sync needs on Day 4.
# MAGIC
# MAGIC ## 4. Can this workspace use the managed parser? (probe, don't assume)
# MAGIC `ai_parse_document` is a Databricks AI Function. Availability depends on region, workspace type and serverless environment version, and it may be in preview. We run it on **one** small file and record exactly what happens.
# MAGIC
# MAGIC **Expected:** either `NATIVE_OK = True` with JSON containing `document`, `elements` and `pages`, or `False` with the exact error. Both outcomes are valid evidence. Paste the error into the chat if it is `False`.

# COMMAND ----------

NATIVE_OK, NATIVE_ERR = False, None
try:
    j = spark.sql(f"""
        SELECT to_json(ai_parse_document(content, map('version', '2.0'))) AS j
        FROM read_files('{DOCS}/DOC002_v1.pdf', format => 'binaryFile')""").first().j
    NATIVE_OK = j is not None and '"elements"' in j
    print(j[:1500])
except Exception as e:
    NATIVE_ERR = str(e)
    print("ai_parse_document unavailable here:\n", NATIVE_ERR[:800])
print("\nNATIVE_OK =", NATIVE_OK)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Parse all 13 documents with both parsers
# MAGIC * **Managed (`ai_parse_document` v2.0):** layout-aware. It returns typed elements (`text`, `section_header`, `table` as HTML, `figure` and others) and runs OCR on image-only pages.
# MAGIC * **Fallback (`pypdf`):** reads the PDF's embedded text layer only. It doesn't see images and doesn't understand tables. It is labelled as a **substitute** so nobody mistakes it for document intelligence.
# MAGIC
# MAGIC We parse **every version, including superseded DOC001_v1.** Retrieval filters on `is_active` (Day 4), while historical and audit questions (eval Q003) still need v1's text.

# COMMAND ----------

# --- Managed parser (only if the probe passed) ---
if NATIVE_OK:
    spark.sql(f"""
    CREATE OR REPLACE TABLE {fq('doc_parsed_raw')} AS
    SELECT element_at(split(path, '/'), -1) AS file_name,
           sha2(content, 256)                AS content_hash,
           ai_parse_document(content, map('version', '2.0')) AS parsed,
           current_timestamp()               AS parsed_at
    FROM read_files('{DOCS}/', format => 'binaryFile')
    """)
    spark.sql(f"""
    CREATE OR REPLACE TABLE {fq('doc_elements_native')} AS
    SELECT r.file_name, r.content_hash, 'ai_parse_document_v2' AS parser,
           element_index,
           variant_get(el, '$.type', 'string')              AS element_type,
           variant_get(el, '$.content', 'string')           AS content,
           try_variant_get(el, '$.bbox[0].page_id', 'int')  AS page_id
    FROM {fq('doc_parsed_raw')} r
    LATERAL VIEW posexplode(variant_get(r.parsed, '$.document.elements', 'ARRAY<VARIANT>')) e AS element_index, el
    """)
    spark.sql(f"""
    CREATE OR REPLACE TABLE {fq('doc_parse_errors_native')} AS
    SELECT file_name, 'ai_parse_document_v2' AS parser, to_json(parsed:error_status) AS parse_error
    FROM {fq('doc_parsed_raw')}
    WHERE parsed:error_status IS NOT NULL AND to_json(parsed:error_status) NOT IN ('[]', '{{}}', 'null')
    """)
    display(spark.sql(f"""SELECT file_name, element_type, count(*) AS n FROM {fq('doc_elements_native')}
                          GROUP BY ALL ORDER BY file_name, element_type"""))

# COMMAND ----------

# --- Fallback parser: pypdf over the same files (one element per page) ---
import pypdf, hashlib

def pypdf_parse(folder):
    elems, errs = [], []
    for f in sorted(os.listdir(folder)):
        path = f"{folder}/{f}"
        data = open(path, "rb").read()
        h = hashlib.sha256(data).hexdigest()
        try:
            reader = pypdf.PdfReader(path)
            for i, page in enumerate(reader.pages):
                elems.append(Row(file_name=f, content_hash=h, parser="pypdf_fallback", element_index=i,
                                 element_type="page_text", content=(page.extract_text() or ""), page_id=i))
        except Exception as e:
            errs.append(Row(file_name=f, parser="pypdf_fallback", parse_error=f"{type(e).__name__}: {e}"[:500]))
    return elems, errs

fb_elems, fb_errs = pypdf_parse(DOCS)
spark.createDataFrame(fb_elems, "file_name STRING, content_hash STRING, parser STRING, element_index INT, "
                                "element_type STRING, content STRING, page_id INT").writeTo(fq("doc_elements_fallback")).createOrReplace()
spark.createDataFrame(fb_errs, "file_name STRING, parser STRING, parse_error STRING").writeTo(fq("doc_parse_errors_fallback")).createOrReplace()

# One table for downstream use, with the parser recorded on every row
parts = ["doc_elements_fallback"] + (["doc_elements_native"] if NATIVE_OK else [])
spark.sql(f"CREATE OR REPLACE TABLE {fq('doc_elements')} AS " +
          " UNION ALL ".join(f"SELECT * FROM {fq(p)}" for p in parts))
err_parts = ["doc_parse_errors_fallback"] + (["doc_parse_errors_native"] if NATIVE_OK else [])
spark.sql(f"CREATE OR REPLACE TABLE {fq('doc_parse_errors')} AS " +
          " UNION ALL ".join(f"SELECT * FROM {fq(p)}" for p in err_parts))
PRIMARY = "ai_parse_document_v2" if NATIVE_OK else "pypdf_fallback"
print("Parsers available:", parts, "| PRIMARY =", PRIMARY)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Processing status: the second reconciliation
# MAGIC Every manifest row gets exactly one status from the **primary** parser:
# MAGIC * `failed`: the parser reported an error
# MAGIC * `pending`: no output at all (the parser never saw it)
# MAGIC * `needs_ocr`: parsed, but fewer than 50 characters of text, which almost always means a scan or image-only file
# MAGIC * `parsed`: usable output, `ready_for_indexing = true`
# MAGIC
# MAGIC **Expected with the managed parser:** 13 parsed and 0 needs_ocr (DOC012 should be OCR'd). If DOC012 shows `needs_ocr`, that is a finding: record it.
# MAGIC **Expected with fallback only:** 12 parsed, **1 needs_ocr (DOC012_v1_scanned.pdf)**, 0 failed, 0 pending.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {fq('doc_processing_status')} AS
WITH stats AS (
  SELECT file_name, count(*) AS element_count,
         count_if(element_type = 'table') AS table_count,
         sum(length(coalesce(content, ''))) AS text_chars
  FROM {fq('doc_elements')} WHERE parser = '{PRIMARY}' GROUP BY file_name),
errs AS (SELECT file_name, first(parse_error) AS parse_error FROM {fq('doc_parse_errors')}
         WHERE parser = '{PRIMARY}' GROUP BY file_name),
base AS (
SELECT m.doc_id, m.version, m.doc_version_id, m.file_name, m.is_active, m.access_group,
       m.effective_from, m.effective_to, '{PRIMARY}' AS parser,
       coalesce(s.element_count, 0) AS element_count, coalesce(s.table_count, 0) AS table_count,
       coalesce(s.text_chars, 0) AS text_chars,
       CASE WHEN e.parse_error IS NOT NULL THEN 'failed'
            WHEN s.file_name IS NULL      THEN 'pending'
            WHEN s.text_chars < 50        THEN 'needs_ocr'
            ELSE 'parsed' END AS processing_status,
       e.parse_error, '{RUN_ID}' AS run_id, current_timestamp() AS checked_at
FROM {fq('doc_manifest')} m
JOIN {fq('doc_reconciliation')} r ON r.file_name = m.file_name AND r.reconcile_status = 'OK'
LEFT JOIN stats s ON s.file_name = m.file_name
LEFT JOIN errs  e ON e.file_name = m.file_name)
-- ready_for_indexing is the ONLY flag Day 4 is allowed to read
SELECT *, processing_status = 'parsed' AS ready_for_indexing FROM base
""")

display(spark.table(fq("doc_processing_status")).orderBy("file_name"))
st = {r.processing_status: r.n for r in spark.sql(
    f"SELECT processing_status, count(*) n FROM {fq('doc_processing_status')} GROUP BY 1").collect()}
print(st)
assert sum(st.values()) == 13, "Every manifest row must have exactly one status"
assert st.get("pending", 0) == 0 and st.get("failed", 0) == 0, "Pending or failed documents. Investigate."
print("PASS: 13 expected = " + " + ".join(f"{v} {k}" for k, v in st.items()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Experiment 2: do the parsers preserve what a user will actually ask about?
# MAGIC Three content checks tied to real questions:
# MAGIC 1. **Table row integrity:** *"What evidence is required for texture claims?"* The correct answer is **Campaign claim registry**, which only works if "Texture" and its evidence stay in the same table row.
# MAGIC 2. **OCR:** *"When is the retail launch in the scanned memo?"* The correct answer is **October 20, 2026** (eval Q009).
# MAGIC 3. **Restricted content exists and is labelled:** DOC007 must carry `access_group = restricted_research` into indexing (eval Q007, enforced on Day 4).
# MAGIC
# MAGIC **Expected, fallback (verified offline with pypdf):** row integrity **false** (each cell comes out on its own line), OCR **false** (0 characters), restricted label true.
# MAGIC **Expected, managed:** row integrity **should** be true (the table comes back as HTML with `<tr>` rows) and OCR **should** be true. Report what you actually see.

# COMMAND ----------

display(spark.sql(f"""
WITH t AS (
  SELECT parser, file_name, content,
         CASE WHEN content LIKE '%<tr%'
              THEN exists(split(content, '</tr>'), r -> r LIKE '%Texture%' AND r LIKE '%Campaign claim registry%')
              ELSE exists(split(content, '\\n'),  r -> r LIKE '%Texture%' AND r LIKE '%Campaign claim registry%')
         END AS row_ok
  FROM {fq('doc_elements')} WHERE file_name IN ('DOC005_v1.pdf', 'DOC001_v2.pdf'))
SELECT parser, 'table_row_integrity (DOC005, DOC001_v2)' AS check_name,
       bool_or(row_ok) AND count(DISTINCT CASE WHEN row_ok THEN file_name END) = 2 AS passed
FROM t GROUP BY parser
UNION ALL
SELECT parser, 'ocr_scanned_memo (DOC012)', bool_or(content LIKE '%October 20, 2026%')
FROM {fq('doc_elements')} WHERE file_name = 'DOC012_v1_scanned.pdf' GROUP BY parser
UNION ALL
SELECT 'manifest', 'restricted_label_present (DOC007)',
       (SELECT access_group = 'restricted_research' FROM {fq('doc_manifest')} WHERE doc_id = 'DOC007')
ORDER BY 1, 2
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Look at the raw evidence yourself:** compare how each parser represents the DOC005 claims table.

# COMMAND ----------

display(spark.sql(f"""SELECT parser, element_index, element_type, content
                      FROM {fq('doc_elements')} WHERE file_name = 'DOC005_v1.pdf'
                      ORDER BY parser, element_index"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎙️ Drill 2: production escalation (brand legal)
# MAGIC **Stakeholder:** *"Your assistant told a marketer that texture claims need 'Approved internal copy review'. That's the evidence for **hydration**. How did that happen, and how do I know it won't happen again?"*
# MAGIC Cover: impact (a wrong compliance answer), cause (table flattened into separate lines, then a chunk boundary split the row), fix (structure-preserving parsing, with each table kept whole as one chunk carrying its header row), and the guard (this exact question added to the evaluation set).
# MAGIC
# MAGIC **Senior architect:** *"The managed parser costs money per page and isn't in every region. Would you send all 20,000 documents through it?"*
# MAGIC A good answer **routes by need**: check whether a text layer exists and whether tables or images are present, send simple text PDFs down the cheap path, and use the managed parser for scans and complex layouts. Then **measure** accuracy and cost on a labelled sample before deciding. State which number would change your mind.
# MAGIC
# MAGIC ### 🎙️ Drill 3: a content conflict engineering can't resolve
# MAGIC Read DOC001_v2 closely. The paragraph approves *"supports a smoother-looking appearance"*, but the table approves *"Creates a smoother-feeling finish"* for texture.
# MAGIC **Kamal:** *"So which wording should the assistant give a copywriter?"*
# MAGIC The strong answer: this is **not an engineering decision.** Show both, each with its citation, flag the conflict to the brand or legal owner, and don't let the LLM pick one quietly. Then propose how conflicts like this get detected and routed in future.
# MAGIC
# MAGIC ## 8. Experiment 3: a corrupt file must fail **its row**, not the whole batch
# MAGIC The scratch folder still contains `DOC013_v1.pdf`, which is not a PDF. Two questions to answer with evidence:
# MAGIC * Does `pypdf` raise an error for that file and carry on with the others? **Expected: yes. 1 error row, and the 12 real files parse.**
# MAGIC * Does `ai_parse_document` return an `error_status` for that row, or does the whole query fail? **Find out and record it.** This decides whether production code must pre-filter files or can rely on per-row error handling.

# COMMAND ----------

sc_elems, sc_errs = pypdf_parse(SCRATCH)
print("pypdf on scratch: files with output =", len({e.file_name for e in sc_elems}), "| errors =", [(e.file_name, e.parse_error) for e in sc_errs])
assert [e.file_name for e in sc_errs] == ["DOC013_v1.pdf"], "Expected exactly one fallback parse error, for DOC013"

if NATIVE_OK:
    try:
        rows = spark.sql(f"""
          SELECT file_name, to_json(p:error_status) AS error_status,
                 size(variant_get(p, '$.document.elements', 'ARRAY<VARIANT>')) AS elements
          FROM (SELECT element_at(split(path, '/'), -1) AS file_name,
                       ai_parse_document(content, map('version','2.0')) AS p
                FROM read_files('{SCRATCH}/DOC013_v1.pdf', format => 'binaryFile'))""").collect()
        print("ai_parse_document returned a row-level result:", rows)
    except Exception as e:
        print("ai_parse_document FAILED THE WHOLE QUERY for one bad file:\n", str(e)[:600])

dbutils.fs.rm(SCRATCH, True)
print("Scratch folder removed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Evidence and exit gate
# MAGIC Append today's results to `dq_results` (the same table as Day 2), so data quality and document quality have one history.

# COMMAND ----------

dq = [("documents", "doc_manifest", "manifest_consistent", "ERROR", 13.0, "13 rows, 1 active per doc, no overlap", "PASS"),
      ("documents", "doc_reconciliation", "files_match_manifest", "ERROR", float(summary.get("OK", 0)), "13 OK", "PASS"),
      ("documents", "scratch", "tamper_detection", "ERROR", 3.0, "1 mismatch + 1 missing + 1 unregistered", "PASS")]
for k, v in st.items():
    dq.append(("documents", "doc_processing_status", f"status_{k}", "WARN" if k == "needs_ocr" else "INFO",
               float(v), f"parser={PRIMARY}", "WARN" if k == "needs_ocr" else "PASS"))
(spark.createDataFrame(dq, "layer STRING, table_name STRING, check_name STRING, severity STRING, metric_value DOUBLE, expected STRING, status STRING")
      .withColumn("batch_id", F.lit(RUN_ID)).withColumn("checked_at", F.current_timestamp())
      .write.mode("append").saveAsTable(fq("dq_results")))
display(spark.sql(f"SELECT * FROM {fq('dq_results')} WHERE batch_id = '{RUN_ID}'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save to `docs/evidence_log.md`
# MAGIC * The `NATIVE_OK` result. If it is False, include the exact error text. Don't paraphrase it.
# MAGIC * The reconciliation (13 OK) and the tamper experiment (11 / 1 / 1 / 1)
# MAGIC * The processing-status counts and the parser used
# MAGIC * The three content checks for each parser, and how the DOC005 table looked in each parser
# MAGIC * Whether a corrupt file failed its row or the whole query
# MAGIC
# MAGIC ### ✅ Day 3 exit gate: go to Day 4 when all of these are true
# MAGIC - [ ] Manifest: 13 rows, consistent
# MAGIC - [ ] Reconciliation: 13/13 OK, and the tamper experiment detected all 3 problems
# MAGIC - [ ] Every document has exactly one status, with 0 pending and 0 failed
# MAGIC - [ ] You know which parser is PRIMARY in this workspace, and why
# MAGIC - [ ] Drills 1–3 done out loud
# MAGIC
# MAGIC **Next (Day 4):** chunk only `ready_for_indexing` documents. Carry `doc_version_id`, `is_active`, `access_group` and page into every chunk. Build an AI Search / Vector Search index, or a clearly labelled local substitute if Free Edition blocks it. Then replace DOC001 v1 with v2 and **prove the stale chunks can no longer be retrieved.**
