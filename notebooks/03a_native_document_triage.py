# Databricks notebook source
# MAGIC %md
# MAGIC # BCIA Day 3A — Databricks-native manifest, managed parsing and quality triage
# MAGIC
# MAGIC **Do this FIRST, before the VS Code / PyMuPDF lab.** Work on one existing synthetic BCIA PDF, not all 20,000 files. This lab does **not** need PyMuPDF, an external OCR engine, or a vision endpoint.
# MAGIC
# MAGIC **Objectives and exit gates:**
# MAGIC 1. Compare the source-of-truth manifest with actual file bytes, using SHA-256. Show missing, changed and extra files.
# MAGIC 2. Parse ONE PDF with Databricks `ai_parse_document` v2.0 (if enabled). Record the exact error otherwise.
# MAGIC 3. Produce elements for paragraphs, tables, figures, captions and page references. Check every page's evidence status.
# MAGIC 4. Inspect a table or figure **against the source**; a parser confidence is NOT ground truth.
# MAGIC 5. Record the evidence in `docs/evidence_log.md` and the architecture decision in D-010.
# MAGIC
# MAGIC **Important difference from the Python exercise:** Managed `ai_parse_document` can OCR and parse mixed documents, but it does **not** directly expose PyMuPDF-style per-page image coverage or invisible text-rendering statistics. Those measurements are Phase 2.
# MAGIC
# MAGIC **Prerequisites:** `BCIA_FULL.zip` unpacked into `workspace.ai_architect_lab.lab_files/beauty_ai_lab_data` (existing Day 3 setup), serverless compute, and permission to create Delta tables in this schema.
# MAGIC
# MAGIC Managed AI functions may be unavailable in a Free Edition region/entitlement. The notebook probes capability and stops the managed parsing step transparently; it never pretends a fallback parser is equivalent.

# COMMAND ----------

import os
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "ai_architect_lab")
dbutils.widgets.text("volume", "lab_files")
dbutils.widgets.text("pdf_name", "DOC002_v1.pdf")
dbutils.widgets.dropdown("RUN_MANAGED_PARSE", "false", ["false", "true"])

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")
PDF_NAME = dbutils.widgets.get("pdf_name").strip()
RUN_MANAGED_PARSE = dbutils.widgets.get("RUN_MANAGED_PARSE") == "true"

assert all(x.replace("_", "").isalnum() for x in [CATALOG, SCHEMA, VOLUME]), "Invalid UC identifier"
assert PDF_NAME.endswith(".pdf") and os.path.basename(PDF_NAME) == PDF_NAME, "Use a PDF filename only"
ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/beauty_ai_lab_data"
DOCS = f"{ROOT}/docs"
MANIFEST_JSON = f"{ROOT}/document_manifest.json"
SELECTED_PDF = f"{DOCS}/{PDF_NAME}"
fq = lambda name: f"`{CATALOG}`.`{SCHEMA}`.`{name}`"

assert os.path.isdir(DOCS), f"Not found: {DOCS}. Upload/unpack BCIA_FULL.zip into the Volume first."
assert os.path.isfile(MANIFEST_JSON), f"Missing source manifest: {MANIFEST_JSON}"
assert os.path.isfile(SELECTED_PDF), f"Missing selected PDF: {SELECTED_PDF}"
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")
print("SELECTED_PDF:", SELECTED_PDF)
print("RUN_MANAGED_PARSE:", RUN_MANAGED_PARSE)
print("Available PDFs:", sorted(f for f in os.listdir(DOCS) if f.lower().endswith(".pdf")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Manifest versus bytes in the Volume — no AI calls
# MAGIC The manifest defines what SHOULD exist. Actual files define what DOES exist.
# MAGIC The manifest should not be rewritten by parsing; processing statuses go to a separate table.
# MAGIC `access_group` is present in the synthetic source. A production manifest should also include `owner`, provenance, source ACL IDs, last modified timestamp and connector checkpoint. Do not invent them if the source does not contain them.

# COMMAND ----------

manifest = (spark.read.option("multiLine", True).json(MANIFEST_JSON)
    .select(
        "doc_id", F.col("version").cast("int").alias("version"),
        F.element_at(F.split(F.col("source_path"), "/"), -1).alias("file_name"),
        F.col("content_hash").alias("expected_hash"), "source_system", "access_group",
        "source_path", "file_type", F.col("page_count").cast("int").alias("expected_pages")
    ))

inventory = (spark.read.format("binaryFile").load(DOCS)
    .filter(F.lower(F.col("path")).endswith(".pdf"))
    .select(F.element_at(F.split("path", "/"), -1).alias("file_name"),
            F.col("path").alias("actual_path"), F.sha2("content", 256).alias("actual_hash"),
            F.col("length").alias("bytes")))

reconciliation = (manifest.alias("m").join(inventory.alias("i"), "file_name", "full")
    .select("file_name", "m.doc_id", "m.version", "m.source_system", "m.access_group",
            "m.expected_hash", "i.actual_hash", "i.actual_path", "i.bytes",
            F.when(F.col("m.doc_id").isNull(), "UNREGISTERED_FILE")
             .when(F.col("i.actual_hash").isNull(), "MISSING_FILE")
             .when(F.col("m.expected_hash") != F.col("i.actual_hash"), "HASH_MISMATCH")
             .otherwise("OK").alias("status")))

inventory.writeTo(fq("bcia_lab_file_inventory")).createOrReplace()
reconciliation.writeTo(fq("bcia_lab_reconciliation")).createOrReplace()
display(reconciliation.orderBy("file_name"))
display(reconciliation.groupBy("status").count().orderBy("status"))
selected = [x for x in reconciliation.filter(F.col("file_name") == PDF_NAME).collect()]
assert len(selected) == 1 and selected[0].status == "OK", (
    f"Selected file {PDF_NAME} is not manifest-verified. Fix before parsing: {selected}"
)
print("PASS: Selected PDF exists and matches its source-manifest SHA-256")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Inspect with Databricks' native Document Parsing UI
# MAGIC In the workspace, find **Document Parsing** (UI location varies by workspace) and upload/select this same PDF.
# MAGIC Compare source and parsed views: headings, columns, tables, figure regions and any suspected OCR errors.
# MAGIC
# MAGIC Then run the next cell with the widget `RUN_MANAGED_PARSE=true`. We disable **AI-generated figure descriptions** on this first pass using `descriptionElementTypes=''` to avoid unnecessary work. The function may still detect figure elements and sometimes structured chart data.
# MAGIC
# MAGIC **What to record:** the first wrong table cell, a missing figure, or a page with suspiciously little content. A successful function call does NOT certify that the values are correct.

# COMMAND ----------

PARSE_OK = False
PARSE_ERROR = None
if not RUN_MANAGED_PARSE:
    print("SKIPPED: Set RUN_MANAGED_PARSE=true and rerun after reviewing the source PDF.")
else:
    try:
        # Literal path is safe here because PDF_NAME contains no directory separators or quotes.
        assert "'" not in SELECTED_PDF
        spark.sql(f"""
            CREATE OR REPLACE TABLE {fq('bcia_lab_parsed_raw')} AS
            SELECT element_at(split(path, '/'), -1) AS file_name,
                   sha2(content, 256) AS sha256,
                   ai_parse_document(content,
                      map('version', '2.0', 'descriptionElementTypes', '')) AS parsed,
                   current_timestamp() AS processed_at
              FROM read_files('{SELECTED_PDF}', format => 'binaryFile')
        """)
        PARSE_OK = True
        print("PASS: Managed parser returned; results in bcia_lab_parsed_raw")
    except Exception as exc:
        PARSE_ERROR = f"{type(exc).__name__}: {exc}"
        print("MANAGED PARSER UNAVAILABLE/FAILED. Record this exact error:")
        print(PARSE_ERROR[:2500])

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Expand structured elements (only when native parse succeeds)
# MAGIC Databricks 2.0 returns `VARIANT`. Every element has a type; supported examples include `text`, `table`, `figure`, `caption`, `section_header`, `signature` and others. `bbox[0].page_id` uses **zero-based** indexing. Table content can be HTML; figure content may be chart JSON, NULL, or other content. Do not treat NULL figure content as proof the figure is unimportant.

# COMMAND ----------

if PARSE_OK:
    spark.sql(f"""
        CREATE OR REPLACE TABLE {fq('bcia_lab_doc_elements')} AS
        SELECT r.file_name,
               element_index,
               try_variant_get(el, '$.id', 'INT') AS element_id,
               try_variant_get(el, '$.type', 'STRING') AS element_type,
               try_variant_get(el, '$.content', 'STRING') AS content,
               try_variant_get(el, '$.description', 'STRING') AS description,
               try_variant_get(el, '$.confidence', 'DOUBLE') AS content_confidence,
               try_variant_get(el, '$.bbox[0].page_id', 'INT') AS page_id_zero_based,
               to_json(variant_get(el, '$.bbox', 'ARRAY<VARIANT>')) AS bbox_json
        FROM {fq('bcia_lab_parsed_raw')} r
        LATERAL VIEW posexplode(variant_get(r.parsed, '$.document.elements', 'ARRAY<VARIANT>')) exploded AS element_index, el
    """)
    display(spark.sql(f"""
      SELECT page_id_zero_based + 1 AS page_number, element_type, count(*) AS elements,
             sum(length(coalesce(content,''))) AS extracted_characters,
             round(avg(content_confidence), 3) AS mean_content_confidence
        FROM {fq('bcia_lab_doc_elements')}
       GROUP BY page_id_zero_based, element_type
       ORDER BY page_number, element_type
    """))
    display(spark.sql(f"SELECT * FROM {fq('bcia_lab_doc_elements')} ORDER BY element_index LIMIT 25"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Page coverage and review queue — not a false claim of verified accuracy
# MAGIC We begin with the parser's explicit page list, then left join extracted elements. This catches pages that return no elements. A table needs row/column verification; a chart requires value/axis verification. For real OCR accuracy, compare selected extracted values with verified labels or the page image.

# COMMAND ----------

if PARSE_OK:
    spark.sql(f"""
      CREATE OR REPLACE TABLE {fq('bcia_lab_page_triage')} AS
      WITH pages AS (
        SELECT file_name, try_variant_get(pg, '$.id', 'INT') AS page_id_zero_based
          FROM {fq('bcia_lab_parsed_raw')}
          LATERAL VIEW explode(variant_get(parsed, '$.document.pages', 'ARRAY<VARIANT>')) exp AS pg
      ), totals AS (
        SELECT file_name, page_id_zero_based,
               sum(length(coalesce(content, ''))) AS extracted_chars,
               sum(CASE WHEN element_type='table' THEN 1 ELSE 0 END) AS tables,
               sum(CASE WHEN element_type='figure' THEN 1 ELSE 0 END) AS figures,
               sum(CASE WHEN element_type='signature' THEN 1 ELSE 0 END) AS signatures,
               count(*) AS total_elements
          FROM {fq('bcia_lab_doc_elements')}
         GROUP BY file_name, page_id_zero_based
      )
      SELECT p.file_name, p.page_id_zero_based, p.page_id_zero_based + 1 AS page_number,
             coalesce(t.extracted_chars, 0) AS extracted_chars,
             coalesce(t.tables, 0) AS tables,
             coalesce(t.figures, 0) AS figures,
             coalesce(t.signatures, 0) AS signatures,
             coalesce(t.total_elements, 0) AS total_elements,
             CASE WHEN coalesce(t.total_elements, 0)=0 THEN 'REVIEW_NO_ELEMENTS'
                  WHEN coalesce(t.figures, 0)>0 THEN 'REVIEW_FIGURE'
                  WHEN coalesce(t.tables, 0)>0 THEN 'REVIEW_TABLE'
                  WHEN coalesce(t.extracted_chars, 0)<50 THEN 'REVIEW_LOW_TEXT'
                  ELSE 'PARSED_CANDIDATE' END AS review_status
        FROM pages p LEFT JOIN totals t
        ON p.file_name=t.file_name AND p.page_id_zero_based=t.page_id_zero_based
    """)
    display(spark.sql(f"SELECT * FROM {fq('bcia_lab_page_triage')} ORDER BY page_number"))
    display(spark.sql(f"SELECT review_status, count(*) pages FROM {fq('bcia_lab_page_triage')} GROUP BY 1 ORDER BY 1"))
    display(spark.sql(f"SELECT file_name, element_id, element_type, content, bbox_json FROM {fq('bcia_lab_doc_elements')} WHERE element_type IN ('table','figure','signature') ORDER BY element_index"))
    print("EXIT GATE: Review at least one table/figure against the source. PARSED_CANDIDATE is not VERIFIED.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Optional follow-up: selectively enable descriptions ONLY after reviewing figures
# MAGIC To inspect figure descriptions on a **single** selected PDF, execute this manually (not as part of default Run All):
# MAGIC ```sql
# MAGIC SELECT to_json(ai_parse_document(content,
# MAGIC     map('version','2.0','descriptionElementTypes','figure'))) AS parsed_with_descriptions
# MAGIC FROM read_files('/Volumes/workspace/ai_architect_lab/lab_files/beauty_ai_lab_data/docs/DOC002_v1.pdf', format => 'binaryFile');
# MAGIC ```
# MAGIC Do NOT call it for all 20,000 files until you've measured important-figure recall and cost. Figure description and numeric validation are different tasks.
# MAGIC
# MAGIC ### Evidence to record after this exercise
# MAGIC - Number of manifest rows vs inventory rows, any hash mismatch, and selected PDF name.
# MAGIC - `PARSE_OK` or the exact `PARSE_ERROR`, number of elements, number of pages requiring table/figure review.
# MAGIC - One checked value/caption with source page and whether it matched.
# MAGIC - Capability gap: managed document parsing does not supply native text visibility/image-coverage detection. That motivates the VS Code PyMuPDF comparison, not an automatic replacement of Databricks parsing.
