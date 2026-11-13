# Databricks notebook source
# MAGIC %md
# MAGIC # Day 2: From CSV files to trustworthy Bronze, Silver and Gold tables
# MAGIC **Beauty Campaign Intelligence Assistant (BCIA). Every row is synthetic training data. None of it is Estée Lauder or C5i data.**
# MAGIC
# MAGIC ### Why the business cares
# MAGIC A marketing director asks: *"Which campaigns are underperforming, and where are we actually selling?"*
# MAGIC Genie, a RAG agent or a dashboard can only answer that correctly if three things are already true:
# MAGIC 1. **Every source row is accounted for.** We can prove nothing was silently dropped.
# MAGIC 2. **Bad rows are isolated, not hidden.** An orphan product or broken arithmetic goes to quarantine with a reason.
# MAGIC 3. **Metrics have one definition.** "Net sales" and "attributed revenue" are different numbers, and the curated views make that hard to get wrong.
# MAGIC
# MAGIC ### Architecture for today
# MAGIC ```
# MAGIC  beauty_ai_lab_data.zip ──upload──▶ UC Volume  /Volumes/workspace/ai_architect_lab/lab_files/
# MAGIC                                         │  (files: governed, not tables)
# MAGIC                                         ▼
# MAGIC  BRONZE  bronze_<table>    all columns STRING + _source_file, _ingested_at, _batch_id   (faithful copy, replayable)
# MAGIC                                         ▼  try_cast types · PK / FK / business rules
# MAGIC  SILVER  silver_<table>    typed, validated, CHECK constraints      ──▶ silver_quarantine (rows + failed rule names)
# MAGIC                                         ▼  joins and metric definitions, documented with column comments
# MAGIC  GOLD    gold_sales_lines · gold_campaign_daily · gold_campaign_summary   (views that Genie will use on Day 5)
# MAGIC                                         ▼
# MAGIC  dq_results  (every check, PASS / FAIL / WARN, per batch)  →  your evidence log
# MAGIC ```
# MAGIC
# MAGIC ### Where to run it
# MAGIC Import this file through **Workspace → (your folder) → ⋮ → Import → File**, attach **Serverless** compute and run the cells from top to bottom.
# MAGIC Cells marked `ASSERT` stop the notebook if a result does not match the expected value. **Do not continue past a failed assert.** Paste the error into the chat instead.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration
# MAGIC If your catalog, schema or volume name differs, change it here and nowhere else.

# COMMAND ----------

import datetime, os, glob, shutil, zipfile
from pyspark.sql import functions as F, Window

CATALOG = "workspace"
SCHEMA  = "ai_architect_lab"
VOLUME  = "lab_files"                       # created below if missing

VOL_ROOT  = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
DATA_ROOT = f"{VOL_ROOT}/beauty_ai_lab_data"
RAW       = f"{DATA_ROOT}/raw"
BATCH_ID  = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# Row counts promised by the generator (data_quality_report.json). Bronze must match these exactly.
EXPECTED_ROWS = {
    "dim_brand": 3, "dim_channel": 4, "dim_market": 6, "dim_product": 24, "dim_campaign": 12,
    "fact_campaign_daily": 5880, "fact_orders": 25000, "fact_inventory": 432, "fact_reviews": 1000,
}

def fq(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{name}"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print("Batch:", BATCH_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Resource audit: what is already in the workspace?
# MAGIC We look before we build, so we don't overwrite or duplicate Day 1 work. New tables in this notebook all start with `bronze_`, `silver_`, `gold_`, `dq_` or `lab_`, so they can't clash with your four existing tables.
# MAGIC
# MAGIC **Expected:** your catalog and schema print. The table list shows your four Day-1 tables with row counts. The volume list may be empty.

# COMMAND ----------

display(spark.sql("SELECT current_catalog() AS catalog, current_schema() AS schema, current_user() AS user"))

existing = [r.tableName for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect() if not r.isTemporary]
audit = []
for t in existing:
    try:
        audit.append((t, spark.table(fq(t)).count(), None))
    except Exception as e:                       # e.g. a broken view
        audit.append((t, None, str(e)[:150]))
display(spark.createDataFrame(audit, "table_name STRING, row_count BIGINT, error STRING")) if audit else print("No tables yet.")
display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Record in `evidence_log.md`:** the names and row counts of your four Day-1 tables. If they are copies of the same CSVs (for example `fact_orders` with 25,000 rows), keep them for now and decide on Day 8 whether to drop them. From today on, the `silver_` and `gold_` tables are the source of truth.
# MAGIC
# MAGIC ## 2. Volume and files
# MAGIC **Why a Volume?** A Volume is a governed folder for *files* (CSVs, PDFs), with Unity Catalog permissions and an audit trail. It is not a table. Raw files stay there so that every table can be rebuilt from them, and so that the PDFs for Day 3 sit next to the data they describe.

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {fq(VOLUME)} COMMENT 'Raw files for the beauty AI lab. SYNTHETIC TRAINING DATA ONLY.'")
print("Volume ready:", VOL_ROOT)
print("Current contents:", os.listdir(VOL_ROOT))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upload (browser, about 1 minute)
# MAGIC 1. Left sidebar: **Catalog**, then **workspace → ai_architect_lab → Volumes → lab_files**.
# MAGIC 2. Click **Upload to this volume** and choose `BCIA_FULL.zip` (or the starter ZIP). Upload it as-is, without unzipping it first. This cell finds the baseline dataset inside, even when it is nested.
# MAGIC 3. Run the next cell. It unzips into `lab_files/beauty_ai_lab_data/`. It extracts to local `/tmp` first and then copies plain files into the volume, because volume paths don't support every file operation (permission and timestamp changes, for example).

# COMMAND ----------

if not os.path.exists(f"{RAW}/fact_orders.csv"):
    # Accept the starter ZIP (with or without a top folder) or BCIA_FULL.zip (starter ZIP nested inside)
    zips = sorted(glob.glob(f"{VOL_ROOT}/*.zip"))
    assert zips, f"No .zip found in {VOL_ROOT}. Upload the starter ZIP or BCIA_FULL.zip first."
    local_dir = "/tmp/beauty_ai_lab_data"
    shutil.rmtree(local_dir, ignore_errors=True)
    for zp in zips:
        print("Extracting", zp)
        shutil.copyfile(zp, "/tmp/upload.zip")
        with zipfile.ZipFile("/tmp/upload.zip") as z:
            z.extractall(local_dir)
    for inner in glob.glob(f"{local_dir}/**/*Starter*.zip", recursive=True):   # nested starter ZIP
        with zipfile.ZipFile(inner) as z:
            z.extractall(local_dir + "/_starter")
    hits = [r for r, _, f in os.walk(local_dir) if "fact_orders.csv" in f and r.endswith("raw")]
    assert hits, "Could not find raw/fact_orders.csv inside the uploaded ZIP(s)"
    src_root = os.path.dirname(hits[0])                     # folder that holds raw/, docs/, manifest
    print("Using dataset root:", src_root)
    for root, _, files in os.walk(src_root):
        target = os.path.join(DATA_ROOT, os.path.relpath(root, src_root))
        os.makedirs(target, exist_ok=True)
        for f in files:
            shutil.copyfile(os.path.join(root, f), os.path.join(target, f))
else:
    print("Already extracted. Skipping.")

# ASSERT: every expected file is present
missing = [t for t in EXPECTED_ROWS if not os.path.exists(f"{RAW}/{t}.csv")]
missing += [f for f in ["document_manifest.json", "eval_questions.csv"] if not os.path.exists(f"{DATA_ROOT}/{f}")]
pdfs = sorted(os.listdir(f"{DATA_ROOT}/docs")) if os.path.exists(f"{DATA_ROOT}/docs") else []
print(f"CSVs present: {sum(os.path.exists(f'{RAW}/{t}.csv') for t in EXPECTED_ROWS)}/9   PDFs: {len(pdfs)}/13")
assert not missing, f"Missing files: {missing}"
assert len(pdfs) == 13, f"Expected 13 PDFs, found {len(pdfs)}: {pdfs}"
print("PASS: all source files present")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bronze: a faithful, replayable copy
# MAGIC **Design choice:** every column is loaded as `STRING`. Bronze answers *"what did the source send us?"*, not *"what does it mean?"*. If we cast types here, a bad date would either fail the whole load or quietly become NULL, and we would lose the original value we need to debug it.
# MAGIC
# MAGIC Three lineage columns are added to every row:
# MAGIC * `_source_file`: the exact file the row came from (Spark's `_metadata.file_path`)
# MAGIC * `_ingested_at`: when we loaded it
# MAGIC * `_batch_id`: which run loaded it, so that DQ results can be tied to a load
# MAGIC
# MAGIC **Expected:** 9 tables, and each row count equals `EXPECTED_ROWS` (fact_orders = 25,000; fact_campaign_daily = 5,880).

# COMMAND ----------

def load_bronze(table: str) -> int:
    df = (spark.read.format("csv")
          .option("header", "true")
          .option("inferSchema", "false")          # all STRING on purpose
          .load(f"{RAW}/{table}.csv")
          .select("*",
                  F.col("_metadata.file_path").alias("_source_file"),
                  F.col("_metadata.file_modification_time").alias("_source_modified_at"))
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_batch_id", F.lit(BATCH_ID)))
    df.writeTo(fq(f"bronze_{table}")).createOrReplace()     # Delta by default; keeps table history
    return spark.table(fq(f"bronze_{table}")).count()

bronze_counts = {t: load_bronze(t) for t in EXPECTED_ROWS}
result = [(t, n, EXPECTED_ROWS[t], "PASS" if n == EXPECTED_ROWS[t] else "FAIL") for t, n in bronze_counts.items()]
display(spark.createDataFrame(result, "table_name STRING, bronze_rows BIGINT, expected BIGINT, status STRING"))
assert all(r[3] == "PASS" for r in result), "Bronze row counts do not match the source. Stop and investigate."

# COMMAND ----------

# MAGIC %md
# MAGIC **Quick look:** check that `campaign_id` is NULL (not an empty string) for organic orders. The CSV reader turns empty fields into NULL. That matters for joins later.
# MAGIC **Expected:** `null_campaign = 15754`, `with_campaign = 9246`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count_if(campaign_id IS NULL) AS null_campaign,
# MAGIC        count_if(campaign_id IS NOT NULL) AS with_campaign,
# MAGIC        min(order_date) AS first_order, max(order_date) AS last_order      -- strings in bronze: '2026-01-01' / '2026-11-06'
# MAGIC FROM bronze_fact_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver: typed, validated, reconciled
# MAGIC **What happens to each table:**
# MAGIC 1. `try_cast` every column to its real type. Serverless runs in ANSI mode, so a plain `CAST('2026-13-45' AS DATE)` *fails the whole job*, while `try_cast` returns NULL and lets a rule catch it.
# MAGIC 2. Look up foreign keys against the already-validated Silver dimensions.
# MAGIC 3. Evaluate named rules. A rule that evaluates to NULL counts as **failed**, never as passed.
# MAGIC 4. Rows that pass go to `silver_<table>`. Rows that fail go to `silver_quarantine` with the rule names and the **original raw record as JSON**.
# MAGIC 5. **Reconcile:** `bronze_rows = silver_rows + quarantined_rows`. This is the same "expected = processed + failed" idea Kamal pushed for with documents.
# MAGIC
# MAGIC **Duplicate primary keys:** *every* copy is quarantined. We can't know which one is correct, and keeping "the latest" would silently pick a winner. The source owner decides.

# COMMAND ----------

QUARANTINE = "silver_quarantine"
spark.sql(f"""CREATE OR REPLACE TABLE {fq(QUARANTINE)} (
  source_table STRING, failed_rules ARRAY<STRING>, raw_record STRING, batch_id STRING, quarantined_at TIMESTAMP)
  COMMENT 'Rows rejected by Silver validation, with the rule names and the original bronze record.'""")

def typed(bronze_df, cols):
    """Keep the raw row as JSON for quarantine, cast business columns, carry lineage columns."""
    return bronze_df.selectExpr("to_json(struct(*)) AS __raw_json", *cols,
                                "_source_file", "_ingested_at", "_batch_id")

def lookup(df, fk_col, dim_table, dim_key, flag, extra=None):
    """LEFT JOIN to a silver dimension. Adds a boolean `flag` (key exists) and optional extra columns."""
    extra = extra or {}
    dim = spark.table(fq(dim_table)).select(F.col(dim_key).alias("__lk"),
                                            *[F.col(k).alias(v) for k, v in extra.items()])
    return (df.join(dim, F.col(fk_col) == F.col("__lk"), "left")
              .withColumn(flag, F.col("__lk").isNotNull())
              .drop("__lk"))

def write_silver(table, df, rules, pk_cols, target=None, quarantine=QUARANTINE):
    target = target or f"silver_{table}"
    df = df.withColumn("__pk_count", F.count(F.lit(1)).over(Window.partitionBy(*pk_cols)))
    all_rules = {"pk_not_null": " AND ".join(f"{c} IS NOT NULL" for c in pk_cols),
                 "pk_unique": "__pk_count = 1", **rules}
    checks = [F.when(~F.coalesce(F.expr(expr), F.lit(False)), F.lit(name)) for name, expr in all_rules.items()]
    df = df.withColumn("__failures", F.array_compact(F.array(*checks)))
    helper_cols = [c for c in df.columns if c.startswith("__")]

    df.filter(F.size("__failures") == 0).drop(*helper_cols).writeTo(fq(target)).createOrReplace()
    (df.filter(F.size("__failures") > 0)
       .select(F.lit(table).alias("source_table"), F.col("__failures").alias("failed_rules"),
               F.col("__raw_json").alias("raw_record"), F.lit(BATCH_ID).alias("batch_id"),
               F.current_timestamp().alias("quarantined_at"))
       .writeTo(fq(quarantine)).append())

    n_in = df.count()
    n_ok = spark.table(fq(target)).count()
    n_bad = spark.table(fq(quarantine)).filter((F.col("source_table") == table) & (F.col("batch_id") == BATCH_ID)).count()
    return {"table": table, "input": n_in, "silver": n_ok, "quarantined": n_bad, "reconciled": n_in == n_ok + n_bad}

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4a. Dimensions first
# MAGIC Facts validate against Silver dimensions, so the dimensions must exist and be clean before any fact is loaded.

# COMMAND ----------

recon = []
b = lambda t: spark.table(fq(f"bronze_{t}"))

recon.append(write_silver("dim_brand",
    typed(b("dim_brand"), ["brand_id", "brand_name", "tier"]),
    {"brand_name_present": "brand_name IS NOT NULL",
     "tier_valid": "tier IN ('premium','prestige','accessible')"}, ["brand_id"]))

recon.append(write_silver("dim_channel",
    typed(b("dim_channel"), ["channel_id", "channel_name"]),
    {"channel_name_present": "channel_name IS NOT NULL"}, ["channel_id"]))

recon.append(write_silver("dim_market",
    typed(b("dim_market"), ["market_id", "region", "country", "market_name"]),
    {"region_present": "region IS NOT NULL", "market_name_present": "market_name IS NOT NULL"}, ["market_id"]))

prod = typed(b("dim_product"), ["product_id", "brand_id", "category", "product_name",
        "try_cast(list_price_usd AS DECIMAL(10,2)) AS list_price_usd",
        "try_cast(launch_date AS DATE) AS launch_date", "ingredient_theme"])
prod = lookup(prod, "brand_id", "silver_dim_brand", "brand_id", "__has_brand")
recon.append(write_silver("dim_product", prod,
    {"fk_brand": "__has_brand",
     "category_valid": "category IN ('skincare','makeup','fragrance','haircare')",
     "price_positive": "list_price_usd > 0",
     "launch_date_valid": "launch_date IS NOT NULL"}, ["product_id"]))

camp = typed(b("dim_campaign"), ["campaign_id", "brand_id", "campaign_name",
        "try_cast(start_date AS DATE) AS start_date", "try_cast(end_date AS DATE) AS end_date",
        "objective", "audience", "try_cast(budget_usd AS DECIMAL(12,2)) AS budget_usd"])
camp = lookup(camp, "brand_id", "silver_dim_brand", "brand_id", "__has_brand")
recon.append(write_silver("dim_campaign", camp,
    {"fk_brand": "__has_brand",
     "dates_valid": "start_date IS NOT NULL AND end_date IS NOT NULL AND end_date >= start_date",
     "budget_positive": "budget_usd > 0"}, ["campaign_id"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b. Facts
# MAGIC The order rules show that **business** rules go beyond **structural** ones. An order tied to a campaign must sell a product from that campaign's brand, and it must fall inside the campaign dates. A foreign-key check alone would never catch a Velvet Grove product credited to an Aurora Botanics campaign.
# MAGIC
# MAGIC `build_orders()` is a function so that the defect experiment in section 8 can push *dirty* input through exactly the same logic.

# COMMAND ----------

def build_orders(bronze_df):
    df = typed(bronze_df, ["order_line_id", "try_cast(order_date AS DATE) AS order_date",
            "product_id", "market_id", "channel_id", "campaign_id",
            "try_cast(quantity AS INT) AS quantity",
            "try_cast(gross_sales_usd AS DECIMAL(12,2)) AS gross_sales_usd",
            "try_cast(discount_usd AS DECIMAL(12,2)) AS discount_usd",
            "try_cast(returns_usd AS DECIMAL(12,2)) AS returns_usd",
            "try_cast(net_sales_usd AS DECIMAL(12,2)) AS net_sales_usd"])
    df = lookup(df, "product_id", "silver_dim_product", "product_id", "__has_product", {"brand_id": "__product_brand"})
    df = lookup(df, "market_id", "silver_dim_market", "market_id", "__has_market")
    df = lookup(df, "channel_id", "silver_dim_channel", "channel_id", "__has_channel")
    df = lookup(df, "campaign_id", "silver_dim_campaign", "campaign_id", "__has_campaign",
                {"brand_id": "__campaign_brand", "start_date": "__campaign_start", "end_date": "__campaign_end"})
    return df

ORDER_RULES = {
    "order_date_valid":     "order_date IS NOT NULL",
    "order_date_not_future":"order_date IS NULL OR order_date <= current_date()",   # null handled by the rule above
    "quantity_positive":    "quantity > 0",
    "amounts_non_negative": "gross_sales_usd >= 0 AND discount_usd >= 0 AND returns_usd >= 0 AND net_sales_usd >= 0",
    "net_sales_formula":    "abs(gross_sales_usd - discount_usd - returns_usd - net_sales_usd) <= 0.01",
    "fk_product":           "__has_product",
    "fk_market":            "__has_market",
    "fk_channel":           "__has_channel",
    "fk_campaign":          "campaign_id IS NULL OR __has_campaign",
    "campaign_brand_match": "campaign_id IS NULL OR __product_brand = __campaign_brand",
    "campaign_window":      "campaign_id IS NULL OR order_date BETWEEN __campaign_start AND __campaign_end",
}
recon.append(write_silver("fact_orders", build_orders(b("fact_orders")), ORDER_RULES, ["order_line_id"]))

cd = typed(b("fact_campaign_daily"), ["campaign_id", "product_id", "market_id", "channel_id",
        "try_cast(event_date AS DATE) AS event_date",
        "try_cast(impressions AS BIGINT) AS impressions", "try_cast(clicks AS BIGINT) AS clicks",
        "try_cast(spend_usd AS DECIMAL(12,2)) AS spend_usd",
        "try_cast(attributed_orders AS INT) AS attributed_orders",
        "try_cast(attributed_revenue_usd AS DECIMAL(12,2)) AS attributed_revenue_usd"])
cd = lookup(cd, "campaign_id", "silver_dim_campaign", "campaign_id", "__has_campaign",
            {"brand_id": "__campaign_brand", "start_date": "__campaign_start", "end_date": "__campaign_end"})
cd = lookup(cd, "product_id", "silver_dim_product", "product_id", "__has_product", {"brand_id": "__product_brand"})
cd = lookup(cd, "market_id", "silver_dim_market", "market_id", "__has_market")
cd = lookup(cd, "channel_id", "silver_dim_channel", "channel_id", "__has_channel")
recon.append(write_silver("fact_campaign_daily", cd, {
    "event_date_valid":       "event_date IS NOT NULL",
    "fk_campaign":            "__has_campaign", "fk_product": "__has_product",
    "fk_market":              "__has_market",   "fk_channel": "__has_channel",
    "campaign_brand_match":   "__product_brand = __campaign_brand",
    "within_campaign_window": "event_date BETWEEN __campaign_start AND __campaign_end",
    "clicks_le_impressions":  "clicks >= 0 AND clicks <= impressions",
    "non_negative_measures":  "spend_usd >= 0 AND attributed_orders >= 0 AND attributed_revenue_usd >= 0",
}, ["campaign_id", "product_id", "market_id", "channel_id", "event_date"]))

inv = typed(b("fact_inventory"), ["snapshot_id", "try_cast(snapshot_date AS DATE) AS snapshot_date",
        "product_id", "market_id", "try_cast(on_hand_units AS INT) AS on_hand_units",
        "try_cast(reserved_units AS INT) AS reserved_units"])
inv = lookup(lookup(inv, "product_id", "silver_dim_product", "product_id", "__has_product"),
             "market_id", "silver_dim_market", "market_id", "__has_market")
recon.append(write_silver("fact_inventory", inv, {
    "snapshot_date_valid": "snapshot_date IS NOT NULL",
    "fk_product": "__has_product", "fk_market": "__has_market",
    "units_valid": "on_hand_units >= 0 AND reserved_units >= 0 AND reserved_units <= on_hand_units"}, ["snapshot_id"]))

rev = typed(b("fact_reviews"), ["review_id", "try_cast(review_date AS DATE) AS review_date",
        "product_id", "market_id", "try_cast(rating AS INT) AS rating", "body", "sentiment_hint"])
rev = lookup(lookup(rev, "product_id", "silver_dim_product", "product_id", "__has_product"),
             "market_id", "silver_dim_market", "market_id", "__has_market")
recon.append(write_silver("fact_reviews", rev, {
    "review_date_valid": "review_date IS NOT NULL",
    "fk_product": "__has_product", "fk_market": "__has_market",
    "rating_range": "rating BETWEEN 1 AND 5",
    "sentiment_valid": "sentiment_hint IN ('positive','neutral','negative')"}, ["review_id"]))

from pyspark.sql import Row
display(spark.createDataFrame([Row(**r) for r in recon]))

# COMMAND ----------

# MAGIC %md
# MAGIC **Expected:** 9 rows, all `reconciled = true`, `quarantined = 0`, and `silver` equal to the bronze counts. The baseline data is clean. Section 8 dirties it on purpose.
# MAGIC
# MAGIC ### 4c. Enforce the most important rules at write time (Delta CHECK constraints)
# MAGIC The quarantine logic only protects writes that go *through this notebook*. A CHECK constraint protects the table against **any** writer: another job, a hurried analyst, or an AI agent with SQL access. Section 8 shows a constraint rejecting a bad insert.

# COMMAND ----------

assert all(r["reconciled"] for r in recon), "Reconciliation failed. See the table above."
assert all(r["silver"] == EXPECTED_ROWS[r["table"]] for r in recon), "Silver counts differ from source. Check silver_quarantine."

CONSTRAINTS = {
    "silver_fact_orders": {
        "net_sales_formula": "abs(gross_sales_usd - discount_usd - returns_usd - net_sales_usd) <= 0.01",
        "quantity_positive": "quantity > 0"},
    "silver_fact_campaign_daily": {
        "clicks_le_impressions": "clicks <= impressions",
        "spend_non_negative": "spend_usd >= 0"},
}
for table, cons in CONSTRAINTS.items():
    for name, expr in cons.items():
        spark.sql(f"ALTER TABLE {fq(table)} DROP CONSTRAINT IF EXISTS {name}")
        spark.sql(f"ALTER TABLE {fq(table)} ADD CONSTRAINT {name} CHECK ({expr})")
display(spark.sql(f"SHOW TBLPROPERTIES {fq('silver_fact_orders')}").filter("key LIKE 'delta.constraints%'"))
print("PASS: silver reconciled, constraints added")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Gold: business-ready views with one definition per metric
# MAGIC **Why views instead of tables?** At 25,000 rows a view costs nothing, can't go stale, and keeps the metric logic in one reviewed place. At scale (or with a strict latency SLA) you would materialise these as tables or a materialized view and refresh them on a schedule.
# MAGIC
# MAGIC **Why the long column comments?** On Day 5, Genie reads them. A comment like *"NET SALES = gross − discount − returns; 'sales' and 'revenue' mean this unless the user says attributed revenue"* is how we stop the agent mixing up two different dollar numbers.
# MAGIC
# MAGIC **Grain** (what one row means) is written into every view comment. Most wrong joins come from mixing grains (see section 9).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fq('gold_sales_lines')} (
  order_line_id  COMMENT 'Unique order line: one product within one order.',
  order_date     COMMENT 'Order date. Use this date for every sales / net sales time filter.',
  order_year,
  order_quarter  COMMENT 'Calendar quarter label, e.g. 2026-Q3 = July to September 2026.',
  order_month    COMMENT 'Month label yyyy-MM.',
  brand_id, brand_name, product_id, product_name,
  category       COMMENT 'Product category: skincare, makeup, fragrance or haircare.',
  market_id,
  market_name    COMMENT 'City-level market, e.g. New York, Boston, Atlanta.',
  region         COMMENT 'US region: Northeast, South, West or Midwest.',
  channel_id,
  channel_name   COMMENT 'Sales channel: ecommerce, retail, marketplace or social.',
  campaign_id    COMMENT 'Campaign linked to the order line. NULL = organic sale with no campaign.',
  campaign_name,
  quantity       COMMENT 'Units sold.',
  gross_sales_usd COMMENT 'Quantity x list price before discounts and returns, USD.',
  discount_usd   COMMENT 'Promotional discount, USD.',
  returns_usd    COMMENT 'Value returned by the customer, USD.',
  net_sales_usd  COMMENT 'NET SALES = gross_sales_usd - discount_usd - returns_usd. Official sales measure. "Sales" or "revenue" means this unless the user explicitly asks for attributed / campaign revenue.'
)
COMMENT 'GOLD. Grain: one row per order line, with brand, product, market, channel and campaign context. SYNTHETIC TRAINING DATA.'
AS SELECT
  o.order_line_id, o.order_date, year(o.order_date),
  concat(year(o.order_date), '-Q', quarter(o.order_date)), date_format(o.order_date, 'yyyy-MM'),
  p.brand_id, b.brand_name, o.product_id, p.product_name, p.category,
  o.market_id, m.market_name, m.region, o.channel_id, ch.channel_name,
  o.campaign_id, c.campaign_name,
  o.quantity, o.gross_sales_usd, o.discount_usd, o.returns_usd, o.net_sales_usd
FROM {fq('silver_fact_orders')} o
JOIN {fq('silver_dim_product')} p  ON o.product_id = p.product_id
JOIN {fq('silver_dim_brand')}   b  ON p.brand_id   = b.brand_id
JOIN {fq('silver_dim_market')}  m  ON o.market_id  = m.market_id
JOIN {fq('silver_dim_channel')} ch ON o.channel_id = ch.channel_id
LEFT JOIN {fq('silver_dim_campaign')} c ON o.campaign_id = c.campaign_id
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {fq('gold_campaign_daily')} (
  event_date     COMMENT 'Campaign reporting date. Use this date (not order_date) for campaign metrics.',
  campaign_id, campaign_name, brand_name,
  objective      COMMENT 'Campaign goal: awareness or conversion.',
  product_id, product_name, category, market_name, region, channel_name,
  impressions, clicks,
  spend_usd      COMMENT 'Media spend, USD.',
  attributed_orders,
  attributed_revenue_usd COMMENT 'Revenue CREDITED to the campaign by the attribution model. NOT net sales. Never add it to or compare it directly with net_sales_usd.'
)
COMMENT 'GOLD. Grain: campaign x product x market x channel x day. SYNTHETIC TRAINING DATA.'
AS SELECT f.event_date, f.campaign_id, c.campaign_name, b.brand_name, c.objective,
  f.product_id, p.product_name, p.category, m.market_name, m.region, ch.channel_name,
  f.impressions, f.clicks, f.spend_usd, f.attributed_orders, f.attributed_revenue_usd
FROM {fq('silver_fact_campaign_daily')} f
JOIN {fq('silver_dim_campaign')} c  ON f.campaign_id = c.campaign_id
JOIN {fq('silver_dim_brand')}    b  ON c.brand_id    = b.brand_id
JOIN {fq('silver_dim_product')}  p  ON f.product_id  = p.product_id
JOIN {fq('silver_dim_market')}   m  ON f.market_id   = m.market_id
JOIN {fq('silver_dim_channel')}  ch ON f.channel_id  = ch.channel_id
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {fq('gold_campaign_summary')} (
  campaign_id, campaign_name, brand_name, objective, audience, start_date, end_date,
  budget_usd     COMMENT 'Planned campaign budget, USD.',
  spend_usd      COMMENT 'Total media spend, USD.',
  attributed_revenue_usd COMMENT 'Total attributed revenue, USD. Not net sales.',
  impressions, clicks, attributed_orders,
  ctr            COMMENT 'Click-through rate = clicks / impressions.',
  cpc_usd        COMMENT 'Cost per click = spend / clicks; NULL when clicks = 0.',
  roas           COMMENT 'Return on ad spend = attributed_revenue_usd / spend_usd; NULL when spend is 0. Below 1.0 means the campaign returned less than it cost.',
  spend_to_budget_ratio COMMENT 'spend_usd / budget_usd. Above 1.0 means overspend.'
)
COMMENT 'GOLD. Grain: one row per campaign. Measures are aggregated BEFORE any join, so nothing fans out. SYNTHETIC TRAINING DATA.'
AS SELECT c.campaign_id, c.campaign_name, b.brand_name, c.objective, c.audience, c.start_date, c.end_date, c.budget_usd,
  sum(f.spend_usd), sum(f.attributed_revenue_usd), sum(f.impressions), sum(f.clicks), sum(f.attributed_orders),
  round(try_divide(sum(f.clicks), sum(f.impressions)), 4),
  CASE WHEN sum(f.clicks)    > 0 THEN round(sum(f.spend_usd) / sum(f.clicks), 4) END,
  CASE WHEN sum(f.spend_usd) > 0 THEN round(sum(f.attributed_revenue_usd) / sum(f.spend_usd), 4) END,
  round(sum(f.spend_usd) / c.budget_usd, 2)
FROM {fq('silver_dim_campaign')} c
JOIN {fq('silver_dim_brand')} b ON c.brand_id = b.brand_id
LEFT JOIN {fq('silver_fact_campaign_daily')} f ON f.campaign_id = c.campaign_id
GROUP BY ALL
""")

gold_counts = {v: spark.table(fq(v)).count() for v in ["gold_sales_lines", "gold_campaign_daily", "gold_campaign_summary"]}
print(gold_counts)
assert gold_counts == {"gold_sales_lines": 25000, "gold_campaign_daily": 5880, "gold_campaign_summary": 12}, \
    "A join dropped or duplicated rows. Inner joins only stay safe while every FK check passes."
print("PASS: gold views have the same grain as their source facts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Acceptance: four business questions with known-correct answers
# MAGIC These are your first **golden test cases**. On Day 5 Genie must reproduce them, and on Day 11 they become an automated evaluation set.
# MAGIC
# MAGIC | # | Business question | Expected answer |
# MAGIC |---|---|---|
# MAGIC | A | Which market had the highest net sales in Q3 2026? *(eval Q005)* | **Atlanta $139,542.19**. Then Boston 136,816.92, Charlotte 136,572.63, Chicago 134,096.27, New York 132,033.87, Los Angeles 131,866.85 |
# MAGIC | B | Which campaigns had the lowest ROAS? *(eval Q004)* | **CAM009 0.5815** (Velvet Grove Radiance 09). Next lowest: CAM004 1.9346, CAM001 1.9998. Highest: CAM003 3.0957 |
# MAGIC | C | Which category led Northeast net sales last quarter (Q3 2026)? | **fragrance $76,770.08**, makeup 71,976.65, skincare 61,597.79, haircare 58,506.27 |
# MAGIC | D | What is spend by channel for CAM009? *(eval Q011)* | ecommerce 64,882.05 · marketplace 64,139.06 · retail 59,882.85 · social 25,236.22 · **total 214,140.18** |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- A: Q3 2026 net sales by market
# MAGIC SELECT market_name, region, round(sum(net_sales_usd), 2) AS net_sales_usd, count(*) AS order_lines
# MAGIC FROM gold_sales_lines
# MAGIC WHERE order_quarter = '2026-Q3'
# MAGIC GROUP BY market_name, region
# MAGIC ORDER BY net_sales_usd DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- B: Campaign ROAS, lowest first
# MAGIC SELECT campaign_id, campaign_name, objective, spend_usd, attributed_revenue_usd, roas, ctr, cpc_usd
# MAGIC FROM gold_campaign_summary
# MAGIC ORDER BY roas ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- C: Northeast, Q3 2026, by category
# MAGIC SELECT category, round(sum(gross_sales_usd),2) AS gross, round(sum(discount_usd),2) AS discount,
# MAGIC        round(sum(returns_usd),2) AS returns, round(sum(net_sales_usd),2) AS net_sales_usd
# MAGIC FROM gold_sales_lines
# MAGIC WHERE region = 'Northeast' AND order_quarter = '2026-Q3'
# MAGIC GROUP BY category
# MAGIC ORDER BY net_sales_usd DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- D: CAM009 spend by channel
# MAGIC SELECT channel_name, round(sum(spend_usd), 2) AS spend_usd
# MAGIC FROM gold_campaign_daily
# MAGIC WHERE campaign_id = 'CAM009'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY spend_usd DESC

# COMMAND ----------

# ASSERT: automated version of the four checks
def close(a, e, tol=0.005):
    return a is not None and abs(float(a) - e) < tol

A = {r.market_name: r.v for r in spark.sql(
    "SELECT market_name, sum(net_sales_usd) v FROM gold_sales_lines WHERE order_quarter='2026-Q3' GROUP BY 1").collect()}
B = {r.campaign_id: r.roas for r in spark.table(fq("gold_campaign_summary")).collect()}
C = {r.category: r.v for r in spark.sql(
    "SELECT category, sum(net_sales_usd) v FROM gold_sales_lines WHERE region='Northeast' AND order_quarter='2026-Q3' GROUP BY 1").collect()}
D = {r.channel_name: r.v for r in spark.sql(
    "SELECT channel_name, sum(spend_usd) v FROM gold_campaign_daily WHERE campaign_id='CAM009' GROUP BY 1").collect()}

EXPECTED = {
  "A": {"Atlanta": 139542.19, "Boston": 136816.92, "Charlotte": 136572.63, "Chicago": 134096.27, "New York": 132033.87, "Los Angeles": 131866.85},
  "B": {"CAM009": 0.5815, "CAM004": 1.9346, "CAM001": 1.9998, "CAM007": 2.0084, "CAM010": 2.0521, "CAM005": 2.2585,
        "CAM008": 2.2665, "CAM011": 2.3728, "CAM002": 2.3963, "CAM012": 2.7527, "CAM006": 2.7903, "CAM003": 3.0957},
  "C": {"fragrance": 76770.08, "makeup": 71976.65, "skincare": 61597.79, "haircare": 58506.27},
  "D": {"ecommerce": 64882.05, "marketplace": 64139.06, "retail": 59882.85, "social": 25236.22},
}
acceptance = []
for q, actual in zip("ABCD", [A, B, C, D]):
    for k, e in EXPECTED[q].items():
        acceptance.append((q, k, float(actual[k]) if actual.get(k) is not None else None, e, close(actual.get(k), e)))
display(spark.createDataFrame(acceptance, "question STRING, item STRING, actual DOUBLE, expected DOUBLE, pass BOOLEAN"))
assert all(r[4] for r in acceptance), "At least one acceptance value differs. Investigate before continuing."
print(f"PASS: {len(acceptance)} / {len(acceptance)} acceptance values match")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎙️ Drill 1: explain the data to the business (2 minutes, out loud, before reading the hints)
# MAGIC **Kamal (client):** *"Skip the medallion talk. I'm the marketing director. Why should I believe that Atlanta was our best market last quarter?"*
# MAGIC
# MAGIC *In the real client round Kamal interrupted twice: "Let's start slow… explain me your business case again. Don't go to technology." Open with the user and the decision, never the stack.*
# MAGIC
# MAGIC Points a strong answer covers (in your own words, not memorised):
# MAGIC * Business first: what the number is (net sales after discounts and returns) and the period (July to September 2026, by order date).
# MAGIC * Completeness: 25,000 source rows in, 25,000 validated, 0 rejected, and the reconciliation is logged.
# MAGIC * Honesty about the margin: Atlanta leads Boston by about $2.7K out of about $137K (2%). That is a narrow lead, so don't build a strategy on the ranking alone.
# MAGIC
# MAGIC **Senior architect:** *"25,000 rows and you built three layers. Isn't this over-engineering for a POC?"*
# MAGIC Think about: replayability (Bronze lets you rebuild without re-requesting the source), auditability (quarantine), and what changes at 50M rows. Then name the trade-off you're accepting, such as more objects to maintain.
# MAGIC
# MAGIC ## 7. Experiment 1: Spark SQL vs the DataFrame API (5 minutes)
# MAGIC **Question:** are `spark.sql(...)` and the DataFrame API different engines? **No.** Both build the same logical plan, and the Catalyst optimizer turns it into the same physical plan. Choose by readability and testability: SQL for analysts and Genie, the DataFrame API for reusable Python functions with unit tests (Day 8).
# MAGIC
# MAGIC **Expected:** 16 rows (4 regions × 4 categories), `difference_rows = 0`, and very similar `== Physical Plan ==` output for both.

# COMMAND ----------

sql_df = spark.sql("""
  SELECT region, category, sum(net_sales_usd) AS net_sales_usd
  FROM gold_sales_lines WHERE order_quarter = '2026-Q3' GROUP BY region, category""")

o, p, m = (spark.table(fq(t)) for t in ["silver_fact_orders", "silver_dim_product", "silver_dim_market"])
api_df = (o.filter(F.col("order_date").between(F.to_date(F.lit("2026-07-01")), F.to_date(F.lit("2026-09-30"))))
           .join(p.select("product_id", "category"), "product_id")
           .join(m.select("market_id", "region"), "market_id")
           .groupBy("region", "category").agg(F.sum("net_sales_usd").alias("net_sales_usd")))

diff = sql_df.exceptAll(api_df).count() + api_df.exceptAll(sql_df).count()
print("rows:", sql_df.count(), "| difference_rows:", diff)
assert diff == 0
sql_df.explain()
api_df.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Experiment 2: inject six defects and prove the pipeline catches each one
# MAGIC We push **25,000 clean rows + 6 bad rows** through the *same* `build_orders()` and `write_silver()` code, into scratch tables (`lab_defect_*`), so the real Silver table isn't touched.
# MAGIC
# MAGIC | Injected row | Defect | Rule that should catch it |
# MAGIC |---|---|---|
# MAGIC | ORDX000001 | product `P999` doesn't exist | `fk_product` |
# MAGIC | ORDX000002 | net = 45.00 but 56.00 − 5.60 − 0 = 50.40 | `net_sales_formula` |
# MAGIC | ORDX000003 | quantity = −2 | `quantity_positive` |
# MAGIC | ORD0000002 | **conflicting duplicate** of a real order line (qty 3 vs 1) | `pk_unique` (quarantines **both** copies) |
# MAGIC | ORDX000005 | date `2026-13-45` | `order_date_valid` |
# MAGIC | ORDX000006 | Velvet Grove product (P017) credited to an Aurora Botanics campaign (CAM001) | `campaign_brand_match` |
# MAGIC
# MAGIC **Expected:** input 25,006 · silver 24,999 · quarantined 7 · reconciled true. Each rule appears once, except `pk_unique`, which appears twice.

# COMMAND ----------

cols = ["order_line_id", "order_date", "product_id", "market_id", "channel_id", "campaign_id",
        "quantity", "gross_sales_usd", "discount_usd", "returns_usd", "net_sales_usd"]
defects = [
    ("ORDX000001", "2026-05-10", "P999", "M01", "C01", None,     "1",  "28.00",  "0.00", "0.00", "28.00"),
    ("ORDX000002", "2026-05-10", "P001", "M01", "C01", None,     "2",  "56.00",  "5.60", "0.00", "45.00"),
    ("ORDX000003", "2026-05-10", "P001", "M01", "C01", None,     "-2", "56.00",  "0.00", "0.00", "56.00"),
    ("ORD0000002", "2026-04-03", "P021", "M05", "C01", "CAM003", "3",  "246.00", "0.00", "0.00", "246.00"),
    ("ORDX000005", "2026-13-45", "P001", "M01", "C01", None,     "1",  "28.00",  "0.00", "0.00", "28.00"),
    ("ORDX000006", "2026-02-01", "P017", "M01", "C01", "CAM001", "1",  "50.00",  "0.00", "0.00", "50.00"),
]
defect_df = (spark.createDataFrame(defects, ", ".join(f"{c} STRING" for c in cols))
             .withColumn("_source_file", F.lit("injected://day2_defect_test"))
             .withColumn("_ingested_at", F.current_timestamp())
             .withColumn("_batch_id", F.lit(BATCH_ID)))
dirty = b("fact_orders").drop("_source_modified_at").unionByName(defect_df)

spark.sql(f"""CREATE OR REPLACE TABLE {fq('lab_defect_quarantine')} (
  source_table STRING, failed_rules ARRAY<STRING>, raw_record STRING, batch_id STRING, quarantined_at TIMESTAMP)""")
defect_recon = write_silver("fact_orders", build_orders(dirty), ORDER_RULES, ["order_line_id"],
                            target="lab_defect_silver_fact_orders", quarantine="lab_defect_quarantine")
print(defect_recon)
display(spark.sql(f"""SELECT get_json_object(raw_record, '$.order_line_id') AS order_line_id, failed_rules,
                             get_json_object(raw_record, '$.order_date') AS raw_order_date, raw_record
                      FROM {fq('lab_defect_quarantine')} ORDER BY order_line_id"""))

rule_counts = {r.rule: r.n for r in spark.sql(f"""
    SELECT rule, count(*) n FROM {fq('lab_defect_quarantine')} LATERAL VIEW explode(failed_rules) t AS rule GROUP BY rule""").collect()}
print(rule_counts)
assert defect_recon == {"table": "fact_orders", "input": 25006, "silver": 24999, "quarantined": 7, "reconciled": True}
assert rule_counts == {"fk_product": 1, "net_sales_formula": 1, "quantity_positive": 1, "pk_unique": 2,
                       "order_date_valid": 1, "campaign_brand_match": 1}, "A defect was missed or double-flagged"
print("PASS: all 6 defects caught by exactly the intended rule")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8b. A bad write that skips the pipeline is still rejected by the constraint
# MAGIC **Expected:** the insert fails with a `DELTA_VIOLATE_CONSTRAINT_WITH_VALUES`-style error that names `net_sales_formula`, and the row count stays 25,000.

# COMMAND ----------

try:
    spark.sql(f"""INSERT INTO {fq('silver_fact_orders')}
        (order_line_id, order_date, product_id, market_id, channel_id, campaign_id,
         quantity, gross_sales_usd, discount_usd, returns_usd, net_sales_usd)
        VALUES ('ORDX999999', DATE'2026-05-01', 'P001', 'M01', 'C01', NULL, 1, 28.00, 0.00, 0.00, 99.00)""")
    print("UNEXPECTED: the insert succeeded. The constraint is not active.")
except Exception as e:
    msg = str(e)
    print("Rejected:\n", msg[:500])
    assert "net_sales_formula" in msg or "CONSTRAINT" in msg.upper(), "Rejected, but NOT by the CHECK constraint. Read the error above."
    print("PASS: rejected by the CHECK constraint")
n = spark.table(fq("silver_fact_orders")).count()
assert n == 25000, f"silver_fact_orders now has {n} rows"
print("PASS: row count unchanged:", n)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎙️ Drill 2
# MAGIC **Business owner:** *"Your pipeline rejected 7 orders. Does that mean the numbers on my dashboard are wrong?"*
# MAGIC Cover: what was excluded and why (by rule), the dollar impact, who fixes the source, and that excluded rows are kept and visible rather than deleted.
# MAGIC
# MAGIC **Senior architect (challenge questions, pick two):**
# MAGIC 1. *Why quarantine **both** duplicate copies instead of keeping the newest?*
# MAGIC 2. *You have quarantine rules **and** CHECK constraints. Isn't that redundant?* (Hint: who each one protects against, and whether it fails the row or the whole write.)
# MAGIC 3. *At what quarantine rate would you stop the load instead of continuing?* Give a number and say who owns that threshold.
# MAGIC
# MAGIC ## 9. Experiment 3: the fan-out trap (the most important join lesson for Genie)
# MAGIC `fact_orders` (grain: order line) and `fact_campaign_daily` (grain: campaign × product × market × channel × day) both have `campaign_id`. Joining them directly multiplies every order line by **490** campaign-day rows.
# MAGIC
# MAGIC **Expected:**
# MAGIC * WRONG query: CAM009 "net sales" = **$48,337,167.20** from 368,480 joined rows. The entire company only sold $2,734,940.30 all year.
# MAGIC * RIGHT query (aggregate each fact to campaign grain, *then* join): net sales **$98,647.28** (752 order lines), spend **$214,140.18**, attributed revenue **$124,513.21**.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- WRONG: fact-to-fact join at different grains
# MAGIC SELECT o.campaign_id, round(sum(o.net_sales_usd), 2) AS net_sales_WRONG, count(*) AS joined_rows
# MAGIC FROM silver_fact_orders o
# MAGIC JOIN silver_fact_campaign_daily f ON o.campaign_id = f.campaign_id
# MAGIC WHERE o.campaign_id = 'CAM009'
# MAGIC GROUP BY o.campaign_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- RIGHT: aggregate to a shared grain first
# MAGIC WITH sales AS (
# MAGIC   SELECT campaign_id, sum(net_sales_usd) AS net_sales_usd, count(*) AS order_lines
# MAGIC   FROM silver_fact_orders WHERE campaign_id = 'CAM009' GROUP BY campaign_id),
# MAGIC perf AS (
# MAGIC   SELECT campaign_id, sum(spend_usd) AS spend_usd, sum(attributed_revenue_usd) AS attributed_revenue_usd
# MAGIC   FROM silver_fact_campaign_daily WHERE campaign_id = 'CAM009' GROUP BY campaign_id)
# MAGIC SELECT * FROM sales JOIN perf USING (campaign_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎙️ Drill 3
# MAGIC **Stakeholder:** *"Last week the campaign dashboard said CAM009 drove $48 million. Today you say $98K. Which is it, and why should I trust you now?"*
# MAGIC Cover: admit the error plainly, explain it without jargon (one sales row was counted 490 times), say what is now true, and describe the permanent guard (one-grain gold views plus a regression test).
# MAGIC
# MAGIC **Senior architect:** *"Genie writes its own SQL. How do you stop **Genie** making this exact mistake?"*
# MAGIC Think about: exposing only campaign-grain and line-grain gold views (not raw facts side by side), verified example SQL, column comments, join definitions in the Genie space, and test questions with known answers. This sets up Day 5.
# MAGIC
# MAGIC ## 10. Structural checks passed. Is the data *believable*?
# MAGIC Every FK, key and arithmetic rule passed. Plausibility checks ask a different question: **would a domain expert accept these numbers?** Run them and look closely.
# MAGIC
# MAGIC **Expected (these are real quirks of the synthetic generator, left in on purpose and not fixed):**
# MAGIC * All 12 campaigns spent **7.1× to 12.2×** their budget (about $210K of spend against $17K–$30K budgets).
# MAGIC * Total attributed revenue **$5,650,832.95** is **2.07×** total company net sales ($2,734,940.30). Attribution claims more revenue than the company earned.
# MAGIC * 134 of 396 reviews rated 1–2 stars (34%) carry `sentiment_hint = 'positive'`.

# COMMAND ----------

plaus = spark.sql(f"""
SELECT 'spend_vs_budget' AS check_name,
       concat(CAST(count_if(spend_to_budget_ratio > 1) AS STRING), ' of ', CAST(count(*) AS STRING), ' campaigns overspent; ratio ',
              CAST(min(spend_to_budget_ratio) AS STRING), 'x to ', CAST(max(spend_to_budget_ratio) AS STRING), 'x') AS finding,
       CASE WHEN count_if(spend_to_budget_ratio > 1.2) > 0 THEN 'WARN' ELSE 'PASS' END AS status
FROM {fq('gold_campaign_summary')}
UNION ALL
SELECT 'attributed_vs_net_sales',
       concat('attributed ', CAST((SELECT round(sum(attributed_revenue_usd),2) FROM {fq('silver_fact_campaign_daily')}) AS STRING),
              ' vs company net ', CAST((SELECT round(sum(net_sales_usd),2) FROM {fq('silver_fact_orders')}) AS STRING), ' = ',
              CAST(round((SELECT sum(attributed_revenue_usd) FROM {fq('silver_fact_campaign_daily')}) /
                    (SELECT sum(net_sales_usd) FROM {fq('silver_fact_orders')}), 2) AS STRING), 'x'),
       CASE WHEN (SELECT sum(attributed_revenue_usd) FROM {fq('silver_fact_campaign_daily')}) >
                 (SELECT sum(net_sales_usd) FROM {fq('silver_fact_orders')}) THEN 'WARN' ELSE 'PASS' END
UNION ALL
SELECT 'rating_vs_sentiment',
       concat(CAST(count_if(rating <= 2 AND sentiment_hint = 'positive') AS STRING), ' of ', CAST(count_if(rating <= 2) AS STRING),
              ' low-rated reviews tagged positive'),
       CASE WHEN count_if(rating <= 2 AND sentiment_hint = 'positive') > 0.1 * count_if(rating <= 2) THEN 'WARN' ELSE 'PASS' END
FROM {fq('silver_fact_reviews')}
""")
display(plaus)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎙️ Drill 4: the most realistic conversation of the day
# MAGIC **Kamal:** *"All your checks passed. So can I hand this to Genie and let the brand teams ask questions?"*
# MAGIC A strong answer separates **structurally valid** (keys, types, arithmetic) from **business-plausible**. It names the overspend and attribution findings, says which questions are safe today (net sales by market or category) and which are not (ROAS, until someone confirms whether `budget_usd` is a monthly or total budget and how attribution is credited), and proposes who confirms and by when.
# MAGIC
# MAGIC **Senior architect:** *"Which of these warnings would you turn into a blocking rule, and which stay as warnings?"* Defend one of each.
# MAGIC
# MAGIC > For your notes: these quirks come from the data generator. We are **not** regenerating the data. Treat them as known issues in the source system. A real client dataset will have the same kind of problems.
# MAGIC
# MAGIC ## 11. Save the evidence: `dq_results`
# MAGIC One row per check per batch, appended, so you can show the history of data quality over time and not just today's state.

# COMMAND ----------

dq_rows = []
for r in recon:
    dq_rows.append(("silver", r["table"], "reconciliation_bronze_eq_silver_plus_quarantine", "ERROR",
                    float(r["quarantined"]), f"{r['input']} = {r['silver']} + {r['quarantined']}",
                    "PASS" if r["reconciled"] else "FAIL"))
    dq_rows.append(("silver", r["table"], "row_count_matches_source", "ERROR", float(r["silver"]),
                    str(EXPECTED_ROWS[r["table"]]), "PASS" if r["silver"] == EXPECTED_ROWS[r["table"]] else "FAIL"))
for q, item, actual, expected, ok in acceptance:
    dq_rows.append(("gold", f"acceptance_{q}", item, "ERROR", actual, str(expected), "PASS" if ok else "FAIL"))
dq_rows.append(("lab", "fact_orders", "defect_injection_all_caught", "ERROR", 7.0, "7 quarantined", "PASS"))
for r in plaus.collect():
    dq_rows.append(("gold", "plausibility", r.check_name, "WARN", None, r.finding, r.status))

(spark.createDataFrame(dq_rows, "layer STRING, table_name STRING, check_name STRING, severity STRING, "
                                "metric_value DOUBLE, expected STRING, status STRING")
      .withColumn("batch_id", F.lit(BATCH_ID)).withColumn("checked_at", F.current_timestamp())
      .write.mode("append").saveAsTable(fq("dq_results")))

display(spark.sql(f"""SELECT severity, status, count(*) AS checks FROM {fq('dq_results')}
                      WHERE batch_id = '{BATCH_ID}' GROUP BY ALL ORDER BY severity, status"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Expected:** `ERROR / PASS = 45` (18 reconciliation, 26 acceptance, 1 defect test) and `WARN / WARN = 3`. **Zero rows with status FAIL.**
# MAGIC
# MAGIC ## 12. Lineage and the evidence to keep
# MAGIC 1. **Catalog → workspace → ai_architect_lab → gold_sales_lines → Lineage tab → See lineage graph.** You should see `gold_sales_lines` fed by the silver tables, and those fed by bronze. Lineage can take a few minutes to appear. If the tab is empty or unavailable in your workspace, write that down rather than assuming it works.
# MAGIC 2. Open `silver_fact_orders` → **History** tab: each `createOrReplace` is a Delta version, and you can query it with `VERSION AS OF`.
# MAGIC 3. Save to `docs/evidence_log.md`: the bronze counts, recon table, acceptance result (26/26), the defect quarantine output, the constraint rejection message, the fan-out numbers, the plausibility warnings and a lineage screenshot.
# MAGIC
# MAGIC **What lineage proves and what it doesn't:** it proves *which objects fed which*. It does **not** prove the logic is correct. That is the job of the acceptance tests.
# MAGIC
# MAGIC ## ✅ Day 2 exit gate: go to Day 3 only if all of these are true
# MAGIC - [ ] Bronze: 9 tables, counts match
# MAGIC - [ ] Silver: 9 tables reconciled, 0 quarantined, constraints visible
# MAGIC - [ ] Gold: 25,000 / 5,880 / 12 rows
# MAGIC - [ ] Acceptance 26/26
# MAGIC - [ ] Defect test: 7 quarantined, constraint rejected the bad insert
# MAGIC - [ ] dq_results: 0 FAIL
# MAGIC - [ ] At least two drills done out loud, with notes on what you would say differently
# MAGIC
# MAGIC **Next (Day 3):** the 13 PDFs are already in `lab_files/beauty_ai_lab_data/docs/`. Day 3 builds the document manifest, checks every file hash against it, and tests document parsing on the native-text, table and scanned PDFs.
