# Databricks notebook source
# MAGIC %md
# MAGIC # Day 5: Structured data for Genie: trends, survey, metric tools and a measured Genie space
# MAGIC **Synthetic training data only. Not Estée Lauder or C5i material.**
# MAGIC
# MAGIC ### Business requirement
# MAGIC Analysts ask numbers questions in plain English: *"Which trend is really emerging?"*, *"What was CAM009's ROAS?"*, *"How many people answered the H2 survey?"*. The answer must equal what a governed SQL query returns, every time.
# MAGIC
# MAGIC ### Why this is a data problem before it's an AI problem
# MAGIC Genie writes SQL from your tables, column comments, instructions and example queries. Three traps in this data would fool it, and a human analyst too:
# MAGIC * a **bot spike** (75,000 mentions, 3 engagements) that makes a fake trend the "#1 emerging trend";
# MAGIC * **5-week months** that make monthly totals look like growth;
# MAGIC * a survey where **sample_n repeats** on every category row (sum = 3,600; truth = 900).
# MAGIC
# MAGIC ### Architecture
# MAGIC ```
# MAGIC  trend CSV ─▶ bronze ─▶ silver (typed, rules, anomaly FLAGGED not deleted) ─▶ gold_trend_weekly / gold_trend_summary
# MAGIC  survey XLSX (3 sheets) ─▶ bronze ─▶ silver ─▶ gold_survey_results (market x category) + gold_survey_market (market)
# MAGIC  Day 2 gold views + informational PK/FK + column comments
# MAGIC        ▼
# MAGIC  UC SQL functions (deterministic tools): campaign_performance(), trend_status()
# MAGIC  Genie space: configured 3 ways (L0 raw → L1 curated → L2 curated + instructions + examples), each MEASURED on 15 questions
# MAGIC ```
# MAGIC **Prerequisites:** Day 2 and Day 4 §1 (extension unpacked); the `beauty-ai-lab` Git folder.

# COMMAND ----------

# MAGIC %pip install -q -U openpyxl databricks-sdk

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys, time
import pandas as pd
from pyspark.sql import Row, functions as F

sys.path.insert(0, os.path.abspath("../src"))
from bcia.config import CATALOG, SCHEMA, VOL_ROOT, EXT_ROOT, fq
from bcia.extension import ensure_extension
from bcia.evaluation import number_in_rows, text_in_rows

spark.sql(f"USE CATALOG {CATALOG}"); spark.sql(f"USE SCHEMA {SCHEMA}")
ensure_extension(VOL_ROOT, EXT_ROOT)
assert spark.catalog.tableExists(fq("gold_campaign_summary")), "Run Day 2 first"
TREND_CSV = f"{EXT_ROOT}/raw_extra/fact_trend_signals.csv"
SURVEY_XLSX = f"{EXT_ROOT}/extra_docs/DOC018_v1.xlsx"
print("ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Trend signals: bronze → silver with an anomaly flag
# MAGIC **Design choice:** the bot spike is **valid data that is not demand**. It isn't quarantined (the row is well-formed); it's **flagged** with a reason, so analysts can see it and gold excludes it. Deleting it would hide evidence; keeping it unflagged would mislead.
# MAGIC
# MAGIC **Anomaly rule:** engagement per mention < 0.001 **and** mentions > 10× that term's median week. Normal rows have 2–4 engagements per mention.
# MAGIC **Expected:** bronze 20,352 rows; all rule violations 0; **exactly 1 anomaly**: 2026-09-12, social_b, mirror mist glitch, M02, 75,000 mentions, 3 engagements.

# COMMAND ----------

(spark.read.option("header", True).option("inferSchema", False).csv(TREND_CSV)
      .select("*", F.col("_metadata.file_path").alias("_source_file"))
      .withColumn("_ingested_at", F.current_timestamp())
      .writeTo(fq("bronze_fact_trend_signals")).createOrReplace())

spark.sql(f"""
CREATE OR REPLACE TABLE {fq('silver_fact_trend_signals')} AS
WITH typed AS (
  SELECT try_cast(signal_date AS DATE) signal_date, platform, trend_term, category, linked_ingredient_theme, market_id,
         try_cast(mentions AS BIGINT) mentions, try_cast(engagement AS BIGINT) engagement,
         try_cast(sentiment_score AS DOUBLE) sentiment_score, _source_file, _ingested_at
  FROM {fq('bronze_fact_trend_signals')}),
med AS (SELECT trend_term, percentile(mentions, 0.5) term_median FROM typed GROUP BY trend_term)
SELECT t.*,
       round(t.engagement / greatest(t.mentions, 1), 4) AS engagement_per_mention,
       (t.engagement / greatest(t.mentions, 1) < 0.001 AND t.mentions > 10 * m.term_median) AS is_anomaly,
       CASE WHEN t.engagement / greatest(t.mentions, 1) < 0.001 AND t.mentions > 10 * m.term_median
            THEN 'suspected bot activity: near-zero engagement on a volume spike' END AS anomaly_reason
FROM typed t JOIN med m USING (trend_term)""")

checks = spark.sql(f"""
SELECT (SELECT count(*) FROM {fq('bronze_fact_trend_signals')})                                        AS bronze_rows,
       count(*)                                                                                       AS silver_rows,
       count_if(t.signal_date IS NULL OR t.mentions IS NULL OR t.engagement IS NULL)                  AS bad_types,
       count(*) - count(DISTINCT t.signal_date, t.platform, t.trend_term, t.market_id)                AS duplicate_grain,
       count_if(t.platform NOT IN ('social_a','social_b','search','retail_reviews'))                  AS bad_platform,
       count_if(m.market_id IS NULL)                                                                  AS orphan_market,
       count_if(t.sentiment_score NOT BETWEEN -1 AND 1 OR t.mentions < 0 OR t.engagement < 0)         AS out_of_range,
       count_if(t.is_anomaly)                                                                         AS anomalies
FROM {fq('silver_fact_trend_signals')} t
LEFT JOIN {fq('silver_dim_market')} m ON t.market_id = m.market_id""").first().asDict()
print(checks)
assert checks == {"bronze_rows": 20352, "silver_rows": 20352, "bad_types": 0, "duplicate_grain": 0, "bad_platform": 0,
                  "orphan_market": 0, "out_of_range": 0, "anomalies": 1}
display(spark.sql(f"SELECT * FROM {fq('silver_fact_trend_signals')} WHERE is_anomaly"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Gold trend views: weekly, anomaly-free, classified
# MAGIC * **Weekly averages, not monthly totals:** months have 4 or 5 Saturdays.
# MAGIC * **emerging** = Sep–Oct 2026 weekly average ≥ 2× the Feb–Jul baseline · **seasonal** = a peak ≥ 3× the median week without that growth · else **stable**.
# MAGIC * **matched_products** = products with the same ingredient theme; empty means "no match", stated plainly.
# MAGIC
# MAGIC **Expected:** barrier comfort ritual **emerging**, growth **4.55** (1,644 → 7,476 per week), matched [P005, P013, P021] · winter glow layering **seasonal**, peak 2025-12-20, peak-to-median **11.8** · the other 14 **stable** · night bloom shimmer: theme `none`, **no products**.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fq('gold_trend_weekly')} (
  trend_term, signal_date COMMENT 'Week ending Saturday.',
  mentions COMMENT 'Mentions across all platforms and markets, EXCLUDING suspected bot activity.',
  engagement, avg_sentiment,
  excluded_anomalous_mentions COMMENT 'Mentions removed because the row was flagged is_anomaly.')
COMMENT 'GOLD. Grain: trend_term x week. Bot-like anomalies excluded. SYNTHETIC.'
AS SELECT trend_term, signal_date,
  sum(CASE WHEN NOT is_anomaly THEN mentions ELSE 0 END), sum(CASE WHEN NOT is_anomaly THEN engagement ELSE 0 END),
  round(avg(CASE WHEN NOT is_anomaly THEN sentiment_score END), 3), sum(CASE WHEN is_anomaly THEN mentions ELSE 0 END)
FROM {fq('silver_fact_trend_signals')} GROUP BY trend_term, signal_date""")

def trend_summary_sql(weekly_source):
    return f"""
WITH w AS ({weekly_source}),
s AS (SELECT trend_term,
        avg(CASE WHEN signal_date BETWEEN DATE'2026-02-01' AND DATE'2026-07-31' THEN mentions END) base_wk,
        avg(CASE WHEN signal_date BETWEEN DATE'2026-09-01' AND DATE'2026-10-31' THEN mentions END) recent_wk,
        max(mentions) peak, max_by(signal_date, mentions) peak_week, percentile(mentions, 0.5) median_wk
      FROM w GROUP BY trend_term),
th AS (SELECT DISTINCT trend_term, category, linked_ingredient_theme FROM {fq('silver_fact_trend_signals')}),
p AS (SELECT ingredient_theme, array_sort(collect_list(product_id)) products FROM {fq('silver_dim_product')} GROUP BY 1)
SELECT trend_term, th.category, th.linked_ingredient_theme,
       round(base_wk, 1) baseline_weekly_mentions, round(recent_wk, 1) recent_weekly_mentions,
       round(recent_wk / base_wk, 2) growth_ratio, peak_week, round(peak / median_wk, 2) peak_to_median,
       CASE WHEN recent_wk / base_wk >= 2 THEN 'emerging' WHEN peak / median_wk >= 3 THEN 'seasonal' ELSE 'stable' END trend_class,
       coalesce(p.products, array()) matched_products
FROM s JOIN th USING (trend_term) LEFT JOIN p ON p.ingredient_theme = th.linked_ingredient_theme"""

spark.sql(f"""
CREATE OR REPLACE VIEW {fq('gold_trend_summary')} (
  trend_term, category, linked_ingredient_theme,
  baseline_weekly_mentions COMMENT 'Average weekly mentions Feb-Jul 2026.',
  recent_weekly_mentions COMMENT 'Average weekly mentions Sep-Oct 2026.',
  growth_ratio COMMENT 'recent / baseline weekly average. 2 or more = emerging.',
  peak_week, peak_to_median COMMENT 'Highest week / median week. 3 or more without growth = seasonal.',
  trend_class COMMENT 'emerging | seasonal | stable. Anomalies excluded.',
  matched_products COMMENT 'Products sharing the ingredient theme. Empty = no product match; do not invent one.')
COMMENT 'GOLD. One row per trend term: classification and product matches. Use this for "is X trending" questions. SYNTHETIC.'
AS {trend_summary_sql(f"SELECT trend_term, signal_date, mentions FROM {fq('gold_trend_weekly')}")}""")

summary = {r.trend_term: r for r in spark.table(fq("gold_trend_summary")).collect()}
display(spark.table(fq("gold_trend_summary")).orderBy(F.desc("growth_ratio")))
bc, wg, nb = summary["barrier comfort ritual"], summary["winter glow layering"], summary["night bloom shimmer"]
assert bc.trend_class == "emerging" and float(bc.growth_ratio) == 4.55 and list(bc.matched_products) == ["P005", "P013", "P021"]
assert wg.trend_class == "seasonal" and str(wg.peak_week) == "2025-12-20" and float(wg.peak_to_median) == 11.8
assert list(nb.matched_products) == [] and sum(r.trend_class == "stable" for r in summary.values()) == 14
print("PASS: 1 emerging, 1 seasonal, 14 stable; product gap stated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Experiment T1: forget to exclude the anomaly
# MAGIC **Predict** which trend tops the "emerging" list if gold uses raw silver.
# MAGIC **Expected:** *mirror mist glitch* becomes **emerging with growth 5.11**, *above* the genuine barrier comfort ritual (4.55). One unflagged row turns a bot event into your #1 recommendation.

# COMMAND ----------

raw_weekly = f"SELECT trend_term, signal_date, sum(mentions) mentions FROM {fq('silver_fact_trend_signals')} GROUP BY ALL"
display(spark.sql(trend_summary_sql(raw_weekly)).where("trend_class = 'emerging'").orderBy(F.desc("growth_ratio")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Experiment T2: monthly totals lie
# MAGIC A completely stable trend, by month. **Expected:** totals jump 25% in 5-week months (Nov, Jan, May, Aug, Oct); the weekly average stays flat (~2,000).

# COMMAND ----------

display(spark.sql(f"""SELECT date_format(signal_date, 'yyyy-MM') month, sum(mentions) monthly_total,
                             count(*) weeks, round(sum(mentions) / count(*)) weekly_average
                      FROM {fq('gold_trend_weekly')} WHERE trend_term = 'daily serum layering' GROUP BY 1 ORDER BY 1"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎙️ Drill 1
# MAGIC **Brand manager:** *"Social says 'mirror mist glitch' exploded in September. Shouldn't we jump on it?"*
# MAGIC Cover what the data shows (one week, one platform, one market, 75,000 mentions and 3 engagements), why that pattern signals bots, what the genuine signal is (barrier comfort ritual, steady since August, matches three existing products), and what you'd need before acting (a second week, a second platform).
# MAGIC
# MAGIC ## 3. Survey workbook → tables (why DOC018 was not chunked on Day 4)
# MAGIC The header is on the 3rd row of each sheet. **Expected:** 24 rows (6 markets × 4 categories); Chicago skincare barrier comfort = **51** (cell E24); total respondents **900**; the naive sum of sample_n = **3,600**.

# COMMAND ----------

def read_sheet(sheet):
    df = pd.read_excel(SURVEY_XLSX, sheet_name=sheet, header=2).dropna(how="all")
    return df.astype(str).assign(_source_file=SURVEY_XLSX, _sheet=sheet)

for sheet, name in [("By Market and Category", "survey_market_category"), ("Respondents Summary", "survey_summary"),
                    ("Definitions and Notes", "survey_definitions")]:
    spark.createDataFrame(read_sheet(sheet)).withColumn("_ingested_at", F.current_timestamp()) \
         .writeTo(fq(f"bronze_{name}")).createOrReplace()

spark.sql(f"""CREATE OR REPLACE TABLE {fq('silver_survey_results')} AS
  SELECT market_id, market_name, category, try_cast(sample_n AS INT) sample_n,
         try_cast(barrier_comfort_pct AS INT) barrier_comfort_pct, try_cast(light_texture_pct AS INT) light_texture_pct, wave
  FROM {fq('bronze_survey_market_category')}""")

spark.sql(f"""CREATE OR REPLACE VIEW {fq('gold_survey_results')} (
  market_id, market_name, category,
  sample_n COMMENT 'Respondents in this MARKET. Repeats on every category row: NEVER sum across rows. Use gold_survey_market for totals.',
  barrier_comfort_pct COMMENT 'Percent choosing barrier comfort for this market and category. Directional research only.',
  light_texture_pct, wave COMMENT 'H2 2026 = July-October mobile-first panel. Not comparable with the spring study (DOC016).')
  COMMENT 'GOLD. H2 2026 survey, grain market x category. Survey results never approve a product claim. SYNTHETIC.'
  AS SELECT * FROM {fq('silver_survey_results')}""")
spark.sql(f"""CREATE OR REPLACE VIEW {fq('gold_survey_market')} (market_id, market_name,
  sample_n COMMENT 'Respondents per market. Sum this for total respondents.')
  COMMENT 'GOLD. One row per market for the H2 2026 survey. SYNTHETIC.'
  AS SELECT market_id, max(market_name), max(sample_n) FROM {fq('silver_survey_results')} GROUP BY market_id""")

s = spark.sql(f"""SELECT (SELECT count(*) FROM {fq('gold_survey_results')}) rows_,
  (SELECT barrier_comfort_pct FROM {fq('gold_survey_results')} WHERE market_id='M06' AND category='skincare') chicago_skincare,
  (SELECT sum(sample_n) FROM {fq('gold_survey_market')}) respondents,
  (SELECT sum(sample_n) FROM {fq('gold_survey_results')}) naive_sum""").first()
print(s.asDict())
assert (s.rows_, s.chicago_skincare, s.respondents, s.naive_sum) == (24, 51, 900, 3600)
print("PASS: survey tables; the naive sum is 4x the truth")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Make the tables explain themselves (informational keys)
# MAGIC Primary and foreign keys in Unity Catalog are **not enforced** (Day 2, Case 2), but Genie and people read them to understand how tables join. Comments already exist on every gold view.
# MAGIC Re-running Day 2 recreates the silver tables and removes these, so this cell is idempotent and should be re-run after Day 2.

# COMMAND ----------

PKS = {"silver_dim_brand": "brand_id", "silver_dim_product": "product_id", "silver_dim_market": "market_id",
       "silver_dim_channel": "channel_id", "silver_dim_campaign": "campaign_id", "silver_fact_orders": "order_line_id"}
FKS = [("silver_fact_orders", "product_id", "silver_dim_product"), ("silver_fact_orders", "market_id", "silver_dim_market"),
       ("silver_fact_orders", "channel_id", "silver_dim_channel"), ("silver_fact_orders", "campaign_id", "silver_dim_campaign"),
       ("silver_dim_product", "brand_id", "silver_dim_brand"), ("silver_dim_campaign", "brand_id", "silver_dim_brand"),
       ("silver_fact_campaign_daily", "campaign_id", "silver_dim_campaign")]
results = []
for t, c, ref in FKS:
    spark.sql(f"ALTER TABLE {fq(t)} DROP CONSTRAINT IF EXISTS fk_{t}_{c}")
for t, c in PKS.items():
    try:
        spark.sql(f"ALTER TABLE {fq(t)} DROP CONSTRAINT IF EXISTS pk_{t} CASCADE")
        spark.sql(f"ALTER TABLE {fq(t)} ALTER COLUMN {c} SET NOT NULL")
        spark.sql(f"ALTER TABLE {fq(t)} ADD CONSTRAINT pk_{t} PRIMARY KEY ({c})")
        results.append((t, f"PK({c})", "ok"))
    except Exception as e:
        results.append((t, f"PK({c})", str(e)[:120]))
for t, c, ref in FKS:
    try:
        spark.sql(f"ALTER TABLE {fq(t)} ADD CONSTRAINT fk_{t}_{c} FOREIGN KEY ({c}) REFERENCES {fq(ref)}")
        results.append((t, f"FK({c}) -> {ref}", "ok"))
    except Exception as e:
        results.append((t, f"FK({c}) -> {ref}", str(e)[:120]))
display(spark.createDataFrame(results, "table_name STRING, constraint_ STRING, result STRING"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Deterministic metric tools (Unity Catalog SQL functions)
# MAGIC For the handful of metrics that **must** be exact (campaign ROAS, trend status), the agent will call a governed function instead of asking an LLM to write SQL. Genie handles exploration; functions handle the critical, repeated questions. These are also the fallback if Genie gets a top metric wrong.
# MAGIC **Expected:** `campaign_performance('CAM009')` → ROAS **0.5815**, spend **214,140.18**; `trend_status('barrier comfort ritual')` → emerging.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {fq('campaign_performance')}(p_campaign_id STRING)
RETURNS TABLE (campaign_id STRING, campaign_name STRING, objective STRING, spend_usd DECIMAL(38,2),
               attributed_revenue_usd DECIMAL(38,2), roas DECIMAL(38,4), ctr DOUBLE, spend_to_budget_ratio DECIMAL(38,2))
COMMENT 'Governed KPIs for one campaign. ROAS = attributed revenue / spend. Attributed revenue is not net sales.'
RETURN SELECT campaign_id, campaign_name, objective, spend_usd, attributed_revenue_usd, roas, ctr, spend_to_budget_ratio
       FROM {fq('gold_campaign_summary')} WHERE campaign_id = p_campaign_id""")
spark.sql(f"""
CREATE OR REPLACE FUNCTION {fq('trend_status')}(p_term STRING)
RETURNS TABLE (trend_term STRING, trend_class STRING, growth_ratio DOUBLE, peak_week DATE, matched_products ARRAY<STRING>)
COMMENT 'Classification of one trend term, anomalies excluded. Empty matched_products = no product match.'
RETURN SELECT trend_term, trend_class, CAST(growth_ratio AS DOUBLE), peak_week, matched_products
       FROM {fq('gold_trend_summary')} WHERE lower(trend_term) = lower(p_term)""")

cp = spark.sql(f"SELECT * FROM {fq('campaign_performance')}('CAM009')").first()
ts = spark.sql(f"SELECT * FROM {fq('trend_status')}('Barrier Comfort Ritual')").first()
print(cp.asDict()); print(ts.asDict())
assert float(cp.roas) == 0.5815 and float(cp.spend_usd) == 214140.18 and ts.trend_class == "emerging"
print("PASS: tools return governed values")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Build the Genie space three ways (UI, about 15 minutes)
# MAGIC Sidebar **Genie Agents → New** → choose the **Serverless Starter Warehouse**. Build it as **L0**, run §7, then edit it into **L1**, run §7, then **L2**, run §7. Same space, three measured configurations.
# MAGIC
# MAGIC | Config | Tables | Instructions | Example SQL |
# MAGIC |---|---|---|---|
# MAGIC | **L0 raw** | `silver_fact_orders`, `silver_fact_campaign_daily`, `silver_dim_*`, `silver_fact_trend_signals`, `silver_survey_results` | none | none |
# MAGIC | **L1 curated** | remove silver; add `gold_sales_lines`, `gold_campaign_summary`, `gold_campaign_daily`, `gold_trend_summary`, `gold_trend_weekly`, `gold_survey_results`, `gold_survey_market` | none | none |
# MAGIC | **L2 curated + guidance** | same as L1 | paste `sql/genie_instructions.md` | add each query in `sql/genie_example_queries.sql` (title = its comment) |
# MAGIC
# MAGIC Copy the **space ID** from the browser address (the long ID after `/genie/rooms/` or similar) into the widget below, and set the config label. Exact UI labels may differ slightly; the three configurations are what matter.

# COMMAND ----------

dbutils.widgets.text("GENIE_SPACE_ID", "")
dbutils.widgets.dropdown("GENIE_CONFIG", "L0", ["L0", "L1", "L2"])
SPACE_ID, CONFIG = dbutils.widgets.get("GENIE_SPACE_ID").strip(), dbutils.widgets.get("GENIE_CONFIG")
print("space:", SPACE_ID or "(not set)", "| config:", CONFIG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Measure Genie: 15 questions with governed answers
# MAGIC For each question: ask Genie, take the **SQL it wrote**, run that SQL ourselves, check the result against the governed answer. Checks: `number` (any cell within tolerance), `top` (first row contains the text), `text` (any cell contains it).
# MAGIC Traps included: **G07** fan-out (join orders to daily campaign rows → millions instead of $98,647.28), **G12** sample_n sum (3,600 instead of 900), **G15** relative date ("last quarter" = 2026-Q3 when asked in Nov 2026), **G08** the bot spike.

# COMMAND ----------

GENIE_EVAL = [
  ("G01", "Which market had the highest net sales in Q3 2026?", "top", "Atlanta"),
  ("G02", "What were total net sales in Q3 2026 across all markets?", "number", (810928.73, 0.01)),
  ("G03", "What is the ROAS of campaign CAM009, to four decimal places?", "number", (0.5815, 0.00005)),
  ("G04", "What was the total media spend for campaign CAM009?", "number", (214140.18, 0.01)),
  ("G05", "Which product category had the highest net sales in the Northeast region in Q3 2026?", "top", "fragrance"),
  ("G06", "What is the total attributed revenue for campaign CAM009?", "number", (124513.21, 0.01)),
  ("G07", "What were the total net sales of order lines linked to campaign CAM009?", "number", (98647.28, 0.01)),
  ("G08", "Which consumer trend has been growing steadily since August 2026?", "top", "barrier comfort ritual"),
  ("G09", "Which trend peaked in December 2025?", "top", "winter glow layering"),
  ("G10", "Which products match the ceramides ingredient theme?", "text", "P005"),
  ("G11", "Which trend has no matching ingredient theme?", "text", "night bloom shimmer"),
  ("G12", "How many respondents took part in the H2 2026 survey in total?", "number", (900, 0.5)),
  ("G13", "What was the barrier comfort preference for skincare in Chicago in the H2 survey?", "number", (51, 0.5)),
  ("G14", "How many distinct platforms are in the trend signal data?", "number", (4, 0)),
  ("G15", "What were total net sales last quarter?", "number", (810928.73, 0.01)),
]

def ask_genie(space_id, question, retries=3):
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    for attempt in range(retries):
        try:
            t0 = time.time()
            msg = w.genie.start_conversation_and_wait(space_id=space_id, content=question)
            sql = text = None
            for a in msg.attachments or []:
                if getattr(a, "query", None) and a.query.query:
                    sql = a.query.query
                if getattr(a, "text", None) and a.text.content:
                    text = a.text.content
            return sql, text, round(time.time() - t0, 1), str(msg.status), None
        except Exception as e:
            if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                time.sleep(20 * (attempt + 1)); continue
            return None, None, None, None, str(e)[:300]
    return None, None, None, None, "rate limited"

def check(kind, expected, rows):
    if not rows:
        return False
    if kind == "number":
        value, tol = expected
        return number_in_rows(rows, value, tol)
    if kind == "top":
        return text_in_rows(rows[:1], expected)
    return text_in_rows(rows, expected)

if not SPACE_ID:
    print("Set GENIE_SPACE_ID first (section 6).")
else:
    out = []
    for qid, q, kind, expected in GENIE_EVAL:
        sql, text, secs, status, err = ask_genie(SPACE_ID, q)
        rows, run_err = [], None
        if sql:
            try:
                rows = [list(r) for r in spark.sql(sql.strip().rstrip(";")).limit(50).collect()]
            except Exception as e:
                run_err = str(e)[:200]
        outcome = ("PASS" if check(kind, expected, rows) else "FAIL") if sql else ("NO_SQL: " + (text or err or "")[:120])
        out.append(Row(config=CONFIG, qid=qid, question=q, outcome=outcome, seconds=secs, genie_sql=sql,
                       first_rows=str(rows[:3])[:400], error=err or run_err))
        time.sleep(3)
    res = spark.createDataFrame(out).withColumn("run_at", F.current_timestamp())
    res.write.mode("append").saveAsTable(fq("genie_eval_results"))
    display(res.select("qid", "outcome", "seconds", "first_rows", "genie_sql"))
    print(f"{CONFIG}: {sum(r.outcome == 'PASS' for r in out)} / {len(out)} PASS")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compare the three configurations
# MAGIC Run after L0, L1 and L2 have each been measured. **Predict before you look:** where does L0 fail (trend and survey questions have no curated tables; G07 fan-out; G12 sum)? What do instructions fix that curated tables alone don't?

# COMMAND ----------

if spark.catalog.tableExists(fq("genie_eval_results")):
    display(spark.sql(f"""
      WITH latest AS (SELECT *, row_number() OVER (PARTITION BY config, qid ORDER BY run_at DESC) rn FROM {fq('genie_eval_results')})
      SELECT qid, first(question) question,
             max(CASE WHEN config='L0' THEN outcome END) L0, max(CASE WHEN config='L1' THEN outcome END) L1,
             max(CASE WHEN config='L2' THEN outcome END) L2
      FROM latest WHERE rn = 1 GROUP BY qid ORDER BY qid"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the results like an architect:**
# MAGIC * Every FAIL gets a cause: *missing table*, *wrong join grain*, *wrong metric definition*, *wrong date logic*, or *ambiguity*. Write it in `docs/failure_log.md` with the fix (curated view, comment, instruction, example SQL, or a deterministic function).
# MAGIC * A question that must **never** be wrong (G03 ROAS) shouldn't depend on Genie at all: the agent calls `campaign_performance()`.
# MAGIC * Re-run the same configuration twice. If an answer flips between PASS and FAIL, that's non-determinism. Record it; it changes how much you'd trust Genie unsupervised.
# MAGIC * Note the `seconds` column: that's your first latency number for the numbers path.
# MAGIC
# MAGIC ### 🎙️ Drill 2: Kamal's question
# MAGIC *"Why Genie and not a vector index for the numbers?"*
# MAGIC A strong answer: embeddings find similar *text*; they can't add, filter or join. Numbers must come from governed SQL so they match the dashboards. Genie writes that SQL, and your L0→L2 scores show what makes it reliable (curated views, definitions, examples). The few metrics that must never be wrong go through deterministic functions. Close with the trade-off: Genie is flexible but non-deterministic, so it's measured on a test set and its top metrics have a fallback.
# MAGIC
# MAGIC ### 🎙️ Drill 2b: Kamal's exact question from the client round
# MAGIC **Verbatim:** *"What are the limitations you found on Genie? When did you use it last, right? I know there are changes happening on Genie very often. What are the changes?"*
# MAGIC
# MAGIC **What happened in the real round:** the answer was "about three months ago… haven't touched it", plus a specific instruction-length limit quoted from memory. After Kamal said Genie changes often, both hurt.
# MAGIC **Now answer from this notebook, not from memory:**
# MAGIC * *When:* "This week, in my lab." Name the current product terms: Genie spaces are now called **Genie Agents**, and Genie One and Genie Ontology were added in 2026. Read the latest release notes the week before the interview.
# MAGIC * *Limitations, with your evidence:* the L0→L1→L2 scores; which questions failed and why (join grain, metric definitions, relative dates, repeated sample sizes); whether answers flipped between runs.
# MAGIC * *What you did about it:* curated gold views, column comments, instructions, verified example SQL, and governed SQL functions for metrics that must never be wrong.
# MAGIC * **Never quote a limit or number you haven't checked recently.** Say "I'd verify the current limit".
# MAGIC
# MAGIC ### 🎙️ Drill 3: stakeholder
# MAGIC **Insights lead:** *"Our survey says 3,600 people answered, but the vendor said 900. Which is right?"*
# MAGIC Explain the repeated sample size in plain words, show both numbers, and say how the data is now structured so neither people nor Genie can make that mistake again.
# MAGIC
# MAGIC ## 8. Evidence and exit gate

# COMMAND ----------

dq = [("structured", "silver_fact_trend_signals", "rules_and_single_anomaly", "ERROR", 1.0, "1 anomaly, 0 violations", "PASS"),
      ("structured", "gold_trend_summary", "classification", "ERROR", 4.55, "barrier emerging 4.55; winter seasonal", "PASS"),
      ("structured", "gold_survey_market", "respondents_not_summed_across_categories", "ERROR", 900.0, "900 (naive 3600)", "PASS"),
      ("structured", "campaign_performance", "tool_matches_governed_roas", "ERROR", 0.5815, "0.5815", "PASS")]
(spark.createDataFrame(dq, "layer STRING, table_name STRING, check_name STRING, severity STRING, metric_value DOUBLE, expected STRING, status STRING")
      .withColumn("batch_id", F.lit("day5")).withColumn("checked_at", F.current_timestamp())
      .write.mode("append").saveAsTable(fq("dq_results")))
print("logged")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Day 5 exit gate
# MAGIC - [ ] Trend silver: 20,352 rows, 1 anomaly flagged; gold classification asserts pass
# MAGIC - [ ] T1 (bot spike becomes "#1 trend") and T2 (monthly totals) observed
# MAGIC - [ ] Survey: 900 vs naive 3,600 shown
# MAGIC - [ ] Keys added (or the error recorded); both functions return governed values
# MAGIC - [ ] Genie measured at L0, L1, L2; every FAIL has a cause and a fix in `docs/failure_log.md`
# MAGIC - [ ] Commit: `git pull`, update logs, push
# MAGIC
# MAGIC **Next (Day 6):** the agent. A router sends numbers to `campaign_performance()`/Genie and wording questions to Day 4's search; the composer cites sources; `check_claims` blocks unapproved wording; MLflow traces every step; the 58 questions become the regression gate.
