# beauty-ai-lab

Beauty Campaign Intelligence Assistant (BCIA): a fictional training lab on Databricks.

A read-only assistant that answers claims, campaign and research questions, citing the
document version in effect and respecting access groups.

## Layout

| Folder | Contents | Runs on |
|---|---|---|
| `notebooks/` | Ingestion (Day 2 tables, Day 3 documents), experiments, eval runs | Databricks |
| `src/bcia/` | Tested Python logic: router, retrieval filters, claim guards, config | Imported by notebooks and the agent |
| `tests/` | pytest unit tests for `src/` (no Databricks needed) | Laptop, CI, or `notebooks/00_run_unit_tests.py` |
| `sql/` | Gold view definitions and Genie verified queries | Databricks SQL |
| `prompts/` | Versioned prompts | Loaded by the agent |
| `evals/` | Acceptance scenarios and evaluation sets | Databricks |
| `app/` | UI (Streamlit or FastAPI) | Databricks Apps, later Docker |
| `resources/`, `databricks.yml` | Bundle config for jobs, endpoints, app | `databricks bundle deploy` |
| `.github/workflows/` | CI: unit tests on every push and pull request | GitHub Actions |
| `docs/` | Problem frame, decision log, evidence log, risk register, failure log | - |

## Rules

1. Data never goes in Git. Upload `BCIA_FULL.zip` to the volume `workspace.ai_architect_lab.lab_files`.
2. Logic that must be correct lives in `src/` with a test. Notebooks call it.
3. Every decision gets a line in `docs/decision_log.md`; every check gets a line in `docs/evidence_log.md`.

## Run the tests

- Laptop: `pip install pytest && pytest`
- Databricks: open `notebooks/00_run_unit_tests.py` and run all.

## Day 3A → Day 3B: native-first document triage (added November 17, 2026)

Start with [`docs/native_first_lab.md`](docs/native_first_lab.md).

1. **Databricks first:** Run [`notebooks/03a_native_document_triage.py`](notebooks/03a_native_document_triage.py) on **one source PDF in your existing Unity Catalog Volume**. Reconcile manifest+SHA256; inspect and optionally run native `ai_parse_document`; create per-page review statuses in Delta. The managed parse must actually succeed before claiming results.
2. **VS Code second:** Install `pip install -e ".[dev,pdf]"`, generate the three page fixtures with [`scripts/build_page_variants.py`](scripts/build_page_variants.py), classify them with [`scripts/run_page_triage.py`](scripts/run_page_triage.py), then run `pytest -q tests/test_parsing.py`. The classifier is in [`src/bcia/parsing.py`](src/bcia/parsing.py).
3. **Expand:** Add independent re-OCR, chart/figure validation, structure-aware chunks, ACL-aware retrieval, evaluation and MLflow tracing only after you have measurable evidence from the earlier phases.

**Data is NOT contained in this source ZIP.** The Day 3 notebook requires `BCIA_FULL.zip` already unpacked in `/Volumes/workspace/ai_architect_lab/lab_files/beauty_ai_lab_data/`. Test fixtures generated locally are excluded from version control.

