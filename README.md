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
