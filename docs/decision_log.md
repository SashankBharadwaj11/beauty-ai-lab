# Decision log

| ID | Date | Decision | Options considered | Why | Revisit if | Evidence |
|---|---|---|---|---|---|---|
| D-001 | 2026-11-12 | Code in GitHub, connected to Databricks as a Git folder | Notebooks only in the workspace; Git | Review, history, CI/CD; workspaces can be deleted | Team standard differs (e.g. Azure DevOps) | Repo created |
| D-002 | 2026-11-12 | Access and version rules written once in `src/bcia/retrieval.py`, applied inside the search query | Filter after retrieval; ask the LLM to withhold | Testable without Databricks; restricted chunks never leave the index | Index cannot express the date rule | `tests/test_retrieval_filters.py` |
| D-003 | 2026-11-12 | Deterministic claim check after generation | Prompt instructions only | A prompt is not a guarantee | False positives block good answers | `tests/test_guards.py` |
| D-004 | 2026-11-12 | Keyword router as the walking-skeleton baseline | LLM classifier first | Gives a first score cheaply; replaced and compared later | - | Router accuracy on eval set (pending) |
