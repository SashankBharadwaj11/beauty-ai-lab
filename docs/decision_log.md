# Decision log

| ID | Date | Decision | Options considered | Why | Revisit if | Evidence |
|---|---|---|---|---|---|---|
| D-001 | 2026-11-12 | Code in GitHub, connected to Databricks as a Git folder | Notebooks only in the workspace; Git | Review, history, CI/CD; workspaces can be deleted | Team standard differs (e.g. Azure DevOps) | Repo created |
| D-002 | 2026-11-12 | Access and version rules written once in `src/bcia/retrieval.py`, applied inside the search query | Filter after retrieval; ask the LLM to withhold | Testable without Databricks; restricted chunks never leave the index | Index cannot express the date rule | `tests/test_retrieval_filters.py` |
| D-003 | 2026-11-12 | Deterministic claim check after generation | Prompt instructions only | A prompt is not a guarantee | False positives block good answers | `tests/test_guards.py` |
| D-004 | 2026-11-12 | Keyword router as the walking-skeleton baseline | LLM classifier first | Gives a first score cheaply; replaced and compared later | - | Router accuracy on eval set (pending) |

| D-010 | 2026-11-17 | Databricks managed parse first on one manifest-verified PDF; compare with local PyMuPDF page preflight afterward. Native status is not content verification. Initial PyMuPDF page-routing thresholds: raster image union >=70%, native text >=100 non-space chars, image-only <=15 chars, hidden text >=10% of traced chars on image-dominant page, real-word ratio >=0.65 over >=8 alphabetic tokens of length >=3. | Blindly trust existing PDF text; OCR every page; skip native managed baseline | Explicit and testable routing, enables cost-aware review of corrupted scanner-OCR layers. All thresholds **pilot hypotheses**, not calibrated precision/recall. | Labeled 200-page sample shows excess false accepts / false re-OCR; multilingual and specialized vocabulary; PDF vector graphics | `tests/test_parsing.py`; `notebooks/03a_native_document_triage.py`; `docs/native_first_lab.md` |
