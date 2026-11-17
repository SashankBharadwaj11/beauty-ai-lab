# BCIA document intelligence lab — native Databricks first, then VS Code Python

This is an **extension to the uploaded `beauty-ai-lab`** repository, not a replacement for its Day 2/Day 3 data setup. The repository ZIP contains source code **but not the source documents**. The existing Day 3 notebook expects **13 PDF versions** in `/Volumes/workspace/ai_architect_lab/lab_files/beauty_ai_lab_data/docs/` plus `document_manifest.json` one directory above.

## Learning objective

Build what a hands-on AI Solutions Architect must be able to demonstrate: **source inventory → integrity → structured extraction → quality gates → page routing → comparison of managed and custom approaches → production decision**. The first two phases deliberately stop before embeddings and LLM answers. The next phase adds those only once evidence quality is acceptable.

## Phase 1: Databricks native (do this first)

Use `notebooks/03a_native_document_triage.py` as a Databricks notebook. It relies on existing Unity Catalog Volumes, Delta tables, Spark SQL, and the managed Databricks `ai_parse_document` function.

1. **Locate actual documents.** Verify the folder, count PDFs, confirm `document_manifest.json`, select `DOC002_v1.pdf` (or another registered PDF). The repository ZIP is **not** the data ZIP. If data is missing, complete the existing Day 3 Volume upload first.
2. **Manifest integrity.** Notebook computes file inventory SHA-256, joins to the registered manifest and writes `bcia_lab_reconciliation`. Pass only if the selected file has status `OK`. Deliberately change a *scratch copy*, never the source manifest, then demonstrate `HASH_MISMATCH` using existing `03_documents_manifest_parsing.py`.
3. **Native UI (visual inspection).** Open Databricks **Document Parsing** and inspect the PDF side-by-side with its parsed text/table/figure output if enabled for your workspace.
4. **Native parse.** Set the `RUN_MANAGED_PARSE` notebook widget to `true`. It calls `ai_parse_document(... map('version','2.0','descriptionElementTypes',''))` for **one** document. Verify `bcia_lab_parsed_raw`; on capability/region/budget failure record the real error. No fake substitute result.
5. **Element and page tables.** The notebook creates `bcia_lab_doc_elements` with type, content, confidence, page and bbox and `bcia_lab_page_triage` with page statuses. Inspect at least one `table`, `figure`, `signature`, or documented absence. A parser result `PARSED_CANDIDATE` is **not a certified accurate document**.
6. **Record evidence.** Fill `docs/evidence_log.md` with workspace/runtime, selected PDF, manifest OK/failed count, parser availability, element types, detected page count, one verified example and one limitation. Don't estimate metrics; record observed values.

**Native-specific architecture considerations:** `ai_parse_document` supports PDF, DOC/DOCX, PPT/PPTX and common image formats, not just PDFs. OCR may be relevant wherever text is embedded in an image. The `descriptionElementTypes=''` option turns off AI-generated figure descriptions in this initial baseline to manage unnecessary work; it does not disable figure identification. Outputs vary by document, and generated chart data still needs verification. Availability depends on region and entitlement, including Free Edition usage limits.

**Exit gate:** one selected PDF reconciles to the manifest; managed parse is either demonstrably working or has a recorded exact blocker; at least one source-versus-parsed element comparison is logged.

## Phase 2: VS Code / Python (do second)

Download **one** synthetic BCIA PDF from the Unity Catalog Volume (UI download; do not commit project data to Git), and place it anywhere accessible on your laptop, e.g. `data/DOC002_v1.pdf`. In the VS Code terminal, from the repo root:

```powershell
py -m venv .venv
.venv\Scripts\python.exe -m pip install -e ".[dev,pdf]"
.venv\Scripts\python.exe scripts\build_page_variants.py data\DOC002_v1.pdf --page 1
.venv\Scripts\python.exe scripts\run_page_triage.py artifacts\page_check\native.pdf artifacts\page_check\scanned.pdf artifacts\page_check\bad_ocr_layer.pdf
.venv\Scripts\python.exe -m pytest -q tests\test_parsing.py
```

No PowerShell activation is required, so this avoids common execution-policy restrictions.

The classifier is `src/bcia/parsing.py::classify_page()`. Its four outcomes are `native`, `ocr_required`, `reocr_required`, and `review_required`. Its response contains `character_count`, **union** raster `image_coverage`, `has_invisible_text`, `invisible_text_ratio`, `real_word_ratio`, raw token counts, the threshold values used, and a reason. The three tests synthesize clean native text, an image-only PDF, and that image plus deliberately garbled hidden text (`render_mode=3`).

The real PDF may be short, image-heavy or contain jargon, so it may legitimately route to `review_required`. The experiment tests *routing*, not correctness of the real content.

**Key cross-platform comparison:** Native Databricks gives structured `elements` / page bbox / parser confidence; PyMuPDF directly inspects PDF text visibility and raster coverage. These are **complementary**, not equivalent quality metrics. Do not call an image-heavy native scan “accurate” just because one parser returned valid-looking text.

**Exit gate:** all three programmatic tests pass; you can explain why their measured characters/image coverage/invisible text/word ratio differ; you have recorded actual local results in the evidence log.

## Phase 3: expand one decision at a time

| Iteration | Hands-on experiment | Quality gate / design choice |
|---|---|---|
| 3A | Compare managed parsing vs native PyMuPDF on 10 clean and 10 scanned pages | Manually label and count numeric/ID errors; no invented confidence |
| 3B | Re-OCR suspicious pages with native Databricks managed parser or independent OCR | Compare critical fields against image crops and known ground truth; quarantine unresolved differences |
| 3C | Extract one research chart as figure + bbox + candidate data + original image | Confirm series, units, weeks and values manually; retain approximate/unverified status |
| 3D | Structure chunks by headings and table rows, keep page citations | Verify that every row/claim remains associated with its heading and page |
| 3E | Index only authorized, approved chunks; test version and ACL filters | No unauthorized hits; evaluate Recall@3/5 and citation accuracy |
| 3F | Add Databricks Workflows/Jobs and MLflow tracing | Measure duration, cost, failure rate, retries, ingestion lag, model changes |

## Things to correct in an architecture interview

- **“OCR is only for PDFs” is too narrow.** Scanned image files and embedded images in Office documents can also require OCR. Route by *content region* when necessary.
- **Manifest hashes detect duplicate bytes, not semantic duplicates.** An exported SharePoint PDF and Confluence HTML page may contain the same information but different SHA-256 hashes; normalized-content or similarity-based deduplication is a separate step.
- **“Confidence score” is not verification.** Parser confidence, dictionary hit rate, and OCR agreement are imperfect signals; critical numbers need source-level or ground-truth checks.
- **“Never cut a table” is a useful intent, not always possible.** Very large tables may require row-group chunking with repeated headers and provenance.
- **Security applies at query time as well as ingestion time.** Source ACLs, revocations and changes must be enforced before retrieved content is shown to the LLM or user.

## Technical references

- Databricks `ai_parse_document` documentation: https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_parse_document
- Document Parsing UI: https://docs.databricks.com/aws/en/agents/agent-bricks/document-parsing
- Free Edition limitations: https://docs.databricks.com/aws/en/getting-started/free-edition-limitations
- PyMuPDF official docs: https://pymupdf.readthedocs.io/en/latest/page.html
