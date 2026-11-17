# D-010 local validation — synthetic fixture only

Date: 2026-11-17

This validation was executed in the assistant's local Python environment. It was **not run in the user's Databricks workspace** and does not prove accuracy on the user's actual Day 3 PDFs.

## Result

- `python -m pytest -q tests/test_parsing.py`: **3 passed**.
- `python -m pytest -q tests`: **17 passed** (includes 14 preexisting tests).
- The synthetic test PDF was built with selectable text. The `scanned.pdf` variant rasterized it at 150 dpi. The `bad_ocr_layer.pdf` variant used the same raster image and added junk invisible text with `render_mode=3`.
- Rendering the two scanned variants at 100 dpi produced pixel-identical page images.

| Variant | Verdict | Non-space characters | Raster image coverage | Invisible text | Dictionary word ratio |
|---|---|---:|---:|---|---:|
| native | `native` | 280 | 0.0% | No | 0.9756 |
| scanned | `ocr_required` | 0 | 99.99% | No | 0.0000 |
| bad_ocr_layer | `reocr_required` | 192 | 99.99% | Yes | 0.0000 |

These are demonstration values from a temporary synthetic page, not universal thresholds or measured performance on BCIA real fixtures.

## Caveats

- Existing hidden OCR may still be accurate; hidden text is a routing signal, not a proof of corruption.
- Real-word ratio is a rough vocabulary heuristic. It is unsuitable for multilingual pages, specialized domain jargon, and numeric correctness without calibration.
- Page-image coverage does not detect vector-graphics-only plots, watermarks or floating text above backgrounds. Complement with layout analysis.
- A genuine PDF can have `get_text()` output with wrong column reading order even if the text is visible; that case needs a layout check.
- The production decision requires an independently labeled sample of pages, critical-field exact-match metrics, re-OCR comparisons and a documented false-acceptance rate.
