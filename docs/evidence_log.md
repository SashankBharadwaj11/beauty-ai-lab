# Evidence log: Beauty Campaign Intelligence Assistant (training lab)

Rule: a module is "done" only when the actual result is pasted next to the expected one. Never write a number you didn't see in your workspace.

## Environment (fill in once)
| Item | Value |
|---|---|
| Edition | Databricks Free Edition (stated, verify in workspace settings) |
| Cloud / region | |
| Catalog.schema | workspace.ai_architect_lab |
| Volume | lab_files |
| Day-1 tables found by the audit (name: rows) | |
| `ai_parse_document` available? (Day 3 probe) | |

## Day 2: Bronze / Silver / Gold
| Check | Expected | Actual | Pass? |
|---|---|---|---|
| Bronze row counts | 3 / 4 / 6 / 24 / 12 / 5,880 / 25,000 / 432 / 1,000 | | |
| Silver reconciled, 0 quarantined | 9/9 tables | | |
| Gold rows (sales_lines / campaign_daily / campaign_summary) | 25,000 / 5,880 / 12 | | |
| A. Top Q3 2026 market | Atlanta 139,542.19 | | |
| B. Lowest ROAS | CAM009 0.5815 | | |
| C. Top Northeast Q3 category | fragrance 76,770.08 | | |
| D. CAM009 total spend | 214,140.18 | | |
| Acceptance values | 26/26 | | |
| SQL vs DataFrame difference rows | 0 | | |
| Defect test | 25,006 in → 24,999 silver + 7 quarantined | | |
| CHECK constraint rejected bad insert | yes, `net_sales_formula` | | |
| Fan-out WRONG vs RIGHT (CAM009 net) | 48,337,167.20 vs 98,647.28 | | |
| Plausibility warnings | 3 WARN (overspend 7.1–12.2x; attributed 2.07x net; 134/396 low ratings tagged positive) | | |
| dq_results this batch | 45 PASS, 3 WARN, 0 FAIL | | |
| Lineage graph visible? | | | |

## Day 3: Documents
| Check | Expected | Actual | Pass? |
|---|---|---|---|
| Manifest | 13 rows, 12 docs, 1 active each, no overlap | | |
| File/hash reconciliation | 13 OK | | |
| Tamper experiment | 11 OK, 1 HASH_MISMATCH, 1 MISSING_FILE, 1 UNREGISTERED_FILE | | |
| PRIMARY parser | ai_parse_document_v2 or pypdf_fallback | | |
| Status (fallback) | 12 parsed, 1 needs_ocr (DOC012) | | |
| Status (managed) | 13 parsed expected; record actual | | |
| Table row integrity: fallback / managed | false / should be true | | |
| OCR of DOC012 ("October 20, 2026"): fallback / managed | false / should be true | | |
| Corrupt file: fails its row or the whole query? | pypdf: row only. Managed: record what happens | | |

## Known source-data issues (kept on purpose, not regenerated)
- Campaign spend is 7.1–12.2x `budget_usd` for every campaign.
- Total attributed revenue (5.65M) is 2.07x total company net sales (2.73M).
- `sentiment_hint` doesn't track `rating`.
- Product names don't match categories (for example "Renewal Cream" is categorized as makeup, "Tinted Balm" as fragrance).
- DOC001_v2 approves two different texture phrasings (paragraph vs table).
- DOC001_v1 has a confusing sentence that labels a phrase "approved phrase" and then says it is NOT approved. It is a retrieval trap.

## Drill notes (what I said → what I'd say next time)
| Drill | First attempt, key points | Feedback | Second attempt |
|---|---|---|---|
| D2-1 Why trust Atlanta? | | | |
| D2-2 Seven quarantined rows | | | |
| D2-3 $48M vs $98K | | | |
| D2-4 Ready for Genie? | | | |
| D3-1 How do you KNOW all docs processed? | | | |
| D3-2 Wrong evidence for texture claims | | | |
| D3-3 Conflicting approved wording | | | |
