# Failure log

Record every failure you trigger or find, with the test that now guards it.

| ID | Date | What failed | Detected by | Root cause | Fix | Test added | Status |
|---|---|---|---|---|---|---|---|
| F-001 | 2026-11-12 | "How did CAM009 perform..." routed as unstructured, not both | `test_mixed_question_is_both` | Keyword list had "performance", not "perform": keyword routers are brittle to word forms | Keyword changed to "perform" (matches both forms) | Existing test now passes | Fixed. Evidence that the keyword router must be compared with an LLM classifier (D-004) |
