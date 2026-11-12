# Risk register

| ID | Risk | Impact | Mitigation | Owner | Status |
|---|---|---|---|---|---|
| R-01 | Superseded or future claim quoted as current | High | Effective-date filter in search; claim guard; tests A1, Q058 | Architect | Open |
| R-02 | Restricted content shown to marketing users | High | access_group filter inside the query; paired persona tests (A2) | Architect | Open |
| R-03 | Free Edition lacks ai_parse_document / Genie / serving | Medium | Probe first; labelled fallbacks | Architect | Check on Day 3 |
| R-04 | Attributed revenue is 2.07x company net sales | High (trust) | Label as "attributed"; confirm definition with data owner | Raj (analytics) | Open |
| R-05 | Draft numbers (DOC019 ROAS 0.62) used as final | Medium | Governed SQL preferred over documents for metrics (A4) | Architect | Open |
