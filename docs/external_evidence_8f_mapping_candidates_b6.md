# Phase 8F — Mapping Candidate Batch B6

Status: `HUMAN_REVIEW_COMPLETED_ACTIVE_OVERLAY`

Batch ID: `8F_MAPPING_CANDIDATES_2026-09-26_B6`

Reviewed at: `2026-09-26T23:28:24+02:00`

B6 deliberately contains 14 candidates rather than forcing the batch to 20. Only subjects with direct documentary support for the currently enabled factor catalog were retained. The user explicitly approved Batch B6, and all 14 mappings become usable no earlier than the review timestamp above.

## Subjects

- SN.L — fx — OTHER_DOCUMENTED
- BAYN.DE — fx — OTHER_DOCUMENTED
- FME.DE — fx — CURRENCY_TRANSLATION
- NXP — fx — CURRENCY_TRANSLATION
- PM — fx — OTHER_DOCUMENTED
- RACE — fx — BALANCE_SHEET_LINK
- ETN — fx — BALANCE_SHEET_LINK
- ADS.DE — fx — OTHER_DOCUMENTED
- VOW3.DE — fx — OTHER_DOCUMENTED
- BAS.DE — fx — BALANCE_SHEET_LINK
- ABBN.SW — fx — CURRENCY_TRANSLATION
- UUUU — uranium — REVENUE_LINK
- ENR.DE — fx — OTHER_DOCUMENTED
- RI.PA — fx — OTHER_DOCUMENTED

## Review rules

- Every source and relationship class was explicitly human-reviewed before activation.
- No mapping is usable before `reviewed_at`.
- No backdating is allowed.
- No sector, name, keyword or LLM inference substitutes for documentary evidence.
- No market direction, signed exposure, weights, thresholds or market outcomes are used.
- `CURRENCY_TRANSLATION` is reserved for explicit translation/reporting-currency exposure.
- `BALANCE_SHEET_LINK` is used only for documented currency-denominated balance-sheet positions.
- `OTHER_DOCUMENTED` is used where an explicit EUR/USD operating or hedging relationship exists without stronger translation semantics.
- UUUU is mapped to uranium only because the issuer directly reports U3O8 sales, realized uranium prices and uranium revenue.

Machine-readable evidence references, summaries and canonical evidence-record SHA-256 fingerprints remain frozen in `configs/external_evidence_8f_mapping_candidates_b6_v1.json`. The explicit human decisions are stored in `configs/external_evidence_8f_mapping_review_decisions_b6_v1.json`, and B6 is activated append-only through `configs/external_evidence_8f_exposure_overlays_v1.json`.
