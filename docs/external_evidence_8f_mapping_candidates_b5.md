# Phase 8F — Mapping Candidate Batch B5

Status: `HUMAN_REVIEW_COMPLETED_ACTIVE_OVERLAY`

Batch ID: `8F_MAPPING_CANDIDATES_2026-09-26_B5`

Human review timestamp: `2026-09-26T23:07:27+02:00`

This batch adds 20 documentary EUR/USD-linked FX mappings that were not among the 60 subjects already materialized after B1-B4. B5 is stored as the first append-only reviewed overlay in `configs/external_evidence_8f_exposure_overlays_v1.json`. The effective exposure map therefore contains 80 reviewed mappings while the B1-B4 base map remains unchanged at 60. The effective map must be materialized into a single frozen artifact before final Phase-8F freeze.

## Subjects

- KLAC — fx — CURRENCY_TRANSLATION
- LRCX — fx — OTHER_DOCUMENTED
- SNOW — fx — OTHER_DOCUMENTED
- FSLR — fx — OTHER_DOCUMENTED
- GMED — fx — CURRENCY_TRANSLATION
- SNN — fx — CURRENCY_TRANSLATION
- TSLA — fx — CURRENCY_TRANSLATION
- NFLX — fx — OTHER_DOCUMENTED
- SPOT — fx — CURRENCY_TRANSLATION
- ADBE — fx — OTHER_DOCUMENTED
- FISV — fx — CURRENCY_TRANSLATION
- PFE — fx — CURRENCY_TRANSLATION
- PEP — fx — CURRENCY_TRANSLATION
- PG — fx — CURRENCY_TRANSLATION
- NKE — fx — OTHER_DOCUMENTED
- GE — fx — OTHER_DOCUMENTED
- KO — fx — OTHER_DOCUMENTED
- JNJ — fx — CURRENCY_TRANSLATION
- UBER — fx — BALANCE_SHEET_LINK
- STLA — fx — CURRENCY_TRANSLATION

## Review rules

- Every source and relationship class was explicitly human-reviewed before activation.
- No B5 mapping is usable before `2026-09-26T23:07:27+02:00`.
- No backdating is allowed.
- No sector, name, keyword or LLM inference substitutes for documentary evidence.
- No direction, signed exposure, weights, thresholds or market outcomes are used.
- `CURRENCY_TRANSLATION` is used only where the source documents translation/net-investment exposure.
- `OTHER_DOCUMENTED` is used where the source documents EUR-linked transaction/revenue/expense/hedging exposure without stronger translation semantics.
- `BALANCE_SHEET_LINK` is used for Uber's documented Euro-denominated senior notes.
- B5 review replay against the effective map must be idempotent.

Machine-readable evidence references, summaries and canonical evidence-record SHA-256 fingerprints remain frozen in `configs/external_evidence_8f_mapping_candidates_b5_v1.json`. The explicit human decisions are stored in `configs/external_evidence_8f_mapping_review_decisions_b5_v1.json`.
