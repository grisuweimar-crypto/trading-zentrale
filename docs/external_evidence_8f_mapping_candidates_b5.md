# Phase 8F — Mapping Candidate Batch B5

Status: `PENDING_HUMAN_REVIEW`

Batch ID: `8F_MAPPING_CANDIDATES_2026-09-26_B5`

This batch adds 20 documentary EUR/USD-linked FX candidates that are not among the 60 subjects already active after B1-B4.

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

- Every source and relationship class must be explicitly human-reviewed before promotion.
- No mapping is usable before `reviewed_at`.
- No backdating is allowed.
- No sector, name, keyword or LLM inference can substitute for documentary evidence.
- No direction, signed exposure, weights, thresholds or market outcomes are used.
- `CURRENCY_TRANSLATION` is used only where the source documents translation/net-investment exposure.
- `OTHER_DOCUMENTED` is used where the source documents EUR-linked transaction/revenue/expense/hedging exposure without stronger translation semantics.
- `BALANCE_SHEET_LINK` is used for Uber's documented Euro-denominated senior notes.

Machine-readable evidence references, summaries and canonical evidence-record SHA-256 fingerprints are frozen in `configs/external_evidence_8f_mapping_candidates_b5_v1.json`.
