# Phase 8F — Mapping Candidate Batch B12

Status: `HUMAN_REVIEWED_ACTIVE`

Batch ID: `8F_MAPPING_CANDIDATES_2026-09-27_B12`

B12 contains 12 documentary candidates drawn only from the currently unaccounted portion of the frozen 207-subject research domain. The corrected effective pre-B12 map contained 153 unique active mappings; seven later human re-reviews from B8/B10 are preserved separately in the append-only correction registry and are not double-counted. After explicit human approval, B12 increases the effective map to 165 unique reviewed mappings and leaves 42 subjects unaccounted.

Reviewed at / valid from: `2026-09-27T09:56:29+02:00`

## Subjects

- NVDA — rates_policy — OTHER_DOCUMENTED
- AXON — rates_policy — OTHER_DOCUMENTED
- TEM — rates_policy — FINANCING_SENSITIVITY
- PL — fx — OTHER_DOCUMENTED
- INCY — rates_policy — OTHER_DOCUMENTED
- CRUS — rates_policy — OTHER_DOCUMENTED
- WYFI — rates_policy — FINANCING_SENSITIVITY
- TE — rates_policy — FINANCING_SENSITIVITY
- MP — rates_policy — OTHER_DOCUMENTED
- QBTS — rates_policy — OTHER_DOCUMENTED
- RHM.DE — fx — OTHER_DOCUMENTED
- PKX — lithium — OTHER_DOCUMENTED

## Factor mix

- rates_policy: 9
- fx: 2
- lithium: 1

## Review notes

- NVDA, INCY, CRUS and MP use quantified or expressly described investment-portfolio interest-rate sensitivity; they are not mislabeled as financing exposure.
- AXON combines quantified investment-rate sensitivity with an undrawn SOFR-linked facility and therefore remains conservatively `OTHER_DOCUMENTED` rather than `FINANCING_SENSITIVITY`.
- TEM, WYFI and TE document actual SOFR/base-rate-linked borrowing structures and therefore use `FINANCING_SENSITIVITY`.
- QBTS documents financing whose draw rate is established using the Prime Rate; because the currently drawn amount is fixed after execution, it remains `OTHER_DOCUMENTED`.
- PL explicitly reports that approximately 30% of fiscal 2026 revenue was denominated in foreign currencies, primarily Euro; no geographic inference is used.
- RHM.DE explicitly reports USD/EUR hedge rates and material currency hedge volumes; no domicile inference is used.
- PKX is mapped to lithium only because POSCO Holdings' official 2025 results release explicitly connects lithium investment/commercial production to business profit recovery; it is not inferred from a generic materials-sector label.
- All 12 source and relationship-class decisions were explicitly human-approved for B12.
- No B12 mapping is usable before `reviewed_at`; no backdating is allowed.
- No market direction, signed exposure, weights, thresholds, market outcomes or Phase-7 decision data are used.

Machine-readable evidence references, summaries and canonical evidence-record SHA-256 fingerprints remain frozen in `configs/external_evidence_8f_mapping_candidates_b12_v1.json`.
