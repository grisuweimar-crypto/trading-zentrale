# Phase 8F — Mapping Candidate Batch B11

Status: `PENDING_HUMAN_REVIEW`

Batch ID: `8F_MAPPING_CANDIDATES_2026-09-27_B11`

B11 contains 11 new documentary candidates drawn only from the 58 subjects still unaccounted after B10. No candidate is added merely to reach a target batch size.

## Subjects

- APP — rates_policy — FINANCING_SENSITIVITY
- IREN — rates_policy — FINANCING_SENSITIVITY
- MELI — rates_policy — OTHER_DOCUMENTED
- ENSG — rates_policy — OTHER_DOCUMENTED
- HIMS — rates_policy — OTHER_DOCUMENTED
- ROL — rates_policy — OTHER_DOCUMENTED
- NRDS — rates_policy — OTHER_DOCUMENTED
- XYZ — rates_policy — FINANCING_SENSITIVITY
- NIO — lithium — INPUT_COST_LINK
- XPEV — lithium — INPUT_COST_LINK
- TTK.DE — fx — CURRENCY_TRANSLATION

## Factor mix

- rates_policy: 8
- lithium: 2
- fx: 1

## Review notes

- APP, IREN and XYZ have explicit current or generally applicable variable-rate debt exposure and therefore use `FINANCING_SENSITIVITY`.
- MELI quantifies material rate sensitivity across loans payable and other financial liabilities, so it remains conservatively `OTHER_DOCUMENTED` rather than being narrowed to financing alone.
- ENSG, HIMS, ROL and NRDS document SOFR-linked revolving financing capacity, but the evidence does not justify asserting a current drawn variable-rate balance; they therefore remain `OTHER_DOCUMENTED`.
- NIO and XPEV directly identify lithium or lithium battery cells as cost-sensitive production inputs; both use `INPUT_COST_LINK` rather than any revenue relationship.
- TAKKT explicitly labels EUR/USD effects on euro-reported sales and earnings as translation risk, so TTK.DE uses `CURRENCY_TRANSLATION`.
- Every source and relationship class must be explicitly human-reviewed before activation.
- No mapping is usable before `reviewed_at`; no backdating is allowed.
- No market direction, signed exposure, weights, thresholds or market outcomes are used.

Machine-readable evidence references, summaries and canonical evidence-record SHA-256 fingerprints are frozen in `configs/external_evidence_8f_mapping_candidates_b11_v1.json`.
