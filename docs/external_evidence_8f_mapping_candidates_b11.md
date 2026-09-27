# Phase 8F — Mapping Candidate Batch B11

Status: `HUMAN_REVIEWED_ACTIVE`

Batch ID: `8F_MAPPING_CANDIDATES_2026-09-27_B11`

Reviewed at / valid from: `2026-09-27T09:13:36+02:00`

B11 contains 11 documentary candidates drawn only from the 58 subjects still unaccounted after B10. All 11 were explicitly human-reviewed and approved. B11 is active only from the review timestamp onward; no backdating is permitted.

After B11 the effective exposure map contains 160 reviewed mappings across the frozen 207-subject research domain, leaving 47 subjects unaccounted.

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
- Candidate records remain immutable source artifacts with `human_reviewed=false`; approval is carried separately by `configs/external_evidence_8f_mapping_review_decisions_b11_v1.json` and applied through the overlay registry.
- No mapping is usable before `reviewed_at`; no backdating is allowed.
- No market direction, signed exposure, weights, thresholds or market outcomes are used.

Machine-readable evidence references, summaries and canonical evidence-record SHA-256 fingerprints are frozen in `configs/external_evidence_8f_mapping_candidates_b11_v1.json`.
