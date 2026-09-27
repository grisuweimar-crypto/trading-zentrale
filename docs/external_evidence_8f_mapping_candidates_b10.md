# Phase 8F — Mapping Candidate Batch B10

Status: `PENDING_HUMAN_REVIEW`

Batch ID: `8F_MAPPING_CANDIDATES_2026-09-27_B10`

B10 contains 14 new documentary candidates. Only direct issuer/SEC evidence for currently enabled Phase-8F factors is accepted; no candidate is added merely to reach a target batch size.

## Subjects

- APLD — rates_policy — FINANCING_SENSITIVITY
- MBLY — fx — OTHER_DOCUMENTED
- ZETA — rates_policy — FINANCING_SENSITIVITY
- TTAN — rates_policy — OTHER_DOCUMENTED
- QS — lithium — INPUT_COST_LINK
- CRWV — rates_policy — FINANCING_SENSITIVITY
- MRNA — fx — OTHER_DOCUMENTED
- SGL.DE — gas — INPUT_COST_LINK
- VZLA.TO — silver — OTHER_DOCUMENTED
- NBIS — fx — OTHER_DOCUMENTED
- SHOP — fx — OTHER_DOCUMENTED
- INOD — fx — OTHER_DOCUMENTED
- AVAV — fx — OTHER_DOCUMENTED
- FLNC — rates_policy — OTHER_DOCUMENTED

## Factor mix

- fx: 6
- rates_policy: 5
- lithium: 1
- gas: 1
- silver: 1

## Review notes

- APLD, ZETA and CRWV have explicitly documented variable-rate/SOFR financing and therefore use `FINANCING_SENSITIVITY`.
- TTAN and FLNC document floating-rate financing capacity, but the evidence does not justify claiming a current drawn rate exposure; they therefore remain conservatively `OTHER_DOCUMENTED`.
- QS is pre-revenue; lithium is mapped as `INPUT_COST_LINK`, not `REVENUE_LINK`, because the filing explicitly connects lithium prices to future battery-cell production costs.
- SGL.DE documents natural gas as a material manufacturing energy input and reports 2025 natural-gas consumption; the mapping is therefore `INPUT_COST_LINK`.
- VZLA.TO uses project economics with explicit silver-price assumptions and therefore remains `OTHER_DOCUMENTED`, not `REVENUE_LINK`.
- MBLY, MRNA, NBIS, SHOP, INOD and AVAV use explicit Euro/EUR disclosures; no geographic, domicile or sector inference is used.
- Every source and relationship class must be explicitly human-reviewed before activation.
- No mapping is usable before `reviewed_at`; no backdating is allowed.
- No market direction, signed exposure, weights, thresholds or market outcomes are used.

Machine-readable evidence references, summaries and canonical evidence-record SHA-256 fingerprints are frozen in `configs/external_evidence_8f_mapping_candidates_b10_v1.json`.
