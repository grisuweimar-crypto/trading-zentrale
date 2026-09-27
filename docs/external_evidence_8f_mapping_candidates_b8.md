# Phase 8F — Mapping Candidate Batch B8

Status: `HUMAN_REVIEWED_ACTIVE`

Batch ID: `8F_MAPPING_CANDIDATES_2026-09-27_B8`

Reviewed at: `2026-09-27T06:51:20+02:00`

B8 contains 15 documentary mappings that were explicitly human-reviewed and activated through the append-only overlay registry. Only direct issuer/SEC evidence for currently enabled Phase-8F factors is accepted; no candidate was added merely to reach a target batch size.

## Subjects

- ABX.TO — gold — REVENUE_LINK
- BTO.TO — gold — REVENUE_LINK
- EDR.TO — silver — REVENUE_LINK
- EQX.TO — gold — REVENUE_LINK
- NEM.AX — gold — REVENUE_LINK
- PAAS.TO — silver — REVENUE_LINK
- DSV.TO — gold — REVENUE_LINK
- VALE — copper — REVENUE_LINK
- LUMN — rates_policy — FINANCING_SENSITIVITY
- KLAR — rates_policy — FINANCING_SENSITIVITY
- PGY — rates_policy — FINANCING_SENSITIVITY
- FIGR — rates_policy — FINANCING_SENSITIVITY
- BLK — rates_policy — OTHER_DOCUMENTED
- CPB — inflation — INPUT_COST_LINK
- INGR — inflation — INPUT_COST_LINK

## Factor mix

- gold: 5
- silver: 2
- copper: 1
- rates_policy: 5
- inflation: 2

## Review notes

- DSV.TO is deliberately mapped to gold rather than silver because its 2025 documented revenue came from gold sales; the company name is not used as mapping evidence.
- Local/alternate listings remain distinct frozen-domain subjects because the pre-8F domain was deduplicated by symbol, not by issuer or ISIN.
- Rates mappings require explicit floating-rate funding, funding-cost or business/AUM sensitivity documentation; generic indebtedness is insufficient.
- Inflation mappings require explicit input-cost inflation evidence; generic macro sensitivity is insufficient.
- All 15 sources and relationship classes were explicitly human-reviewed before activation.
- Every B8 mapping is usable no earlier than `reviewed_at`; no backdating is allowed.
- No market direction, signed exposure, weights, thresholds or market outcomes are used.

Machine-readable evidence references, summaries and canonical evidence-record SHA-256 fingerprints remain frozen in `configs/external_evidence_8f_mapping_candidates_b8_v1.json`. Human-review decisions are recorded separately in `configs/external_evidence_8f_mapping_review_decisions_b8_v1.json`.
