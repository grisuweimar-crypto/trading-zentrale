# Phase 8F — Mapping Candidate Batch B9

Status: `PENDING_HUMAN_REVIEW`

Batch ID: `8F_MAPPING_CANDIDATES_2026-09-27_B9`

B9 contains 12 new documentary candidates. Only direct issuer/SEC evidence for currently enabled Phase-8F factors is accepted; no candidate is added merely to reach a target batch size.

## Subjects

- AAPL — fx — BALANCE_SHEET_LINK
- AMKR — fx — OTHER_DOCUMENTED
- ON — fx — OTHER_DOCUMENTED
- OGN — fx — OTHER_DOCUMENTED
- WM — inflation — INPUT_COST_LINK
- CNC — rates_policy — OTHER_DOCUMENTED
- UNH — rates_policy — OTHER_DOCUMENTED
- SLVR.V — silver — OTHER_DOCUMENTED
- PPTA — gold — OTHER_DOCUMENTED
- MGMA.V — silver — OTHER_DOCUMENTED
- DV.V — silver — OTHER_DOCUMENTED
- AAGFF — silver — OTHER_DOCUMENTED

## Factor mix

- fx: 4
- rates_policy: 2
- inflation: 1
- silver: 4
- gold: 1

## Review notes

- AAPL uses `BALANCE_SHEET_LINK` because the current 2025 10-K exhibit documents material euro-denominated notes; no generic geographic inference is used.
- AMKR, ON and OGN use explicit Euro/EUR foreign-currency or hedging disclosures rather than sector or domicile inference.
- WM uses explicit documented inflationary operating-cost pressure; generic macro exposure is insufficient.
- CNC and UNH use issuer-quantified interest-rate sensitivity; neither is labeled `FINANCING_SENSITIVITY` because the documented relationship also runs through investment income/assets rather than financing alone.
- SLVR.V and PPTA use project-economics disclosures with explicit commodity-price assumptions.
- MGMA.V, DV.V and AAGFF are pre-revenue/resource-stage exposures and therefore use `OTHER_DOCUMENTED`, not `REVENUE_LINK`.
- DV.V is retained because the frozen pre-8F research domain contains the legacy symbol; the mapping documents the frozen subject rather than asserting current standalone listing status.
- Every source and relationship class must be explicitly human-reviewed before activation.
- No mapping is usable before `reviewed_at`; no backdating is allowed.
- No market direction, signed exposure, weights, thresholds or market outcomes are used.

Machine-readable evidence references, summaries and canonical evidence-record SHA-256 fingerprints are frozen in `configs/external_evidence_8f_mapping_candidates_b9_v1.json`.
