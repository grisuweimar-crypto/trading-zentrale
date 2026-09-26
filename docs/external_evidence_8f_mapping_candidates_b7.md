# Phase 8F — Mapping Candidate Batch B7

Status: `PENDING_HUMAN_REVIEW`

Batch ID: `8F_MAPPING_CANDIDATES_2026-09-26_B7`

B7 contains 14 new documentary candidates. The batch is deliberately limited to subjects with direct support for one of the currently enabled Phase-8F factors; no subject was added merely to reach a target batch size.

## Subjects

- 3750.HK — lithium — INPUT_COST_LINK
- XSDG.F — lithium — INPUT_COST_LINK
- SU.PA — fx — CURRENCY_TRANSLATION
- SOLB.BR — fx — CURRENCY_TRANSLATION
- MC.PA — fx — OTHER_DOCUMENTED
- UAA — fx — OTHER_DOCUMENTED
- CSTM — fx — CURRENCY_TRANSLATION
- AUTO.OL — fx — OTHER_DOCUMENTED
- 6503.T — fx — OTHER_DOCUMENTED
- ASX — fx — OTHER_DOCUMENTED
- TSMN.MX — fx — OTHER_DOCUMENTED
- NDA.DE — copper — OTHER_DOCUMENTED
- RIGD.IL — oil — INPUT_COST_LINK
- 2899.HK — copper — REVENUE_LINK

## Factor mix

- fx: 9
- lithium: 2
- copper: 2
- oil: 1

## Review rules

- Every source and relationship class must be explicitly human-reviewed before activation.
- No mapping is usable before `reviewed_at`.
- No backdating is allowed.
- No sector, name, keyword or LLM inference can substitute for documentary evidence.
- No market direction, signed exposure, weights, thresholds or market outcomes are used.
- Lithium mappings are limited to issuers that explicitly document lithium/raw-material price effects on costs or profitability.
- `CURRENCY_TRANSLATION` is used only where explicit reporting-currency translation effects are documented.
- `OTHER_DOCUMENTED` is used for direct operating/hedging relationships without stronger directional semantics.
- Copper and oil relationships remain descriptive; no positive or negative market sign is assigned.

Machine-readable evidence references, summaries and canonical evidence-record SHA-256 fingerprints are frozen in `configs/external_evidence_8f_mapping_candidates_b7_v1.json`.
