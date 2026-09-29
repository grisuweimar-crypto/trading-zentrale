# Phase 8F — Mapping Candidate Batch B14

Status: `HUMAN_REVIEWED_ACTIVE`

Batch ID: `8F_MAPPING_CANDIDATES_2026-09-27_B14`

B14 contains 2 documentary mappings from the 38 still-unaccounted subjects after B13. The batch was intentionally small: only relationships supported directly by current issuer/SEC documentation and compatible with the existing Phase-8F factor semantics were retained.

Human review completed at `2026-09-27T11:28:01+02:00`. Both approved mappings become usable no earlier than that timestamp; no backdating is permitted.

## Subjects

- PATH — rates_policy — OTHER_DOCUMENTED
- 9988.HK — rates_policy — FINANCING_SENSITIVITY

## Factor mix

- rates_policy: 2

## Review notes

- PATH documents a large interest-bearing cash and marketable-securities portfolio and explicitly quantifies interest-rate risk. This is treasury/investment sensitivity, not financing sensitivity, so the relationship remains `OTHER_DOCUMENTED`.
- 9988.HK (Alibaba) explicitly states that certain offshore credit facilities reference Loan Prime Rate, SOFR or HIBOR and that benchmark-rate increases can raise financing costs; this supports `FINANCING_SENSITIVITY`.
- GRAB was investigated but is deliberately excluded from B14 because its current variable-rate borrowings are primarily linked to local Asian benchmarks; Phase 8F has already deferred such local-rate cases rather than automatically equating them with the existing `rates_policy` context.
- Other remaining subjects stay unaccounted until documentary evidence supports an enabled factor or they are explicitly reviewed as unmapped.
- No market direction, signed exposure, weights, thresholds or market outcomes are used.

After B14, the effective map contains 171 unique reviewed mappings across the frozen 207-subject domain; 36 subjects remain unaccounted. Seven explicitly registered later re-reviews remain preserved for audit but are not double-counted.

Machine-readable evidence references, summaries and canonical evidence-record SHA-256 fingerprints remain frozen in `configs/external_evidence_8f_mapping_candidates_b14_v1.json`; the human decisions are stored separately in `configs/external_evidence_8f_mapping_review_decisions_b14_v1.json`.
