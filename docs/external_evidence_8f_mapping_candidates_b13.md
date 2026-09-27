# Phase 8F — Mapping Candidate Batch B13

Status: `PENDING_HUMAN_REVIEW`

Batch ID: `8F_MAPPING_CANDIDATES_2026-09-27_B13`

B13 contains 4 new documentary candidates from the 42 frozen-domain subjects still unaccounted after B12. The batch is intentionally small: only direct documentary relationships to the already-enabled Phase-8F factor catalog are accepted, and no subject is added merely to reach a target batch size.

## Subjects

- ASTS — rates_policy — OTHER_DOCUMENTED
- CDNL — rates_policy — FINANCING_SENSITIVITY
- RGTI — rates_policy — OTHER_DOCUMENTED
- RKLB — rates_policy — OTHER_DOCUMENTED

## Factor mix

- rates_policy: 4

## Review notes

- CDNL reports current SOFR-linked debt and therefore uses `FINANCING_SENSITIVITY`.
- ASTS discloses a Term-SOFR-linked bridge facility at a subsidiary, but the parent is explicitly not borrower or guarantor; it is therefore classified conservatively as `OTHER_DOCUMENTED`, not parent-level `FINANCING_SENSITIVITY`.
- RGTI directly states that lower interest rates would reduce future interest income as U.S. Treasury investments mature and are reinvested; this is treasury/investment sensitivity, not financing sensitivity.
- RKLB explicitly identifies interest rates as a primary market-risk exposure and holds a material marketable-securities portfolio including U.S. Treasury and other debt securities; it therefore remains `OTHER_DOCUMENTED`.
- Local-rate exposures whose documentary link is primarily to non-U.S. benchmarks were deliberately deferred rather than mapped automatically to the current `rates_policy` factor.
- Every source and relationship class must be explicitly human-reviewed before activation.
- No mapping is usable before `reviewed_at`; no backdating is allowed.
- No market direction, signed exposure, weights, thresholds or market outcomes are used.

Machine-readable evidence references, summaries and canonical evidence-record SHA-256 metadata fingerprints are frozen in `configs/external_evidence_8f_mapping_candidates_b13_v1.json`. These fingerprints are metadata fingerprints and are not represented as raw archived document hashes.
