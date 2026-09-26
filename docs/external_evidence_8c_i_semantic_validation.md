# Phase 8C-I — Semantic Challenger Validation Protocol

## Purpose

8C-I freezes the validation procedure for the quarantined 8C-H semantic challenger before any real SEC snapshot is evaluated. It is designed to prevent threshold changes, sampling changes, or label definitions from being adapted after seeing real parser performance.

## Scope

The first validation covers two issuer-side semantic families only:

- regular quarterly dividend declarations with amount per share;
- new share/stock repurchase authorizations with stated maximum amount.

Guidance, capital raises and earnings beat/miss remain outside this validation. Earnings beat/miss remains blocked by the paused Phase 8B historical-consensus dependency.

## Outcome blindness

No market prices, returns, Phase-7 stance, portfolio state, or subsequent outcome may be visible during annotation or parser validation. 8C-I validates extraction correctness only.

## Sampling

Candidate precision audit:

- deterministic SHA-256 ordering using seed `8C-I-v1`;
- up to 100 emitted candidates per family;
- all candidates are used when the available count is <=100.

Independent anchor audit:

- deterministic SHA-256 ordering independent of challenger output;
- up to 200 anchors per family;
- intended to describe false negatives / recall, not to gate the first promotion.

## Ground truth

Each sampled item is reviewed against its accession-bound SEC filing text. Allowed labels are fixed in `configs/external_evidence_8c_semantic_validation_v1.json`.

Annotations marked `UNCERTAIN` do not count as correct and are excluded from the accuracy denominator. Their rate is itself gated.

## Promotion gates

Promotion is evaluated separately for DIVIDEND and BUYBACK. Pooled accuracy may not hide a family failure.

A family requires:

- at least 40 labeled emitted candidates;
- at least 10 distinct issuers;
- Wilson 95% lower bound >=0.90 for candidate precision;
- Wilson 95% lower bound >=0.90 for critical-field accuracy;
- uncertain annotation rate <=0.10;
- zero market-direction violations;
- zero provenance violations;
- complete annotation of the sampled emitted candidates.

Insufficient sample size results in `REMAIN_CHALLENGER`, not a pass.

Passing 8C-I does **not** enable outcome research, production evidence, Phase-7 integration, or portfolio actions. Those remain separate later gates.

## Required real-data input

A real evaluation requires the verified offline SEC snapshot defined in 8C-C3/8C-F, collected outside GitHub-hosted Actions because the SEC currently returns HTTP 403 to that runner environment. The snapshot must preserve source URLs, accession metadata, publication time, raw bytes, and SHA-256 digests.
