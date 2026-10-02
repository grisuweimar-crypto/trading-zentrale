# Phase 8I-E – Extended Reliability Research

## Purpose

Phase 8I-E asks one narrow prospective question: does the already frozen external relation state add information about the later directional quality of the already frozen Phase-7 direction, conditional on the existing Phase-7G reliability state?

8I-E does **not** replace Phase 7G, create a numeric reliability score, recompute Universal Stance, alter Portfolio Action, or authorize orders.

## Preserved baseline

The complete Phase-7G output remains the immutable baseline. Its qualitative reliability states remain unchanged:

- `pending_confirmation`
- `provisional_cross_family_support`
- `provisional_same_family_support`
- `provisional_unopposed_support`
- `blocked_conflict`
- `blocked_insufficient`

8I-E only appends a research cell consisting of:

`phase7_reliability_state + external_relation_state + horizon`

No ordinal ranking is attached to that cell.

## Primary eligibility

Primary reliability research requires a directional Phase-7 core (`POSITIVE` or `NEGATIVE`) and one of the four directional Phase-7G reliability states above.

The primary external relation groups are:

- `CONFIRMING`
- `CONFLICTING`
- `INSUFFICIENT_EXTERNAL`

`MIXED_EXTERNAL` and `UNKNOWN` remain secondary descriptive groups. `EXTERNAL_ONLY` is not a primary reliability group because the Phase-7 core has no direction to evaluate.

## Primary outcome

The frozen primary metric is `direction_aligned_peer_excess`:

- Phase-7 core `POSITIVE`: `peer_excess_H`
- Phase-7 core `NEGATIVE`: `-peer_excess_H`

Therefore, a higher value always means that the later peer-relative outcome is more aligned with the preserved Phase-7 direction. This is a continuous outcome and is not converted into a numeric success probability.

## Frozen hypothesis family

For every horizon `5/20/40/60` and every eligible Phase-7G reliability state, two contrasts are preregistered:

1. `CONFIRMING - INSUFFICIENT_EXTERNAL`
2. `CONFLICTING - INSUFFICIENT_EXTERNAL`

That yields 32 primary hypotheses. Expected signs are recorded before outcome access, but all primary p-values are two-sided and an unexpected sign may not be inverted or relabelled.

Holm correction is applied across the full 32-member family at family-wise alpha 0.05.

## Prospective evidence only

The 8I-E prospective cohort starts no earlier than `2026-09-29T00:00:00+00:00`.

Earlier Phase-7 prospective rows and any 8G/8H training, validation, holdout, or prospective outcomes are not fresh 8I confirmation. The 8I-D prediction delta is a model output, not an outcome.

8I-E uses a single `PROSPECTIVE` partition because no parameter, threshold, sign, horizon, or rule is tuned from that partition. The complete family is evaluated once only after a metadata-only terminal gate is ready.

Before that gate, code may inspect only identity, coverage, and label-availability metadata. Outcome values remain sealed.

## Statistical gate

Each side of every primary contrast must have at least:

- 30 snapshot-level observations, and
- 2 temporal support regions.

Rows are first aggregated to the snapshot level within reliability state, external relation, and horizon. Dependence from overlapping windows is handled with circular moving observation-date blocks of length `2 * H`.

The terminal report includes effect sizes, percentile bootstrap intervals, two-sided centered-bootstrap p-values, Holm-adjusted p-values, and a non-overlap sensitivity requirement from the contract. Significance is never automatic promotion.

## Current repository state

Real 8I-E annotation and real outcome access remain blocked by the existing 8I-B upstream source-identity contract issue. Therefore the current state is fail-closed:

- no real bound external components,
- no real 8I-E prospective manifest,
- no real 8I-E outcomes opened,
- Phase-7G reliability unchanged,
- Extended Reliability disabled,
- Extended Stance disabled,
- Portfolio Action unchanged,
- no orders/trades.

Synthetic execution exists only to prove the contract, identity guards, metric semantics, terminal gate, and multiple-testing logic.

## Completion meaning

Technical completion of 8I-E means the contract, implementation, tests, documentation, and CI guard are complete. It does **not** mean the reliability extension is empirically validated.

Empirical 8I-E completion requires the one-shot prospective family evaluation and a separate review. Only after successful empirical review may a later phase begin preregistering an Extended Stance policy.
