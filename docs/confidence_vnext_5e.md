# Phase 5E — Adaptive Shadow Inference and Promotion Gate

## Purpose

Phase 5E is the technical completion layer for Confidence vNext Phase 5.

Phases 5A–5D already provide the immutable freeze, prospective v2 evidence,
progressive horizon-specific learning, and genuine future walk-forward epochs.
One link is still required for the system to learn and eventually prove whether
that learning helps: a frozen adaptive shadow rule must turn the Phase-5C state
reliability tables into an out-of-sample direction, and the resulting paired
performance must feed the Phase-5A promotion gate.

Phase 5E adds exactly that link. It remains research-only. It does not modify
production Confidence, scanner weights, thresholds, Selection, Timing,
Probability, Risk, R-codes, Depot-Watch, portfolio logic, or trading actions.

## Frozen shadow policy

The policy is fixed in source before prospective v2 outcomes have matured.

For each finalized Phase-5D evaluation row it uses only:

- the exact claim-time state labels already frozen in v2; and
- the exact Phase-5C state reliability tables frozen before that evaluation
  epoch began.

It never uses the evaluation outcome to select its action.

For every exact state with directional training evidence, the training
`mean_signed_peer_excess` is used as empirical evidence for whether the frozen
Phase-4 direction historically worked in that state. State names are never
treated as ordinal.

For multi-state fields, state signals are weighted by their frozen
`directional_N`. Each available field then receives equal weight. The combined
sign has only two actions:

- non-negative signal: follow the frozen Phase-4 direction;
- negative signal: invert the frozen Phase-4 direction.

If no empirical state signal is available, the policy follows the frozen
Phase-4 direction. There is no abstention, no tuned threshold, no raw Scanner
Score input, and no scalar 0–100 Confidence value.

The complete policy contract is canonicalized and SHA-256 fingerprinted. A
policy change therefore creates a new policy identity and cannot silently alter
old evidence.

## Paired comparison with the Frozen Baseline

Every Phase-5D finalized epoch is reconstructed from the prospective v2 archive.
Phase 5E requires the reconstructed row count and evidence fingerprint to match
the immutable Phase-5D record and requires the referenced Phase-5C version
hashes and cutoffs to match exactly.

Directional comparison is paired row-by-row:

- Frozen Baseline: original Phase-4 claim direction;
- Adaptive Shadow: follow or invert that same direction using only frozen
  Phase-5C training evidence.

Because v2 currently stores adverse excursion and drawdown for the observed
long path, an inverted-direction path risk cannot be reconstructed without
inventing a missing favorable-excursion path. Phase 5E therefore does **not**
claim adaptive downside-risk improvement. Promotion advantage is evaluated only
on the paired directional metrics that are actually identifiable from the
archived evidence.

## Robust uncertainty

The inherited uncertainty contract remains unchanged:

- complete observation-date clusters;
- circular moving observation-date bootstrap;
- block length `2 × horizon`;
- at least two time-separated support regions;
- 5T/20T/40T/60T evaluated separately;
- overlapping forward windows are not treated as independent.

A descriptive Phase-5D epoch is retained for diagnostics but is not promotion
evidence until the robust support requirement is satisfied.

## Pre-registered promotion safeguards

These safeguards are fixed before mature v2 outcomes are available:

- at least 2 robust, genuine, non-overlapping epochs for a horizon;
- maximum symbol share 25%;
- maximum observation-date share 60%;
- positive primary paired point delta in every robust epoch for temporal
  stability;
- primary aggregate advantage: 95% robust interval lower bound of paired mean
  signed-peer-excess delta > 0;
- secondary non-degradation: paired direction-hit-rate point delta >= 0.

The existing Phase-5A gates are then applied. Passing all gates means only
`eligible_for_separate_promotion_review`. It does not perform a production
change.

## Staged horizon operation

Promotion assessment is horizon-specific. 5T can accumulate versions, future
epochs and eventually satisfy its own gate while 20T/40T/60T are still
collecting. No shorter-horizon outcome is borrowed by a longer horizon.

This preserves the staged learning architecture installed in Phase 5C.

## Technical completion versus empirical maturity

With Phase 5E installed, Phase 5 is technically complete:

1. 5A — immutable methodology, PIT/Purging and generic promotion contract;
2. 5B — prospective v2 evidence and Frozen Phase-4 Baseline;
3. 5C — progressive horizon-specific learning;
4. 5D — genuine future walk-forward epochs;
5. 5E — frozen adaptive shadow inference and automated promotion assessment.

The current evidence may still be `insufficient_evidence`. That is not an
unfinished implementation. It is the intended prospective state while real
5T/20T/40T/60T outcomes accumulate.

The automated report is published only to the isolated shadow branch as:

- `artifacts/research/confidence_vnext_promotion_5e.json`

No productive scanner artifact is changed.
