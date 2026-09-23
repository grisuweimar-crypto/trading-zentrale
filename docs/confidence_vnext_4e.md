# Phase 4E — prospective Confidence-vNext shadow validation

## Purpose

Phase 4E does **not** create a new production Confidence score. It creates the prospective evidence stream required to answer the only question that matters before any mapping is allowed:

> Do the pre-specified Phase-4 evidence/agreement states actually make later model claims more reliable?

The Phase-2/3 validation period is already spent. It is therefore not reused to select Phase-4 weights, thresholds or a scalar mapping. Phase 4E starts a new, unspent, prospective stream.

## Freeze rule

Every complete scanner publication creates an immutable claim snapshot. A claim is keyed by `snapshot_id + symbol + horizon_sessions`. The claim stores the contemporaneous Phase-4 states plus provenance/fingerprints of the exact Phase-2, Phase-3 and Phase-4 research artifacts used.

A later rerun may be idempotent, but it may not reinterpret the same historical snapshot. If the same natural key produces a different claim payload, the recorder fails closed.

The claims archive intentionally does **not** contain the raw numerical scanner Score as a Confidence-strength field. Selection is represented only by the already defined B0–B5 backbone state/evidence.

## Claim fields

The compact claim archive stores, among other provenance fields:

- `as_of`, `generated_at`, `run_id`, `snapshot_id`
- `symbol`, original `currency`, `horizon_sessions`
- `evidence_version`
- exact SHA-256 fingerprints for Phase-4 report, Phase-2 report, Phase-3 report and optional risk-scale audit
- Selection band/state/direction
- Timing state/direction/matched frozen patterns
- downside Risk state
- Model Agreement state/conflicts
- pre-specified return-claim direction when one unambiguous mature return direction exists
- claim-specific Data Quality proxy states
- current volatility applicability state

Crypto remains outside this stream until non-stock evidence is independently validated.

## Outcomes are separate and later

Claims are never overwritten with future information. Matured outcomes are written to a separate append-only archive keyed by `claim_id`.

For each horizon (5/20/40/60 sessions), Phase 4E records after maturity:

- official adjusted-close forward return using the validated daily price-history session arithmetic
- leave-one-symbol-out same-currency peer median with global peer fallback when no same-currency peer exists, matching the established research convention
- peer excess
- sign-normalized peer excess and directional hit only when the frozen claim had one unambiguous return direction
- future adverse excursion
- future path maximum drawdown

The price outcome is a daily research target, not an execution-price/PnL simulation.

## Pre-specified reliability questions

### Return reliability

For claims with an unambiguous return direction, test whether `compatible` multi-model states show better sign-normalized peer-excess reliability than `single_model` states.

Metrics include directional hit rate, mean/median sign-normalized peer excess and dispersion. No threshold is tuned from the same prospective stream.

### Risk reliability

For positive return claims, test whether pre-specified Risk tension is followed by larger adverse excursion / path max drawdown than comparable positive claims without elevated validated downside risk.

Low risk is never treated as an extra positive-return vote.

### Data Quality

Test whether complete claim-specific provenance/presence states are associated with lower prediction error / fewer unevaluable claims than partial or insufficient states. Missing evidence remains unknown/insufficient, never neutral.

## Dependence and uncertainty

Overlapping outcomes are not independent. Final inference must reuse the corrected Phase-2/3 method:

- circular moving observation-date blocks
- effective block length = `2 × horizon`
- all rows from one observation date stay together
- fixed state/group membership before resampling
- at least two time-separated support regions required
- otherwise robust uncertainty is `None`

IID intervals may be shown only as diagnostics and may not drive a validation decision.

## What Phase 4E cannot do yet

At the moment the stream starts, there are no unspent 5T/20T/40T/60T outcomes. Therefore Phase 4E must initially report `collecting_prospective_evidence`.

It is explicitly forbidden to:

- backfill Phase-4 states into older scanner history using today's evidence
- reuse the spent Phase-2/3 holdout to select a Confidence mapping
- invent a 0–100 score before prospective reliability is demonstrated
- invent HIGH/MED/LOW thresholds
- convert the 2026-09-17 volatility scale break with a guessed factor

## Publication model

Phase 4E runs separately after a successful `Scanner_vNext Autopilot` publication. It reads the just-published research bundle, generates the current guarded Phase-4B–D registry, appends immutable shadow claims, matures any prior claims whose future sessions are now available, and commits only the Phase-4E shadow artifacts.

Because it is a separate workflow, a Phase-4E failure cannot invalidate or block the productive scanner publication.
