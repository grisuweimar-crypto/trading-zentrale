# Phase 5B – Prospective v2 / Frozen Baseline

## Purpose

Phase 5B creates the first prospective evidence schema that can later support a certified Frozen-Baseline evaluation. It does **not** create adaptive weights, a scalar 0–100 Confidence score, HIGH/MED/LOW thresholds, production signals, portfolio actions, or trading decisions.

Phase-4E `phase4e_shadow_v1` remains immutable and audit-only. Phase 5A established that v1 does not integrity-bind two items required for Phase 5:

1. claim-specific Phase-4C Timing/Risk Statistical Context;
2. the exact number of market sessions used to mature a declared 5T/20T/40T/60T outcome.

Those gaps are not backfilled. V2 begins only for scanner publications whose exact `main` commit already contains the Phase-5B contract.

## V2 claim contract

Every v2 claim is tied to the exact scanner publication commit and freezes:

- snapshot / run / symbol / original currency / horizon;
- claim-time start market session and adjusted-close audit value;
- the five-session cooldown context available at claim time;
- Phase-4 evidence fingerprints and source `as_of` markers;
- Selection Statistical Context;
- every currently matched frozen Timing pattern together with its empirical evidence state (`robust`, `directional_only`, `immature`, `mixed`, `unavailable`) without imposing an ordinal ranking;
- counts of Timing patterns that could not be evaluated because prerequisites were missing;
- Risk feature evidence state, claim-time feature value, cutoffs and claim level;
- the frozen Phase-4 Timing/Risk model states, Data-Quality states and Model-Agreement state.

The complete Statistical Context receives its own SHA-256 fingerprint and is also included in the immutable claim hash. Raw Scanner Score is not turned into Confidence strength.

## V2 outcome contract

A v2 outcome is append-only. It is matured only after the target market-session day has completed. The calculation takes the exact frozen start session and advances by the declared horizon inside one freshly evaluated adjusted-price history.

The outcome integrity-binds:

- `elapsed_market_sessions == horizon_sessions`;
- `path_session_count == horizon_sessions + 1`;
- start and end market dates;
- SHA-256 of the ordered session-date path;
- SHA-256 of the ordered `(session date, adjusted close)` path;
- adjusted-price return;
- adverse excursion;
- path max drawdown.

This closes the horizon-provenance gap identified in Phase 5A without rewriting v1.

## Publication isolation

Phase 5B does not publish Research artifacts to `main`. Its workflow waits for successful Phase-4E publication, binds the oldest v1 scanner run that has not yet received a v2 claim, resolves the exact historical `main` scanner-publication commit, and only processes that commit when it already contains the Phase-5B contract.

V2 artifacts are added to the same isolated `phase4e-shadow-data` branch alongside, but not instead of, the existing v1 files:

- `artifacts/research/confidence_vnext_shadow_claims_5b_v2.csv`
- `artifacts/research/confidence_vnext_shadow_outcomes_5b_v2.csv`
- `artifacts/research/confidence_vnext_frozen_baseline_5b.json`

The publication step shares the `phase4e-shadow-publication` concurrency group so Phase-4E and Phase-5B cannot race while pushing the isolated data branch.

## Five-session spacing

The inherited 5-session event spacing remains fixed. V2 freezes, for each claim, the current start session plus the preceding four market sessions. The Frozen Baseline can therefore apply spacing from claim-time information rather than reconstructing a later calendar.

Five-session spacing is not interpreted as independence for 20T/40T/60T outcomes.

## Frozen Baseline evaluator

The Phase-4 architecture remains the non-adaptive reference. The evaluator derives peer outcomes only from matured v2 claims and evaluates each horizon separately.

Reported diagnostics include:

- directional hit rate and direction error rate;
- mean / median / dispersion of signed peer excess;
- adverse excursion;
- path max drawdown;
- symbol, snapshot and observation-date support;
- exact State-group summaries for Selection Statistical Context, Timing matched evidence states, Risk feature evidence states, Data Quality, Model Agreement, Timing model state and Risk model state;
- temporal segment summaries.

State labels are **not** presumed to be correctly ordinal. Phase 5B observes their prospective reliability; it does not rank or remap them in advance.

## Robust uncertainty

The inherited uncertainty contract is unchanged:

- circular moving observation-date blocks;
- full date clusters stay together;
- effective block length `2 × horizon`;
- membership is fixed before resampling;
- at least two time-separated support regions are required;
- otherwise robust intervals are `None`.

IID Wilson/Beta/binomial calculations are not substituted for robust evidence.

## Calibration limitation

The frozen Phase-4 state does not expose a single claim-level forecast probability suitable for a Brier/calibration-error claim. Phase 5B therefore reports directional reliability diagnostics but explicitly does not label them probability calibration error. Creating a new probability model would exceed the Frozen-Baseline scope.

## Status and next gate

At merge, v2 has no legitimate pre-merge history by design. Evidence must accumulate prospectively. 5T will mature first; 20T, 40T and 60T must not be accelerated or inferred from shorter horizons.

Phase 5C may start only from completed, earlier v2 walk-forward evidence and must remain separated from its subsequent evaluation periods. Phase 5B itself performs no adaptive learning and no production change.
