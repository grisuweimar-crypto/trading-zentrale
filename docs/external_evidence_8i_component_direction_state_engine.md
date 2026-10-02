# Phase 8I-D — Component Direction State Engine Preregistration

## Status

`FROZEN_OUTCOME_BLIND_DIRECTION_STATE_PREREGISTRATION_ONLY`

Phase 8I-D defines one narrow interface: how an already promoted, provenance-bound and exact frozen external model pair may later be represented as a standardized component direction state for Phase 8I-C aggregation.

8I-D is research-only and shadow-only. It does not activate real component direction generation, does not aggregate external evidence, does not change Phase 7, does not recompute reliability or stance, does not change portfolio actions and cannot create orders or trades.

The repository remains empirically blocked by the 8I-B upstream source-identity contract issue. There are currently no real bound external components that may enter the direction engine.

## Why a separate direction adapter is necessary

8I-C deliberately refused to infer `POSITIVE` or `NEGATIVE` from raw macro values, model coefficients, p-values or an invented threshold. Doing so would have introduced a new sign convention or threshold after upstream research.

8I-D therefore freezes one outcome-blind adapter before any 8I reliability outcome is opened.

The adapter does **not** ask whether a macro value is economically “good” or “bad”. It asks only whether the exact promoted challenger model moves the frozen `peer_excess_{H}t` prediction up or down relative to its exact paired frozen baseline for the same snapshot, symbol and horizon.

## Frozen formula

For an 8G main effect:

`prediction_delta = challenger_prediction - baseline_prediction`

For an 8H interaction:

`prediction_delta = interaction_challenger_prediction - main_effects_baseline_prediction`

The state mapping is exact and threshold-free:

- `delta > 0` -> `USABLE / POSITIVE`
- `delta < 0` -> `USABLE / NEGATIVE`
- `delta == 0` -> `UNKNOWN_VALID / UNKNOWN`

There is no deadband, rounding, magnitude threshold, confidence weighting or p-value rule. A later outcome-informed epsilon or threshold would be a new research design and cannot be added silently.

## Semantic limitation

The direction is `MODEL_PAIR_INCREMENTAL_PREDICTION_DIRECTION_NOT_CAUSAL_FACTOR_EFFECT`.

Baseline and challenger are separately fitted frozen Ridge models. Shared-feature coefficients may therefore differ between the pair. The delta is consequently the total prediction displacement of the exact paired challenger relative to its exact paired baseline; it is not an isolated causal coefficient attribution to the external factor or interaction.

`POSITIVE` does not mean a high interest rate, FX value or other raw external observation is intrinsically bullish. `NEGATIVE` does not mean the raw observation is intrinsically bearish. Neither state is a trade signal, confidence level or promotion-strength score.

## Model-pair lineage

8I-B's generic `component_artifact_hash` is not silently reinterpreted as a model-pair hash.

For a component to be direction-eligible in 8I-D, its existing 8I-B binding must explicitly have been created with the **exact frozen model-pair artifact** as its component artifact. Therefore:

`binding.component_artifact_hash == sha256(exact_frozen_model_pair_artifact)`

The embedded baseline and challenger model hashes must also verify individually.

This is a stricter downstream eligibility condition, not a retroactive modification of 8I-B. Existing or future 8I-B bindings to some other artifact remain valid for their original research-binding role but are not automatically direction-eligible.

Model substitution after binding is fail-closed.

## PIT and availability

Each direction row is tied to:

- exact component binding
- snapshot ID
- symbol
- horizon
- timezone-aware `generated_at`
- feature `valid_from`
- input PIT status
- exact target `peer_excess_{H}t`
- exact baseline/challenger model hashes
- exact model-pair artifact hash

`feature_valid_from` and the component binding's `valid_from` must not exceed the snapshot timestamp.

An `AVAILABLE` prediction is accepted only with `PIT_ELIGIBLE` input.

Unavailable inputs are preserved as `UNAVAILABLE` with a reason. They are never converted to zero or neutral. Non-finite or invalid model predictions also become `UNAVAILABLE` rather than producing a sign.

## Outcome boundary

Runtime direction generation may not receive realized/forward decision outcomes or decision fields, including:

- observed peer excess
- realized or forward return
- adverse excursion
- path max drawdown
- loss improvement
- p-values or Holm-adjusted p-values
- confidence intervals
- Phase-7 stance
- portfolio action / position information

The upstream training and promotion outcomes used to create and validate the frozen models remain spent/upstream evidence. Hash verification of those artifacts is not a new independent 8I confirmation.

## 8I-C compatibility

The generated component annotation uses the exact 8I-C vocabulary:

- `USABLE` with `POSITIVE` or `NEGATIVE`
- `UNKNOWN_VALID` with `UNKNOWN`
- `UNAVAILABLE` with no direction and an explicit reason

It carries the required direction-adapter identity and `direction_valid_from` and contains no top-level portfolio-action field. Synthetic contract tests pass a generated 8I-D row directly into the frozen 8I-C synthetic aggregator.

Real aggregation remains disabled.

## Current fail-closed state

Until the upstream 8I-B source-identity blocker is resolved and real components are correctly rebound with exact frozen model-pair artifacts:

- real bound component IDs: `[]`
- real direction generation: disabled
- external direction: `INSUFFICIENT_EXTERNAL`
- external evidence state: `INSUFFICIENT_EXTERNAL`
- Phase-7 decision effect: `NO_CHANGE_TO_PHASE7_DECISION`

This is expected and is not an empirical failure of external evidence.

## Completion boundary

8I-D is technically complete only after contract, engine, tests, documentation and CI all pass.

Technical completion does not authorize real direction generation and does not authorize a reliability or stance change.

The next separately started block is `8I-E_RELIABILITY_EXTENSION_RESEARCH`.
