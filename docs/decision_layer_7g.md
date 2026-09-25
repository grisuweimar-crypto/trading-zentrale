# Phase 7G — Reliability & Explainability

Phase 7G explains an already computed Decision-Layer state. It does **not** create a new stance, resolve a conflict, modify a 7E transition, change the 7F action, size a position, or generate an order.

## Purpose

The layer answers five questions for one snapshot:

1. Which directional evidence supports the preserved 7D stance?
2. Which evidence conflicts with it, is directionally unknown, or is ineligible?
3. Which Probability/Confidence annotations and Risk/Elliott context exist without counting as votes?
4. Which coverage, maturity, PIT, integration, cost, P/L, or capacity gaps limit interpretation?
5. Which future source-state changes would require 7D/7E/7F to be recomputed?

## Required inputs

7G consumes four already validated records from the same Decision-Layer snapshot:

- `decision_layer_input_contract_v1` — full 7A evidence packet
- `decision_universal_stance_v1` — preserved 7D stance and evidence topology
- `decision_state_transition_v1` — preserved 7E transition state
- `decision_portfolio_action_v1` — preserved 7F portfolio action and swing-management state

`symbol`, `as_of`, and `source_snapshot_id` must match across all four inputs. The 7D coverage must match the 7A packet; the 7E raw stance must match 7D; and 7F must contain the same stance and transition context. Any mismatch fails closed.

## Reliability semantics

7G deliberately does not create a numeric reliability score. Version 1 exposes one structural state:

- `blocked_conflict`
- `blocked_insufficient`
- `pending_confirmation`
- `provisional_cross_family_support`
- `provisional_same_family_support`
- `provisional_unopposed_support`

These are **descriptive structural states**, not estimated success probabilities or validated trading-edge rankings.

`provisional_cross_family_support` means independent evidence families are directionally aligned in the 7D topology. It does not mean the downstream action has been prospectively validated. Same-family timing support remains explicitly correlated rather than independent.

## Explainability output

The output preserves the current decision path:

`7D Universal Stance -> 7E Transition -> 7F Portfolio Action`

It then lists:

- supporting directional claims;
- counter-directional claims, where present;
- positive and negative conflict sides separately;
- eligible claims whose direction is unknown;
- ineligible directional claims with the recorded eligibility reason;
- Probability and Confidence annotations as non-votes;
- Risk and Elliott context as non-votes;
- support relations and unresolved conflicts from 7D;
- missing or limited evidence families and claims.

For compact traceability, source-reported scalar payload facts may be copied into the explanation. They are not reinterpreted or reweighted by 7G.

## Reliability dimensions

The output also preserves several non-aggregated dimensions:

- 7A admission/coverage state;
- directional PIT profile;
- directional maturity profile;
- directional coverage profile;
- 7D/7E validation status;
- whether 7E hysteresis, 7F portfolio-action logic, and the 7F swing-action edge are empirically validated.

Because those downstream rules are still research-only, 7G remains research-only and promotion remains closed.

## Change triggers

7G distinguishes two classes of trigger.

### Decision-change triggers

These describe source changes that would require upstream recomputation, for example:

- a pending 7E direction reaching its required confirmation depth;
- a future 7D conflict becoming directional;
- first eligible directional evidence appearing after an insufficient state;
- the opposite direction becoming confirmed;
- a valid Elliott reduction/add context appearing or disappearing while a positive long thesis remains confirmed;
- explicit add capacity changing.

A trigger never changes the current decision directly. It states `requires_recompute_from_source=true`.

### Information-completion triggers

These improve explanation quality or enable later analysis without changing direction by themselves, for example:

- transaction-cost data becoming available;
- entry/current prices becoming available for P/L context;
- add capacity becoming explicit;
- a currently missing evidence family becoming PIT-valid and admissible.

## Hard guards

Phase 7G must never:

- recompute or overwrite 7D;
- resolve a 7D conflict;
- recompute or overwrite 7E;
- replace or modify the 7F action;
- invent a new action;
- infer neutral from missing evidence;
- count Probability or Confidence as directional votes;
- count Risk or Elliott as directional votes;
- calculate position size or target weight;
- generate a buy/sell signal or broker order;
- claim a numeric reliability probability.

Every explanation receives a SHA-256 `explanation_id` over the complete unsigned output. The validator recomputes it, so post-build modification is detected.

## Current status

7G is technically testable but remains research-only. `reliability_model_empirically_validated=false`, `productive_integration_enabled=false`, `execution_allowed=false`, and `promotion_eligible=false` remain mandatory until future mature prospective outcomes justify a separate promotion decision.
