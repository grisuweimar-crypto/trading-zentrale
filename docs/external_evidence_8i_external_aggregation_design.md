# Phase 8I-C — Outcome-Blind External Aggregation Design

## Status

8I-C is a **research-only, shadow-mode design freeze**. It does not enable real external direction generation, real component aggregation, extended reliability, extended stance, portfolio-action changes, production integration, or orders/trades.

The frozen parent is Phase 8I-B (`BOUND_FOR_8I_RESEARCH_ONLY` maximum scope). The current repository remains blocked by the open 8I-B upstream source-identity contract issue, so there are currently no real components available to aggregate.

## Purpose

8I-C answers one narrow question before any real 8I outcome is read:

> If multiple independently promoted and provenance-bound external components later exist, how are their already-standardized direction states combined without inventing weights, majority voting, double-counting interactions, or changing the frozen Phase-7 decision?

## What 8I-C deliberately does not define

8I-C does **not** decide how a raw factor value, model coefficient, p-value, challenger prediction, or interaction output becomes `POSITIVE` or `NEGATIVE`.

That conversion requires a separately versioned, outcome-blind direction adapter. Real direction-state generation remains `NOT_YET_FROZEN` and is deferred to the next subblock. This prevents a hidden sign convention or threshold from being chosen after seeing decision outcomes.

## Input unit

Aggregation is strictly per:

- `snapshot_id`
- `symbol`
- `horizon_sessions`

Cross-horizon voting is forbidden. Date-only joins are forbidden.

Every component must carry an exact 8I-B binding receipt with a valid `binding_sha256` and state `BOUND_FOR_8I_RESEARCH_ONLY`.

Synthetic 8I-C contract tests additionally require a versioned direction-adapter identity and a timezone-aware `direction_valid_from <= generated_at`.

## Component observation states

A bound component presented to the aggregator can be:

- `USABLE` with direction `POSITIVE` or `NEGATIVE`
- `UNKNOWN_VALID` with direction exactly `UNKNOWN`
- `UNAVAILABLE`, with an explicit exclusion reason and no directional value

`UNKNOWN_VALID` is never silently dropped or rewritten as neutral. `UNAVAILABLE` is excluded with a reason; it is never zero-filled or treated as a neutral vote.

## Dependency graph

8G main effects are root nodes. 8H interactions are dependent child nodes.

An interaction requires its exact parent main-effect roots at the same horizon. Orphan interactions fail closed.

All factors connected by one or more interactions form one dependency bundle. Overlapping interactions merge their connected bundles. This makes the dependency structure explicit and prevents the same interaction from being counted once for itself and again through its parents.

## Set reducer

There are no numeric votes and no weights. The reducer uses only the set of directional states.

Rules, in order:

1. No usable or valid-unknown evidence -> `INSUFFICIENT_EXTERNAL`
2. Both `POSITIVE` and `NEGATIVE` are present -> `MIXED`
3. `UNKNOWN` is present and no known sign conflict exists -> `UNKNOWN`
4. Only `POSITIVE` is present -> `POSITIVE`
5. Only `NEGATIVE` is present -> `NEGATIVE`

Multiplicity and input order do not change the result.

Example: two positive parents plus one negative interaction are **not** a 2:1 positive majority. They form one connected dependency bundle containing both signs, therefore the bundle state is `MIXED`.

## Two-stage aggregation

The same reducer is used twice:

1. reduce components inside each dependency bundle;
2. reduce the resulting bundle states into one horizon-specific external direction state.

A `MIXED` bundle contributes both known signs. An `UNKNOWN` bundle contributes `UNKNOWN`. An insufficient bundle is excluded with its diagnostic reason.

## Relation to Phase 7

The external direction is computed independently of the Phase-7 core state.

Afterward, an optional descriptive relation can be looked up using the frozen `external_conflict_matrix_v1`:

- `CONFIRMING`
- `CONFLICTING`
- `EXTERNAL_ONLY`
- `MIXED_EXTERNAL`
- `UNKNOWN`
- `INSUFFICIENT_EXTERNAL`

This relation remains descriptive, not policy. `CONFIRMING` cannot promote a Phase-7 stance, `CONFLICTING` cannot demote it, and `EXTERNAL_ONLY` is not a trade signal.

## Current fail-closed state

Current repository state:

- 8I-B upstream source-identity blocker: open
- bound real external components: none
- real direction adapter: not frozen
- real aggregation: disabled
- external direction: `INSUFFICIENT_EXTERNAL`
- decision effect: `NO_CHANGE_TO_PHASE7_DECISION`

The Phase-7 core remains fully available.

## Technical completion criteria

8I-C is technically complete only when:

- the contract is committed;
- the synthetic aggregation implementation is committed;
- set-reducer, dependency-graph, PIT, outcome-boundary and fail-closed tests pass;
- documentation is committed;
- CI passes;
- real aggregation remains disabled.

Technical completion does not grant empirical decision influence.
