# Pattern Discovery Lab v2 — L13 Decision-Layer Challenger Integration

L13 compares explicitly promoted Pattern Discovery v2 evidence with the existing Decision Layer in a **shadow / challenger** path.

It does not modify the productive Decision Layer.

## Core architecture

The existing Decision Layer remains the immutable baseline.

For every symbol, snapshot and horizon L13 builds a separate trace:

1. validate the existing 7A Decision packet
2. read the existing 7C relation topology
3. read the existing 7D portfolio-independent decision context
4. validate exact L12 Pattern admissions
5. require the current L12 promotion registry state to still be `ADMITTED`
6. revalidate the L12 L5/L6/L9/L10 evidence bindings
7. attach only same-snapshot L7 Pattern claims
8. collapse correlated Pattern claims using the L6 dependency graph
9. compare existing Timing evidence with the Pattern challenger
10. persist a research-only shadow trace

The original Decision packet is never rewritten.

## Horizon separation

5T / 20T / 40T / 60T are evaluated separately.

A trace has exactly one horizon. Pattern claims from other horizons are not included.

Different Pattern target/baseline scopes may not be fused inside one trace.

## Relation Graph

L13 adds a challenger relation view without changing the existing 7C graph.

The graph contains:

- existing eligible Timing evidence for the selected horizon
- admitted Pattern Discovery v2 claims
- L6 dependency relations between active Pattern versions
- Pattern evidence clusters
- Pattern-vs-baseline directional relation

There is no numeric vote count.

## Dependency-aware Evidence Fusion

Patterns connected by:

- `DUPLICATE`
- `NESTED`
- `HIGH` dependency
- `CRITICAL` dependency

are collapsed into one evidence cluster.

Two highly correlated Patterns therefore cannot become two independent confirmations.

A cluster with opposite Pattern directions is `conflicted`.

Independent clusters are still not converted into a numeric voting system.

## Shadow states

L13 keeps three separate views:

### Existing Timing baseline

The frozen existing Timing evidence for the selected horizon.

### Pattern challenger

The dependency-aware direction implied only by currently admitted and currently matching Pattern Discovery v2 evidence.

### Shadow fused Timing state

A conservative relation view.

- same direction → corroboration
- opposite direction → conflicted
- Pattern conflict → conflicted
- existing Timing conflict stays conflicted
- existing Timing insufficient + one Pattern direction → shadow direction

This fused state cannot change an action.

## Ablation / incremental value

The empirical L13 comparison is explicitly:

`EXISTING_TIMING_VS_EXISTING_TIMING_PLUS_PROMOTED_PATTERNS`

For clean attribution the paired statistical comparison uses:

- existing Timing direction as baseline
- dependency-aware Pattern-only direction as challenger
- same-snapshot mature L8 outcome
- same target
- same baseline
- same horizon

This separates Pattern value from the safe conflict-preserving shadow fusion.

Primary metrics:

- Directional Hit Rate Lift
- Mean Aligned Outcome Lift

L13 also reports:

- paired directional N
- disagreement N
- added-direction N
- conflicts created
- immature traces
- temporal support regions
- deterministic moving-block bootstrap intervals

## Positive evidence

`INCREMENTAL_VALUE_CANDIDATE` requires all predeclared conditions:

- minimum paired N
- minimum disagreement N
- minimum temporal support
- positive hit-rate lift
- positive aligned-outcome lift
- lower 95% bootstrap bound above zero for both lifts

Otherwise the result is:

- `INSUFFICIENT_SUPPORT`, or
- `NO_INCREMENTAL_VALUE`

A positive L13 result is still **not** regular integration.

## Conflict handling

Pattern count can never overrule an existing conflict.

Pattern count can never resolve Pattern-vs-Timing conflict.

The conflict is preserved as a research result.

## Promotion / rollback binding

L13 accepts a Pattern only if:

- L12 decision status is `ADMITTED`
- Integration Mode is `SHADOW_CHALLENGER`
- Directional Authority is `SHADOW_ONLY`
- promotion registry current state is still `ADMITTED`
- current L12 revalidation returns `CURRENT`

DEMOTE / ROLLBACK or stale L12 evidence immediately blocks further challenger use.

## Productive boundary

L13 does not change:

- Scanner Score
- existing 7A evidence packet
- existing 7C relation graph
- existing Decision state
- state transitions / hysteresis
- portfolio action
- Depot Watch action
- execution

Even `INCREMENTAL_VALUE_CANDIDATE` leaves regular integration closed:

`SEPARATE_POSITIVE_REVIEW_REQUIRED`

## Definition of Done

- Shadow / Challenger is separate from productive Decision behavior
- relation graph is explicit
- correlated Pattern evidence is not double-counted
- no vote counting
- same-snapshot / same-horizon PIT binding is enforced
- Pattern-vs-existing-Timing ablation is explicit
- incremental value requires prospective mature outcomes
- conflict cases remain visible
- positive L13 evidence does not auto-integrate
