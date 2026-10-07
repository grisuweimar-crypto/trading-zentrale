# Pattern Discovery Lab v2 — L12 Promotion Gate

L12 defines the explicit transition from the research laboratory into the later evidence-integration architecture. It does **not** activate productive Decision Layer behavior.

## Core rule

**Rating alone never promotes a Pattern.**

A B- or A-rated Pattern may become eligible for a separate L12 review only when the exact frozen Pattern version is still supported by the latest applicable terminal L9 confirmation result and its dependency state can be handled explicitly.

The admitted identity is always:

- Pattern ID
- Pattern Version
- Pattern Spec Hash

No family-wide or name-only admission exists.

## Promotion Eligibility

The v1 gate requires:

- L10 rating B or A
- latest terminal applicable L9 result = `SUPPORTED`
- explicit L6 Dependency Graph binding
- explicit Integration Mode
- explicit Dependency Handling
- explicit Conflict Handling
- non-Modifier directional Pattern

If any requirement is missing, the review is persisted as `BLOCKED`. No automatic approval occurs.

## Separate Review

Every review is a standalone immutable artifact.

Possible decisions:

- `APPROVE` → `ADMITTED`
- `REJECT` → `REJECTED`
- `DEFER` → `DEFERRED`

APPROVE is only valid for an eligible review and always requires explicit reviewer identity, role, timestamp and reason codes.

## Integration Mode and Directional Authority

L12 permits only:

- `ANNOTATION_ONLY` → Directional Authority `NONE`
- `SHADOW_CHALLENGER` → Directional Authority `SHADOW_ONLY`

Even an admitted Pattern has:

- no productive Timing evidence
- no Decision Layer effect
- no Portfolio Action effect
- no Execution authority

Actual challenger comparison belongs to L13.

## Probability Attachment

Probability is attached only from the latest terminal `SUPPORTED` L9 prospective evidence:

- Direction Probability
- Baseline Probability
- Probability Advantage Lift
- Mean Aligned Outcome
- Effect Size vs Baseline
- Effective-N
- Support Regions
- robust uncertainty

Discovery probability is never substituted and no recalibration is performed in L12.

## Dependency Handling

L12 does not count Patterns as votes.

Allowed handling modes include:

- `INDEPENDENT_IF_NO_HIGH_DEPENDENCY`
- `PRIMARY_REPRESENTATIVE`
- `CORRELATED_NO_COUNTING`
- `ANNOTATION_ONLY`

HIGH/CRITICAL dependency or a DUPLICATE relationship blocks the independent mode.

## Conflict Handling

L12 only records the declared conflict policy. It does not fuse conflicting directional claims.

The normal path is:

`DEFER_TO_L13_RELATION_GRAPH`

Actual dependency-aware evidence fusion belongs to L13.

## Modifier Contract

Modifier/Context-Modifier Patterns are not admitted through the ordinary directional contract. They require a separate future Modifier admission contract and are never counted as an extra directional vote.

## Revalidation

An admission is bound to the exact evidence state used during review:

- frozen record hash
- L10 rating history hash
- terminal L9 look hash
- L6 graph hash

If any binding changes, `validate_admission_current` returns:

`STALE_REVIEW_REQUIRED`

This prevents an old B/A approval from remaining silently active after rating degradation, new confirmation evidence, or dependency changes.

## Rollback / Demotion

Promotion decisions are stored in an append-only hash-chained registry.

An admitted Pattern can be reversed only through an explicit:

- `DEMOTE`
- `ROLLBACK`

event with allowed reason codes and an evidence hash.

The original approval remains in history.

## Productive boundary

L12 never writes productive Scanner/Timing/Decision artifacts and never changes:

- Scanner Score
- Timing
- Universal Stance
- Portfolio Action
- Execution

L13 must perform the separate Shadow / Challenger integration and incremental-value evaluation.

## Definition of Done

- no automatic Promotion from rating
- only an exact Pattern version can be admitted
- admission is explicitly reversible
- Dependency and Conflict handling are recorded
- Modifier is not miscounted as directional evidence
- Probability comes only from prospective L9 evidence
- stale bindings fail closed
- regression tests prevent leakage and double counting
