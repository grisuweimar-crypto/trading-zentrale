# Pattern Discovery Lab v2 — L12 Promotion Gate

L12 defines the explicit transition from governed Pattern Discovery evidence into the later integration architecture.

It does **not** activate a Pattern in the productive Decision Layer. That starts, if ever, only after L13 challenger validation.

## Core rule

A Pattern rating of **B** or **A** is necessary for the v1 gate, but never sufficient for promotion.

The sequence is:

1. exact frozen L5 Pattern version
2. current L10 rating history
3. terminal, prospectively supported L9 confirmation
4. current L6 dependency graph
5. explicit L12 review
6. explicit APPROVE / REJECT / DEFER decision
7. only an APPROVE creates an admission contract
8. even an admitted Pattern still requires L13 before any Decision-Layer effect

## Promotion eligibility

An L12 review is `ELIGIBLE_FOR_REVIEW` only when:

- current L10 rating is B or A;
- the latest terminal L9 confirmation for that exact Pattern version is `SUPPORTED`;
- the exact Pattern version exists in the supplied L6 dependency graph;
- the Pattern is not a Modifier/Context-Modifier requiring a separate future admission contract;
- the requested dependency handling is compatible with the observed L6 dependency state.

If any gate fails, the review remains auditable but is `BLOCKED`.

## Separate review

`build_promotion_review()` only creates an immutable OPEN review.

It never promotes automatically.

An explicit call to `record_promotion_decision()` is required with:

- decision: APPROVE / REJECT / DEFER
- reviewer identity / role
- review timestamp
- reason codes

Blocked reviews cannot be approved, but REJECT/DEFER decisions remain retainable as negative governance evidence.

## Exact admitted Pattern version

An APPROVE decision binds:

- Pattern ID
- Pattern Version
- Pattern Spec Hash
- Pattern Family
- Pattern Type
- Direction
- Target
- Horizon
- Baseline
- L5 frozen-record hash
- L10 rating-history hash
- L9 confirmation-look hash
- L6 dependency-graph hash

A different Pattern version is not covered by the admission.

## Integration mode

L12 v1 deliberately permits only:

- `ANNOTATION_ONLY`
- `SHADOW_CHALLENGER`

Directional authority is therefore limited to:

- `NONE`
- `SHADOW_ONLY`

No productive Timing Evidence is created in L12.

Every admission contains:

- `productive_timing_evidence_active = false`
- `decision_layer_effect_active = false`
- `requires_l13_before_activation = true`

## Probability attachment

Probability is attached only from the terminal supported **L9 prospective evidence**.

The attachment carries:

- Direction Probability
- Baseline Probability
- Probability Advantage / Lift
- Mean Aligned Outcome
- Effect Size vs Baseline
- Effective-N
- Support Regions
- robust uncertainty intervals

Discovery Probability is never substituted and no recalibration is performed.

## Dependency handling

L6 dependency information must be reviewed explicitly.

Allowed handling modes:

- `INDEPENDENT_IF_NO_HIGH_DEPENDENCY`
- `PRIMARY_REPRESENTATIVE`
- `CORRELATED_NO_COUNTING`
- `ANNOTATION_ONLY`

HIGH/CRITICAL dependency or a DUPLICATE relation blocks the independent mode.

All admission contracts carry `vote_counting_allowed = false`. L12 therefore cannot implement “2 positive Patterns beat 1 negative Pattern”.

## Conflict handling

L12 does not perform cross-Pattern directional fusion.

The available modes are:

- `DEFER_TO_L13_RELATION_GRAPH`
- `ANNOTATION_ONLY`

Actual conflict analysis belongs to L13.

## Modifier contract

Directional and Modifier semantics remain separate.

A Pattern whose type is a Modifier/Context-Modifier is blocked by L12 v1 and requires a future dedicated Modifier admission contract. A Modifier can never be silently counted as another directional vote.

## Reversibility / stale admission

An admission is not permanently valid.

`validate_admission_current()` fails closed with `STALE_REVIEW_REQUIRED` when any bound authority changes, including:

- L10 rating history
- terminal L9 confirmation evidence
- L6 dependency graph

Thus a former B/A review cannot remain silently active after materially changed evidence.

The append-only Promotion Registry additionally supports explicit:

- `DEMOTE`
- `ROLLBACK`

with controlled reason codes and evidence hash.

Downstream consumers must check both:

1. current Promotion Registry state is ADMITTED;
2. `validate_admission_current()` returns CURRENT.

## Persistence

Reviews are immutable under:

`artifacts/research/pattern_discovery/promotion/reviews/{pattern_id}/{pattern_version}/{review_id}.json`

Decisions and reversals are hash-chained in:

`artifacts/research/pattern_discovery/promotion/promotion_registry.jsonl`

Runtime writes remain inside the L0 Pattern Discovery research namespace.

## L12 boundaries

L12 does not:

- mutate L5 Pattern specs;
- mutate or recompute L9 confirmation;
- mutate L10 ratings;
- change Scanner Score;
- change Timing;
- change Universal Stance;
- create Portfolio Action;
- execute orders;
- integrate into the productive Decision Layer.

## Definition of Done

The Masterplan L12 DoD is implemented as follows:

- **no automatic promotion through rating alone** — OPEN review plus explicit decision required;
- **only exact Pattern version admitted** — ID/version/spec hash are bound and verified;
- **productive integration reversible** — stale fail-closed validation plus DEMOTE/ROLLBACK registry;
- **regression tests prevent leakage / double counting** — L0–L12, L9/L10/L11 seals, QM-C, Price/Session and QM-B gates run together.
