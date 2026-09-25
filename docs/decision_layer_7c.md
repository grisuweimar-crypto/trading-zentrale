# Phase 7C — Confirmation & Conflict Research

## Purpose

Phase 7C studies when admitted evidence confirms, contradicts or merely
contextualizes another piece of evidence. It remains a research layer and does
not compute Universal Stance, Portfolio Action, hysteresis or orders.

The frozen contract is:

`configs/decision_conflict_research_v1.json`

The historical research implementation is:

`src/scanner/research/decision_layer/conflict_research.py`

The prospective packet relation graph is:

`src/scanner/research/decision_layer/relation_graph.py`

## Discovery versus confirmation

Phase 7B froze the research boundary before 7C started:

- through 2026-09-25: `legacy_replay_spent`
- from 2026-09-26: `prospective_unspent`

7C may use the spent partition for discovery and characterization. A relation
selected or supported on that partition cannot be called independently
validated. Final confirmation requires genuinely later observations from the
prospective-unspent partition after their forward labels have matured.

This means Phase 7C can be implemented completely now while its promotion gate
remains deliberately closed.

## Evidence roles are not votes

The six evidence families keep their upstream semantics.

### Directional claims

Only Selection and Timing may provide an explicit directional claim.

A prospective directional claim participates in the relation graph only when:

- coverage is `available` or `limited`,
- PIT state is `verified` or `partial`, and
- maturity is `robust`, `directional_but_immature` or `not_applicable`.

An unavailable or PIT-invalid directional claim cannot create a confirmation or
conflict merely because it exists in a packet.

### Probability

Probability annotates a referenced Selection or Timing claim. It does not cast a
second vote in the same direction.

### Confidence

Confidence describes reliability of a referenced claim. It does not encode
attractiveness and does not cast a directional vote.

### Risk

Risk is non-directional context. Higher risk is not automatically a negative
thesis and lower risk is not automatically a positive thesis.

### Elliott

Module 6H remains research-only context under its integration contract. Elliott
cannot directly resolve a conflict or create a trade decision in 7C.

## Historical limitation

The historical 7B replay does not fabricate modern Probability-vNext,
Confidence-vNext or Elliott-6H objects for dates on which they did not exist.
Consequently the spent-history part of 7C can study:

- frozen Timing-pattern agreement/disagreement,
- Selection quality-band context,
- raw PIT Risk context,
- future peer-excess and path-risk outcomes.

It cannot honestly reconstruct a full six-family historical conflict graph.
Genuine cross-family Selection-vs-Timing relations with modern annotations begin
prospectively through archived 7A packets.

## Timing topology

For every 5T, 20T, 40T and 60T horizon, each historical row receives one of four
research states:

- `none` — no frozen Timing pattern matched,
- `positive_only` — one or more positive frozen patterns matched and no negative
  frozen pattern matched,
- `negative_only` — one or more negative frozen patterns matched and no positive
  frozen pattern matched,
- `mixed_conflict` — at least one positive and at least one negative frozen
  pattern matched.

The number of matching Timing patterns is diagnostic only. Patterns share atoms
and discovery history and therefore are not treated as independent votes.

`mixed_conflict` is a conflict state. It is not silently averaged to neutral.

## Pre-registered historical comparisons

Before running the 7C real-data analysis, the contract freezes eight comparisons:

- positive-only Timing versus no Timing match at 5T/20T/40T/60T, expected
  direction: positive peer excess;
- negative-only Timing versus no Timing match at 5T/20T/40T/60T, expected
  direction: negative peer excess.

The mixed-conflict state receives no expected return direction and remains a
characterization diagnostic.

Selection-band and Risk interactions are also reported, but no post-hoc weights
or thresholds are optimized from them in 7C.

## Statistical dependence

Daily rows with overlapping forward windows are not iid observations.

For the pre-registered comparisons, 7C therefore uses circular moving blocks of
observation dates with effective block length equal to two times the evaluated
forward horizon. Both compared groups are sampled with the same block sequence.
A robust interval fails closed unless both groups have support in at least two
time-separated regions.

A result can be labeled `discovery_supported` only when the pre-registered sign
is supported by the block-bootstrap mean-difference interval. Even then it
remains discovery support, not independent confirmation.

## Prospective relation graph

A validated 7A packet is mapped to a relation topology without resolving it.
Possible relation states include:

- `insufficient_directional_relation`,
- `single_direction_or_unopposed`,
- `correlated_same_family_support`,
- `cross_family_confirmation_present`,
- `conflict_present`.

Two same-direction Timing patterns create only
`same_family_correlated_support`; they are explicitly not independent votes.
Selection and Timing pointing in the same direction can form a
`cross_family_confirmation`, but 7C still does not convert that relation into a
stance.

Opposite eligible directional claims produce either:

- `within_family_conflict`, or
- `cross_family_conflict`.

Every conflict remains `resolved: false` in Phase 7C.

## Outputs

The historical CLI writes:

`artifacts/research/decision_conflict_7c.json`

The report contains, per horizon:

- topology coverage and outcome summaries,
- the eight pre-registered comparison results,
- mixed-conflict diagnostics,
- Selection quality-band context,
- continuous Risk/outcome diagnostics,
- Timing redundancy diagnostics,
- the closed prospective-confirmation promotion gate.

## Forbidden in 7C

Phase 7C must not produce:

- Universal Stance,
- Portfolio Action,
- BUY/HOLD/SELL,
- position size or target weight,
- order instructions,
- a weighted super-score,
- automatic conflict resolution.

No result from the spent partition may be promoted to production on its own.

## Run

```bash
python scripts/run_decision_conflict_7c.py
```

For validation/CI:

```bash
python scripts/run_decision_conflict_7c.py \
  --output /tmp/decision_conflict_7c.json
```

## Boundary to Phase 7D

Phase 7D may consume the relation topology and the empirically characterized
contexts when defining a portfolio-independent Universal Stance. It must not
reinterpret 7C discovery support as prospective validation, and it must preserve
the distinction between directional claims, annotations, context and missing
information.
