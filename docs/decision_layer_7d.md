# Phase 7D — Universal Stance

## Purpose

Phase 7D is the first Phase-7 component that produces an interpretation rather
than only admitting or studying evidence. Its output answers one deliberately
portfolio-independent question:

> What is the current directional interpretation of this security from the
> admitted evidence, before knowing whether the user owns it?

The frozen contract is:

`configs/decision_universal_stance_v1.json`

The implementation is:

`src/scanner/research/decision_layer/universal_stance.py`

The CLI is:

`scripts/run_universal_stance_7d.py`

Phase 7D remains research-only. It does not produce Portfolio Action, position
size, target weight, hysteresis or an order instruction.

## Why 7D is deterministic rather than another model

Phase 7C showed how admitted claims relate to each other. The current data does
not justify inventing optimized weights for Selection, Timing, Probability,
Confidence, Risk and Elliott. Several of those families are not directional
votes at all, and the modern six-family evidence set did not historically exist
for the spent replay period.

7D therefore does not train a super-score. It performs a semantic interpretation
of the already-typed claims and the 7C relation graph.

Only Selection and Timing possess directional authority. Probability and
Confidence annotate claims. Risk and Elliott provide context. Their presence can
be exposed to later explainability, but none may silently create another vote.

## Universal Stance states

Version 1 emits four states:

- `positive` — every eligible directional claim with known direction points
  positive and there is no conflict;
- `negative` — every eligible directional claim with known direction points
  negative and there is no conflict;
- `conflicted` — eligible positive and negative claims coexist;
- `insufficient_evidence` — no eligible directional claim has a known direction.

There is deliberately no emitted `neutral` state in v1. Missing evidence is not
neutral evidence, and unresolved conflict is not neutral either. A future neutral
state may be added only if an upstream evidence contract defines explicit neutral
directional semantics.

## Relation topology is preserved

The same positive or negative stance can have different evidence structures.
7D records that structure rather than turning it into a numeric strength score:

- `cross_family_confirmation` — Selection and Timing support the same direction;
- `same_family_correlated_support` — multiple same-family claims support the same
  direction, but are not independent votes;
- `single_direction_or_unopposed` — at least one eligible known-direction claim
  exists without an opposing eligible claim;
- `unresolved_conflict` — opposing eligible claims exist;
- `insufficient_directional_relation` — no usable known direction exists.

These are structural descriptions, not rating tiers.

## Conflicts

7D does not resolve a conflict by counting claims, averaging directions or using
Risk as a tiebreaker. `conflict_present` from 7C maps directly to
`universal_stance.state = conflicted`.

This is intentional. A later evidence-backed conflict-resolution rule can only
be introduced after it has a defensible empirical basis and an unspent
confirmation path.

## Probability and Confidence

Probability and Confidence remain attached to their referenced directional
claims. 7D may report their presence, but neither is counted as a second model or
vote. A low probability/reliability annotation therefore cannot mechanically
flip a positive claim to negative, and a high one cannot duplicate the claim.

Future work may use validated annotations to qualify decision authority, but that
requires an explicit, separately validated rule rather than an arbitrary weight.

## Risk and Elliott

Risk remains downside/path context, not thesis direction. Elliott 6H remains a
research context sensor under its frozen integration contract. Neither family can
create, reverse or resolve the Universal Stance in v1.

## Research partition and promotion

The Phase-7 research freeze remains unchanged:

- through 2026-09-25: `legacy_replay_spent`;
- from 2026-09-26: `prospective_unspent`.

A stance computed on the old period is explicitly labelled `spent_replay_only`.
A stance from a genuinely later packet is labelled `prospective_unconfirmed`.

Neither status is production validation. The 7D rule and any later action mapping
must accumulate future matured outcomes before a promotion review can occur.
The discovery findings from 7C are useful context but are not independent
validation of 7D.

## Output shape

Each result contains:

- packet identity (`symbol`, `as_of`, `source_snapshot_id`),
- research partition,
- Universal Stance state and optional direction,
- relation/support structure,
- known, unknown and ineligible directional claims,
- unresolved support/conflict relations,
- annotation/context counts,
- original 7A coverage summary,
- semantic guards,
- validation/promotion state.

The output intentionally contains no portfolio state.

## Run

```bash
python scripts/run_universal_stance_7d.py --input packet.json
```

To persist the normalized result:

```bash
python scripts/run_universal_stance_7d.py \
  --input packet.json \
  --output /tmp/universal_stance_7d.json
```

## Explicitly forbidden in 7D

Phase 7D does not produce or consume:

- current position or quantity,
- entry price,
- portfolio P/L,
- BUY/HOLD/SELL,
- Portfolio Action,
- target weight or position size,
- order instruction,
- previous stance or hysteresis state,
- weighted super-score.

## Boundary to Phase 7E

Phase 7E may add evidence-based state persistence / hysteresis around consecutive
Universal Stance observations. It must preserve the raw 7D stance, must not
rewrite `conflicted` or `insufficient_evidence` to neutral, and must not yet turn
state persistence into a portfolio action.
