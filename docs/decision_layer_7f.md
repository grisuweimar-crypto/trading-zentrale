# Phase 7F — Portfolio Action & Swing Management

## Purpose

Phase 7F is the first position-aware component of the Decision Layer. It answers:

> Given the unchanged portfolio-independent 7D Universal Stance and the 7E
> transition state, what review action is appropriate for this actual position?

The frozen contract is `configs/decision_portfolio_action_v1.json` and the
implementation is `src/scanner/research/decision_layer/portfolio_action.py`.

7F remains research-only. It creates explicit review states, not broker orders.

## Separation from 7D and 7E

The position snapshot may change the **portfolio action**, but it must never
change the Universal Stance. Book profit/loss also cannot turn a negative thesis
positive or a positive thesis negative. The raw 7D stance and 7E transition are
copied into the output only as preserved context.

This means the same positive security can legitimately produce different 7F
outputs:

- flat position: `ENTER_REVIEW`;
- existing long: `HOLD`;
- existing long with an admissible add-review context and explicit remaining
  capacity: `ADD_REVIEW`.

That is position dependence, not a change to the security-level interpretation.

## Position snapshot

7F v1 uses a separate `decision_position_snapshot_v1` object. Required identity
fields are `symbol`, `as_of`, `source_snapshot_id` and `position_state`.

Supported states are deliberately long-only:

- `flat`
- `long`

Quantity, market value, average entry price, current price, currency, add
capacity and transaction costs are optional. Missing values stay missing. There
is no price lookup inside 7F and **Score is never a price proxy**.

The existing `artifacts/portfolio/portfolio.csv` is a model-allocation artifact,
not an authoritative real-depot position source for 7F. A future Depot-Watch
integration must supply an explicit actual position snapshot.

If monetary fields are provided, their original instrument currency must also be
provided. 7F does not perform FX conversion.

## Action states

Version 1 emits:

- `NO_ACTION` — flat and no supported entry review;
- `WAIT_CONFIRMATION` — 7E is still in bootstrap/transition pending;
- `ENTER_REVIEW` — flat with a confirmed positive stance;
- `HOLD` — existing long remains unchanged;
- `ADD_REVIEW` — positive confirmed long plus an add/re-entry review context and
  explicit add capacity;
- `REDUCE_REVIEW` — positive confirmed long plus a partial-reduce/profit-
  protection/larger-reduce review context;
- `EXIT_REVIEW` — existing long with a confirmed negative stance.

The suffix `_REVIEW` is intentional: the state is decision support, not an order.
`execution_allowed` is always false and `order_instruction` is always null.

## Pending, conflict and insufficient evidence

A `bootstrap_pending` or `transition_pending` state maps to
`WAIT_CONFIRMATION`. The current raw stance remains visible even while the stable
anchor still points to the previous direction.

For `conflicted` or `insufficient_evidence`:

- flat => `NO_ACTION`;
- long => `HOLD`.

This does not reinterpret conflict/insufficiency as neutral. It means only that
7F does not create a new exposure or force an exit from a nondirectional state.

## Swing management

7F may consume Elliott-6H review contexts, but only through an explicit
PIT-stamped context object. Allowed contexts remain the frozen 6D/6H review
vocabulary.

Elliott cannot change stance direction. Under a confirmed positive long:

- `entry_or_add_review` / `reentry_or_add_review` may produce `ADD_REVIEW` only
  when add capacity is explicit;
- `partial_reduce_review` / `profit_protection_review` /
  `larger_reduce_or_exit_review` may produce `REDUCE_REVIEW`;
- simultaneous add and reduce contexts remain a conflict and fall back to
  `HOLD` rather than being arbitrarily resolved.

Under a confirmed negative long, 7F emits `EXIT_REVIEW`; Elliott does not rescue
or reverse the negative stance. Conversely, an Elliott exit-style review under a
confirmed positive stance maps at most to `REDUCE_REVIEW`, not an automatic exit.

## Add capacity

7F does not invent capital or portfolio limits. `ADD_REVIEW` requires explicit
capacity through `can_add=true` or a positive `remaining_adds`. Contradictory
capacity fields fail closed. Unknown capacity keeps the base `HOLD` state.

No amount is generated. Position sizing and target weights remain later or
external concerns.

## P/L context

When both average entry price and current price are supplied, 7F reports the
unrealized percentage return. If quantity is also supplied it can report the
unrealized monetary P/L in the supplied original currency.

P/L is explanatory only. It is explicitly marked as unused for stance direction
and action direction. Missing prices stay null.

## Transaction costs

`ENTER_REVIEW`, `ADD_REVIEW`, `REDUCE_REVIEW` and `EXIT_REVIEW` are cost-sensitive.
A supplied transaction-cost estimate is carried as context. If absent, a
net-benefit claim is blocked. Even when costs are known, v1 does not claim a
validated net action edge.

## Validation status

The action mapping is technically deterministic but not empirically promoted.
Outputs therefore keep:

- `research_only = true`
- `portfolio_action_rule_empirically_validated = false`
- `swing_action_edge_empirically_validated = false`
- `productive_integration_enabled = false`
- `execution_allowed = false`
- `promotion_eligible = false`

Future matured prospective observations are required before promotion review.

## CLI

```bash
python scripts/run_portfolio_action_7f.py \
  --transition transition_7e.json \
  --position position.json \
  --swing-context elliott_context.json
```

The swing context is optional.

## Boundary to 7G

Phase 7G may expose reliability and explainability around the action: supporting
and opposing evidence, missing inputs, why an action differs from the Universal
Stance label, and what observable change would alter it. It must not pretend the
unvalidated 7F mapping is already production-proven.
