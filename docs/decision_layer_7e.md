# Phase 7E — Hysteresis & State Transitions

## Purpose

Phase 7E adds temporal persistence around consecutive Phase-7D Universal Stance
observations. It answers a narrower question than a portfolio action:

> Has the raw Universal Stance changed persistently enough to establish a new
> directional state anchor, or is the change still pending/blocked?

7E does **not** rewrite the raw 7D stance. Every output therefore exposes both:

1. the current raw 7D stance, and
2. a separate transition state / stable directional anchor.

A stable anchor is historical state memory. It is not a substitute for the
current raw stance.

## Why the first rule is only a research candidate

There is not yet a mature prospective time series of the complete Phase-7D
Universal Stance. Choosing an allegedly optimal 2-, 3- or 5-observation threshold
from spent history would therefore create false precision and invite overfitting.

The frozen v1 mechanism consequently uses the smallest non-zero hysteresis
candidate:

- a directional state must repeat on **two distinct calendar dates**;
- repeated snapshots on the same calendar date do not count twice;
- source snapshot IDs must be unique;
- `conflicted` and `insufficient_evidence` interrupt and reset a pending change.

This rule is named `minimal_repeat_candidate_v1`. It is explicitly marked
`empirically_validated = false` and `production_eligible = false` everywhere.
It is a mechanism for prospective research, not a proven trading threshold.

## Transition states

7E emits explicit mechanics instead of silently smoothing 7D:

- `bootstrap_pending` — no stable anchor exists and a directional raw stance has
  appeared only once;
- `bootstrap_confirmed` — the initial directional anchor has repeated on the
  required number of distinct dates;
- `stable_confirmed` — raw stance agrees with the existing stable anchor;
- `transition_pending` — raw stance points opposite the stable anchor but has not
  yet repeated enough;
- `transition_confirmed` — the opposite direction has met the candidate repeat
  requirement and becomes the new stable anchor;
- `blocked_conflict` — current raw stance is `conflicted`;
- `blocked_insufficient` — current raw stance is `insufficient_evidence`.

The last two states are not neutral and do not erase the prior stable anchor.
They merely prevent the anchor from being presented as the current stance.

## Example

Raw 7D sequence:

`positive → positive → negative → conflicted → negative → negative`

Under the frozen v1 candidate:

- the second positive establishes a positive anchor;
- the first negative creates `transition_pending`;
- `conflicted` blocks and resets that pending change;
- the next negative starts a new pending change;
- only the following negative confirms the transition to a negative anchor.

At the conflict observation the output still says raw stance = `conflicted`.
The remembered positive anchor is not displayed as if the current system view
were positive.

## Candidate comparison

The CLI can also replay confirmation depths `1, 2, 3, 5` and report purely
mechanical quantities such as confirmed transitions, pending observations and
blocked observations.

It deliberately does **not** select a winner. Lower historical churn is not proof
of better investment outcomes. Promotion requires future matured outcomes from
the prospective period and an explicit validation design.

## Point-in-time and ordering guards

A 7E history must:

- contain only one symbol;
- contain valid 7D outputs;
- have strictly increasing `as_of` instants;
- use unique source snapshot IDs.

Confirmation dates use the calendar date encoded in the original timestamp, so
the same timezone-safe boundary established in 7D is preserved.

## Run

Build the frozen candidate state machine from an ordered JSON array of 7D outputs:

```bash
python scripts/run_state_transition_7e.py --input stance_history.json
```

Compare research candidate depths without selecting a winner:

```bash
python scripts/run_state_transition_7e.py --input stance_history.json --compare
```

## Explicitly forbidden in 7E

7E does not produce or consume:

- current portfolio quantity or entry price;
- portfolio P/L;
- BUY/HOLD/SELL;
- Portfolio Action;
- target weights or position sizes;
- order instructions;
- an optimized threshold selected from spent data;
- a weighted super-score.

## Validation status

The transition engine, guards and replay mechanics can be tested now. The
**financial validity of the hysteresis rule cannot**. The candidate therefore
remains research-only and non-promotable until enough genuinely prospective 7D
observations have matured.

## Boundary to Phase 7F

Phase 7F may consume the raw 7D stance together with the 7E transition state and
then introduce portfolio context. It must preserve the distinction between:

- current raw interpretation,
- stable/pending transition state,
- and portfolio-specific action.

7E itself never emits a portfolio action.
