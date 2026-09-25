# Phase 7I — Validation & Promotion

Phase 7I does **not** add a new trading rule. It decides whether the already frozen Decision Layer has enough independent evidence to be considered for a later promotion, while keeping technical correctness, empirical validation and production enablement strictly separate.

## Freeze boundary

The Phase-7 research architecture fixes:

- legacy replay spent through `2026-09-25`;
- prospective unspent evidence from `2026-09-26`;
- horizons `5 / 20 / 40 / 60` sessions;
- the Phase-7C minimum group size and temporal-dependence treatment already frozen before prospective evidence begins.

Phase 7I is frozen on `2026-09-25`, before the prospective partition starts. It therefore cannot use any later outcome to choose a more favorable validation rule.

## Three different questions

7I deliberately separates three questions that must never be collapsed into one:

1. **Technical readiness** — do contracts, validators, PIT guards, same-snapshot rules and tests work as designed?
2. **Empirical readiness for promotion review** — is there genuinely prospective, mature, dependence-aware evidence for the frozen Decision Layer?
3. **Productive enablement** — has a later explicit review approved changing a source contract from research/shadow use into productive integration?

A green CI suite can answer only the first question. It is not empirical validation.

## Current result at the freeze

At the 2026-09-25 freeze:

- 7A–7H are technically testable;
- all Decision-Layer contracts remain `research_only=true`;
- all productive integration flags remain false;
- 7F/7G/7H execution remains disabled;
- the prospective partition has not started yet;
- `artifacts/research/decision_evidence_7a.jsonl` is not present yet, which is explicit zero prospective coverage rather than an error;
- no prospective forward outcome can be mature before the prospective partition starts.

Therefore the only permissible positive result now is **shadow-collection eligibility**. Productive promotion is not eligible and broker execution remains impossible.

## Readiness states

The report advances only through explicit data states:

- `pre_prospective_start`
- `shadow_capture_not_started`
- `collecting_prospective_evidence`
- `collecting_prospective_dataset`
- `awaiting_mature_outcomes`
- `awaiting_downstream_shadow_trace`
- `metrics_ready_for_promotion_review`

`metrics_ready_for_promotion_review` still does **not** mean that promotion has passed. It means only that the frozen metrics can be reviewed without immediately failing for missing prospective evidence.

## Prospective evidence sources

7I checks three separate evidence streams.

### 1. Typed 7A evidence archive

`artifacts/research/decision_evidence_7a.jsonl` contains genuine typed evidence packets when they begin to exist prospectively. A missing archive before the first prospective packet is zero coverage, not a failure.

Legacy replay packets never count as independent confirmation.

### 2. 7B forward outcomes

7I rebuilds the Decision Research Dataset from the canonical research inputs and only counts rows whose `obs_date` is on or before the requested review cutoff.

Only the `prospective_unspent` partition can contribute to empirical readiness. Spent rows are still visible for diagnostics, but are explicitly excluded from empirical confirmation.

Forward labels remain labels, never features.

### 3. Private downstream shadow trace

7D–7H cannot be validated only from the 7B dataset, because those downstream states do not exist historically without retrojection and 7F/7H are position-aware.

A future runtime may supply a compact `decision_shadow_trace_summary_v1` with:

- prospective row count;
- symbol count;
- captured layers;
- minimum and maximum trace dates.

The summary must state that raw position values are absent and public repository persistence is disabled. Quantities, market values, entry prices, current prices or broker/account identifiers do not belong in the public validation path.

## Statistical rules

7I does not invent a new performance threshold after seeing outcomes.

It inherits the frozen Phase-7C research rules:

- minimum group N = 30;
- at least two temporal support regions;
- circular moving observation-date blocks;
- block length = `2 × evaluated horizon`;
- overlapping windows are not IID;
- horizons = 5, 20, 40 and 60 sessions.

These inherited rules define when a prospective comparison can be statistically characterized. They do not automatically turn a favorable result into production approval.

## Layer-specific review

### 7C — confirmation/conflict

Only the already frozen pre-registered comparisons may be rerun on prospective mature outcomes. Historical discovery support cannot be renamed validation.

### 7D — Universal Stance

Positive, negative, conflicted and insufficient states must be evaluated prospectively without changing the mapping after outcomes are known. Missing evidence remains missing and conflict remains conflict.

### 7E — hysteresis

The depth-2 rule remains an unvalidated candidate. Depths 1/3/5 remain comparison candidates. Historical churn reduction cannot select a production winner; outcome behavior must be assessed prospectively.

### 7F — Portfolio Action & Swing Management

Portfolio action is evaluated separately from Universal Stance. Position state may be captured privately, but no missing cost, price or P/L value may be invented. Review actions remain review actions, not broker orders.

### 7G — Reliability & Explainability

Explanation fidelity is structural. Reliability states remain descriptive and must not be converted into a numeric probability. Any outcome stratification is evaluated prospectively as a separate question.

### 7H — Depot-Watch

Same-snapshot integration, unavailable states and position-context binding are validated prospectively. Real positions and downstream traces remain private. 7H cannot become productive while its upstream action path remains unpromoted.

## Promotion boundary

Phase 7I itself can never:

- flip any 7A–7H productive flag;
- mark productive promotion as approved;
- enable broker execution;
- generate position size or target weight;
- convert a technically green run into empirical validation;
- count replay data as independent confirmation;
- choose new thresholds after prospective outcomes are observed.

Even when the report eventually reaches `metrics_ready_for_promotion_review`, a **separate explicit source-contract change after review** is required before any productive integration can be enabled.

## CLI

```bash
python scripts/run_decision_promotion_7i.py \
  --output /tmp/decision_promotion_7i.json
```

The default review cutoff comes from `artifacts/research/history_metadata.json`.

A future private shadow-trace summary can be supplied explicitly:

```bash
python scripts/run_decision_promotion_7i.py \
  --trace-summary /private/path/decision_shadow_trace_summary.json \
  --output /tmp/decision_promotion_7i.json
```

No real portfolio file is committed by this workflow.
