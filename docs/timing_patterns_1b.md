# Phase 1B — Timing patterns

Research-only. This module does **not** change scanner scoring, R0-R5, the daily watch, or trading logic.

## Semantics

- Scanner score is a composite quality/regime descriptor, not a buy/sell signal.
- R0-R5 is a downstream quality/regime class, not a buy/sell signal.
- Phase 1B studies whether **changes** in score and component indicators contain timing information.

## Inputs tested

Point-in-time deltas over 1 / 5 / 10 scanner observations:

- score
- opportunity
- risk
- RS3M
- Trend200
- Cycle

Additional states/transitions:

- Trend200 crossing zero
- Cycle crossing 25 / 50 / 75
- R-code upgrades/downgrades and R4/R5 vs lower states where historically stored
- Elliott state/transition only if it exists in historical point-in-time data

Elliott is never reconstructed from present-day values. A transition is emitted only when a prior Elliott state is actually known. If historical Elliott is absent, the report says so.

## Validation design

- Stable history starts 2026-04-15.
- Discovery window: 2026-04-15 through 2026-07-31.
- Validation window: 2026-08-01 onward.
- Horizons: 5 / 20 / 40 / 60 trading sessions.
- A discovery observation is eligible only if its horizon-specific target date is also on or before 2026-07-31. This purges overlapping labels from the validation period.
- Same-currency peer medians are computed from the full daily cross-section before any cooldown. Currency cohorts need at least two valid members; otherwise the broader daily median is used when available.
- Repeated occurrences use a 5-session cooldown **after** a pattern has matched, not by arbitrarily sampling every fifth scanner row.
- Pattern combinations are limited to 1–3 fixed atomic conditions.
- Atomic features and combinations are screened and ranked using discovery data only.
- To control the search size, only the strongest discovery-only atomic conditions are used for combinations.
- The best positive and negative discovery candidates are then frozen. Validation is consulted once afterward and cannot add, remove, or rank candidates.
- Continuous-change and categorical summaries are reported separately for discovery and validation.
- Default minimums are 30 discovery occurrences and 20 validation occurrences for interpreting a frozen pattern as sufficiently represented in holdout.

The output remains descriptive research, not a trading rule. Phase 2 can later add statistical uncertainty, shrinkage, multiple-testing controls, and calibrated probability estimates.

## Run

```bash
python scripts/run_timing_patterns_1b.py
```

Output:

`artifacts/research/timing_patterns_1b.json`
