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

Elliott is never reconstructed from present-day values. If historical Elliott is absent, the report says so.

## Validation design

- Stable history starts 2026-04-15.
- Discovery window: 2026-04-15 through 2026-07-31.
- Validation window: 2026-08-01 onward.
- Horizons: 5 / 20 / 40 / 60 trading sessions.
- Repeated observations are reduced with a 5-session cooldown.
- Relative outcome is measured against the same-currency daily peer median, with global daily median only as fallback.
- Pattern combinations are limited to 1–3 atomic conditions.
- A combination must have at least 30 discovery cases and 20 validation cases and keep the same effect direction in both windows.

The output is descriptive research, not a trading rule. Phase 2 can later add statistical uncertainty, shrinkage, and probability estimates.

## Run

```bash
python scripts/run_timing_patterns_1b.py
```

Output:

`artifacts/research/timing_patterns_1b.json`
