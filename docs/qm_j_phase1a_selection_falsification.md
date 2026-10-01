# QM-J / BA-QM7 — Phase 1A Selection Permutation Falsification

This is the first concrete QM-J application to an existing Scanner-vNext research estimand. It reuses the frozen Phase-1A cross-sectional Selection logic rather than creating a new success metric.

## Predeclared primary test

The plan was frozen before the first real QM-J execution and is bound to Git commit `94de2cfe54d04782c8348b99a81b6962e8a63709`.

Primary endpoint only:

- horizon: 20 observed sessions;
- metric: Phase-1A `mean_daily_spearman` between daily `score_pct_full` and 20-session forward return;
- direction: higher is better;
- negative controls: 64 deterministic score permutations;
- permutation is performed only within the same scanner observation date;
- one null comparator is used: the higher 95th percentile of those 64 placebo metrics;
- frozen similarity margin: 0.01 Spearman points;
- no 5/40/60-session horizon may be used to select a more favorable conclusion.

## What the placebo destroys

The negative control changes only which symbol receives which already-observed daily score percentile. It preserves:

- scanner observation date;
- symbol/event identity;
- eligible sample;
- forward-return input;
- number of observations;
- Phase-1A computation code;
- the daily cross-sectional score distribution.

This destroys the symbol-to-score alignment while holding the comparison context fixed.

## Falsification interpretation

The real metric and the placebo 95th-percentile metric are passed through the generic QM-J falsification gate.

A placebo that is within 0.01 of the real metric, or stronger, yields:

`PROMOTION_STOP_INVESTIGATION_CAPA_REQUIRED`

This is a valid scientific result and therefore does not make CI fail. CI fails only for implementation, schema, comparability, PIT, reproducibility or data errors.

If the placebo is not similarly strong, the result is:

`NEGATIVE_CONTROLS_NOT_SIMILARLY_STRONG`

That does **not** validate or promote Selection. It only means this predeclared permutation control did not reproduce the real 20-session cross-sectional Selection metric.

## Source integrity

The execution hashes the raw bytes of `history_analysis.csv` and `price_backfill.csv` into the result artifact. The research inputs are read-only. The test never writes modified history or price data back to the repository.

## BA-QM7 status

This application advances BA-QM7 but does not close it. Further concrete negative-control applications remain necessary at additional Research, Decision-Layer and isolated end-to-end levels before BA-QM7 closure is justified.
