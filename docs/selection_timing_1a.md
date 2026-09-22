# Phase 1A — Selection vs Timing

This is a research-only diagnostic. It does **not** change scanner scoring, R0-R5,
the daily watch, or any trading logic.

## Semantics

The scanner `score` is a regime-dependent composite of opportunity and risk
factors. It is **not** a buy/sell recommendation.

There are two different regime concepts in the code and they must not be mixed:

- the **market regime** (`bull`, `neutral`, `bear`) changes the opportunity/risk
  weighting used to calculate the score;
- the **R-regime / r_code** (`R0` to `R5`) is a downstream quality class derived
  from score percentile plus score status, TrendOK and LiquidityOK.

R0-R5 is an input to the daily interpretation, not a trade signal.

Phase 1A separates two empirical questions:

1. **Cross-sectional quality:** Do rows with a higher score percentile within the
   same scanner run tend to rank better in later returns than lower-quality rows?
2. **Within-stock timing:** When the same stock has an unusually high score
   relative to its own prior history, does that by itself improve later relative
   performance?

## R-score backbone

Historical `r_code` is only stored for the recent part of the archive. Therefore
Phase 1A does **not** invent historical R4/R5 values.

Instead it also reports a score-percentile backbone:

- `B1`: below 20th score percentile
- `B2`: 20th to below 45th
- `B3`: 45th to below 75th
- `B4`: 75th to below 90th
- `B5`: 90th percentile and above

These thresholds mirror the score-percentile thresholds used by R1-R5, but B4
and B5 are **not exact R4/R5**, because the historical TrendOK/LiquidityOK gates
are not fully reconstructible. Score-zero rows are reported separately as
`B0_score0`.

## Method

Default analysis starts on 2026-04-15 because the historical score distribution
changes materially before that date and is much more stable afterwards.

- Scanner rows: `artifacts/research/history_analysis.csv`
- Prices: `artifacts/research/price_backfill.csv`
- Stocks only; crypto is excluded from this phase.
- Same-day reruns collapse to the last published state while preserving append
  order, so later same-day runs win deterministically.
- Weekend/holiday repeats that point to the same market session are collapsed.
- Horizons: 5 / 20 / 40 / 60 trading sessions.
- Cross-sectional quality uses the same direction as R-code score percentile:
  higher percentile = higher score quality.
- Within-stock timing uses a 5-session cooldown to reduce serial duplication.
- Within-stock relative performance is measured against the same-date median of
  the same currency bucket when available; the global median is only a fallback.
- A point-in-time own-history percentile requires 20 prior score observations.
- Empty/no-overlap runs return a valid zero-coverage report instead of failing.

Exact historical R0-R5 outcome testing is intentionally **not backfilled by
guessing**. It becomes valid only where the exact R-code was stored or where all
historic gate inputs can be reconstructed exactly.

## Run

```bash
python scripts/run_selection_timing_1a.py
```

Output:

`artifacts/research/selection_timing_1a.json`

The output contains cross-sectional quality statistics, ordinary quality
quintiles, the R-score backbone, within-stock timing statistics, point-in-time
own-score comparisons and currency-level diagnostics.
