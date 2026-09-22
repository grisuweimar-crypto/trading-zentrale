# Phase 1A — Selection vs Timing

This is a research-only diagnostic. It does **not** change scanner scoring, R0-R5,
the daily watch, or any trading logic.

## Semantics

The scanner `score` is a regime-dependent composite of opportunity and risk
factors. It is **not** a buy/sell recommendation.

`r_code` (R0-R5) is a downstream quality/regime class. It combines the score
percentile inside the current universe with score status, TrendOK and
LiquidityOK. It is also **not** a trade signal.

Phase 1A separates two empirical questions:

1. **Cross-sectional selection:** Do higher-quality rows tend to rank better than
   lower-quality rows in subsequent returns?
2. **Within-stock timing:** When the same stock has an unusually high score
   relative to its own history, does that itself imply better subsequent timing?

## Method

Default analysis starts on 2026-04-15 because the historical score distribution
is materially more stable from that point onward.

- Scanner rows: `artifacts/research/history_analysis.csv`
- Prices: `artifacts/research/price_backfill.csv`
- Stocks only; crypto is excluded in this module.
- Same-day reruns collapse to the last published state.
- Weekend/holiday repeats that point to the same market session are collapsed.
- Horizons: 5 / 20 / 40 / 60 trading sessions.
- Cross-sectional quality uses the same direction as the R-code score
  percentile: higher percentile = higher score quality.
- Within-stock timing uses a 5-session cooldown to reduce serial duplication.
- A point-in-time own-history percentile requires 20 prior score observations.

Exact historical R0-R5 outcome testing is intentionally **not backfilled by
guessing**. It is only valid where `r_code` was actually stored, unless all
historical R-code inputs can later be reconstructed exactly.

## Run

```bash
python scripts/run_selection_timing_1a.py
```

Output:

`artifacts/research/selection_timing_1a.json`

The output contains cross-sectional selection statistics, quality-band tail
rates, within-stock timing statistics, point-in-time own-score comparisons and
currency-level diagnostics.
