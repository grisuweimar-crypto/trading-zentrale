# Phase 2 — Probability calibration and uncertainty

Research-only. Phase 2 does **not** change scanner scoring, R0-R5, the daily watch, or trading logic.

## Goal

Phase 1A separated cross-sectional selection quality from within-stock timing. Phase 1B discovered timing patterns using a purged discovery window and a later holdout. Phase 2 keeps that separation and asks a different question:

> Given a quality band or a frozen timing pattern, how large is the historically observed probability of outperforming the validated peer benchmark, and how uncertain is that estimate?

The target is `peer_excess > 0`, where peer excess is the same leave-one-symbol-out, same-currency peer comparison used by the corrected Phase 1 research. Returns use adjusted close prices.

## Windows

Default windows stay unchanged:

- stable history start: 2026-04-15
- discovery: 2026-04-15 through 2026-07-31, with the horizon target required to finish by the discovery cutoff
- validation: 2026-08-01 onward
- horizons: 5 / 20 / 40 / 60 trading sessions

Validation is never used to select timing patterns. Phase 2 first reuses the frozen Phase 1B candidates and then calibrates them.

## Selection calibration

Selection remains a cross-sectional quality question. Phase 2 reports the score-percentile backbone separately for B0/B1/B2/B3/B4/B5. B4/B5 are still **not** claimed to be exact historical R4/R5 because historical gate inputs are incomplete.

For each band and window the report contains:

- raw positive peer-excess rate
- Wilson 95% interval
- baseline positive peer-excess rate
- Beta-shrunk positive peer-excess probability
- approximate 95% interval around the shrunk probability
- probability advantage versus the window baseline
- mean and median peer excess
- day-cluster bootstrap interval for mean peer excess
- sample size, distinct symbols/days and top-symbol concentration

## Timing-pattern calibration

The Phase 1B discovery algorithm remains responsible for freezing candidates. Phase 2 does not search the holdout for replacements.

For every frozen pattern it reports the same probability and peer-excess statistics separately for discovery and validation, plus:

- whether validation has the configured minimum number of occurrences
- whether validation confirms the discovery direction
- approximate binomial p-value versus the window baseline
- Bonferroni-adjusted discovery diagnostic using the number of discovery candidates searched

The p-values are deliberately labelled diagnostic: event independence is not assumed to be proven. Day-cluster bootstrap intervals are included because scanner observations on the same date can share market shocks.

## Shrinkage

The raw hit rate is not presented as if it were perfectly known. A Beta prior is centred on the contemporaneous baseline positive peer-excess rate. Default prior strength is 20 pseudo-observations. Small samples are therefore pulled more strongly toward the baseline than large samples.

This is intentionally transparent and deterministic rather than a black-box machine-learning model.

## Run

```bash
python scripts/run_probability_calibration_2.py
```

Output:

`artifacts/research/probability_calibration_2.json`

The first Phase 2 implementation is a research layer only. Integration into the scanner UI or daily watch should happen only after the calibration output itself has passed tests and been reviewed on the full corrected history.
