# Phase 2 — Probability calibration and uncertainty

Research-only. Phase 2 does **not** change scanner scoring, R0-R5, the daily watch, or trading logic.

## Goal

Phase 1A separated cross-sectional selection quality from within-stock timing. Phase 1B discovered timing patterns using a purged discovery window and a later holdout. Phase 2 keeps that separation and asks:

> Given a quality band or a timing pattern frozen by Phase 1B, how large is the historically observed probability of outperforming the validated peer benchmark, and how uncertain is that estimate?

The target is `peer_excess > 0`, using the corrected leave-one-symbol-out peer benchmark and adjusted-close returns.

## Frozen Phase 1B candidates

Phase 2 does **not** call the Phase 1B discovery search again. The corrected Phase 1B candidates are frozen in:

`artifacts/research/timing_patterns_1b_frozen.json`

This is important both statistically and operationally:

- new holdout observations cannot change which patterns were selected;
- Phase 2 remains a calibration layer rather than another discovery pass;
- the expensive combinatorial Phase 1B search is not repeated for every Phase 2 run.

The frozen catalog is tied to the original discovery window. Phase 2 rejects it if that window does not match its configured discovery window.

## Windows

Defaults remain:

- stable history start: 2026-04-15
- discovery: 2026-04-15 through 2026-07-31, with each horizon target required to finish by the discovery cutoff
- validation: 2026-08-01 onward
- horizons: 5 / 20 / 40 / 60 trading sessions

Each horizon reports validation maturity explicitly. A horizon with no completed validation targets is `not_yet_mature`, not a failed signal.

## Selection calibration

Selection remains a cross-sectional quality question. Phase 2 reports the score-percentile backbone separately for B0/B1/B2/B3/B4/B5. B4/B5 are still **not** claimed to be exact historical R4/R5 because historical gate inputs are incomplete.

For each band and window the report contains:

- raw positive peer-excess rate and Wilson 95% interval as an iid diagnostic
- baseline positive peer-excess rate
- Beta-shrunk positive peer-excess probability and approximate 95% interval as an iid diagnostic
- probability advantage versus the window baseline
- mean and median peer excess
- circular moving-block bootstrap intervals for mean peer excess and probability advantage
- sample size, distinct symbols/days and top-symbol concentration

## Timing-pattern calibration

For every frozen pattern Phase 2 reports discovery and validation statistics and separates several evidence levels:

- `alpha_direction_confirmed`: mean peer excess in validation has the Phase 1B discovery direction
- `probability_direction_confirmed`: probability advantage has that same direction
- `joint_direction_confirmed`: both are true and the validation sample meets minimum N
- `alpha_interval_confirmed`: moving-block alpha interval stays entirely on the expected side of zero
- `probability_interval_confirmed`: moving-block probability-advantage interval stays entirely on the expected side of zero
- `strong_validation`: sufficient N plus joint direction plus both robust interval checks

The report therefore no longer treats a matching mean-alpha sign alone as proof that the probability layer is confirmed.

Approximate Wilson/Beta intervals and binomial/Bonferroni values remain iid diagnostics only. Strong validation uses a circular moving observation-date block bootstrap with effective block length 2 × horizon; every eligible date remains a possible block start, and robust intervals require at least two time-separated occurrence support regions. The report records both the base horizon and the effective moving-block length explicitly so the uncertainty method is auditable. Positive regression fixtures likewise span at least two such support regions; sparse one-region evidence must fail closed.

## Final empirical result — snapshot 2026-09-22

### 5 trading sessions

Validation contains **4,970 mature target events**. Of the frozen Phase 1B timing patterns:

- 17 retain the expected mean-alpha direction,
- 16 retain both alpha and probability direction,
- **2 remain `strong_validation` after the final circular 10-session moving-block bootstrap.**

The two robust patterns are:

1. `trend200_d10_down & trend200_d1_down & rs3m_d1_up`
   - validation N: **241**
   - mean peer excess: **+0.635%**
   - median peer excess: **+0.694%**
   - probability advantage vs baseline: **+7.55 pp**
   - moving-block mean-alpha 95% interval: **+0.202% to +1.047%**
   - moving-block probability-advantage 95% interval: **+2.56 pp to +11.13 pp**
   - time-separated occurrence support regions: **4**

2. `trend200_d5_down & trend200_d10_down & rs3m_d1_up`
   - validation N: **342**
   - mean peer excess: **+0.611%**
   - median peer excess: **+0.492%**
   - probability advantage vs baseline: **+6.00 pp**
   - moving-block mean-alpha 95% interval: **+0.105% to +0.994%**
   - moving-block probability-advantage 95% interval: **+0.01 pp to +9.99 pp**
   - time-separated occurrence support regions: **4**

The second pattern's probability interval is only narrowly above zero, so its statistical margin is materially weaker than the first pattern even though it satisfies the formal `strong_validation` rule.

A pattern that appeared strong under the intermediate fixed-block method, `trend200_d5_down & trend200_d1_down & rs3m_d1_up`, is **not** strong under the final method. Its mean-alpha interval remains positive, but its probability-advantage interval is approximately **-0.27 pp to +8.65 pp**, crossing zero.

### 20 trading sessions

Validation contains **2,487 mature target events**. Eight frozen patterns retain the expected alpha direction and seven retain both alpha and probability direction, but **none is `strong_validation`** under the final 40-session moving-block method.

The strongest selection band remains B5 as a point estimate:

- validation N: **269**
- mean peer excess: **+2.248%**
- median peer excess: **+2.424%**
- Beta-shrunk positive peer-excess probability: **67.00%**
- probability advantage vs baseline: **+18.83 pp**

However, the validation window contains only **one independent 40-session occurrence-support region** for this B5 sample. Robust moving-block alpha and probability-advantage intervals are therefore correctly reported as unavailable (`None`). The strong point estimates must not be described as confirmed 20T evidence yet.

### 40 / 60 trading sessions

There are currently no mature validation targets for 40T or 60T. These horizons remain `not_yet_mature` rather than failed.

## Shrinkage

The raw hit rate is not treated as perfectly known. A Beta prior is centred on the contemporaneous baseline rate with default strength 20. A 0.5 shape floor keeps the prior proper even when a finite window's baseline rate is exactly 0 or 1. Configured prior strengths below 1 are rejected.

## Automation

The Phase 2 workflow runs:

- on pull requests affecting the Phase 2 layer,
- manually,
- downstream of a successful `Scanner_vNext Autopilot` run,
- and once nightly as a fallback.

A freshness gate compares the scanner `snapshot_id` in `history_metadata.json` with the source snapshot stored in the last Phase 2 report. Repeated catch-up scanner workflows therefore skip the expensive calibration when no new research snapshot exists.

Successful non-PR runs persist the current report to:

`artifacts/research/probability_calibration_2.json`

## Run locally

```bash
python scripts/run_probability_calibration_2.py
```

Phase 2 remains a research layer. UI / Depot-Watch interpretation belongs to the later interpretation phase after this calibration layer is accepted.
