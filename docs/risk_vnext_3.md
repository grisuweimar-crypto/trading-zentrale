# Phase 3 — Risk vNext audit

Phase 3 is a research-only audit of the existing Scanner-vNext risk layer. It does not change production scoring, risk weights, R0-R5, Confidence, the daily watch, or portfolio decisions.

## Questions

Phase 3 deliberately separates two questions that are often mixed together:

1. **Protection effect** — does a higher current risk reading predict a larger future adverse excursion or peak-to-trough drawdown?
2. **Return effect** — does a lower current risk reading predict better future peer-relative return or a higher probability of outperformance?

A factor may be useful for protection even when it does not improve return. Such a factor must not be discarded merely because alpha is neutral.

## Point-in-time rule

Only risk values actually stored with the historical scanner observation are used. Missing historical fundamentals, liquidity metrics, beta, downside deviation, or other components are never reconstructed from current values.

The audit checks these canonical research candidates when present:

- aggregate scanner `risk`
- `volatility`
- stored `drawdown`
- `debt_ratio`
- `liquidity_risk`
- `downside_dev`
- `beta`

The report includes explicit coverage counts, ratios, symbols, and first/last available dates for every candidate.

## Outcomes

For 5, 20, 40 and 60 trading sessions, the audit uses the validated adjusted-price research pipeline and calculates:

- forward return
- leave-one-symbol-out peer excess
- entry-relative adverse excursion
- peak-to-trough maximum drawdown inside the future path
- tail drawdown rate at the configured threshold (default 10%)

Repeated observations are thinned with the existing five-session cooldown. Discovery is purged at 2026-07-31 and validation begins 2026-08-01, matching the completed Phase 1B/2 research architecture.

## Factor diagnostics

For each available risk factor and horizon, the report includes:

- outcome-specific N, symbols, days and coverage
- Spearman correlation versus peer excess
- Spearman correlation versus forward return
- Spearman correlation versus adverse excursion
- Spearman correlation versus future path max drawdown
- low-risk and high-risk quintile statistics
- low-risk outperformance rate versus high-risk outperformance rate
- high-minus-low protection gaps
- low-risk-minus-high-risk peer-alpha advantage
- circular moving-block bootstrap uncertainty for group differences

Return/alpha and protection outcomes use independent available samples; neither is restricted to their complete-case intersection.

## Uncertainty method

Forward outcomes overlap whenever the event cooldown is shorter than the evaluated horizon. Independent per-day bootstrap resampling therefore understates dependence for 20T/40T/60T and can make confidence intervals too narrow.

Phase 3 uses a **circular moving observation-date block bootstrap** with an effective block length of **2 × the evaluated forward horizon**:

- 5T outcomes use moving blocks of 10 observation sessions
- 20T outcomes use moving blocks of 40 observation sessions
- 40T outcomes use moving blocks of 80 observation sessions
- 60T outcomes use moving blocks of 120 observation sessions
- every eligible observation date can be a block start, including trailing dates
- blocks wrap circularly so dependence is preserved across former fixed block boundaries
- all observations sharing a date remain together
- quantile membership is fixed before resampling
- uncertainty is unavailable unless both low- and high-risk groups have support in at least two time-separated block-length regions
- disabled resampling also reports uncertainty as unavailable rather than a zero-width pseudo interval
- fewer than 20 configured or surviving bootstrap replicates are treated as insufficient for a percentile 95% interval; configuration values 1–19 are rejected

This fails closed when independent temporal support is insufficient. The observed means, medians, correlations and group-rate point estimates do not depend on the bootstrap method, but claims about statistical strength do.

## Danelfin

Danelfin Low Risk remains an external research reference because it was the strongest Danelfin component in the completed benchmark. Phase 3 does **not** import Danelfin's aggregate AI Score, Low Risk score, or weights into Scanner-vNext.

## Final empirical result — snapshot 2026-09-22

The full audit used 18,090 scanner events. Historical coverage is the main limitation:

- `volatility`: 26,472 stored scanner rows, 203 symbols, 2026-04-15 through 2026-09-22 (98.85% coverage)
- `drawdown`: same coverage as volatility
- aggregate `risk`: only 1,163 rows from 2026-09-17 onward
- `debt_ratio`: only 804 rows from 2026-09-17 onward
- `liquidity_risk`: no usable historical observations
- `downside_dev`: not stored historically
- `beta`: not stored historically

Therefore only **volatility** and **drawdown** currently have enough point-in-time history for a real discovery/validation test. The other factors are classified as *not yet empirically testable*, not as ineffective.

### 5 trading sessions — validation

Validation contained 1,153 mature return and protection events.

**Volatility**:

- Spearman vs future path max drawdown: **+0.398**
- Spearman vs entry-relative adverse excursion: **+0.247**
- high-risk minus low-risk path max drawdown: **+4.59 percentage points**
- moving-block 95% interval: **+3.76 pp to +5.44 pp**
- high-risk minus low-risk adverse excursion: **+3.38 pp**
- moving-block 95% interval: **+2.63 pp to +4.40 pp**
- 10% tail-drawdown rate: **22.94% high-volatility vs 0.43% low-volatility**
- observed low-risk peer-alpha advantage: **-0.55 pp**
- alpha moving-block 95% interval: **-3.09 pp to +2.22 pp**

**Stored drawdown**:

- Spearman vs future path max drawdown: **+0.370**
- Spearman vs adverse excursion: **+0.238**
- high-risk minus low-risk path max drawdown: **+3.87 pp**
- moving-block 95% interval: **+3.50 pp to +4.26 pp**
- high-risk minus low-risk adverse excursion: **+2.91 pp**
- moving-block 95% interval: **+2.27 pp to +3.65 pp**
- 10% tail-drawdown rate: **17.80% high-drawdown vs 0.85% low-drawdown**
- observed low-risk peer-alpha advantage: **-0.75 pp**
- alpha moving-block 95% interval: **-2.72 pp to +1.66 pp**

**Interpretation:** the 5T downside-protection effect is robust for both historically testable factors. Volatility shows the stronger observed separation. Neither volatility nor stored drawdown has a robust 5T alpha advantage.

### 20 trading sessions — validation

Validation contained 638 mature return and protection events.

**Volatility**:

- Spearman vs future path max drawdown: **+0.630**
- Spearman vs adverse excursion: **+0.260**
- high-risk minus low-risk path max drawdown: **+12.32 pp**
- high-risk minus low-risk adverse excursion: **+8.09 pp**
- 10% tail-drawdown rate: **78.91% high-volatility vs 9.38% low-volatility**
- observed low-risk peer-alpha advantage: **+3.02 pp**

**Stored drawdown**:

- Spearman vs future path max drawdown: **+0.533**
- Spearman vs adverse excursion: **+0.215**
- high-risk minus low-risk path max drawdown: **+10.29 pp**
- high-risk minus low-risk adverse excursion: **+6.82 pp**
- 10% tail-drawdown rate: **75.38% high-drawdown vs 13.95% low-drawdown**
- observed low-risk peer-alpha advantage: **+0.54 pp**

The 20T point estimates are substantial, but the corrected 40-session moving-block rule does **not** find enough independent temporal support to estimate robust intervals. The 20T effects therefore remain promising observations rather than confirmed statistical evidence.

### 40 / 60 trading sessions

Discovery observations remain exploratory. The validation window is not yet mature for 40T or 60T, so those horizons must not be treated as confirmed evidence.

### Phase-3 conclusion

1. **Volatility has a robust 5T downside-protection relationship** and is the strongest currently testable factor.
2. **Stored drawdown also has a robust 5T downside-protection relationship.**
3. Neither factor is validated as a reliable alpha generator at 5T.
4. The 20T protection and alpha point estimates are not yet supported by enough independent temporal regions for robust uncertainty intervals.
5. Aggregate risk, debt, liquidity risk, downside deviation, beta and other production components cannot yet be reweighted because their historical point-in-time coverage is absent or too short.
6. **No production weights are changed in Phase 3.**
7. The next data-architecture requirement is to retain every individual production risk component point-in-time so later walk-forward validation can test them without reconstruction or leakage.

## Run

```bash
python scripts/run_risk_vnext_3.py
```

Output:

```text
artifacts/research/risk_vnext_3.json
```
