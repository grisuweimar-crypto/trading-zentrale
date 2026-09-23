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

- N, symbols, days and mature-cohort coverage
- Spearman correlation versus peer excess
- Spearman correlation versus forward return
- Spearman correlation versus adverse excursion
- Spearman correlation versus future path max drawdown
- low-risk and high-risk quintile statistics
- low-risk outperformance rate versus high-risk outperformance rate
- high-minus-low protection gaps
- low-risk-minus-high-risk peer-alpha advantage
- day-cluster bootstrap intervals for those group differences

Higher stored values are interpreted as riskier. A useful protection factor should therefore normally have positive association with future adverse excursion/drawdown. A useful return filter would normally show a positive low-risk alpha advantage. These are diagnostics, not trade instructions.

## Danelfin

Danelfin Low Risk remains an external research reference because it was the strongest Danelfin component in the completed benchmark. Phase 3 does **not** import Danelfin's aggregate AI Score, Low Risk score, or weights into Scanner-vNext.

## Baseline result — snapshot 2026-09-22

The first full audit used 18,090 scanner events. Historical coverage is the main limitation:

- `volatility`: 26,472 stored scanner rows, 203 symbols, 2026-04-15 through 2026-09-22 (98.85% coverage)
- `drawdown`: same coverage as volatility
- aggregate `risk`: only 1,163 rows from 2026-09-17 onward
- `debt_ratio`: only 804 rows from 2026-09-17 onward
- `liquidity_risk`: no usable historical observations
- `downside_dev`: not stored historically
- `beta`: not stored historically

Therefore only **volatility** and **drawdown** currently have enough point-in-time history for a real discovery/validation test. The other factors are classified as *not yet empirically testable*, not as ineffective.

### 5 trading sessions — validation

Validation contains 1,153 mature events.

**Volatility** has a clear protection effect:

- Spearman vs future path max drawdown: **+0.398**
- Spearman vs entry-relative adverse excursion: **+0.247**
- high-risk minus low-risk path max drawdown: **+4.59 percentage points** (day-cluster bootstrap 95%: **+3.28 to +5.61 pp**)
- high-risk minus low-risk adverse excursion: **+3.38 pp** (95%: **+2.30 to +4.68 pp**)
- 10% tail-drawdown rate: **22.94% high-volatility vs 0.43% low-volatility**

**Stored drawdown** is also validated as a protection factor, but somewhat weaker:

- Spearman vs future path max drawdown: **+0.370**
- Spearman vs adverse excursion: **+0.238**
- high-risk minus low-risk path max drawdown: **+3.87 pp** (95%: **+3.07 to +4.49 pp**)
- high-risk minus low-risk adverse excursion: **+2.91 pp** (95%: **+1.72 to +3.67 pp**)
- 10% tail-drawdown rate: **17.80% high-drawdown vs 0.85% low-drawdown**

Neither factor has a validated 5T alpha advantage: the low-risk-minus-high-risk peer-alpha bootstrap intervals cross zero.

### 20 trading sessions — validation

Validation contains 638 mature events. The protection effect becomes materially stronger.

**Volatility**:

- Spearman vs future path max drawdown: **+0.630**
- Spearman vs adverse excursion: **+0.260**
- high-risk minus low-risk path max drawdown: **+12.32 pp** (95%: **+9.18 to +13.40 pp**)
- high-risk minus low-risk adverse excursion: **+8.09 pp** (95%: **+4.90 to +11.03 pp**)
- 10% tail-drawdown rate: **78.91% high-volatility vs 9.38% low-volatility**
- outperformance rate: **53.13% low-volatility vs 44.53% high-volatility**
- low-risk peer-alpha advantage: **+3.02 pp**, but its bootstrap interval (**-2.43 to +7.62 pp**) still crosses zero

**Stored drawdown**:

- Spearman vs future path max drawdown: **+0.533**
- Spearman vs adverse excursion: **+0.215**
- high-risk minus low-risk path max drawdown: **+10.29 pp** (95%: **+7.34 to +12.00 pp**)
- high-risk minus low-risk adverse excursion: **+6.82 pp** (95%: **+1.84 to +10.27 pp**)
- 10% tail-drawdown rate: **75.38% high-drawdown vs 13.95% low-drawdown**
- low-risk peer-alpha advantage: **+0.54 pp**, with a bootstrap interval crossing zero

### 40 / 60 trading sessions

Discovery results strengthen further, especially for volatility, but the validation window is **not yet mature** for 40T or 60T. Those longer-horizon observations remain exploratory and must not be treated as confirmed evidence yet.

### Phase-3 interpretation

1. **Volatility is currently the strongest validated downside-protection factor in Scanner-vNext.**
2. **Stored drawdown is also a real protection factor**, but weaker than volatility in the mature holdout.
3. The evidence does **not** yet justify treating either factor as a reliable alpha generator. Their primary validated role is risk protection.
4. This is directionally consistent with the earlier Danelfin comparison, where Low Risk was the only Danelfin component with useful longer-horizon behavior, but it does not prove that the two systems measure the same underlying risk mechanism.
5. Aggregate risk, debt, liquidity risk, downside deviation, beta and other production components cannot yet be reweighted from this audit because their historical point-in-time coverage is absent or too short.
6. **No production weights are changed in Phase 3.** The next data-architecture requirement is to retain every individual production risk component point-in-time so later walk-forward validation can test them without reconstruction or leakage.

## Run

```bash
python scripts/run_risk_vnext_3.py
```

Output:

```text
artifacts/research/risk_vnext_3.json
```
