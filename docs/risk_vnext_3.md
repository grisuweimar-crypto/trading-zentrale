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

## Run

```bash
python scripts/run_risk_vnext_3.py
```

Output:

```text
artifacts/research/risk_vnext_3.json
```
