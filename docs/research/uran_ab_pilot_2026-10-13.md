# A/B-Uran-Pilot – 2026-10-13 (Research only)

Authoritativer Scanner-Snapshot: `ff86b0b4-8b91-444d-a80f-2ad2ff40547a`; 220/220 Titel. Für die beiden Titel liegen derzeit **keine privat ausgeführten 7H-Position-Watches** als Testeingabe vor. Die Zahlen unten stammen ausschließlich aus `daily_research.json`, nicht aus einer berechneten B-Entscheidung.

| Merkmal | UEC | UUUU |
| --- | ---: | ---: |
| Rang | 210 | 194 |
| Score | 8.47 | 12.56 |
| RS3M | -14.70% | -23.17% |
| Trend200 | -35.59% | -44.47% |
| Cycle | 25.10 | 22.13 |
| Confidence | 38.8 LOW | 36.8 LOW |
| Score-Delta 5D | -1.47 | -0.42 |
| R-Code | R1 | R1 |
| 5T positive / N | 31/68 = 45.6% | 18/48 = 37.5% |
| 10T positive / N | 38/65 = 58.5% | 7/33 = 21.2% |
| 20T positive / N | 38/59 = 64.4% | 1/9 = 11.1% |
| 40T positive / N | 40/53 = 75.5% | 0/0 – unavailable |
| 20T median raw return | +3.78% | -8.27% |
| 40T median raw return | +12.79% | unavailable |

**Comparability guard:** UEC level_2 uses bottom10 + RS3M[-20,0) + Trend200<-20%; UUUU level_1 uses 80..90 percentile + R1 + RS3M<-20% + Trend200<-20%. These are **not matched cohorts**, thus the different positive rates are **not** evidence that one stock or one algorithm outperforms the other. Outcomes are raw historical returns, not cost-adjusted alpha. Overlapping dates and within-sector dependence require further controls.

**PIT guard:** W10 stage `phase6_elliott` was `not_supplied`; do not retrofit Elliott contexts to this sealed snapshot.

## Frozen B hypotheses (not yet confirmed)
1. R1 + strongly negative Trend200 + negative RS3M predicts positive sector-adjusted 20T or 40T alpha, independently of the baseline scanner/7F state.
2. A fully point-in-time Elliott-Fibonacci confirmation improves expected net alpha or drawdown versus (1), conditional on sufficient observations.
3. For held uranium positions, separate entry/add, hold, trim, and exit evaluation has higher net decision utility than using a single raw return probability.

## Preregistered evaluation constraints
- Use exact snapshot-bound A baseline, no retrospective action modification.
- Freeze feature and pattern versions, eligibility, horizons and missing-data rules **before** looking at test outcomes.
- Compare net outcomes using SPY and a uranium-sector proxy when an appropriate point-in-time benchmark is available.
- Use chronological holdout, embargo / de-overlap, sector/date clustered uncertainty, transaction-cost scenarios and a separate prospective confirmation period.
- A pattern with inadequate independent events returns **INSUFFICIENT_EVIDENCE**, not a favorable probability.
- Candidate observations can be recorded in Pattern Discovery Lab, but productive promotion requires its existing governance gates.

## Not yet executed
- Private A-Watch replay for UEC/UUUU (requires runtime-only position book and complete matching 7A–7G packets).
- Actual B challenger run and out-of-sample test.
- Sector-neutral alpha comparison after costs.

Do not infer trade orders from this document.
