# Phase 8F Mapping Candidates — Batch B3

Status: `PENDING_HUMAN_REVIEW`

Batch B3 deliberately increases the review batch size from 10 to 20 only for candidates with direct documentary support. Difficult or ambiguous subjects are excluded from this batch and will be researched separately rather than inferred from sector, name, theme or model judgment.

## Candidates

| Subject | Factor | Relationship | Documentary basis |
|---|---|---|---|
| ASM.TO | silver | REVENUE_LINK | 2025 issuer results report silver share of revenue and realized silver price. |
| HMY | gold | REVENUE_LINK | FY2026 issuer results report gold production, gold price received and production profit. |
| WPM.TO | gold | REVENUE_LINK | 2025 issuer results report 62% of revenue from gold and higher realized precious-metals prices. |
| ASML | fx | CURRENCY_TRANSLATION | 2025 annual report quantifies USD-versus-EUR foreign-currency sensitivity. |
| IFX.DE | fx | CURRENCY_TRANSLATION | 2025 annual report quantifies EUR/USD effects on revenue and Segment Result. |
| RR.L | fx | CURRENCY_TRANSLATION | 2025 annual report identifies significant USD and EUR cash-flow exposure. |
| SAP.DE | fx | CURRENCY_TRANSLATION | 2025 annual report / reported outlook explicitly models USD/EUR currency effects. |
| DB1.DE | rates_policy | OTHER_DOCUMENTED | 2025 annual report states market-rate changes affect net income and treasury results. |
| ALV.DE | rates_policy | OTHER_DOCUMENTED | 2025 annual report documents interest-rate risk and explicit sensitivity limits. |
| NEE | rates_policy | FINANCING_SENSITIVITY | 2025 annual report documents substantial variable/floating-rate debt. |
| KEP | gas | INPUT_COST_LINK | Official KEPCO fuel-cost formula explicitly includes LNG prices and weighting. |
| CME | rates_policy | OTHER_DOCUMENTED | 2025 10-K documents interest-rate products as largest volume category and links activity to Fed-policy uncertainty. |
| ICE | rates_policy | OTHER_DOCUMENTED | Official 2025 market statistics document large interest-rate derivatives activity tied to monetary-policy shifts. |
| MA | fx | CURRENCY_TRANSLATION | 2025 10-K documents foreign-currency translation and euro functional revenue exposure. |
| V | fx | CURRENCY_TRANSLATION | 2025 10-K quantifies EUR/USD translation exposure through Visa Europe. |
| PYPL | fx | CURRENCY_TRANSLATION | 2025 10-K quantifies foreign-exchange effects on revenue and operating income and includes euro exposure. |
| COIN | rates_policy | OTHER_DOCUMENTED | 2025 10-K quantifies interest-rate sensitivity of cash interest and USDC-related stablecoin revenue. |
| AMZN | fx | CURRENCY_TRANSLATION | 2025 10-K documents euro-denominated international sales/expenses and material FX effects. |
| MSFT | fx | CURRENCY_TRANSLATION | 2025 10-K identifies euro as a principal currency exposure and quantifies FX sensitivity. |
| META | fx | CURRENCY_TRANSLATION | 2025 10-K states the majority of non-USD revenue/expense currency exposure is in euros. |

Machine-readable evidence URLs, evidence summaries and canonical evidence-record SHA-256 fingerprints are frozen in `configs/external_evidence_8f_mapping_candidates_b3_v1.json`.

## Review rules

- No candidate may enter the active exposure map before explicit human review.
- No mapping may be backdated before the actual review timestamp.
- Sector, industry, company name, thematic pillar and LLM inference are not evidence.
- No market outcomes may be consulted in this review.
- No exposure direction, sign, weight or threshold is assigned in Phase 8F.
- `OTHER_DOCUMENTED` means only that a direct documentary relationship exists but does not fit the narrower revenue/input-cost/financing/currency/balance-sheet classes; it does not imply predictive direction.
- Difficult subjects remain outside B3 and require deeper documentary research rather than forced classification.
