# Phase 8F — Mapping Candidate Batch B18

Status: `PENDING_HUMAN_REVIEW`

Batch ID: `8F_MAPPING_CANDIDATES_2026-09-27_B18`

B18 starts from the reviewed/active B1-B17 state: 200 unique active mappings across the frozen 207-subject research domain, with 7 subjects still unaccounted.

The seven remaining subjects before B18 are:
`6861.T`, `FANUY`, `8035.T`, `9888.HK`, `BIDU`, `DVLT`, `JD`.

B18 deliberately separates documentary mappings from explicit-unmapped accounting. Six subjects have defensible current documentary relationships to existing Phase-8F factors. Tokyo Electron (`8035.T`) remains a separate pending `EXPLICIT_UNMAPPED` proposal rather than being forced into the EUR/USD-oriented `fx` factor.

## Mapping candidates

| Subject | Factor | Relationship class | Documentary basis |
| --- | --- | --- | --- |
| 6861.T | fx | OTHER_DOCUMENTED | KEYENCE quantifies foreign-exchange impact and operating-income sensitivity to both USD and EUR moves. |
| FANUY | fx | OTHER_DOCUMENTED | FANUC reports and forecasts both Yen/USD and Yen/EUR rates and identifies foreign-exchange fluctuations as an operating uncertainty. |
| 9888.HK | rates_policy | OTHER_DOCUMENTED | Baidu reports very large interest-earning investment balances and outstanding interest-bearing loans in its 2025 Form 20-F. |
| BIDU | rates_policy | OTHER_DOCUMENTED | Same Baidu issuer filing applies to the separate frozen-domain ADR subject. |
| DVLT | rates_policy | REVENUE_LINK | Datavault AI states that elevated interest rates may reduce IT spending and demand for its products and solutions. |
| JD | rates_policy | FINANCING_SENSITIVITY | JD Property has a material term loan priced relative to Loan Prime Rate with scheduled repayments through 2028. |

## Explicit-unmapped proposal

`8035.T` (Tokyo Electron) is proposed as `EXPLICIT_UNMAPPED`, pending human review.

Tokyo Electron documents that export sales are generally yen-denominated and that some sales and expenses are denominated in foreign currencies, while the profit impact of exchange-rate fluctuations is generally negligible unless fluctuations are extreme. The reviewed evidence does not establish a sufficiently specific EUR/USD relationship or another defensible relationship to the current Phase-8F factor catalog. The subject therefore remains in the frozen denominator without a forced factor mapping.

This is not a claim that Tokyo Electron has no macro exposure. It is a narrower statement: under the current Phase-8F factor definitions and documentary standard, no mapping is accepted from the reviewed evidence.

## Guardrails

- Mapping candidate count: 6.
- Pending explicit-unmapped proposals: 1.
- Mapping factor mix: `fx` = 2; `rates_policy` = 4.
- Effective active mapping count remains 200 while B18 is pending.
- Research-domain `explicit_unmapped` remains empty until explicit human approval.
- No market outcomes were read or used.
- No market direction, exposure sign, weights or thresholds were assigned.
- No subject was mapped from sector, domicile, ticker, listing currency or assumed business model alone.
- `reviewed_at` and mapping `valid_from` remain unset until explicit human approval.
- No B18 overlay entry exists while B18 remains pending.
- If all six mappings and the single explicit-unmapped proposal are approved, Phase-8F domain accounting becomes complete: 206/207 mapped + 1/207 explicit-unmapped = 207/207 accounted, with zero unaccounted subjects.
- Completion of domain accounting does not itself freeze Phase 8F; the separate real macro-ledger and completion requirements still apply.
