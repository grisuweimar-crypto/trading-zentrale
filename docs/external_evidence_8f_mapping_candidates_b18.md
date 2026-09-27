# Phase 8F — Mapping Candidate Batch B18

Status: `HUMAN_REVIEWED_ACTIVE_FINAL_ACCOUNTING`

Batch ID: `8F_MAPPING_CANDIDATES_2026-09-27_B18`

Reviewed at: `2026-09-27T17:42+02:00`

B18 started from the reviewed/active B1-B17 state: 200 unique active mappings across the frozen 207-subject research domain, with 7 subjects unaccounted.

The seven subjects entering B18 were:
`6861.T`, `FANUY`, `8035.T`, `9888.HK`, `BIDU`, `DVLT`, `JD`.

B18 deliberately separates documentary mappings from explicit-unmapped accounting. Six subjects were human-approved into existing Phase-8F factors. Tokyo Electron (`8035.T`) was human-approved as `EXPLICIT_UNMAPPED` rather than being forced into the EUR/USD-oriented `fx` factor.

## Active B18 mappings

| Subject | Factor | Relationship class | Documentary basis |
| --- | --- | --- | --- |
| 6861.T | fx | OTHER_DOCUMENTED | KEYENCE quantifies foreign-exchange impact and operating-income sensitivity to both USD and EUR moves. |
| FANUY | fx | OTHER_DOCUMENTED | FANUC reports and forecasts both Yen/USD and Yen/EUR rates and identifies foreign-exchange fluctuations as an operating uncertainty. |
| 9888.HK | rates_policy | OTHER_DOCUMENTED | Baidu reports very large interest-earning investment balances and outstanding interest-bearing loans in its 2025 Form 20-F. |
| BIDU | rates_policy | OTHER_DOCUMENTED | Same Baidu issuer filing applies to the separate frozen-domain ADR subject. |
| DVLT | rates_policy | REVENUE_LINK | Datavault AI states that elevated interest rates may reduce IT spending and demand for its products and solutions. |
| JD | rates_policy | FINANCING_SENSITIVITY | JD Property has a material term loan priced relative to Loan Prime Rate with scheduled repayments through 2028. |

All six mappings have `reviewed_at` and `valid_from` of `2026-09-27T17:42+02:00` and remain outcome-blind, unsigned and unweighted.

## Explicit unmapped — Tokyo Electron

`8035.T` (Tokyo Electron) is now human-reviewed `EXPLICIT_UNMAPPED`.

Tokyo Electron documents that export sales are generally yen-denominated and that some sales and expenses are denominated in foreign currencies, while the profit impact of exchange-rate fluctuations is generally negligible unless fluctuations are extreme. The reviewed evidence does not establish a sufficiently specific EUR/USD relationship or another defensible relationship to the current Phase-8F factor catalog. The subject therefore remains in the frozen denominator without a forced factor mapping.

This is not a claim that Tokyo Electron has no macro exposure. It is a narrower statement: under the current Phase-8F factor definitions and documentary standard, no mapping is accepted from the reviewed evidence.

## Final accounting state

- Active reviewed mappings: **206/207**.
- Explicit unmapped: **1/207** (`8035.T`).
- Unaccounted subjects: **0**.
- Total frozen-domain accounting: **207/207**.
- B18 is overlay order 18.
- Seven earlier registered re-reviews remain audit-only and are not double-counted.
- No market outcomes were read or used.
- No market direction, exposure sign, weights or thresholds were assigned.
- No subject was mapped from sector, domicile, ticker, listing currency or assumed business model alone.
- Full domain accounting does not by itself freeze Phase 8F; the separate real macro-ledger and completion requirements remain in force.
