# Phase 8F — Mapping Candidate Batch B18

Status: `HUMAN_REVIEWED_ACTIVE_REPAIRED_HISTORY`

Batch ID: `8F_MAPPING_CANDIDATES_2026-09-27_B18`

Reviewed at: `2026-09-27T17:42+02:00`

B18 must be read in the repaired coverage history, not the originally overstated running totals. Before B18, the effective map contained **183 current ACTIVE subjects** after accounting correctly for B16 re-reviews/reclassifications and the seven genuinely new B17 subjects.

B18 added six genuinely new documentary mappings and one human-reviewed `EXPLICIT_UNMAPPED` subject, Tokyo Electron (`8035.T`). Therefore the repaired state immediately after B18 was **189 ACTIVE mapped + 1 explicit-unmapped = 190/207 accounted**, with 17 subjects still unaccounted.

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

`8035.T` (Tokyo Electron) is human-reviewed `EXPLICIT_UNMAPPED`.

Tokyo Electron documents that export sales are generally yen-denominated and that some sales and expenses are denominated in foreign currencies, while the profit impact of exchange-rate fluctuations is generally negligible unless fluctuations are extreme. The reviewed evidence does not establish a sufficiently specific EUR/USD relationship or another defensible relationship to the current Phase-8F factor catalog. The subject therefore remains in the frozen denominator without a forced factor mapping.

This is not a claim that Tokyo Electron has no macro exposure. It is a narrower statement: under the current Phase-8F factor definitions and documentary standard, no mapping is accepted from the reviewed evidence.

## Repaired accounting after B18

- ACTIVE reviewed mapped subjects: **189/207**.
- Explicit unmapped: **1/207** (`8035.T`).
- Unaccounted subjects: **17/207**.
- Accounted after B18: **190/207**.
- B18 is overlay order 18.
- Previously identified re-reviews remain represented in the separate resolution ledger and are not double-counted.
- No market outcomes were read or used.
- No market direction, exposure sign, weights or thresholds were assigned.
- No subject was mapped from sector, domicile, ticker, listing currency or assumed business model alone.

B19 later resolves the genuine 17-subject remainder; the current final state must not be retroactively attributed to B18.
