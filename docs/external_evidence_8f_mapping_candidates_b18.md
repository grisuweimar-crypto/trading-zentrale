# Phase 8F — Mapping Candidate Batch B18

Status: `HUMAN_REVIEWED_ACTIVE_PARTIAL_ACCOUNTING`

Batch ID: `8F_MAPPING_CANDIDATES_2026-09-27_B18`

Reviewed at: `2026-09-27T17:42+02:00`

## Repair note

The original B18 documentation incorrectly declared full 207/207 accounting because the preceding coverage counters had double-counted re-reviews. After repair, B18 starts from **183 active mapped subjects** and **24 unaccounted subjects**.

B18 adds six genuinely new active mappings and one human-reviewed explicit-unmapped subject:

- new mapped: `6861.T`, `FANUY`, `9888.HK`, `BIDU`, `DVLT`, `JD`
- explicit-unmapped: `8035.T`

B18 therefore increases frozen-domain accounting by seven subjects, from 183/207 to **190/207**, not to 207/207.

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

## Correct accounting after B18

- Historical mapping intervals in the repaired effective map: **195**.
- Active mapped subjects: **189/207**.
- Superseded historical intervals: **6**.
- Explicit-unmapped subjects: **1/207** (`8035.T`).
- Total accounted frozen-domain subjects: **190/207**.
- Unaccounted subjects: **17**.

The exact 17 unaccounted subjects are:
`GOT.V`, `LGO`, `SGM.AX`, `LC0A.MU`, `MNSO`, `NOVO-B.CO`, `TME`, `RCAT`, `ACB.TO`, `SMX`, `NPN.JO`, `SE`, `GRAB`, `SPCX`, `OCGN`, `DRO.AX`, `1211.HK`.

## Resolution/audit state

- 18 identical re-reviews are registered audit-only and are not double-counted.
- 6 conflicting later human reviews are represented as versioned supersessions rather than retroactive rewrites.
- No market outcomes were read or used.
- No market direction, exposure sign, weights or thresholds were assigned.
- No subject was mapped from sector, domicile, ticker, listing currency or assumed business model alone.
- Phase 8F remains unfrozen. The remaining 17 subjects must be mapped or explicitly-unmapped through the same documentary/human-review process, and the separate macro-ledger/completion requirements must also pass.
- CI polling remains deferred until the complete 207-subject accounting is finished.
