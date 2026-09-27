# Phase 8F — Mapping Candidate Batch B16

Status: `PENDING_HUMAN_REVIEW`

Batch ID: `8F_MAPPING_CANDIDATES_2026-09-27_B16`

B16 starts from the reviewed/active B1-B15 state: 176 unique active mappings across the frozen 207-subject research domain, with 31 subjects still unaccounted.

The 31 remaining subjects are exactly:
`1810.HK`, `6861.T`, `9880.HK`, `ABT`, `CGNX`, `FANUY`, `ISRG`, `ROK`, `SYK`, `TER`, `YASKY`, `000660.KS`, `005930.KS`, `0700.HK`, `8035.T`, `9888.HK`, `AMAT`, `AMZN`, `ASML`, `BIDU`, `DVLT`, `GOOGL`, `IFX.DE`, `JD`, `META`, `MSFT`, `MU`, `ORCL`, `PLTR`, `SAP.DE`, `YASKY`.

B16 contains 14 documentary FX candidates selected only where current SEC/issuer documentation explicitly identifies EUR/USD-linked revenue, expense, monetary, hedge or translation exposure. B16 is not active and does not modify the effective exposure map.

## Candidate subjects

| Subject | Factor | Relationship class | Documentary basis |
| --- | --- | --- | --- |
| ABT | fx | CURRENCY_TRANSLATION | Abbott reports large Euro forward positions and foreign-currency translation effects. |
| ISRG | fx | OTHER_DOCUMENTED | Intuitive Surgical explicitly hedges Euro-denominated revenue, expenses and monetary balances. |
| ROK | fx | CURRENCY_TRANSLATION | Rockwell uses net-investment hedges of Euro-functional subsidiaries with EUR/USD cross-currency swaps. |
| SYK | fx | OTHER_DOCUMENTED | Stryker identifies Euro among principal currency exposures and reports currency effects on sales. |
| TER | fx | OTHER_DOCUMENTED | Teradyne hedges Euro monetary exposures and has entered Euro purchase forwards. |
| AMAT | fx | OTHER_DOCUMENTED | Applied Materials hedges Euro-denominated forecast revenues, costs and cash flows. |
| AMZN | fx | CURRENCY_TRANSLATION | Amazon reports Euro-denominated international operations and Euro senior notes designated as net-investment hedges. |
| ASML | fx | CURRENCY_TRANSLATION | ASML reports in EUR and explicitly quantifies USD exposure against EUR. |
| GOOGL | fx | OTHER_DOCUMENTED | Alphabet identifies Euro among principal currency exposures and hedges revenue, monetary and net-investment exposures. |
| META | fx | OTHER_DOCUMENTED | Meta states that the majority of its non-USD revenue/expense currency exposure is Euro. |
| MSFT | fx | OTHER_DOCUMENTED | Microsoft identifies Euro among principal exposures and quantifies hypothetical FX impact on revenue. |
| MU | fx | OTHER_DOCUMENTED | Micron explicitly identifies Euro exposure in operating expenses, capex, assets and liabilities. |
| ORCL | fx | CURRENCY_TRANSLATION | Oracle translates international revenues, expenses, assets and liabilities to USD and identifies Euro as a principal exposure. |
| PLTR | fx | OTHER_DOCUMENTED | Palantir states that its operations and cash flows are particularly exposed to Euro exchange-rate changes. |

## Guardrails

- Candidate count: 14.
- Factor mix: `fx` = 14.
- No market outcomes were read or used.
- No market direction, exposure sign, weights or thresholds were assigned.
- No candidate was selected solely from sector, domicile, name or assumed global footprint.
- `reviewed_at` and `valid_from` remain unset until explicit human approval.
- No overlay entry is created while B16 remains pending.
- If all 14 are later approved, effective coverage would move from 176/207 to 190/207 and 17 subjects would remain unaccounted.

The remaining difficult block after a full B16 approval would mainly comprise Asian/Japanese/Korean/Chinese issuers and a few U.S./European names for which the currently reviewed evidence does not yet cleanly match the existing Phase-8F factor semantics. Those subjects remain unaccounted rather than forced into a mapping.
