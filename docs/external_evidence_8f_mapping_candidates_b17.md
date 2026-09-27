# Phase 8F — Mapping Candidate Batch B17

Status: `HUMAN_REVIEWED_RESOLVED`

Batch ID: `8F_MAPPING_CANDIDATES_2026-09-27_B17`

Human review / valid-from timestamp: `2026-09-27T16:38+02:00`

## Repair note

The original B17 accounting incorrectly counted all 10 human-approved candidates as new coverage. Under the repaired effective-map composition, B17 contains three different cases:

- **7 genuinely new subjects:** `1810.HK`, `9880.HK`, `000660.KS`, `005930.KS`, `0700.HK`, `YASKY`, `6506.T`.
- **2 versioned re-classifications:** `CGNX`, `IFX.DE`.
- **1 identical audit-only re-review:** `SAP.DE`.

The two re-classifications preserve their previous v1 intervals and open v2 intervals only from the B17 human-review timestamp. The SAP.DE re-review does not add or replace an active mapping.

## Reviewed B17 mappings

| Subject | Factor | Approved relationship class | Resolution |
| --- | --- | --- | --- |
| 1810.HK | rates_policy | FINANCING_SENSITIVITY | new active subject |
| 9880.HK | rates_policy | FINANCING_SENSITIVITY | new active subject |
| CGNX | fx | CURRENCY_TRANSLATION | v1 OTHER_DOCUMENTED superseded by v2 |
| 000660.KS | rates_policy | FINANCING_SENSITIVITY | new active subject |
| 005930.KS | fx | OTHER_DOCUMENTED | new active subject |
| 0700.HK | rates_policy | FINANCING_SENSITIVITY | new active subject |
| IFX.DE | fx | OTHER_DOCUMENTED | v1 CURRENCY_TRANSLATION superseded by v2 |
| SAP.DE | fx | CURRENCY_TRANSLATION | audit-only identical re-review |
| YASKY | fx | OTHER_DOCUMENTED | new active subject |
| 6506.T | fx | OTHER_DOCUMENTED | new active subject |

## Correct accounting after B17

- Active mapped subjects before B17: **176/207**.
- New subjects added by B17: **7**.
- Active mapped subjects after B17: **183/207**.
- Explicit-unmapped subjects at this point: **0**.
- Unaccounted subjects after B17: **24**.

The correct 24 unaccounted subjects after B17 were:
`6861.T`, `FANUY`, `8035.T`, `9888.HK`, `BIDU`, `DVLT`, `JD`, `GOT.V`, `LGO`, `SGM.AX`, `LC0A.MU`, `MNSO`, `NOVO-B.CO`, `TME`, `RCAT`, `ACB.TO`, `SMX`, `NPN.JO`, `SE`, `GRAB`, `SPCX`, `OCGN`, `DRO.AX`, `1211.HK`.

## Guardrails

- No market outcomes were read or used.
- No market direction, exposure sign, weights or thresholds were assigned.
- No candidate was selected solely from sector, domicile, company name or presumed global footprint.
- YASKY and 6506.T remain separate frozen-domain subjects even though they rely on the same issuer disclosure.
- B17 resolution is explicit and append-only; unregistered conflicting duplicates remain fail-closed.
