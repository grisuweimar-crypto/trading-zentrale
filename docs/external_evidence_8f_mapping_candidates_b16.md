# Phase 8F — Mapping Candidate Batch B16

Status: `HUMAN_REVIEWED_RESOLVED`

Batch ID: `8F_MAPPING_CANDIDATES_2026-09-27_B16`

Human review timestamp: `2026-09-27T14:34:54+02:00`

## Repair note

The original B16 documentation incorrectly treated all 14 reviewed candidates as new coverage. The repaired effective-map composition shows that all 14 subjects were already mapped before B16.

B16 therefore changed **zero** frozen-domain coverage subjects:

- 10 reviews are identical re-reviews and are registered audit-only: `ABT`, `ISRG`, `ROK`, `TER`, `AMAT`, `AMZN`, `ASML`, `MU`, `ORCL`, `PLTR`.
- 4 reviews are later human-approved relationship-class changes and are represented as versioned supersessions from the B16 review timestamp: `SYK`, `GOOGL`, `META`, `MSFT`.
- The prior v1 interval is preserved historically and closed at the B16 review timestamp; a v2 interval opens at the same timestamp. No prior mapping is rewritten or backdated.

## Reviewed B16 subjects

| Subject | Factor | Approved relationship class | Resolution |
| --- | --- | --- | --- |
| ABT | fx | CURRENCY_TRANSLATION | audit-only identical re-review |
| ISRG | fx | OTHER_DOCUMENTED | audit-only identical re-review |
| ROK | fx | CURRENCY_TRANSLATION | audit-only identical re-review |
| SYK | fx | OTHER_DOCUMENTED | v1 CURRENCY_TRANSLATION superseded by v2 |
| TER | fx | OTHER_DOCUMENTED | audit-only identical re-review |
| AMAT | fx | OTHER_DOCUMENTED | audit-only identical re-review |
| AMZN | fx | CURRENCY_TRANSLATION | audit-only identical re-review |
| ASML | fx | CURRENCY_TRANSLATION | audit-only identical re-review |
| GOOGL | fx | OTHER_DOCUMENTED | v1 CURRENCY_TRANSLATION superseded by v2 |
| META | fx | OTHER_DOCUMENTED | v1 CURRENCY_TRANSLATION superseded by v2 |
| MSFT | fx | OTHER_DOCUMENTED | v1 CURRENCY_TRANSLATION superseded by v2 |
| MU | fx | OTHER_DOCUMENTED | audit-only identical re-review |
| ORCL | fx | CURRENCY_TRANSLATION | audit-only identical re-review |
| PLTR | fx | OTHER_DOCUMENTED | audit-only identical re-review |

## Correct accounting after B16

- Active mapped subjects before B16: **176/207**.
- New subjects added by B16: **0**.
- Active mapped subjects after B16: **176/207**.
- Explicit-unmapped subjects at this point: **0**.
- Unaccounted subjects after B16: **31**.

The correct 31 unaccounted subjects after B16 were:
`1810.HK`, `6861.T`, `9880.HK`, `FANUY`, `YASKY`, `000660.KS`, `005930.KS`, `0700.HK`, `8035.T`, `9888.HK`, `BIDU`, `DVLT`, `JD`, `6506.T`, `GOT.V`, `LGO`, `SGM.AX`, `LC0A.MU`, `MNSO`, `NOVO-B.CO`, `TME`, `RCAT`, `ACB.TO`, `SMX`, `NPN.JO`, `SE`, `GRAB`, `SPCX`, `OCGN`, `DRO.AX`, `1211.HK`.

## Guardrails

- No market outcomes were read or used.
- No market direction, exposure sign, weights or thresholds were assigned.
- No candidate was selected solely from sector, domicile, name or assumed global footprint.
- No backdating occurred.
- B16 resolution is explicit and append-only; unregistered conflicts remain fail-closed.
