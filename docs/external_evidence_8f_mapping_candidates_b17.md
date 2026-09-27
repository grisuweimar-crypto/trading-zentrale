# Phase 8F — Mapping Candidate Batch B17

Status: `PENDING_HUMAN_REVIEW`

Batch ID: `8F_MAPPING_CANDIDATES_2026-09-27_B17`

B17 starts from the reviewed/active B1-B16 state: 190 unique active mappings across the frozen 207-subject research domain, with 17 subjects still unaccounted.

The 17 remaining subjects before B17 are:
`1810.HK`, `6861.T`, `9880.HK`, `CGNX`, `FANUY`, `YASKY`, `000660.KS`, `005930.KS`, `0700.HK`, `8035.T`, `9888.HK`, `BIDU`, `DVLT`, `IFX.DE`, `JD`, `SAP.DE`, `6506.T`.

B17 contains 10 documentary candidates. Four are direct financing-sensitivity mappings to `rates_policy`; six are documentary FX mappings with explicit USD/EUR or EUR/USD exposure. B17 is not active and does not modify the effective exposure map.

## Candidate subjects

| Subject | Factor | Relationship class | Documentary basis |
| --- | --- | --- | --- |
| 1810.HK | rates_policy | FINANCING_SENSITIVITY | Xiaomi quantifies the profit-before-tax effect of a 50 bp change in floating borrowing rates. |
| 9880.HK | rates_policy | FINANCING_SENSITIVITY | UBTECH reports floating-rate long-term borrowings and quantifies a 50 bp sensitivity. |
| CGNX | fx | CURRENCY_TRANSLATION | Cognex reports euro/USD European sales and translation of foreign subsidiaries into USD. |
| 000660.KS | rates_policy | FINANCING_SENSITIVITY | SK hynix quantifies a 1 pp rate shock and documents floating-rate borrowing exposure managed with swaps. |
| 005930.KS | fx | OTHER_DOCUMENTED | Samsung identifies USD and EUR as main currency exposures and quantifies a 5% FX sensitivity. |
| 0700.HK | rates_policy | FINANCING_SENSITIVITY | Tencent documents floating-rate borrowings and interest-rate swaps covering borrowing principal. |
| IFX.DE | fx | OTHER_DOCUMENTED | Infineon directly quantifies EUR/USD effects on revenue and Segment Result. |
| SAP.DE | fx | CURRENCY_TRANSLATION | SAP reports in EUR, translates non-EUR revenue into EUR and quantifies the reported revenue impact of exchange-rate changes. |
| YASKY | fx | OTHER_DOCUMENTED | Yaskawa explicitly identifies USD and EUR operating currencies and hedging of currency exposure. |
| 6506.T | fx | OTHER_DOCUMENTED | Same Yaskawa issuer disclosure applies to the separate frozen-domain Tokyo listing. |

## Deliberately deferred subjects

The following seven subjects remain outside B17 because the currently reviewed documentation does not yet meet the same relationship-specific standard against the existing Phase-8F factor catalog:
`6861.T`, `FANUY`, `8035.T`, `9888.HK`, `BIDU`, `DVLT`, `JD`.

In particular, Baidu's current filing documents broad RMB/USD foreign-exchange exposure, but B17 does not promote that as a substitute for the current EUR/USD-oriented `fx` macro series without a cleaner semantic match. Keyence, FANUC, Tokyo Electron, Datavault AI and JD remain for deeper documentary review rather than sector/name-based inference.

## Guardrails

- Candidate count: 10.
- Factor mix: `fx` = 6; `rates_policy` = 4.
- No market outcomes were read or used.
- No market direction, exposure sign, weights or thresholds were assigned.
- No candidate was selected solely from sector, domicile, company name or presumed global footprint.
- YASKY and 6506.T remain separate frozen-domain subjects even though they rely on the same issuer disclosure.
- `reviewed_at` and `valid_from` remain unset until explicit human approval.
- No B17 overlay entry is created while B17 remains pending.
- If all 10 are later approved, effective coverage would move from 190/207 to 200/207 and 7 subjects would remain unaccounted.
