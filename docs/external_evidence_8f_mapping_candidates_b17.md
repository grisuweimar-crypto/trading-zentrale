# Phase 8F — Mapping Candidate Batch B17

Status: `HUMAN_REVIEWED_ACTIVE`

Batch ID: `8F_MAPPING_CANDIDATES_2026-09-27_B17`

Human review / valid-from timestamp: `2026-09-27T16:38+02:00`

B17 started from the reviewed/active B1-B16 state at 190/207. All 10 documentary candidates were explicitly approved by the human reviewer and are now registered through the append-only overlay registry. Effective active coverage is therefore 200/207, with 7 subjects still unaccounted.

## Active B17 mappings

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

## Remaining seven subjects

The following seven subjects remain unaccounted and are deliberately not forced into an existing factor mapping:
`6861.T`, `FANUY`, `8035.T`, `9888.HK`, `BIDU`, `DVLT`, `JD`.

Baidu's current filing documents broad RMB/USD foreign-exchange exposure, but that is not promoted as a substitute for the current EUR/USD-oriented `fx` macro series without a cleaner semantic match. Keyence, FANUC, Tokyo Electron, Datavault AI and JD likewise remain for deeper documentary review rather than sector/name-based inference.

## Guardrails

- Approved mapping count: 10.
- Factor mix: `fx` = 6; `rates_policy` = 4.
- Effective coverage after B17: 200/207.
- Unaccounted after B17: 7.
- `reviewed_at` and `valid_from` are no earlier than the explicit human approval timestamp.
- No market outcomes were read or used.
- No market direction, exposure sign, weights or thresholds were assigned.
- No candidate was selected solely from sector, domicile, company name or presumed global footprint.
- YASKY and 6506.T remain separate frozen-domain subjects even though they rely on the same issuer disclosure.
- B17 replay against the current effective map is idempotent and does not add duplicates.
