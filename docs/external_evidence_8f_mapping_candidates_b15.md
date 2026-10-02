# Phase 8F — Mapping Candidate Batch B15

Status: `HUMAN_REVIEWED_ACTIVE`

Batch ID: `8F_MAPPING_CANDIDATES_2026-09-27_B15`

Reviewed at / valid from: `2026-09-27T13:46:34+02:00`

B15 started from the reviewed/active B1-B14 state of 171 unique active mappings across the frozen 207-subject domain, with 36 subjects still unaccounted. The five documentary candidates were explicitly human-reviewed and approved. B15 is registered as the eleventh append-only human-reviewed overlay.

After B15, the effective exposure map contains **176 unique reviewed mappings across the frozen 207-subject domain**, leaving **31 subjects unaccounted**. The seven explicitly registered later re-reviews from B8/B10 remain audit-only and are not double-counted.

## Active subjects

| Subject | Factor | Relationship class | Documentary basis |
| --- | --- | --- | --- |
| SYM | rates_policy | OTHER_DOCUMENTED | Symbotic 2025 Form 10-K explicitly identifies changes in market interest rates as a market-risk exposure for its large short-duration cash/investment position. |
| USAR | rates_policy | FINANCING_SENSITIVITY | A current post-merger SEC filing states that USAR assumed Serra Verde's DFC debt; the Initial Loan bears Term SOFR plus 4.0%. |
| NESN.SW | rates_policy | FINANCING_SENSITIVITY | Nestlé 2025 financial statements identify USD/EUR interest-rate exposure on financial debt and quantify a 100 bp sensitivity to net financing cost. |
| TOM.OL | fx | CURRENCY_TRANSLATION | TOMRA 2025 annual report identifies EUR as its main currency exposure/presentation currency and states that currency gains and losses are mostly exposed to EUR/USD. |
| UMI.BR | fx | OTHER_DOCUMENTED | Umicore FY2025 results document structural FX hedging including a specifically quantified EUR/USD hedge and unhedged translation effects into EUR. |

## Factor mix

- rates_policy: 3
- fx: 2

## Review notes

- `SYM` remains `OTHER_DOCUMENTED`, not `FINANCING_SENSITIVITY`: the filing documents treasury/investment rate exposure and explicitly says the short-duration position was not materially exposed to rate changes.
- `USAR` is `FINANCING_SENSITIVITY`: the Serra Verde transaction closed on 3 September 2026, and a subsequent USAR filing states that the surviving subsidiary assumed the DFC financing and that the Initial Loan is priced at Term SOFR plus 4.0%. The mapping therefore uses post-close evidence rather than projecting pre-close target debt onto USAR.
- `NESN.SW` is `FINANCING_SENSITIVITY` because the issuer directly quantifies interest-rate sensitivity on financial debt after derivatives.
- `TOM.OL` is `CURRENCY_TRANSLATION` because EUR is the presentation currency and the issuer explicitly identifies EUR/USD as the principal driver of currency gains/losses in the financial statements.
- `UMI.BR` remains conservatively `OTHER_DOCUMENTED`: the source directly documents EUR/USD structural exposure and hedging, while also noting unhedged translation effects; no signed FX direction is inferred.
- No mapping was selected from sector/name intuition alone. Several remaining Asian/local-rate cases continue to be deferred where documentary exposure is primarily to local benchmarks or currencies not represented by the current Phase-8F factor semantics.
- Exploration/project names without an explicit economic price linkage were not promoted merely because their deposits contain a commodity covered by the factor catalog.
- No market outcomes, market direction, signed exposure, weights, thresholds or Phase-7 decision outputs were used.
- All five mappings are usable **no earlier than `2026-09-27T13:46:34+02:00`**. No backdating is permitted.

Machine-readable evidence references, summaries and canonical metadata fingerprints remain frozen in `configs/external_evidence_8f_mapping_candidates_b15_v1.json`. The explicit human decisions are stored separately in `configs/external_evidence_8f_mapping_review_decisions_b15_v1.json`.
