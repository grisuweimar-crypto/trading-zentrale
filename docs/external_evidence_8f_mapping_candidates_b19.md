# Phase 8F — Mapping Candidate Batch B19

Status: `PENDING_HUMAN_REVIEW`

Batch ID: `8F_MAPPING_CANDIDATES_2026-09-28_B19`

B19 starts from the repaired B1-B18 state: **189 current ACTIVE mapped subjects**, **1 human-reviewed explicit-unmapped subject** (`8035.T`) and **17 unaccounted subjects** across the frozen 207-subject domain.

The exact unaccounted set entering B19 is:
`GOT.V`, `LGO`, `SGM.AX`, `LC0A.MU`, `MNSO`, `NOVO-B.CO`, `TME`, `RCAT`, `ACB.TO`, `SMX`, `NPN.JO`, `SE`, `GRAB`, `SPCX`, `OCGN`, `DRO.AX`, `1211.HK`.

B19 deliberately separates defensible documentary mappings from explicit-unmapped proposals. Nothing in this batch is active before explicit human review.

## Documentary mapping candidates

| Subject | Factor | Relationship class | Documentary basis |
| --- | --- | --- | --- |
| GOT.V | gold | OTHER_DOCUMENTED | Goliath describes Surebet as a large high-grade gold system and publishes gold assay/project disclosure. Because the issuer remains exploration-stage, the relationship is project/documentary rather than revenue-linked. |
| SGM.AX | copper | OTHER_DOCUMENTED | Sims reports FY25 earnings support from non-ferrous markets; its annual disclosure identifies copper among principal non-ferrous metals hedged for commodity-price risk. |
| TME | rates_policy | FINANCING_SENSITIVITY | Tencent Music reports RMB3.0bn drawn under facilities whose interest is based on Loan Prime Rate or fixed rate. |
| GRAB | rates_policy | FINANCING_SENSITIVITY | Grab identifies long-term variable-rate borrowings as its main interest-rate risk and states that they are contractually repriced. |
| OCGN | rates_policy | FINANCING_SENSITIVITY | Ocugen documents variable-rate Avenue Capital debt priced from Prime Rate plus a contractual spread subject to a floor. |
| NPN.JO | fx | CURRENCY_TRANSLATION | Naspers reports in USD, identifies EUR among its significant currency exposures, and quantifies sensitivity to a 10% USD move against EUR. |

Candidate factor mix: `rates_policy` = 3, `gold` = 1, `copper` = 1, `fx` = 1.

## Explicit-unmapped proposals

| Subject | Documentary conclusion |
| --- | --- |
| LGO | Strong documented vanadium exposure, but vanadium is outside the current Phase-8F factor catalog. No substitute commodity is inferred. |
| LC0A.MU | Luckin documents RMB/USD risk; current evidence does not establish EUR/USD or another sufficiently specific allowed-factor relationship. |
| MNSO | MINISO has broad multinational FX exposure including EUR, but the documented structure is currencies translated into RMB and separate RMB/USD risk, not a sufficiently specific EUR/USD relationship. |
| NOVO-B.CO | Novo Nordisk identifies USD as its largest FX exposure versus DKK and explicitly regards EUR risk as low because of Denmark's fixed EUR policy; treating that as direct EUR/USD exposure would require inference. |
| RCAT | Reviewed 2025 filing does not establish a current factor-specific revenue, input-cost, financing, currency-translation or balance-sheet relationship meeting the Phase-8F standard. |
| ACB.TO | Reviewed annual disclosure does not provide a sufficiently specific match to the current EUR/USD context or another accepted factor. |
| SMX | Structured/fixed financing and valuation-model rate inputs do not establish a current benchmark-linked financing exposure suitable for promotion. |
| SE | Sea documents material operating FX risk across Asian/Latin-American currencies, but not a sufficiently specific EUR/USD or benchmark-linked rates relationship. |
| SPCX | Public issuer material for private SpaceX does not provide sufficient periodic financial-risk disclosure to verify a relationship-specific mapping. No industry inference is used. |
| DRO.AX | DroneShield reports AUD sensitivity versus USD, GBP and EUR and insignificant rate risk; converting AUD cross-rates into EUR/USD would require prohibited inference. |
| 1211.HK | BYD warns about volatility in key raw-material prices, but reviewed current disclosure does not identify a sufficiently specific lithium-price relationship. EV/battery activity alone is not used as evidence. |

`EXPLICIT_UNMAPPED` is not a claim of no macro exposure. It means only that the reviewed evidence does not justify a mapping to the **current** Phase-8F factor catalog under the relationship-specific documentary standard.

## Pending-state accounting

Until human approval:

- Current ACTIVE mapped subjects: **189/207**.
- Current explicit-unmapped: **1/207** (`8035.T`).
- Current unaccounted: **17/207**.
- B19 mapping candidates active: **0**.
- B19 explicit-unmapped proposals active: **0**.

If all B19 proposals are explicitly approved without modification:

- ACTIVE mapped subjects would become **195/207**.
- Explicit-unmapped subjects would become **12/207**.
- Unaccounted subjects would become **0**.
- Frozen-domain accounting would become **207/207**.

Full accounting would not by itself authorize market-direction research, signed exposure, weights, thresholds, cross-factor optimization, Phase-7 integration or production enablement. Separate Phase-8F completion requirements would still apply.

## Guardrails

- No market outcomes were read or used.
- No market direction or exposure sign was assigned.
- No weights or thresholds were selected.
- No sector/name/listing-currency inference was promoted.
- No mapping or explicit-unmapped proposal can activate automatically.
- `reviewed_at` and `valid_from` do not exist for B19 until explicit human approval.
- CI polling remains deferred until the frozen domain is fully accounted, per project instruction.
