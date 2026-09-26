# Phase 8F – Uranium and Lithium Market Proxy Challengers

Status: outcome-blind challenger preparation only.

## Purpose

Phase 8F may use exchange-traded instruments as market proxies where no timely open commodity benchmark is available, but a proxy must never be relabelled as the underlying commodity spot price.

## Uranium

Preferred challenger hierarchy:

1. **Sprott Physical Uranium Trust NAV (U.UN / U.U)** — commodity-like proxy from 2021-07-19 onward because the trust holds substantially all assets as physical U3O8. The NAV is preferred to the exchange price because market units can trade at material premiums or discounts to NAV. Rights for automated/persistent NAV/benchmark use still require review.
2. **Global X Uranium ETF (URA)** — longer-history equity-sector proxy from 2010-11-04 onward. It contains uranium miners and nuclear-component companies and therefore includes equity beta, issuer risk, FX and industry effects beyond uranium itself.

SPUT and URA remain separate features. They may not be spliced into one synthetic uranium history.

## Lithium

Preferred challenger hierarchy:

1. **CME Lithium Hydroxide / Lithium Carbonate CIF CJK futures** — preferred commodity-like challenger if CME market-data rights, Fastmarkets benchmark rights, contract liquidity and an explicit roll methodology pass review. The contracts are financially settled against lithium price assessments rather than an equity basket.
2. **Global X Lithium & Battery Tech ETF (LIT)** — long-history equity-value-chain proxy from 2010-07-22 onward. It contains lithium mining/refining and battery companies, so it is not a pure lithium-price series.

CME lithium futures and LIT remain separate features. Continuous futures must not be back-adjusted or rolled retrospectively without explicit provenance and a frozen roll contract.

## Hard guards

- proxy != spot;
- no synthetic history made by stitching different proxies;
- no direction assignment;
- no factor weight or threshold selection;
- no outcome inspection in 8F;
- no adjusted historical ETF series without explicit corporate-action provenance;
- no futures continuous-history construction without explicit contract/roll provenance;
- no persistent price/NAV ingestion until the relevant data-use rights are cleared.

Predictive/incremental value belongs to Phase 8G only after the 8F source and mapping layer is frozen.
