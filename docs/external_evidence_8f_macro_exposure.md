# Phase 8F – Macro & Exposure Context

Status: **source/PIT foundation, first adapters, append-only ledger and completion gate implemented; Phase 8F is not yet frozen**.

Phase 8F remains a separate external-evidence family on top of the frozen Phase-7 Core. It does not alter Selection, Timing, Probability, Risk, Confidence, Elliott or the Phase-7 decision layer.

## Hard boundary

The current implementation remains outcome-blind:

- no market outcome reading;
- no bullish/bearish assignment;
- no signed exposure inference;
- no weights;
- no thresholds;
- no cross-factor interaction research;
- no Phase-7 integration;
- no production external evidence.

The dedicated completion gate prevents Phase 8F from being declared finished while real PIT observations and reviewed exposure mappings are still missing.

## PIT / vintage contract

Two historical-availability modes are supported:

1. **Exact independently proven publication timestamp**: `valid_from` may equal the proven `published_at` timestamp.
2. **Day-level historical vintage only**: the conservative earliest `valid_from` remains 00:00 UTC on the following calendar day.

Prospective observations without independent historical proof use `valid_from >= ingested_at`.

Later revisions are separate append-only observations and may never overwrite an earlier vintage.

UNKNOWN/STALE/LICENSED_OUT/LOW_COVERAGE states may not smuggle a numeric zero as a fake neutral value.

## Source blocker: FRED / ALFRED

FRED/ALFRED remains blocked for Scanner_vNext ingestion under the terms reviewed on 2026-09-26. It is retained only as a conceptual vintage-semantics reference. No FRED/ALFRED data are persisted by Phase 8F.

## Implemented source adapters

### BLS CPI – retrospective exact PIT

Adapter: `bls_cpi_8f.py`

Implemented release-level series:

- `BLS_CPI_U_ALL_ITEMS_MOM_SA_RELEASE_PCT`
- `BLS_CPI_U_ALL_ITEMS_YOY_NSA_RELEASE_PCT`

The adapter parses archived CPI news releases, including the release/embargo timestamp. Historical use is allowed only when the archived release itself independently proves the exact publication time. The source text is SHA-256 fingerprinted and later revisions remain separate.

### U.S. EIA – prospective energy snapshots

Adapter: `eia_energy_8f.py`

Implemented series:

- `PET.RWTC.D` – WTI spot;
- `PET.RBRTE.D` – Brent spot;
- `NG.RNGWHHD.D` – Henry Hub natural gas.

Current-history/API rows are **not** retrojected as historical PIT evidence. They become strict PIT only from actual ingestion unless a separate historical archive proves earlier availability.

### Federal Reserve Board H.15 – prospective rates / curve snapshots

Adapter: `fed_h15_8f.py`

Implemented raw series:

- `RIFSPFF_N.D` – effective federal funds rate;
- `RIFLGFCY02_N.B` – 2-year Treasury constant maturity;
- `RIFLGFCY10_N.B` – 10-year Treasury constant maturity.

No spread, direction or threshold is derived in 8F. Current downloadable history remains prospective-only for strict PIT unless original-vintage archive proof is introduced.

### ECB – prospective USD/EUR FX snapshots

Adapter: `ecb_fx_8f.py`

Implemented series:

- `EXR.D.USD.EUR.SP00.A` – USD per EUR reference rate.

Current ECB history is not retrojected as original-vintage evidence. Strict PIT starts at actual ingestion unless archived release/revision proof is added.

## Multiple-series context semantics

A factor may legitimately contain several raw series. Phase 8F therefore keeps the latest knowable revision **per series**, not one arbitrary series per factor.

Examples:

- inflation: CPI MoM and CPI YoY remain separate;
- oil: WTI and Brent remain separate;
- yield curve: 2Y and 10Y remain separate.

The foundation does not collapse them into a spread, score, sign or weighted aggregate.

## Uranium and lithium market proxies

The proxy contract is separate from the commodity-source contract.

### Uranium

- Sprott Physical Uranium Trust NAV: commodity-like physical-trust challenger from 2021;
- URA: long-history uranium/nuclear equity-sector challenger from 2010.

They may not be spliced into one synthetic history and may not be relabelled as uranium spot.

### Lithium

- CME lithium futures: preferred commodity-like challenger if exchange/benchmark rights pass;
- LIT: long-history lithium/battery equity-sector challenger from 2010.

LIT is not a lithium spot-price substitute because it contains mining, refining and downstream battery-equity effects.

## Gold, silver and copper

World Bank Pink Sheet remains the preferred monthly open-source candidate. The source review found a World Bank commodity-price dataset licensed CC BY 4.0 and archived monthly Pink Sheet publications with publication metadata.

The remaining gate is not the general license principle but exact first-release/vintage reconstruction: historical values must be tied to the publication that actually contained them rather than today's revised workbook.

IMF remains fallback only.

## Series catalog and source routing

`series_catalog_8f.py` verifies that implemented adapters and configured series agree with the source-routing contract, cannot use blocked sources, and cannot silently change a factor/source relationship.

Current implemented raw macro series count: **9** across:

- inflation;
- rates_policy;
- yield_curve;
- FX;
- oil;
- gas.

Gold, silver and copper remain archive-validation candidates. Uranium and lithium are represented by separate challenger proxy contracts, not fake spot series.

## Append-only macro ledger

`macro_ledger_8f.py` provides the Phase-8F observation ledger.

It enforces:

- append-only observation identity;
- no revision overwrite;
- collision failure if the same revision identity appears with changed semantic content;
- as-of exclusion of future `valid_from` rows;
- coverage summaries by source, factor and series;
- separation of exact historical-release proof from prospective-ingestion proof.

Synthetic test rows validate the code but do **not** count as real 8F evidence.

## Exposure map

`configs/external_evidence_8f_exposure_map_v1.json` remains intentionally empty at this point.

Every promoted mapping must have:

- subject ID;
- factor ID;
- documentary evidence reference;
- SHA-256 evidence fingerprint;
- evidence-valid-from time;
- explicit human review;
- review timestamp;
- mapping validity interval;
- descriptive relationship class only.

Automatic sector/name/keyword/LLM inference remains forbidden. A mapping cannot be retrojected before documentary evidence and review.

## Completion / freeze gate

`completion_8f.py` and `configs/external_evidence_8f_completion_v1.json` make completion fail closed.

Phase 8F may be frozen only when all of the following are true:

1. foundation/source/series/proxy contracts pass;
2. a **real** append-only macro ledger contains knowable observations;
3. at least one active documentary human-reviewed exposure mapping exists;
4. the intended research domain is explicitly defined;
5. every subject in that domain is accounted for as mapped or explicitly unmapped;
6. all outcome/direction/threshold/Phase-7 guards remain disabled.

Current expected completion state: **BLOCKED**, because the real prospective ledger and reviewed research-domain mapping set have not yet been populated. This is a guardrail, not a test failure.

## Current implementation status

Completed technically:

- 8F-A1 macro PIT/vintage contract;
- 8F-A2 versioned exposure-map contract;
- 8F-A3 factor-specific source routing and FRED blocker;
- BLS historical CPI adapter;
- EIA prospective oil/gas adapter;
- Fed H.15 prospective rates/yield adapter;
- ECB prospective FX adapter;
- uranium/lithium proxy contract;
- multi-series-per-factor context fix;
- append-only macro ledger and coverage audit;
- fail-closed 8F completion/freeze gate;
- CI tests for all of the above without live network calls.

Still required before 8F freeze:

1. real PIT/prospective data collection into the append-only ledger;
2. archive validation / optional adapter for World Bank gold, silver and copper;
3. explicit research-domain definition;
4. documentary human-reviewed exposure-map population for that domain;
5. real coverage/context audit;
6. final 8F freeze artifact.

Only after that may Phase 8G inspect outcomes and test incremental predictive value.
