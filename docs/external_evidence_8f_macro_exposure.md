# Phase 8F – Macro & Exposure Context

Status: **8F-A1/A2 foundation implemented, outcome-blind; source routing reviewed**.

Phase 8F starts only after the Phase-8E source/PIT/event-identity layer. It remains a separate external-evidence family and does not alter the frozen Phase-7 Core.

## Purpose

Phase 8F prepares the contracts required for PIT-safe macro observations and a versioned, documented asset-to-factor exposure map.

It does **not** yet test predictive value, assign bullish/bearish direction, select thresholds or weights, run cross-factor interactions, integrate with Phase 7, or enable production external evidence.

## Why the exposure map comes first

The Phase-8 research plan states that Macro & Exposure Context is valid only with a versioned exposure map. A theoretical relationship is not sufficient evidence.

Therefore this foundation fails closed:

- no sector-to-factor auto-mapping;
- no keyword or company-name mapping;
- no LLM-generated mapping;
- no signed exposure;
- no factor weight;
- no threshold;
- no retrospective mapping from today's company description or business mix.

Each future asset-factor mapping must carry a documentary evidence reference, SHA-256 fingerprint, explicit human review, review timestamp and validity interval.

A mapping researched today cannot be silently projected backward into historical scanner dates. It becomes usable no earlier than its review time and documentary evidence availability.

## Macro vintage contract

The Phase-8A registry identified `fred_alfred_realtime` as technically attractive for PIT macro reconstruction because ALFRED provides real-time periods and historical vintage dates.

That remains a useful reference model for calendar-date PIT semantics, but Phase 8F no longer assumes that FRED/ALFRED will be the actual data source.

For any independently proven historical day-level vintage source:

- the vintage date may prove that the value existed on that calendar date;
- it may **not** be treated as proof that the value was available at 00:00 or before an intraday scanner run;
- Phase 8F therefore sets the earliest generic historical `valid_from` to **00:00 UTC on the following calendar day** unless a separate source proves a more precise publication timestamp.

For prospective records without independent historical-vintage proof:

- `valid_from >= ingested_at`.

Later revisions remain separate versioned observations and may never overwrite the original vintage in history.

## FRED/ALFRED source blocker

The 8F source review on 2026-09-26 found that current official FRED Terms of Use prohibit FRED API access in connection with development of a software program/system and prohibit API use in connection with storing, caching or archiving FRED content.

Scanner_vNext is a software/research system whose Phase-8 design requires persisted, versioned evidence. Therefore:

- `fred_alfred_realtime` is **not enabled** as an 8F ingestion source;
- no FRED/ALFRED data are collected or persisted by this implementation;
- ALFRED is retained only as a PIT/vintage-semantics reference candidate;
- FRED/ALFRED could be reconsidered only after explicit permission/clarification or changed terms.

## Alternative source routing

Phase 8F does not require one provider for all factors. The preferred design is now **primary source by factor**.

The detailed routing matrix is frozen in `configs/external_evidence_8f_source_candidates_v1.json`.

### Inflation

**BLS archived CPI releases** are the strongest initial retrospective candidate.

- archived CPI releases are available on the BLS site back to 1994;
- releases carry explicit embargo/release timestamps (08:30 America/New_York for the 2026 schedule);
- BLS states that its published material is public domain except separately copyrighted images/illustrations;
- seasonally adjusted values can be revised, so historical research must consume the archived release version rather than today's latest API value.

Status: `CLEAR_FOR_ADAPTER_BUILD`.

### U.S. policy rates / yield curve

**Federal Reserve Board H.15** provides direct official interest-rate data and is published daily at 16:15 America/New_York.

The Board states that website information is public domain unless otherwise indicated.

Historical downloadable H.15 series can contain later corrections, so current history is not automatically an original-vintage panel. Strict retrospective use therefore requires archived release proof; prospective snapshots can be clean from actual ingestion onward.

Status: `CLEAR_FOR_PROSPECTIVE_ADAPTER_BUILD`.

**U.S. Treasury Daily Treasury Rates** provide direct nominal and real yield-curve archives, including nominal history from 1990 and real curves from 2003. Treasury states that indicative quotations underlying the nominal curve are obtained around 15:30 America/New_York.

Status: useful direct yield-curve candidate, with the same original-vintage caution for historical corrections.

### Euro rates / yield curve / FX

**ECB Data Portal** is a strong European primary source.

- ESCB statistics may be freely reused with source attribution, subject to the ECB reuse policy and third-party exclusions;
- euro-area yield curves are published daily at noon CET and are available from 2004-09-06;
- ECB reference FX rates are normally updated around 16:00 CET after the 14:15 concertation procedure;
- the API supports SDMX retrieval and update/revision queries.

Current historical data may still reflect later revisions, so historical original-vintage use remains separate from prospective collection unless revision history is independently preserved.

Status: `CLEAR_FOR_PROSPECTIVE_ADAPTER_BUILD`.

### Oil and gas

**U.S. EIA Open Data** is the preferred high-frequency energy candidate.

- EIA API terms explicitly allow development of services that retrieve, display and analyze EIA data;
- long daily histories exist for WTI and Brent crude oil spot prices;
- long daily Henry Hub natural-gas spot-price history is available.

Current historical series may contain corrections, so prospective snapshotting is strict PIT; retrospective use requires release-vintage validation where corrections matter.

Status: `CLEAR_FOR_PROSPECTIVE_ADAPTER_BUILD`.

### Gold, silver and copper

**World Bank Pink Sheet** is the preferred open monthly candidate for these factors and can also provide monthly oil/gas context.

- monthly Pink Sheet publications and historical monthly workbooks are available;
- World Bank-produced open datasets default to CC BY 4.0 unless dataset metadata states otherwise;
- monthly documents carry publication metadata.

Before activation we still need to verify the exact Pink Sheet dataset license metadata and confirm that archived monthly publications preserve the values required for first-release reconstruction.

Status: `CLEAR_FOR_ARCHIVE_AND_LICENSE_VALIDATION`.

**IMF Primary Commodity Prices** remains a secondary fallback. IMF statistical-data terms permit downloading, extracting, copying, transforming and distributing IMF Data with attribution, but historical vintage semantics are not yet proven for our use and IMF's general automated bulk-access restrictions make it less attractive than EIA/World Bank for this project.

### Uranium

EIA publishes an official **Uranium Marketing Annual Report** with explicit release dates and revisions. It is usable as slow structural context, but annual frequency is too low to replace a timely uranium market/spot factor.

Status: source exists, but **primary timely uranium context remains a source gap**.

### Lithium

No clean open standardized lithium price series has yet passed the source gate. World Bank methodology explicitly notes that lithium is not covered by its GEM/Pink Sheet price data and uses S&P Global Market Intelligence in another methodology context.

Status: **deferred source gap** rather than inserting a weak proxy.

## Factor catalog / current routing

- `rates_policy`: Fed Board H.15 + ECB candidates;
- `yield_curve`: U.S. Treasury / Fed H.15 + ECB candidates;
- `inflation`: BLS archived CPI releases;
- `fx`: ECB Data Portal;
- `oil`: EIA primary, World Bank/IMF slower fallbacks;
- `gas`: EIA primary, World Bank/IMF slower fallbacks;
- `gold`: World Bank monthly candidate, IMF fallback;
- `silver`: World Bank monthly candidate, IMF fallback;
- `copper`: World Bank monthly candidate, IMF fallback;
- `uranium`: EIA annual only for structural context; timely source gap;
- `lithium`: source gap.

## Code contract

`src/scanner/research/external_evidence/macro_exposure_8f.py` implements:

1. validation of versioned PIT macro observations;
2. conservative day-level historical-vintage handling;
3. explicit UNKNOWN/non-neutral missingness;
4. validation of documented human-reviewed exposure mappings;
5. prevention of retroactive mapping;
6. deterministic as-of joining of the latest macro revision actually knowable at the requested timestamp;
7. hard prohibition of direction, outcome, weight and threshold fields in the foundation layer;
8. an executable 8F contract gate.

## Current boundary

Implemented:

- **8F-A1:** Macro vintage/PIT contract;
- **8F-A2:** versioned exposure-map contract and deterministic validator;
- **8F-A3:** alternative-source feasibility/routing review, including explicit FRED blocker and factor-specific candidates.

Next:

- validate and implement the first adapters, beginning with the cleanest source families;
- series-level license/copyright checks before activation;
- populate documented exposure mappings without retrojection;
- run real coverage/context audit;
- freeze 8F before 8G incremental outcome research.

## Hard guards

The current layer keeps disabled:

- market outcome reading;
- market direction assignment;
- signed exposure inference;
- weight selection;
- threshold selection;
- cross-factor interactions;
- Phase-7 integration;
- production external evidence.
