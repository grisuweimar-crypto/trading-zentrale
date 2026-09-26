# Phase 8F – Macro & Exposure Context

Status: **8F-A1/A2 foundation implemented, outcome-blind**.

Phase 8F starts only after the Phase-8E source/PIT/event-identity layer. It remains a separate external-evidence family and does not alter the frozen Phase-7 Core.

## Purpose

Phase 8F prepares PIT-safe macro observations and a versioned, documented asset-to-factor exposure map.

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

The initial macro source authority is the already registered `fred_alfred_realtime` source.

Official FRED/ALFRED API semantics provide:

- `realtime_start` / `realtime_end` real-time periods;
- historical `vintage_dates`;
- observation downloads for data as it existed on specified historical dates.

This is suitable for PIT reconstruction at the calendar-date level, but it does not provide one universal authoritative intraday publication time for every series.

### Conservative intraday rule

For an independently proven historical ALFRED day-level vintage:

- the vintage date may prove that the value existed on that calendar date;
- it may **not** be treated as proof that the value was available at 00:00 or before an intraday scanner run;
- Phase 8F therefore sets the earliest generic historical `valid_from` to **00:00 UTC on the following calendar day** unless a separate source proves a more precise publication timestamp.

For prospective records without independent historical-vintage proof:

- `valid_from >= ingested_at`.

Later revisions remain separate versioned observations and may never overwrite the original vintage in history.

## Licensing gate

FRED/ALFRED access does not automatically clear every underlying series for redistribution or persistent public artifacts. Some series have third-party copyright restrictions.

Therefore factor-level series selection is still pending and every chosen series must pass a series-specific license/copyright review before persistent use.

CI performs no live FRED/ALFRED requests.

## Factor catalog prepared

The foundation reserves the roadmap factor families without enabling any concrete series yet:

- policy rates;
- yield curve;
- inflation;
- FX;
- oil;
- gas;
- gold;
- silver;
- uranium;
- copper;
- lithium.

All are currently `SOURCE_SERIES_SELECTION_PENDING`.

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

## Current freeze boundary

Implemented now:

- **8F-A1:** Macro vintage/PIT contract;
- **8F-A2:** versioned exposure-map contract and deterministic validator.

Not yet implemented:

- **8F-A3:** concrete series selection + series-level license review;
- **8F-B:** documented population of asset-factor mappings;
- **8F-C:** real PIT macro ingestion;
- **8F-D:** real coverage/context audit;
- **8G:** incremental outcome research against the frozen Phase-7 Core.

## Hard guards

The current layer must keep all of these disabled:

- market outcome reading;
- market direction assignment;
- signed exposure inference;
- weight selection;
- threshold selection;
- cross-factor interactions;
- Phase-7 integration;
- production external evidence.

## Source references

- FRED/ALFRED API observations and vintage-date parameters: `https://fred.stlouisfed.org/docs/api/fred/series_observations.html`
- FRED real-time periods: `https://fred.stlouisfed.org/docs/api/fred/realtime_period.html`
- FRED series vintage dates: `https://fred.stlouisfed.org/docs/api/fred/series_vintagedates.html`
- FRED terms and series-specific copyright restrictions: `https://fred.stlouisfed.org/legal/terms/`
