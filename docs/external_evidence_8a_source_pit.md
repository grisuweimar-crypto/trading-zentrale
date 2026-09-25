# Phase 8A – External Source & PIT Contract

Status: ACTIVE / initial source classification completed on 2026-09-26.

## Objective

Phase 8A establishes which external data may enter Phase-8 research at all. It does not test predictive value yet and it does not modify the frozen Phase-7 Core.

Every source is classified on coverage, point-in-time semantics, license/access, history, publication timing, revision/restatement behaviour and validated domain. A source being interesting or inexpensive is not enough.

## Non-negotiable PIT rule

A value may be joined to a historical scanner observation only if the project can prove when that value first became publicly usable. Fiscal period, settlement date, event date and observation date are not interchangeable with `published_at`/`valid_from`.

Current values may never be retrojected merely because they refer to a historical quarter, settlement date or company.

## Initial source inventory

The machine-readable inventory lives in `configs/external_source_registry_v1.json`.

### Revisions

1. **Alpha Vantage Earnings Estimates** – `PARTIAL`
   - official documentation states EPS/revenue estimates, analyst count and revision history;
   - documentation reviewed so far does not prove immutable historical as-of snapshots with per-observation publication timestamps;
   - candidate for a low-friction field-level validation, not yet a backtest source.

2. **EODHD Earnings Trends** – `PARTIAL`
   - exposes current consensus, 7/30/60/90-day EPS trend values and revision counts;
   - documentation describes a historical set of fiscal-period records, but fiscal-period `date` is not enough to establish the timestamp of the estimate observation;
   - requires a licensed sample and vintage-stability test.

3. **Intrinio/Zacks Estimates** – `SAFE` for source semantics, `RESTRICTED` for access/license
   - vendor explicitly documents historical estimate/revision data and long history;
   - this is the strongest PIT candidate found in the first survey;
   - it is not usable by the project until commercial access and field-level timestamps/terms are confirmed.

4. **FMP Analyst Estimates** – `UNSAFE` for historical revisions under the currently documented endpoint
   - current/future fiscal-period consensus does not establish a historical consensus time series;
   - historical ratings are not a substitute for dated EPS/revenue estimate vintages.

**8B is therefore not opened yet.** The next 8A task is to validate Alpha Vantage and EODHD at field level. If both fail strict PIT and Intrinio is not economically acceptable, the pre-authorised fallback is to move 8C Fundamentals/Structured Corporate Events ahead of 8B.

### Fundamentals

**SEC EDGAR Company Facts/XBRL** is currently `PARTIAL` rather than `SAFE`.

The source has filing/accession metadata and real-time dissemination, but the aggregate Company Facts endpoint is not itself a ready-made PIT panel. The Phase-8 ingestion path must reconstruct first-known values by accession/filed time and preserve amendments/restatements as later versions.

### Structured Corporate Events

**SEC EDGAR submissions (8-K/6-K and related forms)** are `SAFE` for filing timestamp/accession identity. This does not validate an event classifier. Event extraction and taxonomy remain a later Phase-8C/8E problem.

### Positioning

**FINRA Equity Short Interest** is `PARTIAL` for historical PIT research.

The settlement date and publication date are explicitly different and `valid_from` must be the publication date. FINRA also states that corrections may replace prior values and only the most recent corrected value is available in the interactive/API data. Without archived original releases, a corrected historical row can contaminate a backtest. Prospective ingestion can be made PIT-safe by storing every publication as received.

### Macro

**FRED/ALFRED** is `SAFE` at the data-vintage layer because real-time periods/vintage dates are explicit. It is not yet promotable evidence because:
- underlying-series licenses must be checked per series;
- a versioned security-to-macro exposure mapping does not yet exist;
- macro data cannot become evidence merely by theoretical sector intuition.

## As-of Universe / Coverage Ledger

`configs/external_universe_coverage_contract_v1.json` is mandatory before any family backtest.

It prevents four major leakage paths:
1. keeping only symbols that still exist today;
2. keeping only symbols for which a vendor has data today;
3. silently dropping renamed/delisted instruments;
4. converting missing vendor coverage into neutral evidence.

Historical project observability comes first from the scanner/history itself. External listing/delisting sources are cross-checks, not substitutes.

## 8A acceptance state

Completed in this initial slice:
- source-registry instance exists;
- PIT/access/coverage classifications are explicit;
- no source is promoted;
- revisions decision remains gated;
- As-of-Universe/Coverage contract exists;
- failure states stay visible instead of becoming neutral.

Still required before 8A can close:
- field-level API validation for the preferred revisions candidates;
- exact access/license/cost decision for whichever source survives;
- sample coverage measurement against the project universe;
- ingestion fixture proving `published_at`, `valid_from`, vintage and identifier mapping;
- automated registry/ledger guards green on CI.
