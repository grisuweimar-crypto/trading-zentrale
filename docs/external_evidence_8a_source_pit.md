# Phase 8A – External Source & PIT Contract

Status: **COMPLETE** on 2026-09-26. Next active work: **8C – Fundamentals & Structured Corporate Events**. Phase 8B is deferred, not cancelled.

## Objective

Phase 8A establishes which external data may enter Phase-8 research at all. It does not test predictive value and it does not modify the frozen Phase-7 Core.

Every source is classified on coverage, point-in-time semantics, license/access, history, publication timing, revision/restatement behaviour and validated domain. A source being interesting or inexpensive is not enough.

## Non-negotiable PIT rule

A value may be joined to a historical scanner observation only if the project can prove when that value first became publicly usable. Fiscal period, settlement date, event date and observation date are not interchangeable with `published_at`/`valid_from`.

Current values may never be retrojected merely because they refer to a historical quarter, settlement date or company.

## Revisions field/provenance probe

The reproducible decision contract is `configs/external_revision_source_probe_v1.json`; `scripts/run_external_revision_source_probe_8a.py` evaluates the contract without consulting outcomes.

### Alpha Vantage Earnings Estimates – `UNSAFE` for retrospective 8B

Official documentation lists only `function`, `symbol` and `apikey` for `EARNINGS_ESTIMATES`. It does not expose a historical `date`/`as_of` parameter and does not establish an immutable observation/publication timestamp for each consensus vintage.

The endpoint may still be useful prospectively if responses are archived from ingestion onward, but a current response may not be assigned to a historical scanner date.

### EODHD Earnings Trends – `UNSAFE` for retrospective 8B

The provider explicitly documents that the trends endpoint has no date parameters; supplying `from`/`to` does not change the response. The response `date` is the fiscal period end, while estimate fields move as analysts publish. The 7/30/60/90-day values describe movement relative to the response state; they are not a queryable historical vintage ledger.

This also remains a possible prospective source if snapshots are stored from now onward.

### Intrinio/Zacks Estimates – PIT-capable, but not access/economically cleared

Intrinio documents historical estimate/revision feeds with date filtering and 20+ years of history. However, EPS estimates are Enterprise-only and historical access requires separate commercial access/payment. Therefore source semantics can be PIT-capable while the project still correctly rejects it for current 8B use.

### Financial Modeling Prep Analyst Estimates – `UNSAFE` for retrospective 8B

The documented analyst-estimate endpoint does not establish immutable historical consensus vintages suitable for the Phase-8 contract. Current fiscal-period estimates may not be retrojected.

## Final 8A revisions decision

No revisions source found in 8A satisfies all of the following simultaneously:

1. historical consensus vintages;
2. observation/publication timestamp or equivalent strict as-of reconstruction;
3. usable historical access;
4. acceptable license/access status;
5. economically cleared access for this project.

Therefore:

- **8B Revisions Single-Family Pilot is DEFERRED, not cancelled.**
- **8C Fundamentals & Structured Corporate Events becomes the next active Phase-8 workstream.**
- 8B may reopen if a PIT-safe historical revisions feed is obtained under acceptable terms, or after a sufficiently long prospective revisions ledger has been accumulated without retrojection.

This is a data-validity decision, not an outcome/performance decision. No revisions outcomes were inspected to choose this route.

## Why 8C is viable now

### SEC EDGAR Company Facts/XBRL – `PARTIAL`, usable after versioned reconstruction

The source carries filing/accession metadata, but aggregate Company Facts is not treated as a ready-made PIT panel. 8C must reconstruct first-known values by accession/filed time and preserve amendments/restatements as later versions.

### SEC EDGAR submissions / 8-K / 6-K – `SAFE` for publication identity and filing time

Filing timestamp/accession identity are suitable anchors for structured corporate-event research. This does not yet validate an event classifier; event extraction remains separate.

This combination gives 8C a low-cost, auditable starting point without violating the frozen Phase-7 baseline.

## Positioning and Macro retained for later work

**FINRA Equity Short Interest** remains `PARTIAL`: settlement and publication dates differ, and corrected values can overwrite prior values unless original releases were archived.

**FRED/ALFRED** remains `SAFE` at the vintage layer, but macro evidence still requires a versioned security-to-exposure map and series-specific license checks.

## As-of Universe / Coverage Ledger

`configs/external_universe_coverage_contract_v1.json` remains mandatory for all later family backtests. It prevents:

1. keeping only symbols that still exist today;
2. keeping only symbols for which a vendor has data today;
3. silently dropping renamed/delisted instruments;
4. converting missing vendor coverage into neutral evidence.

Historical project observability comes first from the scanner/history itself. External listing/delisting sources are cross-checks, not substitutes.

## 8A acceptance state

Completed:
- source-registry instance and schema exist;
- PIT/access/coverage classifications are explicit;
- As-of-Universe/Coverage contract exists;
- revision-source probe is reproducible and outcome-independent;
- Alpha Vantage/EODHD retrospective PIT failure is explicit;
- Intrinio access/economic restriction is explicit;
- no source is silently promoted;
- missing/unknown remains non-neutral;
- automated guards enforce the route decision;
- final route is frozen as **8C next**.

8A is closed. The next branch should implement 8C's versioned SEC fundamental/event ingestion contract before any predictive-value research is performed.
