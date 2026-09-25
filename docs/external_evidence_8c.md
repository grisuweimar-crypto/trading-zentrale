# Phase 8C – Fundamentals & Structured Corporate Events

Status: **ACTIVE – 8C-A PIT contract and SEC/EDGAR normalization**

Start: 2026-09-26

## Why 8C is active before 8B

Phase 8A found no analyst-revisions source that was simultaneously historical PIT-safe, accessible/licensed and economically acceptable for this project. 8B remains deferred rather than cancelled. A future path is to archive our own prospective revision snapshots and reopen 8B once a sufficiently long unspent history exists.

Phase 8C can start now because SEC/EDGAR supplies public filing history and XBRL data without an API key, with filings disseminated into the APIs in near real time. The SEC also publishes bulk submission and Company Facts archives for efficient historical ingestion.

## 8C-A objective

Build a deterministic point-in-time layer before looking at any market outcome.

Current components:
- `configs/external_evidence_8c_contract_v1.json`
- `configs/external_event_taxonomy_8c_v1.json`
- `src/scanner/research/external_evidence/sec_edgar.py`
- `scripts/run_external_evidence_8c_probe.py`
- `tests/test_external_evidence_8c_pit.py`

Outcome research and Decision-Layer integration remain disabled.

## PIT source of truth

### Filing publication

The preferred publication timestamp is SEC `acceptanceDateTime` from the Submissions API. `valid_from` may never be earlier than this value.

If the exact timestamp is unavailable but `filingDate` exists, the record is conservatively delayed until 00:00 US/Eastern on the following calendar day. It is never permitted to become valid intraday on the filing date merely because that date is known.

`reportDate`, fiscal-period end, XBRL `end`, earnings period and event time are descriptive dates. They do **not** establish when the project could have known the information.

### XBRL facts

Company Facts is an aggregate view across filings. Therefore:
- every fact must retain its SEC accession number;
- the accession must be joined to the filing publication timestamp;
- multiple values for the same concept/period are all preserved;
- amendments and restatements are later vintages, never replacements for earlier history;
- a current aggregate value must never be retrojected into an earlier scanner state.

A fact whose accession cannot yet be resolved to a submission timestamp is retained with an explicit reason code and is not considered PIT-safe.

## Publication stages

8C uses neutral provenance stages rather than guessing accounting finality:
- `CURRENT_REPORT` – 8-K / 6-K
- `PERIODIC_REPORT` – 10-Q / 10-K / 20-F / 40-F
- `AMENDMENT` – `/A` forms
- `UNKNOWN`

An earnings release is not automatically labelled `preliminary` or `final`; such a label would require evidence from the filing content.

## Fundamental families

The first candidate concept families are:
- revenue
- operating income
- net income
- diluted EPS
- operating cash flow
- capital expenditure
- cash
- assets
- shareholders' equity
- debt

These are **candidate standard tags**, not a universal mapping. Custom company tags may not be silently mapped to a standard concept.

Derived candidates such as revenue-growth change, operating-margin change, FCF change or leverage change require validated matching period, unit/currency and publication vintage. No mismatched contexts may be combined just because the arithmetic is possible.

## Structured events

The SEC Submissions `items` field is used only to create filing-level event candidates. Examples:
- 8-K 2.02 -> `RESULTS_OF_OPERATIONS_RELEASE`
- 8-K 3.02 -> `UNREGISTERED_SECURITIES_SALE`
- 8-K 5.02 -> `MANAGEMENT_OR_DIRECTOR_CHANGE`
- 8-K 7.01 -> `REGULATION_FD_DISCLOSURE`

These labels describe filing subject matter only.

They do **not** imply direction, materiality or a trade action. In particular:
- 2.02 does not mean earnings beat;
- 3.02 does not automatically mean bearish capital raise;
- 5.02 does not mean a negative management change;
- 8.01 has no fixed semantic meaning.

6-K filings without 8-K-style item codes remain unclassified until content extraction exists.

Later semantic targets include guidance raises/cuts, dividend changes, buybacks, capital raises, earnings beat/miss, major contracts, acquisitions and production disruptions. Those require deterministic content evidence and source hierarchy.

## Historical coverage warning

`filings.recent` is only the current compact block. SEC may expose older history through entries in `filings.files`. 8C may not claim historical coverage or run a backtest until all required historical files have been ingested and the as-of coverage ledger is complete.

For large backfills, SEC bulk files are preferable to high-volume per-company requests. Automated access must remain within SEC fair-access guidance; the current SEC guideline is no more than 10 requests per second in aggregate.

## 8C build sequence

### 8C-A – PIT contract & normalizer — **current**
- accession-based identity
- publication-time guard
- immutable vintages
- non-directional filing taxonomy
- deterministic tests / CLI probe

### 8C-B – Full-history ingestion & coverage
- `filings.files` pagination / bulk strategy
- historical identifier coverage
- accession timestamp completeness
- XBRL concept/unit/context coverage
- as-of universe ledger join

### 8C-C – Fundamental change engine
- validated concept resolution
- comparable-period pairing
- revenue / margin / FCF / leverage / EPS change candidates
- missingness and restatement sensitivity

### 8C-D – Structured content extraction
- filing/exhibit source hierarchy
- first-public-release preservation
- explicit guidance/dividend/buyback/capital-raise/event rules
- conflict state when sources disagree

### 8C-E – Research-ready freeze
- feature definitions frozen before outcome inspection
- hypothesis family registered
- coverage domain fixed
- only then hand off individual families to Phase 8G incremental research against frozen Phase 7

## Current acceptance boundary

8C-A is complete only when:
1. contract and taxonomy guards are green;
2. accession versions are preserved in tests;
3. exact acceptance timestamp controls `valid_from`;
4. date-only fallback cannot leak same-day information;
5. 8-K/6-K candidates remain non-directional;
6. Company Facts cannot become PIT-safe without accession publication provenance;
7. the probe explicitly warns that recent-only data is not full historical coverage.

No BUY/HOLD/SELL, portfolio action, Phase-7 rewrite or outcome optimization belongs in 8C-A.
