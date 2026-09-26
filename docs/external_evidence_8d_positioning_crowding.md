# Phase 8D — Positioning / Crowding

Status: `8D_A1_FINRA_IMPLEMENTED_OUTCOME_BLIND`

Phase 8D follows the completed Phase 8C external-evidence foundation. Phase 8C did not enable production external evidence or Phase-7 integration. The same separation remains mandatory here.

## Goal

Build point-in-time safe descriptive positioning evidence without assigning bullish/bearish direction in advance. The first families are:

1. FINRA short interest
2. SEC Form 4 insider activity
3. security-level borrow rates only if a historically usable PIT-safe source becomes available on acceptable terms

No market outcome may be read while defining or validating the source semantics and feature contracts.

## 8D-A — FINRA short interest

Authoritative sources:

- https://www.finra.org/finra-data/browse-catalog/equity-short-interest
- https://developer.finra.org/docs
- https://www.finra.org/filing-reporting/regulatory-filing-systems/short-interest

FINRA requires short-interest reporting twice monthly. The Consolidated Short Interest developer dataset is public and exposes position quantity, prior position, change, average daily volume, days-to-cover, settlement date and revision metadata. FINRA states that the dataset is available by 4:40 PM ET on the publication date.

### PIT rule

`settlementDate` is the economic observation date, not the time the value became public. Historical research may use a short-interest observation only from its publication time onward.

FINRA states that corrected records receive a `Revision Flag` and that only the most recent data is made available. Therefore historical API/download backfill is classified as `LATEST_AVAILABLE_VINTAGE_NOT_ORIGINAL_PUBLICATION_VINTAGE`. It must not be presented as strict original-vintage PIT evidence.

Strict PIT evidence is built prospectively by storing each publication snapshot with:

- settlement date
- publication date and 4:40 PM America/New_York availability time
- `ingested_at`
- source URL / dataset identity
- raw page SHA-256 hashes
- canonical raw-snapshot SHA-256
- record-level revision flag

### 8D-A1 implementation

Implemented on branch `phase8d-positioning-crowding-foundation`:

- `src/scanner/research/external_evidence/finra_short_interest.py`
  - deterministic CSV/JSON normalization
  - explicit historical-vintage vs prospective-snapshot modes
  - New York DST-aware 4:40 PM publication timestamp
  - raw-source SHA-256
  - settlement-date/event-time separation
  - strict-PIT eligibility only for prospectively captured snapshots
  - consistency checks for reported quantity/percent changes
  - no market direction assignment
- `src/scanner/research/external_evidence/finra_short_interest_acquisition.py`
  - filtered POST acquisition for one settlement date
  - synchronous pagination with FINRA `Record-Total` reconciliation when present
  - maximum 5,000 records per request
  - conservative request spacing
  - raw response page retention and page hashes
  - fail-closed rejection if a filtered response contains another settlement date
  - prospective collection forbidden before the documented publication time
- `scripts/import_external_evidence_8d_finra_short_interest.py`
  - imports an existing FINRA CSV/JSON export
  - historical imports cannot become strict-PIT evidence
- `scripts/collect_external_evidence_8d_finra_short_interest.py`
  - performs one prospective publication-snapshot acquisition
- `tests/test_external_evidence_8d_finra_short_interest.py`
  - DST publication-time tests
  - historical-vintage quarantine
  - prospective publication-time gate
  - outcome/direction guards
  - pagination and raw-hash retention
  - wrong-settlement-date rejection

GitHub Actions validates the implementation with synthetic data only. CI performs no live FINRA requests.

### First descriptive features

- short position quantity
- change in short position quantity
- change in short position percent
- days to cover

`short_interest_percent_float` remains disabled until a PIT-safe historical float denominator exists. Daily short-sale volume must never be substituted for short interest.

No absolute high/low value is pre-assigned bullish or bearish meaning.

### 8D-A2 next gate

Before outcome research, measure current-universe and as-of identity coverage:

- exact symbol matches
- ambiguous/missing identities
- market-class distinctions
- symbol-change handling
- latest available FINRA observation per covered security
- explicit `UNKNOWN` / uncovered state

This is descriptive coverage only. It may not select thresholds, infer direction or read future returns.

## 8D-B — SEC insider activity

Authoritative source:

- U.S. SEC EDGAR Forms 4 and 4/A ownership XML
- https://www.sec.gov/submit-filings/technical-specifications

PIT identity is based on issuer CIK plus accession. `transaction_date` is the economic event date; the SEC acceptance timestamp is the earliest research availability time.

The high-precision first challenger uses only transaction codes:

- `P`: purchase of securities on an exchange or from another person
- `S`: sale of securities on an exchange or to another person

Other transaction codes are not silently interpreted as discretionary purchases or sales. Grants/awards, exercises/conversions, tax withholding, gifts, transfers back to the company, swaps/hedges and generic `J` transactions remain outside this first scope.

Forms 4/A are retained as separate versioned evidence. They may correct prior filings but may not silently overwrite the originally available historical state.

Potential descriptive features after source/field validation:

- discretionary purchase/sale count
- discretionary purchase/sale shares
- value when a transaction price is explicitly available
- distinct buyer/seller count
- reporting-owner role mix

No sign or action implication is assigned in advance.

## 8D-C — Borrow rates

Current status: `SOURCE_GAP_DEFERRED`.

Commercial sources exist with historical securities-lending fees and utilization. Their licensing, economics and historical-vintage semantics require separate evaluation before adoption. Current broker borrow fees must not be retrojected into history.

SEC Rule 10c-1a is expected eventually to provide public securities-lending rate information through FINRA, but current public dissemination is not yet available for this project window. The SEC's December 3, 2025 exemptive order moved the Rule 10c-1a reporting compliance date to September 28, 2028 and the public dissemination date to March 29, 2029. Therefore SLATE is not a current 2026 borrow-rate data source.

Fails-to-deliver and short-sale volume are not accepted substitutes for borrow fees.

## Hard boundaries

- no market outcomes during source-contract/source-coverage validation
- no Phase-7 or production integration
- no direction from high or low absolute positioning values
- no current-value retrojection
- no silent neutral for missing or uncovered evidence
- as-of universe identity required before historical feature research
- settlement date is not publication time
- transaction date is not filing availability time
- revisions/amendments remain versioned evidence
- historical FINRA backfills are not strict original-vintage evidence

## Next executable slices

1. `8D-A2`: current-universe coverage and symbol-identity audit over FINRA short interest; no outcomes.
2. `8D-B1`: SEC Form 4/4-A acquisition for verified SEC issuers using the established SEC evidence transport discipline.
3. `8D-B2`: deterministic P/S transaction parser and provenance validation; no outcomes.
4. Only after source and PIT coverage are measured: pre-register descriptive feature validation and later incremental-value research.
