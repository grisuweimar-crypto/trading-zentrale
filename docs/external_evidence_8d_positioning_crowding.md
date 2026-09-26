# Phase 8D — Positioning / Crowding

Status: `8D_A1_A2_B1_B2_IMPLEMENTED_OUTCOME_BLIND`

Phase 8D follows the completed Phase 8C external-evidence foundation. Production external evidence, Phase-7 integration, market-direction assignment and outcome research remain disabled.

## Goal

Build point-in-time safe descriptive positioning evidence before any outcome-based research. Initial families:

1. FINRA short interest
2. SEC Form 4/4-A insider activity
3. security-level borrow rates only if a historically usable PIT-safe source becomes available on acceptable terms

No absolute high/low positioning value is assigned bullish or bearish meaning in advance.

## 8D-A — FINRA short interest

Authoritative sources:

- https://www.finra.org/finra-data/browse-catalog/equity-short-interest
- https://developer.finra.org/docs
- https://www.finra.org/filing-reporting/regulatory-filing-systems/short-interest

FINRA publishes consolidated short-interest data twice monthly. `settlementDate` is the economic observation date, not the public availability time.

### PIT / vintage rule

FINRA states that corrected records can carry a Revision Flag and that only the most recent data is made available. Therefore:

- historical backfill = `LATEST_AVAILABLE_VINTAGE_NOT_ORIGINAL_PUBLICATION_VINTAGE`
- prospective snapshots are stored with actual `ingested_at`
- scheduled FINRA publication time is retained as `published_at`
- a later retrieval is never retrojected backward to the scheduled publication time
- strict PIT use begins at the actual ingestion timestamp unless original-vintage availability is independently proven

### 8D-A1 — acquisition and normalization

Implemented:

- `src/scanner/research/external_evidence/finra_short_interest.py`
- `src/scanner/research/external_evidence/finra_short_interest_acquisition.py`
- `scripts/import_external_evidence_8d_finra_short_interest.py`
- `scripts/collect_external_evidence_8d_finra_short_interest.py`

Key guards:

- one settlement date per prospective snapshot
- filtered POST + pagination
- max 5,000 rows per synchronous request
- reconciliation to FINRA `Record-Total` when supplied
- raw page SHA-256 and canonical raw-snapshot SHA-256
- wrong settlement date fails closed
- no live FINRA requests in CI

First descriptive fields:

- short position quantity
- change quantity
- change percent
- days to cover

`short_interest_percent_float` remains disabled until a PIT-safe historical float denominator exists. Short-sale volume is not substituted for short interest.

### 8D-A2 — current-universe coverage

Implemented:

- `src/scanner/research/external_evidence/finra_short_interest_coverage.py`
- `scripts/run_external_evidence_8d_finra_coverage.py`

Coverage is outcome-blind and uses only exact scanner-symbol matching.

Forbidden shortcuts:

- ticker suffix stripping
- ADR substitution
- fuzzy-name matching
- automatic market-class selection

Missing matches are `UNKNOWN_NOT_IN_FINRA_SNAPSHOT`. Multiple exact rows remain `AMBIGUOUS_MULTIPLE_FINRA_ROWS`. Coverage accepts only a single-settlement-date FINRA snapshot.

## 8D-B — SEC insider activity

Authoritative sources:

- https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets
- https://www.sec.gov/files/insider_transactions_readme.pdf

The SEC Insider Transactions Data Sets are quarterly, flattened extracts of Ownership XML Forms 3/4/5 and amendments. Phase 8D uses only the official `SUBMISSION`, `REPORTINGOWNER`, and `NONDERIV_TRANS` tables for the first challenger.

The SEC documentation defines:

- `P` = open-market or private purchase
- `S` = open-market or private sale

### 8D-B1 — acquisition / PIT provenance

Implemented:

- `src/scanner/research/external_evidence/sec_insider_bulk.py`
- `scripts/import_external_evidence_8d_sec_insider_bulk.py`

The quarterly SEC bulk ZIP is not allowed to invent historical availability from its download date. Instead every Form 4/4-A row must join by exact accession number and issuer CIK to the already operator-attested SEC submissions bundle from Phase 8C. That bundle supplies the filing acceptance metadata and conservative PIT `valid_from`.

Hard requirements:

- official `sec.gov` bulk source URL
- direct official SEC bulk bundle from Phase 8C
- exact accession join
- exact issuer-CIK join
- source ZIP SHA-256
- individual table SHA-256
- current ticker is descriptive metadata only, never historical stable identity
- Form 4/A remains separate versioned evidence and never silently overwrites the original

### 8D-B2 — high-precision P/S semantic challenger

Only non-derivative Table-I transactions with code `P` or `S` enter the P/S evidence set.

A row is eligible for the initial **discretionary** high-precision candidate only when all of the following hold:

- filing is Form 4 or 4/A
- `TRANS_FORM_TYPE == 4`
- `P` is paired with acquired code `A`, or `S` with disposed code `D`
- no equity swap is involved
- `AFF10B5ONE` is explicitly false
- transaction shares are known
- accession/issuer identity and PIT provenance are valid

Fail-closed states remain separate evidence, not discretionary trades:

- `CONFLICTING_ACQUIRED_DISPOSED_CODE`
- `EXCLUDED_NON_FORM4_TRANSACTION`
- `EXCLUDED_EQUITY_SWAP`
- `EXCLUDED_10B5_1_PLAN`
- `P_S_DISCRETIONARY_UNRESOLVED_10B5_1`
- `P_S_PARTIAL_MISSING_SHARES`

Other Section-16 transaction codes such as grants, exercises, tax-withholding, gifts, transfers, derivatives and generic other transactions are not silently mapped to discretionary purchase/sale semantics.

Potential later descriptive features, only after source/coverage validation:

- discretionary purchase/sale count
- discretionary purchase/sale shares
- value when price is explicitly known
- distinct buyer/seller count
- reporting-owner relationship mix

No market direction or portfolio action is assigned.

## 8D-C — borrow rates

Status: `SOURCE_GAP_DEFERRED`.

A source must provide security-level historical borrow fee/rebate observations with immutable as-of timestamps and acceptable licensing/economics. The following are forbidden substitutes:

- fails-to-deliver as borrow rate
- short-sale volume as borrow rate
- current borrow fees retrojected historically

## Hard boundaries

- no market outcomes during source/coverage/semantic validation
- no threshold selection
- no Phase-7 or production integration
- no current-value retrojection
- no silent neutral for missing evidence
- as-of universe identity required before historical feature research
- settlement date is not FINRA publication/observation time
- transaction date is not SEC filing availability time
- amendments/revisions remain versioned evidence

## Next executable slice

`8D-B3 / 8D validation preparation`: run real quarterly SEC insider bulk data through B1/B2, measure issuer/accession/P-S coverage and candidate-state counts, and freeze a prospective validation protocol before any market-outcome research. FINRA A1/A2 likewise still requires a real publication snapshot before its coverage can be measured empirically.
