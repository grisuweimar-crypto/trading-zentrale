# Phase 8E Completion Report

Status: `PHASE_8E_SOURCE_LAYER_COMPLETE_OUTCOME_RESEARCH_PENDING_8G`

## Scope completed

Phase 8E now has a frozen source/PIT/event-identity layer for structured events and discovery-only news.

Implemented components:

1. Event ledger with deterministic event identity, source hierarchy, conflict handling and separate `published_at` / `first_public_release_at` / `ingested_at` / `valid_from` semantics.
2. Conservative reuse of already validated Phase-8C SEC management-change evidence without semantic upgrading of ambiguous filing items.
3. FDA Drugs@FDA regulatory-approval adapter using exact `Approval` actions and ingestion-based PIT when historical publication time is not independently proven.
4. DOJ Antitrust civil/criminal case-filing RSS adapter for `LITIGATION_FILED`, with source-reported publication metadata retained but no historical backdating from later snapshots.
5. FTC Competition and DOJ Antitrust press-release RSS ingestion as discovery-only evidence that cannot create canonical events or market direction.
6. A generic **prospective structured primary-release challenger** for FTC/DOJ authority releases and issuer IR. It accepts only source-native structured labels or explicitly human-reviewed structured labels; it does not auto-classify free text.
7. Explicit coverage state for every event type in the frozen Phase-8E taxonomy.
8. Completion gate that validates taxonomy coverage, source status, PIT boundaries, the primary-release challenger contract, and disabled outcome/production/Phase-7 gates.
9. CI coverage for the event ledger, 8C reuse, FDA adapter, DOJ adapter, discovery-only boundary, primary-release challenger and completion gate.

## Implemented source-native / validated event types

- `REGULATORY_APPROVAL` — FDA Drugs@FDA; prospectively PIT-safe from actual ingestion, while historical First Public Release remains unresolved unless independently proven.
- `LITIGATION_FILED` — DOJ Antitrust official case-filing RSS; prospectively PIT-safe from actual ingestion.
- `MANAGEMENT_CHANGE` — conservative reuse of validated Phase-8C SEC metadata.

## Prospective structured challenger event types

The following event types have a usable source-layer path, but their event semantics are **not promoted**. They require source-native structured labels or human-reviewed structured intake and later family-specific validation:

- `GUIDANCE_RAISE`
- `GUIDANCE_CUT`
- `REGULATORY_REJECTION`
- `MAJOR_CONTRACT`
- `ACQUISITION_ANNOUNCEMENT`
- `TAKEOVER_OFFER`
- `ACQUISITION_COMPLETION`
- `CAPITAL_RAISE`
- `LITIGATION_RULING`
- `LITIGATION_SETTLEMENT`
- `PRODUCT_LAUNCH`
- `PRODUCTION_DISRUPTION`

A challenger state is not neutral evidence and is not production-eligible. In particular, the existing Phase-8C guidance and capital-raise semantic boundaries remain in force; the primary-release challenger does not silently promote them.

## PIT conclusions

### Phase-8C SEC reuse

Existing validated SEC filing availability remains the historical PIT source. No second SEC parser is introduced.

### FDA Drugs@FDA

Approval state is authoritative. The current bulk file does not independently prove exact historical First Public Release. Historical action dates are retained as event dates; `valid_from` begins at actual ingestion unless separate archive/publication proof exists.

### DOJ Antitrust RSS

The official case-filing feeds explicitly identify filing events. RSS publication dates are retained as source metadata, but a later feed snapshot is not treated as immutable proof of historical observability. `valid_from` begins at actual ingestion without separate archive proof.

### Structured primary releases

For FTC/DOJ authority releases and issuer IR, a page/RSS date is retained only as a source claim unless independently proven by immutable archival evidence. Without that proof:

- `published_at = null`;
- `public_release_proof_status = INGESTION_ONLY`;
- `valid_from = ingested_at`;
- First Public Release remains unresolved.

Actual ingestion proves that the release was observable by that moment; it does **not** fabricate an earlier source publication timestamp.

### News discovery

FTC/DOJ press-release feeds are discovery-only. They never independently create canonical events, event direction or production evidence.

## Hard boundaries retained

- market outcomes not read;
- no bullish/bearish direction assignment;
- generic sentiment disabled;
- no threshold selection;
- no interaction research;
- no Phase-7 integration;
- no production external evidence;
- no current news retrojection;
- no fuzzy event deduplication;
- no missing -> neutral fallback;
- no semantic promotion from ambiguous Phase-8C filing metadata;
- no ingestion timestamp masquerading as independently proven First Public Release.

## What Phase 8E completion does and does not mean

Phase 8E completion means the structured-event source layer is deterministic, testable, fail-closed and has an explicit status for every taxonomy family: implemented source-native/validated, prospective challenger, discovery-only or explicitly unavailable.

It does **not** mean any event family has demonstrated predictive value. Incremental/OOS outcome testing is intentionally deferred to Phase 8G. Cross-factor interactions remain Phase 8H work and Decision Layer integration remains Phase 8I work.
