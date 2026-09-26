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
6. Explicit coverage matrix for every event type in the initial Phase-8E taxonomy.
7. Completion gate that fails if any event type is unclassified, any source remains an unresolved generic candidate, or any outcome/production/Phase-7 gate is enabled.
8. CI coverage for the event ledger, 8C reuse, FDA adapter, DOJ adapter, discovery-only boundary and completion gate.

## Implemented event types

- `REGULATORY_APPROVAL`
- `LITIGATION_FILED`
- `MANAGEMENT_CHANGE`

## Explicitly deferred or source-gap event types

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

Each deferred state carries an explicit reason in `configs/external_evidence_8e_structured_events_v1.json`. Deferred means not safely promotable in Phase 8E; it does not mean neutral evidence.

## PIT conclusions

### Phase-8C SEC reuse

Existing validated SEC filing availability remains the historical PIT source. No second SEC parser is introduced.

### FDA Drugs@FDA

Approval state is authoritative. The current bulk file does not independently prove exact historical First Public Release. Historical action dates are retained as event dates; `valid_from` begins at actual ingestion unless separate archive/publication proof exists.

### DOJ Antitrust RSS

The official case-filing feeds explicitly identify filing events. RSS publication dates are retained as source metadata, but a later feed snapshot is not treated as immutable proof of historical observability. `valid_from` begins at actual ingestion without separate archive proof.

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
- no semantic promotion from ambiguous 8C filing metadata.

## What Phase 8E completion does and does not mean

Phase 8E completion means the structured-event source layer has a deterministic, testable and fail-closed architecture with implemented authoritative adapters and explicit non-promotable gaps.

It does **not** mean that FDA approvals, litigation filings or management changes have demonstrated predictive value. Incremental/OOS outcome testing is intentionally deferred to Phase 8G. Cross-factor interactions remain Phase 8H work and Decision Layer integration remains Phase 8I work.
