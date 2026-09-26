# Phase 8E – Structured Events & News

Status: `8E-A SOURCE HIERARCHY + FIRST PUBLIC RELEASE IMPLEMENTED; 8E-B1 PHASE-8C REUSE IMPLEMENTED; OUTCOME-BLIND`

## Objective

Phase 8E adds event-centric external evidence on top of the already validated Phase-8 foundations. It starts with structured, timestampable events. Generic sentiment remains disabled.

The initial taxonomy follows the Phase-8 research plan:

- guidance raise / cut;
- regulatory approval / rejection;
- major contract;
- acquisition announcement / takeover offer / completion;
- capital raise;
- litigation filing / ruling / settlement;
- management change;
- product launch;
- production disruption.

No event type carries a predefined bullish/bearish market direction.

## Relationship to Phase 8C

Phase 8C remains the source of truth for SEC filing/event extraction that has already been built and validated there. Phase 8E does not create a second SEC content parser.

8E adds:

1. deterministic event identity supplied by a domain adapter;
2. First Public Release provenance;
3. source-authority conflict resolution;
4. event-state versioning across multiple primary/authoritative sources.

### 8E-B1 conservative 8C reuse

The first executable reuse adapter is intentionally narrow.

The validated 8C filing-metadata label `DIRECTOR_OR_OFFICER_CHANGE` is mapped to the broad 8E taxonomy value `MANAGEMENT_CHANGE`. The adapter preserves SEC `published_at` and `valid_from`, the accession/item identity and an SHA-256 hash of the exact upstream 8C row.

The following 8C metadata labels are **not** promoted into richer 8E semantics:

- `MATERIAL_DEFINITIVE_AGREEMENT`;
- `ACQUISITION_OR_DISPOSITION_COMPLETED`;
- `UNREGISTERED_EQUITY_SALE`;
- `REGULATION_FD_DISCLOSURE`;
- `OTHER_MATERIAL_EVENT`;
- `RESULTS_RELEASE`.

For example, Item 2.01 cannot be silently treated as an acquisition because the validated 8C label includes acquisitions **or dispositions**. Item 1.01 cannot be assumed to be a major contract or takeover agreement. Capital raise and guidance semantics remain blocked until independently validated content evidence exists.

B1 therefore reuses 8C provenance without upgrading 8C metadata beyond what it actually proves.

## Source hierarchy

The initial hierarchy is:

1. `PUBLIC_AUTHORITY_PRIMARY` – regulator/government/court authority for the state within its jurisdiction;
2. `MANDATORY_ISSUER_FILING` – legally mandated issuer filing such as SEC EDGAR;
3. `OFFICIAL_EXCHANGE_DISCLOSURE`;
4. `ISSUER_PRIMARY_RELEASE` – issuer IR/official release;
5. `COUNTERPARTY_OR_AWARDING_AUTHORITY_RELEASE`;
6. `REPUTABLE_NEWS_DISCOVERY_ONLY` – discovery only, never sufficient alone for a canonical promoted event.

The rank resolves conflicting event states. It does **not** choose the earliest timestamp.

## First Public Release vs valid_from

These fields are intentionally separate.

- `published_at`: source publication timestamp when independently provable;
- `first_public_release_at`: earliest proven `published_at` across strict-PIT source evidence for the canonical event;
- `ingested_at`: when this project actually acquired the evidence;
- `valid_from`: earliest time at which the evidence is safe to use under the source-specific PIT contract.

Without independent historical publication proof, `valid_from` may not precede `ingested_at`.

A later high-authority source may confirm or correct event state, but may never retroactively move `valid_from` or First Public Release backward.

## Initial source candidates

### Existing Phase-8C SEC evidence

Status: conservative reuse implemented for the validated management-change metadata domain. Additional 8C event semantics require their own validated content evidence before reuse.

### FDA / Drugs@FDA

Candidate domain: `REGULATORY_APPROVAL`.

FDA states that Drugs@FDA contains approved human drug products, regulatory history and approval letters; coverage extends back decades and the database is updated daily. Approval state can therefore come from an authoritative public source. Exact historical public-availability time still requires source-specific validation before an approval date is used as `valid_from`.

Official references:
- https://www.fda.gov/drugs/drug-approvals-and-databases/about-drugsfda
- https://open.fda.gov/data/drugsfda/

### FTC merger/case sources

Candidate domains: regulatory action, merger review and litigation states.

FTC provides official merger materials, cases/proceedings and press releases. Historical timestamp/archive semantics must be validated before retrospective PIT use beyond the explicitly timestamped public record.

Official reference:
- https://www.ftc.gov/merger

### DOJ Antitrust

Candidate domains: merger enforcement, antitrust litigation filing/ruling/settlement.

DOJ publishes official Antitrust Division press releases and case filings. A source-specific adapter must preserve the original public timestamp and immutable document identity before the record can become strict-PIT evidence.

Official references:
- https://www.justice.gov/atr/press-releases
- https://www.justice.gov/atr/antitrust-case-filings

### Issuer IR

Candidate domains: major contract, acquisition/takeover announcement, management change, product launch and production disruption.

Issuer pages are primary company statements but are not automatically historical PIT sources. A page date alone is insufficient if later edits cannot be excluded. Retrospective use requires immutable publication/archive proof; prospective snapshots are usable from actual ingestion onward.

## Canonical event identity

8E-A deliberately does not perform fuzzy deduplication. Every adapter must provide a deterministic `canonical_event_key`. News similarity, entity-name similarity or LLM semantic similarity may not silently merge events.

The 8E-B1 8C reuse adapter uses an accession-scoped deterministic key. It does not claim that an issuer press release and an SEC filing are already cross-source-deduplicated. A later validated identity resolver may link them only with explicit collision/false-merge QA before outcomes are opened.

## Current hard boundaries

- no market outcomes read;
- no market direction assigned;
- no generic sentiment;
- no threshold selection;
- no interaction research;
- no Phase-7 integration;
- no production external evidence;
- no current news retrojection;
- discovery-only news cannot create a canonical known event by itself;
- source hierarchy cannot retroactively backdate knowledge;
- Phase-8C SEC semantics are not relabeled without independent validation.

## Next slice

8E-B2 should validate and implement the first new external authority adapter. Recommended order:

1. FDA regulatory approvals;
2. FTC/DOJ regulatory/litigation events;
3. issuer primary releases only after a timestamp/archive contract is frozen.

Each adapter must pass source/PIT/coverage validation before any event-outcome research. Predictive testing belongs to Phase 8G, not 8E.
