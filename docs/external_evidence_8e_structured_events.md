# Phase 8E – Structured Events & News

Status: `SOURCE_LAYER_IMPLEMENTATION_COMPLETE_OUTCOME_RESEARCH_DEFERRED_TO_8G`

## Objective

Phase 8E builds an event-centric external-evidence layer for structured, timestampable events. It does **not** test predictive value. Generic sentiment, market-direction assignment, Phase-7 integration and production external evidence remain disabled.

Initial taxonomy:

- guidance raise / cut;
- regulatory approval / rejection;
- major contract;
- acquisition announcement / takeover offer / completion;
- capital raise;
- litigation filing / ruling / settlement;
- management change;
- product launch;
- production disruption.

Every taxonomy member now has an explicit source/coverage state. Missing source or unvalidated semantics are represented as deferred/source-gap states rather than silently treated as neutral.

## 8E-A – event ledger, source hierarchy and First Public Release

Implemented:

- `src/scanner/research/external_evidence/structured_events_8e.py`
- `scripts/run_external_evidence_8e_event_ledger.py`
- `configs/external_evidence_8e_structured_events_v1.json`

The ledger separates:

- `published_at`: independently supported source-publication timestamp;
- `first_public_release_at`: earliest proven publication timestamp across eligible evidence;
- `ingested_at`: when this project actually acquired the evidence;
- `valid_from`: earliest timestamp at which the evidence is safe to use under the source-specific PIT contract.

A later observation may never backdate knowledge. Source authority resolves conflicting states, not historical availability time. Fuzzy cross-source event deduplication is disabled; each domain adapter supplies a deterministic `canonical_event_key`.

## 8E-B1 – conservative Phase-8C reuse

Implemented:

- `src/scanner/research/external_evidence/structured_events_8e_phase8c_adapter.py`
- `scripts/run_external_evidence_8e_phase8c_reuse.py`

Only the already validated 8C filing-metadata domain `DIRECTOR_OR_OFFICER_CHANGE` is promoted to broad 8E `MANAGEMENT_CHANGE` evidence.

The following 8C labels remain deliberately unmapped to richer 8E semantics:

- `MATERIAL_DEFINITIVE_AGREEMENT`;
- `ACQUISITION_OR_DISPOSITION_COMPLETED`;
- `UNREGISTERED_EQUITY_SALE`;
- `REGULATION_FD_DISCLOSURE`;
- `OTHER_MATERIAL_EVENT`;
- `RESULTS_RELEASE`.

This prevents Item 1.01 from becoming a guessed major contract/takeover, Item 2.01 from becoming a guessed acquisition rather than disposition, and generic disclosure from becoming guidance or capital-raise evidence.

## 8E-B2 – FDA Drugs@FDA regulatory approvals

Implemented:

- `configs/external_evidence_8e_fda_approval_v1.json`
- `src/scanner/research/external_evidence/fda_drugsatfda_8e.py`
- `scripts/import_external_evidence_8e_fda_drugsatfda.py`
- `tests/test_external_evidence_8e_fda_drugsatfda.py`

Authoritative source:

- `https://www.fda.gov/drugs/drug-approvals-and-databases/drugsfda-data-files`

Only the exact Drugs@FDA action `Approval` emits `REGULATORY_APPROVAL`. `Tentative Approval` is explicitly excluded.

PIT contract:

- `SubmissionStatusDate` is an FDA action/event date, not automatically publication time;
- current bulk snapshots cannot reconstruct historical First Public Release by themselves;
- without independent archive/publication proof, `valid_from = ingested_at`;
- sponsor-name fuzzy mapping to securities is disabled;
- source ZIP, member tables and normalized rows retain SHA-256 provenance.

The adapter is therefore strict-PIT eligible prospectively from actual ingestion, while historical First Public Release remains unresolved from the current bulk file alone.

## 8E-B3 – DOJ Antitrust case-filing feeds

Implemented:

- `configs/external_evidence_8e_doj_antitrust_rss_v1.json`
- `src/scanner/research/external_evidence/doj_antitrust_rss_8e.py`
- `scripts/import_external_evidence_8e_doj_antitrust_rss.py`
- `tests/test_external_evidence_8e_doj_antitrust_rss.py`

Official DOJ Antitrust provides dedicated Civil Case Filings and Criminal Case Filings feeds. These feeds are used only for the source semantics they explicitly prove: `LITIGATION_FILED`.

PIT contract:

- RSS `pubDate` is retained as source-reported publication metadata;
- a later RSS snapshot does not independently prove that the item was immutable and observable at that historical timestamp;
- therefore `valid_from = ingested_at` unless independent archive proof exists;
- titles are not semantically reclassified into rulings, settlements, merger approvals or rejections;
- company-name/ticker inference is disabled.

## 8E-B4 – discovery-only news feeds

Implemented:

- `configs/external_evidence_8e_news_discovery_v1.json`
- `src/scanner/research/external_evidence/news_discovery_8e.py`
- `scripts/import_external_evidence_8e_news_discovery.py`
- `tests/test_external_evidence_8e_news_discovery.py`

Initial official discovery feeds:

- FTC Competition Press Releases: `https://www.ftc.gov/feeds/press-release-competition.xml`
- DOJ Antitrust press-release RSS.

These records remain strictly discovery-only:

- no `canonical_event_key`;
- no event type;
- no generic sentiment;
- no market direction;
- no promotion eligibility;
- they may only trigger resolution of a primary/authoritative structured source.

## Explicit event coverage states

Implemented structured event types:

- `REGULATORY_APPROVAL` – FDA Drugs@FDA;
- `LITIGATION_FILED` – DOJ Antitrust official case-filing feeds;
- `MANAGEMENT_CHANGE` – conservative validated Phase-8C reuse.

Explicitly deferred/source-gap event types:

- `GUIDANCE_RAISE`, `GUIDANCE_CUT` – validated guidance semantics not yet available;
- `REGULATORY_REJECTION` – no safe generic rejection adapter frozen;
- `MAJOR_CONTRACT` – SEC Item 1.01 alone is not semantically sufficient;
- `ACQUISITION_ANNOUNCEMENT`, `TAKEOVER_OFFER` – require validated primary-release semantics;
- `ACQUISITION_COMPLETION` – SEC Item 2.01 remains acquisition-or-disposition ambiguous;
- `CAPITAL_RAISE` – validated capital-raise semantics remain unavailable;
- `LITIGATION_RULING`, `LITIGATION_SETTLEMENT` – source-specific adapters are deferred;
- `PRODUCT_LAUNCH`, `PRODUCTION_DISRUPTION` – issuer-primary-release adapters require source-specific archive/timestamp and semantic validation.

These are not implementation omissions hidden as neutral data. They are explicit non-promotable states.

## Source hierarchy

1. `PUBLIC_AUTHORITY_PRIMARY`
2. `MANDATORY_ISSUER_FILING`
3. `OFFICIAL_EXCHANGE_DISCLOSURE`
4. `ISSUER_PRIMARY_RELEASE`
5. `COUNTERPARTY_OR_AWARDING_AUTHORITY_RELEASE`
6. `REPUTABLE_NEWS_DISCOVERY_ONLY`

Authority rank resolves contradictory state evidence only. It may not move knowledge backward in time.

## Hard boundaries

- no market outcomes;
- no bullish/bearish direction;
- no generic sentiment;
- no threshold selection;
- no interaction research;
- no Phase-7 integration;
- no production external evidence;
- no current news retrojection;
- no fuzzy event merge;
- no missing evidence -> neutral fallback;
- no unvalidated 8C semantic upgrade.

## Completion boundary

Phase 8E is complete as a **source/PIT/event-identity layer** once the completion gate passes. This does not mean the implemented event families are predictive or production-promoted.

Predictive/incremental testing belongs to **Phase 8G**. Cross-factor interactions belong to **8H** and any Decision Layer use belongs to **8I**.
