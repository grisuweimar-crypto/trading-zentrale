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

Every taxonomy member has an explicit source-layer state. A prospective challenger is not the same as validated or promoted evidence.

## 8E-A – event ledger, source hierarchy and First Public Release

Implemented:

- `src/scanner/research/external_evidence/structured_events_8e.py`
- `scripts/run_external_evidence_8e_event_ledger.py`
- `configs/external_evidence_8e_structured_events_v1.json`

The ledger separates `published_at`, `first_public_release_at`, `ingested_at` and `valid_from`. A later observation may never backdate knowledge. Source authority resolves conflicting states, not historical availability time. Fuzzy cross-source event deduplication is disabled; each domain adapter supplies a deterministic `canonical_event_key`.

## 8E-B1 – conservative Phase-8C reuse

Only the already validated 8C filing-metadata domain `DIRECTOR_OR_OFFICER_CHANGE` is reused as broad `MANAGEMENT_CHANGE` evidence. Ambiguous 8C labels such as Item 1.01, 2.01, Regulation FD or generic results disclosures are not silently upgraded into contracts, acquisitions, guidance or capital raises.

## 8E-B2 – FDA Drugs@FDA regulatory approvals

Implemented:

- `configs/external_evidence_8e_fda_approval_v1.json`
- `src/scanner/research/external_evidence/fda_drugsatfda_8e.py`
- `scripts/import_external_evidence_8e_fda_drugsatfda.py`
- `tests/test_external_evidence_8e_fda_drugsatfda.py`

Only exact Drugs@FDA `Approval` actions emit `REGULATORY_APPROVAL`; `Tentative Approval` is excluded. `SubmissionStatusDate` is retained as event/action date, not silently treated as historical publication time. Without independent publication proof, `published_at = null` and `valid_from = ingested_at`.

## 8E-B3 – DOJ Antitrust case-filing feeds

Implemented:

- `configs/external_evidence_8e_doj_antitrust_rss_v1.json`
- `src/scanner/research/external_evidence/doj_antitrust_rss_8e.py`
- `scripts/import_external_evidence_8e_doj_antitrust_rss.py`
- `tests/test_external_evidence_8e_doj_antitrust_rss.py`

Official DOJ Antitrust civil/criminal case-filing feeds are used only for the source semantics they explicitly prove: `LITIGATION_FILED`. RSS `pubDate` is retained as source metadata, but a later feed snapshot does not backdate `valid_from` without independent archive proof.

## 8E-B4 – discovery-only news feeds

FTC Competition and DOJ Antitrust press-release feeds are ingested as discovery-only evidence. They carry no canonical event identity, event type, generic sentiment, direction or promotion eligibility. They may only lead to resolution of a primary/authoritative structured source.

## 8E-B5 – structured primary-release challenger

Implemented:

- `configs/external_evidence_8e_primary_release_v1.json`
- `src/scanner/research/external_evidence/primary_release_8e.py`
- `scripts/import_external_evidence_8e_primary_release.py`
- `tests/test_external_evidence_8e_primary_release.py`

This adapter provides a prospective source-layer path for FTC/DOJ authority releases and issuer IR without pretending to have validated generic free-text semantics.

Rules:

- event type and event state must be source-native structured labels or explicitly human-reviewed structured labels;
- automatic free-text event classification is disabled;
- issuer IR requires an attested issuer domain;
- source/page dates are preserved only as `source_claimed_published_at` unless independently proven;
- without immutable publication proof: `published_at = null`, `public_release_proof_status = INGESTION_ONLY`, `valid_from = ingested_at`;
- independently proven historical timestamps additionally require archival-proof SHA-256;
- no market direction is assigned.

This is deliberately a **challenger infrastructure**, not a promoted semantic family.

## Event coverage states

Source-native / validated implementations:

- `REGULATORY_APPROVAL` – FDA Drugs@FDA;
- `LITIGATION_FILED` – DOJ Antitrust official case-filing feeds;
- `MANAGEMENT_CHANGE` – conservative validated Phase-8C reuse.

Prospective structured challengers:

- `GUIDANCE_RAISE`, `GUIDANCE_CUT` – issuer-primary-release structured challenger; Phase-8C guidance promotion boundaries remain in force;
- `REGULATORY_REJECTION` – FTC/DOJ structured primary-release challenger;
- `MAJOR_CONTRACT` – issuer-primary-release challenger;
- `ACQUISITION_ANNOUNCEMENT`, `TAKEOVER_OFFER`, `ACQUISITION_COMPLETION` – issuer-primary-release challengers;
- `CAPITAL_RAISE` – issuer-primary-release challenger; Phase-8C capital-raise promotion boundaries remain in force;
- `LITIGATION_RULING`, `LITIGATION_SETTLEMENT` – FTC/DOJ structured primary-release challengers;
- `PRODUCT_LAUNCH`, `PRODUCTION_DISRUPTION` – issuer-primary-release challengers.

A challenger state is explicit non-production evidence. It is neither validated predictive evidence nor a neutral/missing fallback.

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
- no unvalidated 8C semantic upgrade;
- no ingestion timestamp fabricated as historical First Public Release.

## Completion boundary

Phase 8E is complete as a **source/PIT/event-identity layer** once the completion gate passes across the event ledger, Phase-8C reuse, FDA, DOJ case filings, discovery-only feeds and primary-release challenger.

This does not mean any family has demonstrated predictive value. Incremental/OOS testing belongs to **8G**, cross-factor interactions to **8H**, and Decision Layer integration to **8I**.
