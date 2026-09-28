# Phase 8F – Macro & Exposure Context

Status: **FROZEN — source/exposure layer complete; outcome research pending Phase 8G**.

Canonical freeze artifact: `configs/external_evidence_8f_freeze_v1.json`  
Completion report: `docs/external_evidence_8f_completion.md`

Phase 8F remains a separate external-evidence family on top of the frozen Phase-7 Core. It does not alter Selection, Timing, Probability, Risk, Confidence, Elliott or the Phase-7 decision layer.

## Hard boundary

The frozen implementation remains outcome-blind:

- no market outcome reading;
- no bullish/bearish assignment;
- no signed exposure inference;
- no weights;
- no thresholds;
- no cross-factor interaction research;
- no Phase-7 integration;
- no production external evidence.

Missing, deferred or unobserved macro evidence may never be converted to a numeric zero or neutral state.

## PIT / vintage contract

Two historical-availability modes are supported:

1. **Exact independently proven publication timestamp**: `valid_from` may equal the proven `published_at` timestamp.
2. **Day-level historical vintage only**: the conservative earliest `valid_from` remains 00:00 UTC on the following calendar day.

Prospective observations without independent historical proof use `valid_from >= ingested_at`. Later revisions are separate append-only observations and may never overwrite an earlier vintage.

## Source status

### Implemented adapters

- BLS archived CPI releases — inflation;
- Federal Reserve H.15 — effective federal funds rate and 2Y/10Y Treasury constant maturities;
- ECB Data Portal — USD/EUR reference rate;
- EIA Open Data — WTI, Brent and Henry Hub prospective adapters.

FRED/ALFRED remains blocked under the terms reviewed for 8F and is not an enabled ingestion source.

### Real freeze snapshot

The real prospective collection used for the freeze was produced by GitHub Actions run `36381751961` from Phase-8F head `10f37e7d28e533d1ec9599ab75edc5beb0e82e6f`.

It contains 25 knowable append-only PIT rows across four observed series:

- ECB USD/EUR: 10 rows;
- Fed H.15 effective federal funds: 5 rows;
- Fed H.15 2Y Treasury: 5 rows;
- Fed H.15 10Y Treasury: 5 rows.

The run had no `EIA_API_KEY`; oil/gas collection was therefore explicitly `SKIPPED_NO_API_KEY`. That state is not treated as neutral evidence and did not fabricate oil/gas observations.

### Gold, silver and copper

World Bank Pink Sheet remains the preferred open candidate. Official monthly publications and reusable dataset metadata exist, but Phase 8F did not establish a complete row-level historical first-release/vintage reconstruction meeting the strict PIT contract.

Accordingly, `gold`, `silver` and `copper` are frozen as **deferred historical-vintage paths, not promoted series**. This closes the 8F source-scope decision without overstating validation.

### Uranium and lithium

Registered market proxies remain challenger-only. They may not be relabelled as underlying commodity spot prices and were not promoted by the 8F freeze.

## Multiple-series context semantics

A factor may legitimately contain several raw series. Phase 8F keeps the latest knowable revision **per series**, rather than choosing one arbitrary series per factor.

Examples:

- inflation: CPI MoM and CPI YoY remain separate;
- oil: WTI and Brent remain separate;
- yield curve: 2Y and 10Y remain separate.

No spread, score, sign or weighted aggregate is created in 8F.

## Append-only macro ledger

`macro_ledger_8f.py` enforces:

- append-only observation identity;
- no revision overwrite;
- collision failure if the same revision identity changes semantic content;
- as-of exclusion of future `valid_from` rows;
- coverage summaries by source, factor and series;
- separation of exact historical-release proof from prospective-ingestion proof.

Synthetic rows validate code but cannot satisfy the real-ledger completion requirement.

## Exposure map

The effective exposure map is fully documentary and human-reviewed.

Frozen effective state:

- **195 ACTIVE mappings**;
- **6 SUPERSEDED historical intervals** preserved for audit history;
- **201 total historical mapping intervals**;
- 18 identical later re-reviews retained as audit-only rather than double-counted.

Every promoted mapping has a subject, factor, documentary evidence reference/fingerprint, evidence-valid-from time, human review timestamp, validity interval and descriptive relationship class. Automatic sector/name inference remains forbidden.

## Research-domain accounting

The denominator is frozen independently of mapping convenience and outcomes to the exact pre-8F active-stock universe:

- source ref: `feef4e572283613739b2f24cf42b837ff68a1508`;
- `universe_master.csv` blob: `2c6efec912ea7478096f77dc8f88d7eed2a0581b`;
- frozen unique subjects: **207**;
- mapped: **195**;
- explicit-unmapped: **12**;
- unaccounted: **0**.

Unmapped subjects remain in the denominator and may not be silently dropped.

## Completion / freeze gate

The real freeze run returned:

- `PASS_8F_COMPLETION`;
- `freeze_allowed=true`;
- blockers: none;
- active reviewed mappings: 195;
- domain accounted: 207/207;
- knowable real ledger rows: 25;
- observed ledger series: 4.

`configs/external_evidence_8f_freeze_v1.json` binds the passing run, artifact digest, ledger hash, domain-audit hash, completion-gate hash and raw source-record hashes.

The live collection workflow now runs the completion gate with `--require-pass`. Foundation CI separately verifies the frozen manifest, mappings, domain accounting and fail-closed contracts without requiring live network access.

## Completion meaning

Phase 8F is complete as a deterministic, auditable **source and exposure layer**. This does not claim that any macro factor predicts returns and does not promote deferred/challenger factors.

Only Phase **8G** may inspect outcomes and test incremental predictive value versus the frozen Phase-7 Core. Cross-factor interactions remain Phase 8H work and Decision Layer integration remains Phase 8I work.
