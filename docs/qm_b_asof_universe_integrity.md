# QM-B – As-of Universe, Coverage, Survivorship & Investability

Status: **research governance / validation infrastructure**  
Productive integration: **disabled**

## Purpose

QM-B prevents historical research from quietly replacing the historical world with
the current one. It makes instrument identity, universe membership, listing state,
investability, provider coverage, outcome availability and taxonomy assignments
explicitly point-in-time.

The module is fail-closed. `UNKNOWN` is a real state and is never converted to a
neutral value or silently removed from a research denominator.

QM-B does **not** change scanner scoring, Selection, Timing, Probability, Risk,
Confidence, Elliott, Decision Layer, portfolio state or order logic.

## Baseline / existing mechanisms reused

QM-B deliberately reuses existing Phase-0–8 protections instead of replacing them:

- `configs/external_universe_coverage_contract_v1.json` already defines the core
  Phase-8 anti-survivorship rules and the membership/coverage vocabularies.
- `src/scanner/research/external_evidence/universe_coverage.py` supplies current SEC
  coverage aggregation and exact-current-ticker matching.
- `scripts/run_external_evidence_8c_universe_coverage.py` explicitly labels its
  output as current-snapshot feasibility, not a historical universe.
- `src/scanner/research/external_evidence/sec_identity_bootstrap.py` correctly treats
  current SEC submissions as current identity authority, not historical ticker
  authority.
- `configs/market_context_assignment_schema_v1.json` and
  `configs/market_context_history_schema_v1.json` already demonstrate effective-dated
  PIT assignments and are retained.
- `artifacts/research/history_analysis.csv` preserves actual historical scanner
  observations, but its completeness gate is not itself proof of intended historical
  universe membership.
- `data/inputs/universe_master.csv` is a current master input and therefore may be
  inspected by QM-B but must never be back-projected into history.

## Gap closed by QM-B

Before QM-B there was no single general-purpose, versioned structure that could
answer all of these questions together:

1. Which stable instrument did this historical identifier refer to on date *t*?
2. Was that instrument explicitly in/out/unknown for the scanner universe on *t*?
3. Was the listing active, not yet listed, delisted, suspended or unknown on *t*?
4. Was it investable, restricted, not investable or unknown on *t*?
5. Did provider/family *P/F* actually cover it on *t*?
6. Was the required outcome available on *t*, under the frozen label definition?
7. Which sector/domain/category/pillar assignment was valid on *t*?
8. Did a research sample silently drop rows that are no longer present today?

`qm_b.py` provides that common contract and validation/query layer.

## Machine-readable contract

`configs/qm_b_universe_integrity_v1.json`

The contract contains six collections:

- `instruments`
- `identifier_aliases`
- `universe_membership`
- `provider_coverage`
- `outcome_availability`
- `taxonomy_assignments`

A normalized bundle uses schema
`qm_b_universe_integrity_bundle_v1`.

### Stable instrument identity

`instrument_id` is an internal stable identifier. It must not merely equal a ticker.
Ticker, Yahoo symbol, ISIN, CIK, FIGI, CUSIP, SEDOL and exchange symbols are aliases
of the stable instrument.

Alias validity is effective-dated using closed-open intervals:

`[valid_from, valid_to)`

A known historical ticker may therefore end on the same date on which the successor
ticker starts, without overlap.

If the same identifier/venue resolves to different instruments during overlapping
periods, QM-B rejects the bundle unless the underlying rows are explicitly marked as
conflicting. Query-time conflict is never silently resolved.

### Universe membership

Membership is explicit per `as_of_date` and contains:

- stable `instrument_id`
- historical symbol and listing venue
- `IN_SCOPE`, `OUT_OF_SCOPE`, `NOT_YET_LISTED`, `DELISTED` or `UNKNOWN`
- listing date / delisting date when known
- listing status
- investability status
- whether the instrument was scanner-observable
- source and PIT-verification state
- reason codes

An `IN_SCOPE` + PIT-verified row is accepted only if:

- the stable instrument identity is `VERIFIED`, and
- a PIT-verified historical symbol alias is valid on that date.

Known listing/delisting dates are checked against the claimed membership state.

### Provider coverage

Coverage is keyed by:

`as_of_date + instrument_id + source_id + external_family`

The status vocabulary is intentionally aligned with the existing Phase-8 contract:

- `KNOWN`
- `UNKNOWN`
- `NOT_APPLICABLE`
- `STALE`
- `LICENSED_OUT`
- `LOW_COVERAGE`
- `CONFLICTING_SOURCES`

No provider record is interpreted as `UNKNOWN`, never as no event, no fundamental
change or neutral evidence.

### Outcome availability

Outcome availability is a separate ledger. An available outcome requires:

- a stable instrument
- an `outcome_id`
- a frozen `label_definition_hash`
- `available_from`
- source and PIT verification

A row cannot claim `AVAILABLE` before `available_from`. Missing rows resolve to
`UNKNOWN`.

### Historical taxonomy

Sector, industry, domain, category, pillar and bucket assignments are separately
effective-dated. A current sector classification is therefore not automatically a
historical sector classification.

## Survivorship/sample audit

`UniverseIntegrityBundle.audit_sample()` audits every supplied
`as_of_date + instrument_id` row without dropping failures.

Results are:

- `ELIGIBLE`
- `NOT_ELIGIBLE`
- `FAIL_CLOSED`

Rows with unknown membership or unverifiable identity stay in the audit and cause
`fail_closed=true`. A delisted/out-of-scope row stays visible as `NOT_ELIGIBLE`; it is
not erased from the denominator review merely because it is absent today.

## Immutable versions for QM-A

A validated bundle emits deterministic SHA-256 versions:

- `instrument_master_version`
- `universe_ledger_version`
- `bundle_hash`

The first two fields are directly suitable for the immutable QM-A analysis identity.
This binds a frozen analysis to the exact instrument/alias/taxonomy and
membership/coverage/outcome state it consumed.

## Current-universe inventory is deliberately non-historical

The command:

```bash
PYTHONPATH=src python scripts/qm_b_universe_integrity.py \
  inventory-current --universe data/inputs/universe_master.csv
```

reports duplicate symbols/ISINs and missing current identifiers. Its output is
explicitly marked:

`CURRENT_STATE_ONLY_NOT_HISTORICAL_MEMBERSHIP`

It never creates historical `valid_from`, membership, listing or investability facts
from the current CSV.

## CLI

Validate a bundle:

```bash
PYTHONPATH=src python scripts/qm_b_universe_integrity.py verify --bundle path/to/qm_b_bundle.json
```

Attach deterministic hashes:

```bash
PYTHONPATH=src python scripts/qm_b_universe_integrity.py normalize \
  --bundle input.json --output normalized.json
```

Resolve a historical identifier:

```bash
PYTHONPATH=src python scripts/qm_b_universe_integrity.py resolve \
  --bundle normalized.json --type SYMBOL --value OLD --as-of 2023-06-01 --venue XNYS
```

Audit a research sample:

```bash
PYTHONPATH=src python scripts/qm_b_universe_integrity.py audit-sample \
  --bundle normalized.json --sample sample.csv --output audit.json
```

The sample-audit command returns exit code `2` when any row is fail-closed.

## Scope boundary

This first QM-B implementation creates the **contract, validator, immutable version
identity, sample audit and current-state inventory**. It does not invent historical
membership records.

Historical population must come from PIT-defensible evidence such as preserved
scanner history, verified listing/delisting records, explicit symbol-change evidence
and provider-specific historical coverage. Where those sources are incomplete,
QM-B records `UNKNOWN` rather than reconstructing a convenient history.

## Definition of Done for QM-B infrastructure

The infrastructure package is complete when:

1. current state cannot be silently back-projected into history;
2. stable instrument IDs are separated from historical aliases;
3. alias conflicts and overlapping identity claims fail closed;
4. universe membership is explicit per date;
5. listing/delisting dates cannot contradict membership claims;
6. investability is explicit and can be `UNKNOWN`;
7. provider/family coverage is explicit per date;
8. outcome availability is explicit and label-versioned;
9. historical taxonomy is effective-dated;
10. sample audits retain unknown/out-of-scope rows instead of silently dropping them;
11. deterministic versions feed QM-A `instrument_master_version` and
    `universe_ledger_version`;
12. Phase-8 membership and coverage vocabularies remain aligned;
13. current-universe inventory never claims historical truth;
14. regression tests protect all invariants;
15. productive scanner/Decision semantics remain unchanged.

## Next QM-B data step

After this infrastructure is merged, the next work package is **historical ledger
population and reconciliation**:

1. derive observed project membership from preserved scanner-history rows;
2. resolve stable identities and historical aliases without current-ticker guessing;
3. add listing/delisting evidence where defensible;
4. classify unresolved records as `UNKNOWN`;
5. attach provider/family coverage by date;
6. build outcome-availability rows for each frozen label/horizon;
7. run a survivorship audit against existing research datasets;
8. route any historical identity or membership defect into QM-H impact assessment.
