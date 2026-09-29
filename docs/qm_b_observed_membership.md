# QM-B – Observed Historical Membership Evidence

Status: **research validation / historical-evidence staging**  
Productive integration: **disabled**

## Purpose

The strict QM-B universe ledger requires stable instrument identity, explicit
point-in-time membership, listing state and investability. The preserved scanner
history does not contain enough identity fields to satisfy that contract directly.

This package therefore creates a deliberately weaker but auditable evidence layer:

> **Which symbols were actually observed by the scanner on which dates?**

It does not answer:

- which stable security the symbol represented without separate PIT identity evidence;
- whether a missing symbol was outside the intended universe;
- whether a currently active symbol existed under the same identifier historically;
- whether the full intended universe was complete on a date;
- whether a listing was investable or delisted without separate evidence.

## Existing project semantics reused

`src/scanner/reports/research_views.py` already defines an observation as scanner
evidence only when:

- `observation_type` is blank or `observed_scanner`; and
- `data_source` is blank or `scanner_run`.

The blank values are retained for legacy observations that the project already treats
as scanner-compatible. New validated observations are written explicitly as
`observed_scanner` + `scanner_run`.

`docs/research_data_architecture.md` also states that legacy/new market-data rows stay
outside Recent/Latest scanner views and that backfill prices never generate scanner
metrics.

QM-B reuses these semantics instead of inventing another historical-source rule.

## Evidence classes

Every preserved archive row becomes one immutable candidate row with one of:

### `SCANNER_OBSERVED`

Positive historical presence evidence.

Membership claim:

`OBSERVED_IN_SCANNER`

Meaning only:

> This historical symbol is supported as an observed scanner row on this date.

It does **not** mean stable identity is known. Every candidate remains
`identity_status=UNRESOLVED` until a separate historical identity reconciliation is
performed.

### `BACKFILL_DERIVED`

Rows explicitly identified as price/market backfill, including known project markers
such as `observation_type=backfill` or `data_source=yfinance`.

Membership claim:

`NOT_MEMBERSHIP_EVIDENCE`

A price backfill may support price/outcome calculations under its own contract, but
it can never prove that the scanner contained the instrument on that date.

### `UNKNOWN_PROVENANCE`

Any source combination outside the established scanner and backfill contracts.

Membership claim:

`UNKNOWN`

It fails closed and is not promoted to universe membership.

## Candidate provenance

Each candidate retains:

- historical date and observed symbol;
- name/currency/sector when present;
- run/config/universe versions when present;
- source observation/data-source markers;
- source path and CSV row number;
- SHA-256 of the entire source file;
- SHA-256 of the source row;
- evidence class and membership claim;
- unresolved identity state;
- reason codes.

Repeated historical observations are deliberately retained. A repeated symbol or
same-day rerun is not silently collapsed because the research architecture itself is
append-only and preserves real repeated observations.

## Absence is not negative membership evidence

This package can create positive evidence (`OBSERVED_IN_SCANNER`) but never creates:

- `OUT_OF_SCOPE`
- `DELISTED`
- `NOT_YET_LISTED`
- `NOT_INVESTABLE`

from absence.

A missing archive row can have many causes: the symbol was outside the universe, the
universe changed, an old run is not preserved, a mapping changed, data is incomplete,
or the asset was genuinely absent. Without separate evidence, the state is unknown.

## Current-universe comparison

The CLI may compare the set of historically observed symbols with the current active
`data/inputs/universe_master.csv`.

It reports two diagnostic sets:

- historically observed symbols absent from the current active master;
- current active master symbols never observed in the preserved scanner history.

These are **review triggers only**. Neither set proves survivorship bias, delisting,
new listing, ticker change, or historical exclusion.

## Relation to strict QM-B universe membership

The flow is intentionally staged:

```text
preserved history row
→ provenance classification
→ observed-symbol evidence
→ historical identity reconciliation
→ stable instrument_id
→ listing / delisting / investability evidence
→ strict QM-B as-of universe membership
→ survivorship/sample audit
```

No arrow may be skipped by using the current universe or current ticker as a shortcut.

## CLI

Analyze the preserved project archive:

```bash
PYTHONPATH=src python scripts/qm_b_observed_membership.py \
  --history artifacts/research/history_analysis.csv \
  --current-universe data/inputs/universe_master.csv
```

Optional outputs:

```bash
--summary-output summary.json
--candidate-output candidates.json
--scanner-evidence-output scanner_observed_presence.json
```

The scanner-evidence output explicitly declares:

- `strict_membership_ledger=false`
- `stable_identity_verified=false`
- `absence_interpreted_as_out_of_scope=false`

## Current `history_recent.csv` state

At implementation time the checked-in `artifacts/research/history_recent.csv` on
`main` is empty. This package does not interpret that as historical absence. The
preserved archive remains the source used for this evidence classification, while
Recent remains governed by the existing publication/validation pipeline.

If the empty Recent file persists after a confirmed complete scanner publication,
that is a separate production/data-quality investigation and belongs in the
appropriate QM-H / production audit path rather than being repaired by historical
inference here.

## Definition of Done for this work package

1. scanner-observation classification exactly matches established project semantics;
2. price/market backfill can never become membership evidence;
3. unknown provenance fails closed;
4. observed historical presence retains full source provenance and hashes;
5. stable identity remains unresolved at this stage;
6. absence never creates negative membership state;
7. repeated real observations remain retained;
8. current-vs-historical symbol differences remain diagnostics only;
9. the real preserved `history_analysis.csv` is checked in CI;
10. no productive scanner, score, Decision Layer, portfolio or order behavior changes.

## Next QM-B step

After this package, historical identity reconciliation can begin. Each historically
observed symbol/date must be mapped to a stable instrument only where PIT-defensible
identifier evidence exists. Ticker changes, reuse of symbols, listing venue and
listing/delisting history must be resolved before candidate observations can become
strict `IN_SCOPE` membership rows.
