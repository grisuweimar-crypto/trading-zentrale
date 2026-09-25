# Phase 8C-B – Full SEC History & Coverage

Status: **ACTIVE – no outcome research**

## Goal

8C-B proves whether the SEC data required by 8C is historically complete enough to build PIT-safe research features. It does not yet calculate predictive returns or compare against Phase 7.

## Why `filings.recent` is insufficient

The SEC Submissions endpoint keeps a compact recent filing block and can reference older filing history through `filings.files`. A recent-only dataset is therefore a diagnostic snapshot, not a historical panel.

Phase 8C-B fails closed:
- every referenced historical submissions file must be loaded before `history_complete=true`;
- a missing file makes the entity `INCOMPLETE_HISTORY`;
- partial history may be inspected diagnostically but may not enter research;
- the live probe exposes `--include-history` to load all referenced blocks.

## Stable identity

CIK + accession number are the primary entity/filing identities.

Ticker symbols are retained as descriptive metadata but are not treated as stable historical IDs because tickers and listings can change. The separate Phase-8 As-of-Universe/Coverage ledger remains mandatory for mapping scanner symbols to historical entities.

## Duplicate accessions

The same accession may appear in overlapping SEC blocks. Exact duplicate metadata is deduplicated by accession identity. Conflicting metadata for the same accession raises a contract error.

There is deliberately no `latest row wins` rule.

## Publication coverage

Coverage reports distinguish:
- exact timezone-aware `acceptanceDateTime` -> `SAFE`;
- filing-date-only fallback -> `DATE_ONLY_DELAYED`;
- unresolved publication time -> `UNKNOWN`.

An entity is not `research_ready` if publication time remains unknown or rows have no accession identity.

## Company Facts coverage

After the complete submission history is assembled, every Company Facts row is rejoined by accession.

The coverage report measures:
- total fact rows;
- resolved accessions;
- unresolved accessions;
- rows without accession;
- accession-resolution rate.

Unresolved facts are retained with explicit reason codes. They are not dropped and they are not converted into neutral evidence.

## Live probe

Example shape:

```bash
python scripts/run_external_evidence_8c_probe.py \
  --cik 320193 \
  --user-agent "Trading-Zentrale contact@example.invalid" \
  --include-history
```

The live probe fetches the main Submissions payload, every referenced `filings.files` history block, then Company Facts. It inserts a small delay between history requests so the project remains comfortably below the SEC fair-access ceiling.

The probe never downloads or inspects market outcomes.

## Research gate

8C-B must be green before 8C-C feature construction:
1. all referenced history files loaded;
2. duplicate accession conflicts absent;
3. historical rows have accession identity;
4. publication time is exact or conservatively delayed;
5. Company Facts accession-resolution coverage is measured;
6. missingness remains explicit;
7. historical entity mapping can be joined to the As-of-Universe ledger;
8. no coverage threshold is selected after inspecting returns.

Only after these conditions are satisfied may 8C-C start building comparable fundamental changes such as revenue growth, margins, FCF and leverage.
