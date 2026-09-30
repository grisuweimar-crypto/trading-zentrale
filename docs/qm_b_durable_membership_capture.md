# QM-B – Durable Prospective Project-Universe Membership Capture

Status: **research validation**  
Productive integration: **disabled**

## Purpose

The prospective membership layer can describe one observed `universe_master.csv`
state. This package makes those observations durable from now on.

For each genuinely new universe content state it preserves:

1. the exact triggering `universe_master.csv` bytes;
2. the normalized QM-B prospective membership snapshot;
3. the append-only hash-chained membership ledger event;
4. a replaceable `latest.json` convenience manifest.

All durable data is written below:

`artifacts/research/qm/qm_b_membership/`

## Point-in-time rule

The capture timestamp is the actual workflow observation time:

`membership_valid_from = universe_observed_at = actual capture time`

The exact source bytes are read from the triggering Git commit. If `main` has advanced
before the archive write begins, the later file is **not** substituted for the
triggering state.

A source commit therefore becomes part of the evidence identifier:

`git:<commit_sha>:data/inputs/universe_master.csv`

No commit timestamp, file effective date, ticker metadata or later universe state may
move membership evidence backwards.

## Change-only capture

The durable deduplication key is the SHA-256 of the exact universe CSV bytes.

- new SHA-256 -> `ARCHIVED`;
- already archived SHA-256 -> `NO_CHANGE`.

A manual re-run or unrelated code commit therefore does not create artificial repeated
membership observations when the universe content is unchanged.

## Archive layout

```text
artifacts/research/qm/qm_b_membership/
├── membership_events.jsonl
├── latest.json
├── raw/
│   └── <membership_snapshot_id>_universe_master.csv
└── snapshots/
    └── <membership_snapshot_id>.json
```

`membership_events.jsonl` remains the authoritative append-only event chain.
`latest.json` is only a convenience pointer and is not historical authority.

## GitHub Actions behavior

The durable-capture workflow has two roles.

### Pull request validation

It runs:

- dedicated durable-capture tests;
- the existing prospective-membership tests;
- a local end-to-end archive smoke test.

It has no reason to write repository evidence during a pull request.

### Main-branch capture

On relevant pushes to `main` and on manual dispatch it:

1. checks out enough Git history to read the exact triggering commit;
2. copies `data/inputs/universe_master.csv` from that exact commit to a temporary file;
3. records one actual UTC observation timestamp;
4. rebases the archive operation onto the newest `origin/main` state;
5. runs durable capture using the saved triggering bytes;
6. commits only `artifacts/research/qm/qm_b_membership/**` when the content hash is new;
7. pushes without force.

If another commit reaches `main` first, the workflow does not overwrite it. It reloads
the newest ledger and retries the same triggering evidence. If that exact universe hash
has already been archived meanwhile, the retry becomes `NO_CHANGE`.

## Explicit non-claims

Durable project-universe membership evidence still does **not** prove:

- exchange listing state;
- listing venue;
- market tradability;
- broker availability;
- project investability;
- negative historical membership from absence;
- crypto stable identity;
- strict `UniverseIntegrityBundle.universe_membership` eligibility.

Those remain separate QM-B evidence classes.

## Why the workflow writes an audit commit

A workflow artifact alone is temporary and does not create a durable PIT history in the
repository. The archive commit makes the observed state and its hash chain part of Git
history. The commit changes research/QM artifacts only and uses a CI-skip marker so it
does not intentionally start unrelated production pipelines.

## Definition of Done

1. exact triggering universe bytes are captured rather than a later substitute;
2. only a new universe SHA-256 creates a new event;
3. raw and normalized snapshots are durable repository artifacts;
4. membership ledger remains hash-chained and append-only;
5. capture timestamps are timezone-aware actual observation times;
6. push conflicts are fail-safe and never force-pushed;
7. archive commits touch only QM research artifacts;
8. listing/tradability/investability remain unpromoted;
9. tests cover exact bytes, deduplication, new states and tamper blocking;
10. merge of the package bootstraps the current universe prospectively on `main`.
