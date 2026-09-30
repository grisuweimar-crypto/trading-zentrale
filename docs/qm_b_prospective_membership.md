# QM-B – Prospective Project-Universe Membership Evidence

Status: **research validation**  
Productive integration: **disabled**

## Purpose

This package records what the project universe configuration explicitly showed at a
known point in time. It is the prospective membership-evidence layer between stable
instrument identity and the later strict QM-B As-of Universe bundle.

It deliberately does **not** claim exchange listing, venue, market tradability or
project investability.

## Core PIT rule

`membership_valid_from = universe_observed_at`

The observation timestamp must be timezone-aware. No current or later universe state
may be moved backwards into earlier scanner observations.

## Subject of membership

Membership is aggregated at stable ISIN security identity:

`urn:scanner:isin:<ISIN>`

The current package auto-handles only `stock`, `etf` and `fund` rows with valid ISINs.
Crypto remains outside automatic stable-identity promotion.

Multiple universe rows sharing one ISIN produce one security-level claim. Duplicate
or mixed rows are preserved as quality flags rather than silently discarded.

## Positive evidence

At least one explicit active row for a supported stable identity yields:

`OBSERVED_IN_PROJECT_UNIVERSE`

This is positive evidence that the project configuration included that security at the
snapshot observation time.

It does not imply:

- exchange listing verification;
- a verified listing venue;
- market tradability;
- project investability;
- historical membership before the observation time;
- strict QM-B bundle promotion.

## Negative evidence is intentionally weaker

Rows that are all explicitly inactive yield:

`EXPLICIT_INACTIVE_ROWS_ONLY`

This is retained as an observed configuration fact. It is **not** automatically
promoted to `OUT_OF_SCOPE`.

Most importantly, an instrument that is absent from a later snapshot becomes:

`UNKNOWN_ABSENT_FROM_LATEST_SNAPSHOT`

Absence never closes an older positive membership interval. Older positive evidence is
reported only as audit context and is never carried forward as current membership.

## Unresolved rows

Rows remain outside stable membership claims when they have:

- unsupported asset type;
- missing or invalid ISIN;
- invalid/unknown active flag.

These rows are preserved in the snapshot's `unresolved_rows` collection and counted by
status.

## Snapshot integrity

Every membership snapshot binds:

- source universe snapshot ID;
- actual observation timestamp;
- SHA-256 of the observed universe CSV;
- normalized claims and unresolved rows;
- its own deterministic SHA-256.

The membership snapshot ID is derived from source snapshot ID, observation timestamp
and source SHA-256.

## Append-only ledger

The JSONL ledger stores complete membership snapshots in a hash chain. It rejects:

- duplicate membership snapshot IDs;
- duplicate observation timestamps;
- broken sequence numbers;
- broken previous-hash links;
- tampered snapshot hashes;
- tampered event hashes.

A fail-closed lock file prevents concurrent local writers from appending to the same
ledger at once.

## As-of query semantics

`membership_as_of()` selects the latest membership snapshot observed at or before the
query timestamp.

- instrument positively present in latest snapshot -> `OBSERVED_IN_PROJECT_UNIVERSE`;
- explicit inactive rows only -> `EXPLICIT_INACTIVE_ROWS_ONLY`;
- instrument absent from latest snapshot -> `UNKNOWN_ABSENT_FROM_LATEST_SNAPSHOT`;
- no snapshot yet -> `UNKNOWN_NO_SNAPSHOT_AT_OR_BEFORE_QUERY`.

The query never returns `OUT_OF_SCOPE` in this layer.

## CLI

Build one snapshot and optionally append it:

```bash
PYTHONPATH=src python scripts/qm_b_prospective_membership.py build \
  --universe data/inputs/universe_master.csv \
  --universe-snapshot-id <immutable-id> \
  --observed-at 2026-09-30T05:00:00Z \
  --output /tmp/membership_snapshot.json \
  --ledger /tmp/membership_ledger.jsonl
```

Verify the ledger:

```bash
PYTHONPATH=src python scripts/qm_b_prospective_membership.py verify \
  --ledger /tmp/membership_ledger.jsonl
```

As-of query:

```bash
PYTHONPATH=src python scripts/qm_b_prospective_membership.py query \
  --ledger /tmp/membership_ledger.jsonl \
  --instrument-id urn:scanner:isin:US0378331005 \
  --as-of 2026-09-30T06:00:00Z
```

## Deliberate boundary

This package is not yet the strict `UniverseIntegrityBundle.universe_membership`
collection. That strict collection also needs evidence for listing state/venue and
investability semantics. Those fields remain `UNKNOWN` here rather than being filled
from ticker suffixes, today's metadata or other inference.

## Definition of Done

1. membership observation starts only at actual universe observation time;
2. active supported valid-ISIN rows produce positive project-membership evidence;
3. duplicate ISIN rows aggregate to one security identity with quality flags;
4. inactive rows never become automatic `OUT_OF_SCOPE`;
5. snapshot absence remains `UNKNOWN` and cannot terminate/carry forward prior state;
6. crypto and invalid identities remain unresolved;
7. listing, tradability and investability remain `UNKNOWN`;
8. snapshots and ledger are hash-verified and append-only;
9. tests cover tampering, duplicates, PIT timestamps and as-of absence semantics;
10. no productive scanner behavior is changed.
