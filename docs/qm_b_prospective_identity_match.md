# QM-B – Prospective Listing Snapshot Identity Match

Status: **research validation**  
Productive integration: **disabled**

## Purpose

This package connects an already archived prospective listing-source snapshot to a
separately observed universe-master snapshot.  Its only automatic promotion target is
a **stable instrument candidate** keyed by a valid ISIN.

It does **not** promote historical universe membership, listing history, tradability,
or project investability.

## Inputs

1. prospective listing snapshot from `qm_b_prospective_listing_snapshots.py`;
2. universe-master CSV bytes;
3. immutable universe snapshot ID;
4. timezone-aware universe observation timestamp;
5. SHA-256 of the universe bytes.

The match becomes knowable only when both inputs are available:

`identity_valid_from = max(listing_snapshot.valid_from, universe_observed_at)`

No source effective date can move this boundary backwards.

## Automatic match rule

A source record is `MATCHED` only when all of these are true:

- the source exposes an exact identifier (`source_symbol`, `cqs_symbol`, or
  `nasdaq_symbol`);
- one and only one active universe row has exactly the same symbol;
- the universe row belongs to an allowed security asset type;
- that row has a syntactically valid ISIN.

The resulting candidate ID is `urn:scanner:isin:<ISIN>`.

Forbidden automatic behavior:

- punctuation normalization (`BRK.B` is not silently changed to `BRK-B`);
- exchange-suffix guessing;
- ticker-root matching;
- fuzzy/name matching;
- current venue inference;
- use of crypto base tokens as stable security identity.

A source-provided alias such as `NASDAQ Symbol` may match if the alias itself exactly
matches the universe symbol.  That is evidence supplied by the source, not an inferred
rewrite.

## Fail-closed outcomes

- `UNMATCHED`: no exact active master row or no valid ISIN;
- `AMBIGUOUS`: exact identifier maps to multiple distinct ISINs;
- `REVIEW_REQUIRED_DUPLICATE_MASTER_ROWS`: more than one active master row matches,
  even when the duplicated rows carry the same ISIN;
- `UNSUPPORTED`: the source record has no supported exact identifier.

Missing or ambiguous matches never become negative universe membership.

## Output safety fields

Every matched row states explicitly:

- `historical_membership_verified=false`;
- `tradability_verified=false`;
- `project_investability_verified=false`.

The whole result states that no corresponding promotions were performed.

## CLI

```bash
PYTHONPATH=src python scripts/qm_b_prospective_identity_match.py \
  --snapshot /path/to/normalized_snapshot.json \
  --universe data/inputs/universe_master.csv \
  --universe-snapshot-id <immutable-id> \
  --universe-observed-at 2026-09-30T04:00:00+00:00 \
  --output /tmp/qm_b_identity_match.json
```

The universe SHA-256 is computed from the actual bytes by the CLI and embedded in the
result.

## Definition of Done

1. only exact source-provided identifiers can auto-match;
2. unique active master row + valid ISIN is required;
3. duplicate master rows are not silently accepted;
4. multiple ISINs fail ambiguous;
5. match validity begins no earlier than the later evidence input;
6. timezone-less observation timestamps fail closed;
7. listing identity does not imply membership/tradability/investability;
8. no productive scanner or portfolio semantics are changed.
