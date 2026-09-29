# QM-B – Prospective Listing Snapshot Architecture

Status: **research validation / forward evidence collection**  
Productive integration: **disabled**

## Purpose

Historical listing metadata remains source-blocked for strict retrospective reconstruction. This package prevents the same gap from continuing into the future by defining an immutable prospective snapshot path.

It does **not** claim that future collection repairs earlier dates.

## Core PIT rule

For every archived snapshot:

`valid_from = retrieved_at`

The project therefore never treats a source record as known before it was actually captured by the project.

Older effective dates, symbol-change dates or source creation markers may be stored as evidence but may never move `valid_from` backwards.

## Snapshot identity and hashes

Each snapshot identity is derived from `source_id`, timezone-aware `retrieved_at`, and the raw-payload SHA256.

The archive keeps raw and normalized payload hashes separately, parser ID/version, source URL, actual retrieval timestamp, source-generated marker if available, archive paths, record count, and optional HTTP ETag / Last-Modified metadata.

The metadata ledger is append-only and hash chained.

## Concurrency / write safety

The ledger uses an exclusive local lock file. A concurrent writer fails closed instead of appending against a stale ledger head.

Before writing, archive paths are checked: path traversal outside the repository root is rejected, existing paths with different bytes are rejected, raw and normalized hashes are verified after writing, duplicate snapshot identities are rejected, and ledger tampering breaks hash-chain verification.

## Initial adapter – Nasdaq Symbol Directory

The first parser supports `nasdaqlisted.txt` and `otherlisted.txt`.

The raw Nasdaq `File Creation Time` marker is preserved, but no timezone is inferred from formatting alone.

Nasdaq-listed records use the source venue namespace `nasdaq_symbol_directory` and venue code `NASDAQ`. Other-exchange-listed records preserve the source exchange code in namespace `nasdaq_symbol_directory_exchange_code`.

Those source codes are not converted to MICs until a separate venue-master mapping is validated.

## Explicit non-inferences

The parser does not infer listing start from first appearance, delisting from absence, tradability from listing presence, project investability from listing/tradability, or source timestamp timezone from formatting. Unknown values stay `UNKNOWN` or null.

## Acquisition boundary

Network acquisition is intentionally outside the parser/archive core.

`scripts/qm_b_archive_listing_snapshot.py` accepts an already acquired raw file plus the **actual timezone-aware retrieval timestamp**. A later operations package may automate retrieval, but it must preserve the same timing contract.

## Evidence impact

This package changes no historical observation and no productive scanner output.

Future snapshots can become prospective PIT evidence only from their real project retrieval timestamp onward. Historical strict listing reconstruction remains blocked by the documented source gap.

## Remaining work

Before a strict As-of Universe Membership Ledger can be promoted, QM-B still needs stable instrument ↔ source-record matching, venue-master mapping where needed, symbol-change continuity rules, market-tradability evidence, a separate project-investability contract, coverage/missingness policy, and crypto stable-object semantics.
