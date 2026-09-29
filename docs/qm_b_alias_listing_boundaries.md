# QM-B – Alias PIT Boundaries & Listing Evidence Audit

Status: **research validation / boundary audit**  
Productive integration: **disabled**

## Purpose

This package follows the historical identity reconciliation package. Stable non-crypto instrument identity is broadly reconstructed, but a historical identity match does not automatically prove that a given alias was already supported at every earlier scanner observation.

This package therefore answers two separate questions:

1. Which frozen scanner observations are already inside a point-in-time supported identity/alias boundary?
2. Does the repository contain explicit historical evidence for listing venue, listing date, delisting date or investability?

The package deliberately does **not** create a strict Instrument Master, Alias Ledger or As-of Universe Membership Ledger.

## Formal audit freeze

The formal sample remains pinned to audit input commit:

`46cd7f53f239fff6f9c2e3f52ee7cc3b6eb8a6ef`

Frozen inputs are materialized from that commit during CI. Later scanner publications or universe changes therefore cannot alter this package's sample.

## Frozen observation result

The sample contains:

- **37,113** scanner observations;
- **300** distinct observed identifier values.

The identity/alias boundary audit classifies them as:

- **35,059** `PIT_IDENTITY_ALIAS_SUPPORTED`;
- **761** `PRE_ALIAS_PIT_BOUNDARY_NON_CRYPTO`;
- **1,293** `CRYPTO_STABLE_OBJECT_UNRESOLVED`;
- **0** `IDENTITY_OR_BOUNDARY_UNRESOLVED`.

Thus the previously known 2,054 PIT-unverified observations are fully decomposed. There is no residual generic identity/boundary failure class.

The 761 early non-crypto rows span **266 identifier values**. They are observations that pre-date the first conservative repository evidence currently accepted for their stable identity/alias relation. They are not treated as incorrect identities and are not treated as out-of-scope observations.

The 1,293 crypto rows span **13 identifier forms**. Crypto remains separate because equality of base token such as `BTC` does not yet define whether the stable object is the asset, quote pair, provider instrument or another identifier layer.

## Listing and investability evidence inside the repository

The complete frozen `data/inputs/universe_master.csv` Git history through the audit cutoff contains **15 commits**.

Across those snapshots the audit found no explicit historical field for:

- listing venue;
- listing date;
- delisting date;
- investability/tradability state.

Accordingly:

- a suffix such as `.TO`, `.DE` or `.PA` is retained only as a namespace hint and is **not** promoted to a verified listing venue;
- first scanner observation is **not** treated as listing date;
- disappearance or absence is **not** treated as delisting;
- scanner inclusion or `active=1` is **not** treated as verified investability.

These fields remain `UNKNOWN` until an explicit PIT-capable source is introduced.

## Legacy root watchlist audit

Before the vNext migration the repository used a root-level `watchlist.csv`. Its history provides earlier evidence that particular identifier strings were configured in the scanner, but its schema changed over time and it does not provide a uniform explicit stable-identity mapping.

The audit therefore uses this source only as **alias-presence evidence**. It cannot upgrade stable identity, listing venue, membership or investability.

Historical source window:

- **29** root-watchlist commits;
- earliest commit date in the audited history: **2026-01-27**;
- latest commit date: **2026-02-14**;
- every snapshot becomes usable only from the following calendar day.

The parser is schema-aware: old snapshots may contain only `Ticker`, while later snapshots may also contain `Yahoo`. A snapshot is accepted when at least one configured identifier field actually exists; later columns are never retroactively required from older files.

### Result inside the 761 early non-crypto observations

- **606 observations / 141 identifiers** have prior PIT-supported legacy alias presence;
- **155 observations / 125 identifiers** have no prior legacy alias-presence evidence in the retained root-watchlist history.

This finding does **not** change the strict identity/alias PIT count. The 606 rows remain outside the strict stable-identity boundary because the legacy watchlist proves only that the identifier string was present, not that its later stable instrument mapping was known at that historical time.

## Safety invariants

The package enforces:

- stable identity evidence and alias-string presence remain separate;
- current mappings are not back-projected;
- same-day repository snapshots are not used; day-granularity evidence starts the next calendar day;
- symbol suffixes never verify listing venue;
- scanner presence never verifies listing date or investability;
- absence never verifies delisting;
- crypto base-token equality never becomes stable instrument identity;
- legacy alias presence never upgrades stable identity;
- no strict Instrument Master, Alias Ledger or Membership Ledger is written.

## Tests and regression gates

The CI package exercises the boundary auditor together with the prior QM-B identity, repo-history, observed-membership and crypto controls.

Frozen regression expectations include:

- 37,113 total scanner observations;
- 35,059 PIT identity/alias-supported;
- 761 pre-boundary non-crypto observations across 266 identifiers;
- 1,293 unresolved crypto observations across 13 identifier forms;
- 0 generic identity/boundary unresolved observations;
- 15 `universe_master.csv` history commits;
- no explicit repository fields for listing venue/date, delisting date or investability;
- 29 legacy root-watchlist commits;
- 606 legacy alias-presence-only observations across 141 identifiers;
- 155 observations across 125 identifiers without earlier retained legacy alias presence.

## Evidence impact

No historical research observation is deleted, relabelled neutral or automatically excluded by this package.

The package improves the uncertainty taxonomy:

- 35,059 observations have strict identity/alias PIT support;
- 606 additional early observations have weaker alias-presence evidence only;
- 155 early non-crypto observations have no earlier retained alias-presence source;
- 1,293 crypto observations remain blocked by stable-object semantics.

These evidence levels must remain distinguishable in later sample construction.

## Remaining risks / next dependency

The repository itself cannot complete the strict as-of universe requirements for listing venue, listing/delisting and investability. The next QM-B package therefore needs an explicit historical listing-metadata evidence contract and source evaluation before those fields can be promoted.

Crypto stable-object semantics remains a separate QM-B research dependency and must not be solved implicitly inside listing metadata work.
