# QM-B – Historical Identity Reconciliation Candidates

Status: **research validation / candidate reconciliation**  
Productive integration: **disabled**

## Purpose

This package is the bridge between:

1. preserved scanner-observed identifiers; and
2. the strict QM-B Instrument Master / Identifier Alias Ledger.

It answers a narrower question:

> Which historical scanner identifiers can be linked to a stable instrument candidate, and from what date is that link actually supported by point-in-time repository evidence?

It does **not** yet create the strict historical universe ledger.

## Evidence hierarchy

The package deliberately separates four evidence levels.

### 1. Historical explicit ISIN evidence

If an observed identifier is a checksum-valid ISIN and an immutable historical repository snapshot explicitly contains the same value in an `isin` field, the stable instrument candidate is:

`urn:scanner:isin:<ISIN>`

The instrument identity may be marked `VERIFIED`, but observations before the snapshot became available remain PIT-unverified.

### 2. Historical symbol → ISIN evidence

If an immutable historical repository snapshot explicitly maps a symbol to one ISIN, the symbol can be attached to the corresponding stable ISIN instrument candidate.

The alias is PIT-usable only from the conservative availability date of that snapshot.

### 3. Current-only matches

A match that exists only in the current `universe_master.csv` is a reconciliation candidate, never historical proof.

Such mappings remain `PARTIAL`, have no historical PIT boundary, and cannot be promoted into strict historical identity solely from the current master.

### 4. Crypto base-token lineage

`CRYPTO:BTC`, `BTC-USD` and `BTC-EUR` may share the base `BTC` for lineage analysis, but the base token is not treated as a stable instrument ID.

Crypto mappings remain `PARTIAL` until the project explicitly defines whether the stable object is the crypto asset, the quote pair, the provider instrument, or another identifier layer.

## Historical repository snapshots

The contract pins immutable repository sources:

### Legacy vNext watchlist

- commit: `3fee0bf4198bbe0fc6a740b777ccaf803b2f316f`
- path: `data/inputs/watchlist.csv`
- snapshot date: `2026-02-15`
- conservative PIT availability: `2026-02-16`

This snapshot is used to confirm historical identifier roles such as explicit `ISIN`, `Symbol` and `YahooSymbol` fields.

### First retained universe master

- commit: `9336cbea166ec425e43ccc20b25c864c54c80f85`
- path: `data/inputs/universe_master.csv`
- snapshot date: `2026-03-07`
- conservative PIT availability: `2026-03-08`

This snapshot contains explicit symbol↔ISIN relationships such as `AAGFF ↔ CA00831V2057`, `AEM ↔ CA0084741085`, `RI.PA ↔ FR0000120693`, plus the then-current crypto pairs.

Because repository evidence is available only at day granularity here, the package uses the **following calendar day** as the first PIT-usable date. This prevents same-day look-ahead.

## ISIN validation

A historical value is not accepted merely because it visually resembles an ISIN.

The implementation requires:

- 12-character ISIN syntax; and
- a valid ISO 6166/Luhn check digit.

Invalid identifiers fail closed and cannot create an ISIN instrument candidate.

## Candidate output

The reconciliation output contains:

- one row per unique scanner-observed identifier;
- first/last scanner-observed date;
- scanner observation count;
- candidate class;
- candidate stable instrument ID when available;
- canonical ISIN when available;
- identity status;
- historical evidence source IDs;
- first PIT-usable alias date;
- count of observations before/after PIT availability;
- explicit flags that neither aliases nor membership have been promoted.

Candidate classes are:

- `HISTORICAL_ISIN_SNAPSHOT_MATCH`
- `HISTORICAL_SYMBOL_SNAPSHOT_MATCH`
- `CURRENT_ISIN_ONLY_MATCH`
- `CURRENT_SYMBOL_ONLY_MATCH`
- `CRYPTO_BASE_LINEAGE`
- `AMBIGUOUS`
- `UNRESOLVED`

## PIT alias seeds

Historical snapshots also produce **candidate PIT alias seeds**.

A seed contains:

- stable ISIN-based instrument ID;
- identifier type/value;
- conservative `valid_from` date;
- historical source ID;
- `pit_verified=true` from that boundary forward;
- `candidate_only=true`;
- `promotion_requires_interval_review=true`.

These seeds are intentionally not written into the strict QM-B alias ledger automatically. A later promotion step must still review interval conflicts, ticker reuse, listing venue and any later symbol changes.

## What remains unknown

Even a verified identity candidate does not by itself prove:

- intended historical universe completeness;
- absence before/after an observation;
- listing or delisting state;
- investability;
- provider coverage;
- outcome availability;
- quote-pair equivalence for crypto;
- full alias validity before the first historical snapshot.

Missing evidence therefore remains `UNKNOWN`, never neutral.

## CLI

```bash
PYTHONPATH=src python scripts/qm_b_identity_reconciliation.py \
  --history artifacts/research/history_analysis.csv \
  --current-universe data/inputs/universe_master.csv \
  --repo-root .
```

Optional:

```bash
--output /tmp/qm_b_identity_reconciliation.json
```

The repository checkout must contain the pinned historical commits. CI therefore uses a full git history checkout.

## Definition of Done for this package

1. historical scanner evidence remains filtered through the existing observed-membership contract;
2. market/backfill rows never enter identity reconciliation;
3. ISIN candidates require a valid checksum;
4. historical snapshot mappings are immutable and commit-pinned;
5. same-day look-ahead is prevented by next-day PIT availability;
6. current-only matches remain `PARTIAL`;
7. ambiguous mappings fail closed;
8. crypto base-token matching cannot become stable identity;
9. candidate aliases are never silently promoted to the strict alias ledger;
10. real repository history and real `history_analysis.csv` are exercised in CI;
11. no productive scanner, scoring, Decision Layer, portfolio or order semantics change.

## Next QM-B step

After this package, the candidate set can be audited for:

- unresolved/ambiguous identifiers;
- alias interval continuity and ticker changes;
- listing venue identity;
- historical listing/delisting state;
- historical investability.

Only then should eligible candidates be promoted into the strict QM-B Instrument Master / Alias Ledger and used to construct the as-of universe membership ledger.
