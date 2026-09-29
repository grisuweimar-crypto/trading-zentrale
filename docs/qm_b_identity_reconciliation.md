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

## Formal audit freeze

The formal package result is frozen at repository commit:

`46cd7f53f239fff6f9c2e3f52ee7cc3b6eb8a6ef`

Both validation inputs are materialized from that commit in CI:

- `artifacts/research/history_analysis.csv`
- `data/inputs/universe_master.csv`

This prevents later scanner publications or universe edits on `main` from changing the evidentiary sample while the same QM package is under review.

Frozen input SHA-256 values:

- history: `4861bacd140b332699c02cb5c941970ff3e4bb54d218a0eb546ca2c6e4a04382`
- universe master: `7e3ff13933334fbef4cabe478a2b2759105d0068d129a6531582bbeb328bd181`

The supplemental universe-master git-history audit is frozen to the same commit. Repository changes after that cutoff are unavailable to this package by contract.

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

Such mappings initially remain `PARTIAL`, have no historical PIT boundary, and cannot be promoted into historical identity solely from the current master. They are then checked independently against the **frozen historical git history** of `data/inputs/universe_master.csv`. Only an exact historical symbol↔ISIN match may upgrade them to `VERIFIED`.

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

### Frozen full universe-master history

The two early snapshots leave later-added symbols initially `PARTIAL`. The package therefore inspects every retained commit touching `data/inputs/universe_master.csv` up to the formal audit cutoff.

The frozen history contains **15 universe-master commits**. A later symbol is upgraded only when the historical repository contains the same symbol with the same checksum-valid ISIN. A symbol mapping to different valid ISINs anywhere inside the frozen history would become `CONFLICTING` and fail closed.

## ISIN validation

A historical value is not accepted merely because it visually resembles an ISIN.

The implementation requires:

- 12-character ISIN syntax; and
- a valid ISO 6166/Luhn check digit.

Invalid identifiers fail closed and cannot create an ISIN instrument candidate.

## Frozen real-history result

The formal audit contains **37,113 scanner-observed rows** and **300 unique historical identifier values**.

The initial two-snapshot reconciliation yields:

| Candidate class | Unique identifiers |
| --- | ---: |
| `HISTORICAL_ISIN_SNAPSHOT_MATCH` | 80 |
| `HISTORICAL_SYMBOL_SNAPSHOT_MATCH` | 152 |
| `CURRENT_SYMBOL_ONLY_MATCH` | 55 |
| `CRYPTO_BASE_LINEAGE` | 13 |
| `AMBIGUOUS` / `UNRESOLVED` | 0 |

Thus **232/300 identifiers** are already `VERIFIED` from the two early snapshots, while 68 are initially `PARTIAL`.

The frozen full universe-master history then audits all **55 later stock-symbol candidates**:

- 55/55 have an exact historical symbol↔ISIN match;
- 55/55 are upgraded from `PARTIAL` to historically supported `VERIFIED` identity candidates;
- 0 remain partial in that stock-symbol group;
- 0 conflicting symbol↔ISIN histories were found;
- the 55 identifiers account for 3,321 scanner observations;
- 3,270 of those observations occur on or after the conservative historical PIT boundary;
- 51 occur before that boundary and remain PIT-unverified.

Therefore all **287 non-crypto identifier values** in the frozen sample have historical repository identity support. The only deliberately `PARTIAL` identity group is the set of **13 crypto identifier forms** covering the six previously identified crypto bases.

### PIT coverage is not the same as identity support

The two early snapshots support **31,789** scanner observations at their PIT alias boundary. The frozen git-history audit adds **3,270** later-symbol observations with historical PIT support.

Combined identity-alias PIT support is therefore:

- **35,059 / 37,113 scanner observations** PIT-supported;
- **2,054 / 37,113** remain identity-alias PIT-unverified.

This does **not** mean 2,054 observations have wrong identity. It means the package cannot prove that the relevant alias/identity relationship was available early enough for those observations under the conservative PIT rule. The remaining set includes the crypto namespace and observations predating the first usable repository evidence.

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

Historical snapshots produce **411 candidate PIT alias seeds** in the frozen base audit.

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
- full alias validity before the first historical evidence date.

Missing evidence therefore remains `UNKNOWN`, never neutral.

## CLI

Working-tree diagnostic:

```bash
PYTHONPATH=src python scripts/qm_b_identity_reconciliation.py \
  --history artifacts/research/history_analysis.csv \
  --current-universe data/inputs/universe_master.csv \
  --repo-root .
```

Supplemental frozen-history audit:

```bash
PYTHONPATH=src python scripts/qm_b_identity_repo_history.py \
  --history artifacts/research/history_analysis.csv \
  --current-universe data/inputs/universe_master.csv \
  --repo-root .
```

The formal CI does not use moving working-tree inputs. It materializes both input files from the frozen audit commit first. The repository checkout must contain full git history so pinned snapshots and the frozen universe-master commit sequence can be inspected.

## Definition of Done for this package

1. historical scanner evidence remains filtered through the existing observed-membership contract;
2. market/backfill rows never enter identity reconciliation;
3. ISIN candidates require a valid checksum;
4. historical snapshot mappings are immutable and commit-pinned;
5. formal validation inputs and supplemental git history are frozen at the audit-start commit;
6. same-day look-ahead is prevented by next-day PIT availability;
7. current-only matches cannot become historical truth without an independent exact historical repo match;
8. conflicting historical mappings fail closed;
9. crypto base-token matching cannot become stable identity;
10. candidate aliases are never silently promoted to the strict alias ledger;
11. real frozen repository history and frozen `history_analysis.csv` are exercised in CI;
12. no productive scanner, scoring, Decision Layer, portfolio or order semantics change.

## Next QM-B step

Identity reconciliation is now sufficient for the **non-crypto identity layer**, but not for strict historical membership promotion.

The next package must audit:

- alias interval continuity and ticker changes;
- listing venue identity;
- historical listing/delisting state;
- historical investability;
- the 2,054 observations without PIT-verified identity alias;
- crypto stable-object semantics separately.

Only eligible rows should then be promoted into the strict QM-B Instrument Master / Alias Ledger and used to construct the as-of universe membership ledger.
