# QM-B – Crypto Identifier Migration Diagnostic

Status: **research validation / identifier-lineage candidate analysis**  
Productive integration: **disabled**

## Purpose

The observed-membership audit found seven historical crypto identifier values that do
not match the current universe master by current `symbol` or `isin`:

- `ADA-EUR`
- `CRYPTO:ADA`
- `CRYPTO:BTC`
- `CRYPTO:DOGE`
- `CRYPTO:ETH`
- `CRYPTO:SOL`
- `CRYPTO:XRP`

This package investigates whether they are identifier-namespace migration artifacts.
It does **not** promote them to stable instrument identity.

## Repository evidence before the diagnostic

Immutable repository history already demonstrates multiple crypto identifier roles:

1. Commit `8eab71c55d7e8a3c17529adbc121764be2348343` (2026-02-14) contains the old root
   `watchlist.csv` with pair-style crypto identifiers including `BTC-USD`, `ETH-USD`,
   `ADA-EUR`, `DOGE-EUR`, `SOL-EUR`, and `XRP-EUR`. For some rows the fetch symbol
   differs from the ticker, for example Bitcoin with a USD ticker and EUR Yahoo pair.
2. Scanner-vNext merge `3fee0bf4198bbe0fc6a740b777ccaf803b2f316f` (2026-02-15) introduced an explicit
   separation in `scripts/watchlist_migrate_v2.py`:
   - `ISIN` = identity/reference;
   - `Symbol` = human-facing display key;
   - `YahooSymbol` = fetch/link key.
   The migration code explicitly reduces crypto pairs such as `BTC-USD` and `ETH-EUR`
   to their base token for display purposes.
3. The same merge still contains `BTC-USD` and `ETH-USD` in
   `artifacts/snapshots/score_history.csv`; no `CRYPTO:` identifier is present there.
   Therefore the `CRYPTO:<BASE>` namespace was introduced later than that preserved
   2026-02-15 snapshot.

These facts justify investigating base-token continuity. They do not establish when a
stable identifier changed or prove that different quote pairs are the same historical
instrument record.

## Conservative parser

`src/scanner/research/governance/qm_b_crypto_identifier.py` recognizes only:

- `CRYPTO:<BASE>`;
- `<BASE>-USD`;
- `<BASE>-EUR`;
- `<BASE>-USDT`;
- `<BASE>-USDC`.

The base token is used only as a reconciliation candidate key. It is explicitly **not
an instrument ID**.

## Inputs

- `artifacts/research/history_analysis.csv`
- `data/inputs/universe_master.csv`

Historical rows first pass through the existing QM-B observed-membership classifier.
Only `SCANNER_OBSERVED` + `OBSERVED_IN_SCANNER` rows enter the crypto timeline.
Price/market backfill and unknown provenance cannot create crypto lineage evidence.

## Output per base token

For each candidate base the diagnostic reports:

- every historical recognized identifier;
- first and last scanner-observed date per identifier;
- row counts;
- observed names and currencies;
- first/last date of `CRYPTO:<BASE>` identifiers;
- first/last date of quote-pair identifiers;
- dates on which both styles coexist;
- current active `asset_type=crypto` symbols with the same base;
- `lineage_status=CANDIDATE` only when historical and current evidence both exist.

Every base record also states:

- `stable_identity_verified=false`;
- `quote_pair_equivalence_verified=false`.

The top-level result states:

- `research_only=true`;
- `productive_integration_enabled=false`;
- `base_token_is_identity=false`;
- `absence_interpreted_as_out_of_scope=false`.

## Real-history result

CI evaluated the preserved `artifacts/research/history_analysis.csv` with source SHA-256
`1c0a9263370424badd759b01f19c413520497ebf69a9ef09dd9fddeb82050be0`.
It found **1,287 scanner-observed crypto rows** matching the conservative identifier
parser: **27 quote-pair rows** and **1,260 `CRYPTO:<BASE>` rows**.

| Base | Historical quote-pair identifiers | Quote-pair last seen | `CRYPTO:*` first seen | Current active crypto symbol | Overlap days |
| --- | --- | --- | --- | --- | ---: |
| ADA | `ADA-EUR`, `ADA-USD` | 2026-02-11 | 2026-03-01 | `ADA-USD` | 0 |
| BTC | `BTC-USD` | 2026-02-11 | 2026-03-01 | `BTC-USD` | 0 |
| DOGE | `DOGE-EUR` | 2026-02-11 | 2026-03-01 | `DOGE-EUR` | 0 |
| ETH | `ETH-USD` | 2026-02-11 | 2026-03-01 | `ETH-USD` | 0 |
| SOL | `SOL-EUR` | 2026-02-11 | 2026-03-01 | `SOL-EUR` | 0 |
| XRP | `XRP-EUR` | 2026-02-11 | 2026-03-01 | `XRP-EUR` | 0 |

For all six bases the pair-style observations begin on 2026-02-10, while the internal
`CRYPTO:<BASE>` observations run from 2026-03-01 through the latest preserved scanner
date 2026-09-28.

This does **not** prove that the identifier switch happened exactly on 2026-03-01.
The preserved evidence only brackets it: pair-style identifiers are last observed on
2026-02-11 and `CRYPTO:<BASE>` identifiers are first observed on 2026-03-01. Therefore
QM-B may state only that the namespace transition occurred **after 2026-02-11 and no
later than 2026-03-01**, subject to the preserved-history coverage gap.

The seven previously unmatched historical crypto values are therefore explained as
part of one coherent identifier-namespace migration candidate set. They are no longer
interpreted as seven independent disappearance/survivorship events. This is a
reconciliation finding, not yet stable-identity promotion.

## Interpretation rule

A shared base token and a clean temporal transition can support an
**identifier-lineage candidate**. It cannot by itself prove:

- stable instrument identity;
- equivalence of USD/EUR quote pairs;
- listing venue continuity;
- historical universe completeness;
- delisting or absence;
- investability.

Any future promotion into the strict QM-B Instrument Master / Alias Ledger needs a
separate PIT-defensible identity decision with explicit provenance.

## CLI

```bash
PYTHONPATH=src python scripts/qm_b_crypto_identifier.py \
  --history artifacts/research/history_analysis.csv \
  --current-universe data/inputs/universe_master.csv
```

Optional output:

```bash
--output /tmp/qm_b_crypto_identifier.json
```

## Definition of Done

1. only scanner-observed rows enter the timeline;
2. parser accepts only conservative project crypto identifier forms;
3. first/last dates are derived from preserved history, never guessed;
4. overlap between old/new forms is reported rather than collapsed;
5. current master matches are restricted to active `asset_type=crypto` rows;
6. base-token matching never verifies stable identity;
7. real-history CI confirms the expected historical crypto bases and prints the actual
   transition boundaries;
8. no productive files or historical observations are rewritten.
