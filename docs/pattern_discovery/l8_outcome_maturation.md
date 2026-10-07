# Pattern Discovery Lab v2 — L8 Outcome Maturation

## Purpose

L8 evaluates whether an immutable L7 prospective claim has reached its complete
forward market-session horizon.

It creates an immutable outcome record only after the entire horizon is
observable from validated stored market sessions.

L8 does **not** confirm or falsify a Pattern. It does not compute probabilities,
hit rates, sequential looks, ratings or promotion decisions. Those remain L9
and later responsibilities.

## Inputs

L8 consumes:

- one hash-valid L7 capture report,
- the exact scanner snapshot used by that capture as peer-cohort evidence,
- an explicit market-session binding file containing
  `symbol + session_id + calendar_id + session_date + start_at + source`,
- validated research price rows,
- an explicit price-as-of date,
- an explicit check timestamp,
- SHA-256 hashes of the peer snapshot and price input files.

The peer-snapshot SHA-256 must equal the snapshot-file SHA-256 already frozen
inside the L7 capture report.

The session-binding file additionally reconstructs the L7 session-map identity
from `symbol + session_id + calendar_id + start_at + source`. That hash must
equal the immutable `session_map_hash` in the L7 capture. The added
`session_date` enriches that exact L7 session identity; it cannot replace the
identity with a different calendar/session record.

This prevents a later watchlist, universe or newly guessed session map from
being used retroactively for an older prospective claim.

## Exact market-session semantics

L8 reuses the existing price-session integrity model.

The validated stored price bars define observed market sessions.

L8 does not:

- convert horizons into calendar days,
- assume Monday-Friday trading,
- invent holidays,
- forward-fill missing bars,
- synthesize exchange sessions.

For the subject claim:

1. the L8 session-binding input must contain the subject symbol;
2. its `session_id`, `calendar_id`, `start_at` and `source` must exactly
   match the immutable L7 start-market-session identity;
3. its explicit `session_date` supplies the market trading date;
4. that date is **not** derived from the UTC date of `start_at`;
5. a valid stored price bar must exist on exactly that `session_date`;
6. the H-th later valid observed session is the target session;
7. the path therefore contains H + 1 observed sessions including the start.

If the exact start bar is absent, the claim remains
`MISSING_START_SESSION`.

If fewer than H later observed sessions are available at `price_as_of`, the
claim remains `IMMATURE_HORIZON`.

No later session is substituted for a missing subject start session.

## Adjusted price integrity

Raw close is used only to establish that an observed price bar/session exists.

Every return and path metric requires `adj_close`.

There is no fallback from missing `adj_close` to raw `close`.

If any required adjusted price from start through target is missing, the claim
remains `MISSING_ADJUSTED_PRICE`.

This preserves the existing research-return rule that stock splits and reverse
splits cannot become fake investment returns.

## Original currency

L8 performs no currency conversion.

The complete subject path must have one non-empty, stable original currency.
A missing currency yields `CURRENCY_UNAVAILABLE`; a currency change inside
the path yields `CURRENCY_MISMATCH`.

Returns remain dimensionless within the original observed price series.

## Return

For a complete path:

`return = target_adj_close / start_adj_close - 1`

The start and target session dates and every observed session date in between
are stored as horizon provenance.

## Peer / reference outcome

The peer population comes from the exact L7 scanner snapshot.

The subject symbol is always excluded.

For each other snapshot symbol, L8 requires its own explicit market-session
binding and an exact price bar on that peer's bound `session_date`. From that
session it builds the complete H-session adjusted-price path.

A missing peer binding is excluded as
`START_SESSION_BINDING_UNAVAILABLE`; no exchange date is guessed from ticker,
UTC time or the subject's calendar. Other incomplete or invalid peer paths are
also excluded and counted by reason.

Reference selection is:

1. leave-one-symbol-out median of complete peers with the same original
   currency as the subject;
2. if no same-currency peer is available, leave-one-symbol-out global median of
   all complete peers.

This reuses the existing research-integrity semantics instead of creating a new
peer model.

For a relative-alpha Pattern:

`peer_excess = subject_return - peer_median_return`

A relative-alpha claim cannot mature without a valid peer reference.

A directional claim can mature without a peer reference because its primary
target is the subject return; the missing reference remains explicit.

L8 does not infer a benchmark symbol from free-text baseline labels.

## Path metrics

For every matured subject path L8 stores:

### Adverse excursion

The minimum cumulative move from the start after aligning the raw price path to
the frozen expected direction.

The start observation is included, so the value is never artificially improved
by dropping the entry session.

### Path max drawdown

The minimum:

`adj_close / running_peak - 1`

over the complete start-to-target adjusted-price path.

These are descriptive outcome/path features only. They do not assign Pattern
quality.

## Target binding

L8 v1 supports the target families already admitted by L3:

- `return_5t/20t/40t/60t_gt_0`
- `return_5t/20t/40t/60t_lt_0`
- `peer_excess_5t/20t/40t/60t_gt_0`
- `peer_excess_5t/20t/40t/60t_lt_0`

The target horizon and direction must match the frozen L7 claim exactly.

The horizons remain separate research objects.

A mature 5T outcome never satisfies a 20T, 40T or 60T claim.

## Statuses

A check can classify a claim as:

- `MATURED`
- `IMMATURE_HORIZON`
- `START_SESSION_BINDING_UNAVAILABLE`
- `MISSING_START_SESSION`
- `MISSING_ADJUSTED_PRICE`
- `CURRENCY_UNAVAILABLE`
- `CURRENCY_MISMATCH`
- `REFERENCE_UNAVAILABLE`

Only `MATURED` creates an immutable outcome record.

Non-mature states are recorded in the immutable check report and can be
re-evaluated later.

## Integrity hashes

Each maturation check binds:

- L8 contract hash,
- L7 capture hash,
- exact peer-snapshot hash and membership hash,
- exact explicit start-session binding file hash and normalized session hash,
- exact price-file hash,
- normalized validated price-input hash,
- price-as-of,
- check timestamp.

Each matured outcome binds:

- exact L7 claim ID and claim hash,
- PAT ID/version/spec hash,
- exact horizon and target,
- exact start and target sessions,
- every session date in the subject path,
- subject adjusted-price path hash,
- peer snapshot binding,
- peer-set hashes,
- immutable outcome hash.

## Append-only storage

Matured outcomes are written to:

`artifacts/research/pattern_discovery/outcome_maturations.jsonl`

The registry is:

- append-only,
- hash-chained,
- one outcome per L7 claim,
- idempotent for an identical replay,
- fail-closed if the same claim later produces different outcome content.

Daily/periodic maturation checks are stored separately under:

`artifacts/research/pattern_discovery/outcome_maturation_checks/{capture_id}/{check_id}.json`

An immature check therefore never mutates the original L7 claim and does not
reserve a false final outcome.

## Runner

The operational runner is:

`scripts/pattern_discovery/run_l8_outcome_maturation.py`

Required arguments:

- `--capture-report`
- `--peer-snapshot`
- `--start-sessions`
- `--prices`
- `--checked-at`
- `--price-as-of`
- `--actor-id`

The runner hashes the exact peer snapshot and price files it reads before
building the check.

## Definition of Done

L8 is complete when:

- only hash-valid immutable L7 claims are accepted;
- L7 claims remain byte/content unchanged;
- start and target are defined by exact observed market sessions;
- start-session dates are explicit market-calendar facts and are never derived
  from UTC timestamp dates;
- 5T/20T/40T/60T remain separate;
- no outcome is created before the full horizon exists;
- adjusted prices are mandatory;
- raw close cannot silently replace adjusted close;
- original currency is preserved without FX conversion;
- missing prices remain missing;
- Return, Peer Excess, Adverse Excursion and Path Max Drawdown are deterministic
  and hash-bound;
- peer cohorts are bound to the exact L7 capture snapshot;
- matured outcomes are append-only and immutable;
- no L9 confirmation, sequential monitoring, rating or promotion is performed;
- no Decision Layer, portfolio or execution authority is created;
- the full L0-L8 regression suite and relevant price/session plus QM-C
  compatibility suites are green.

## Next phase

L9 — Confirmation & Falsification Engine.
