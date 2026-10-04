# Elliott vNext – Stage 4 Historical Validation

## Status

Post-activation work package.

Stage 1–3 activated Elliott vNext technically. Stage 4 is the first explicit
post-activation research stage and executes the already frozen 6G historical
validation on the real project OHLCV history.

Stage 4 is not a new Elliott counting rule and not a production promotion.

## Goal

Use the existing large historical OHLCV base immediately instead of waiting for
months of prospective captures before doing further work.

The full real history is replayed causally through:

```text
6A Pivots
-> 6B Scenarios
-> 6C Fibonacci / projection zones
-> 6D Review Routing
-> 6G Historical Validation
```

Only data available at each historical `as_of` may influence that state.

## Real source

Primary source:

`artifacts/market_data/yahoo_ohlcv.csv`

The Stage-4 artifact binds:

- source Git commit,
- SHA-256 of the price source,
- row count,
- symbol count,
- first and last source dates,
- `stable_start`,
- Module-6 freeze date.

## What Stage 4 measures

### Structural validation

For motive structures, 6G separates:

- `progressed`
- `invalidated`
- `unresolved`

`unresolved` is not silently counted as failure.

Structural fit is kept separate from performance.

### Projection validation

Existing 6C projection zones are evaluated at:

- 5 sessions
- 10 sessions
- 20 sessions
- 40 sessions
- 60 sessions

Measured through the existing 6G contract:

- future zone hit,
- signed forward return,
- MFE / MAE where adjusted path data are valid.

A hit in the signal session itself is forbidden.

### Review-routing validation

Existing 6D review contexts are evaluated directionally, including:

- `entry_or_add_review`
- `partial_reduce_review`
- `reentry_or_add_review`
- `profit_protection_review`
- `larger_reduce_or_exit_review`

This remains review research. No order semantics are introduced.

## Evidence partition

The Module-6 rule freeze remains:

`2026-09-25`

Therefore:

- historical/pre-freeze observations are
  `legacy_development_descriptive_only`;
- post-freeze observations can be
  `prospective_unspent`.

The large existing History is intentionally used now for descriptive historical
validation, defect finding, effect-size estimation and prioritisation.

It is **not** relabelled as fresh independent confirmation.

## Hard guards

Every Stage-4 run fails closed if any replay state:

- is not prefix-only,
- uses future rows,
- uses performance to build the Elliott state,
- ceases to be research-only,
- contains a trade decision,
- contains an order instruction.

Stage 4 also requires the 6G result to keep:

- automatic promotion disabled,
- no BUY/HOLD/REDUCE/SELL instruction,
- no order instruction,
- no raw-close fallback for performance,
- no invented round-trip P&L,
- no promoted W5 numeric level,
- no Degree reducer.

## Output

Canonical result:

`artifacts/research/elliott_vnext_stage4_historical_validation.json`

The output is a compact result, not the full replay state archive.

It contains:

- real source identity,
- replay coverage,
- replay guard review,
- 6G coverage,
- structural-resolution summary,
- projection summaries,
- routing summaries,
- evidence policy,
- hard boundaries,
- deterministic Stage-4 result hash.

## Technical completion

Stage 4 is technically complete when the real full-history workflow has:

1. passed Stage-4 and frozen-6G regressions;
2. replayed the complete available OHLCV universe;
3. produced no PIT / future-data guard violation;
4. generated the canonical Stage-4 result artifact;
5. published that artifact with source identity;
6. retained all research-only / no-promotion boundaries.

Technical completion does not mean that Elliott has been empirically promoted.

## Next stage

Stage 5 is deliberately separated from this Stage-4 baseline.

Stage 5 will evaluate **incremental cross-system value**:

- Elliott vs. existing Scanner/Decision-Layer information,
- confirmation / redundancy,
- Elliott Rescue / Scanner Rescue,
- conflicts,
- stage-specific incremental value,
- review-context value versus the existing baseline.

That separation prevents the historical Elliott-only baseline from being mixed
with the later cross-system comparison.
