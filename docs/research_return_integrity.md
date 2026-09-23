# Research return integrity hotfix

Phase 1A/1B research must never treat stock splits or reverse splits as investment returns.

Required implementation:

1. Extend the observed Yahoo market-price pipeline with a persisted `adj_close` field while remaining backward-compatible with the existing cache/backfill files that do not yet contain it.
2. `scripts/prefetch_market_history.py` must capture Yahoo `Adj Close` when using `auto_adjust=False`. Existing historical rows with missing `adj_close` must trigger a one-time history bootstrap and may be enriched with the provider `adj_close` without overwriting already-persisted raw OHLCV observations.
3. `artifacts/research/price_backfill.csv` must publish `adj_close` after refresh. Legacy schema is accepted only for migration; research returns must not silently fall back to raw `close` when `adj_close` is unavailable.
4. Phase 1A/1B forward returns must use adjusted prices. Add a regression test representing a reverse split where raw close jumps ~13x but adjusted close is continuous; the research return must follow adjusted close, not raw close.
5. Peer-relative baselines must exclude the subject symbol. Use a leave-one-symbol-out same-currency median, falling back to a leave-one-symbol-out global daily median when there is no other same-currency peer. Apply the same semantics in Phase 1B.
6. Empty quality bands must not emit NaN. Skip empty groups or emit JSON `null`; research JSON writers should use strict serialization (`allow_nan=False`) so invalid NaN artifacts fail tests.
7. Add regression tests for the three Codex findings and preserve all existing point-in-time, holdout, cooldown, and peer-purge safeguards from Phase 1A/1B.
8. Add a CI/research path that refreshes/migrates adjusted prices in the runner and reruns both Phase 1A and 1B on the corrected price base so the historical reports can be compared before merge.

This work is research/data-integrity only. It must not change the production score, R0-R5 semantics, watchlist, portfolio logic, or Depot-Watch decisions.
