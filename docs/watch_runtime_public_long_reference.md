# Decision Watch public long reference

The compact Watch runtime publishes two additional portfolio-free artifacts after each successful W10 integration:

- `artifacts/research/watch_runtime/public_long_reference.json`
- `artifacts/research/watch_runtime/public_long_reference.csv`

They run the canonical 7D -> 7H chain for every scanner symbol under one explicit hypothetical position assumption: `position_state=long` with quantity, market value, entry price, current price, P/L, add capacity and remaining adds all missing.

This is not a model portfolio and does not contain actual holdings. It exists only as a pragmatic transport surface for private Depot-Watch clients whose mapped rows use the same minimal long-only position context. Such clients can join their private symbol list locally against the public reference without downloading the large 7A archive or publishing private depot data.

The JSON retains typed Decision fields needed for explanation and W8 path/history semantics. The CSV is a compact presentation surface for interactive retrieval. Both are generated from the same sealed W10 snapshot and canonical orchestrator as the private Watch.

W11 semantics remain fail-closed. A partial Watch passes W11 only when every unavailable row is explicitly prefixed `UNMAPPED:` and fails specifically as `symbol_not_in_daily_research`. Any missing mapped symbol, invalid bundle, snapshot mismatch, position-context mismatch, scalar fallback, Phase-8 effect, super-score, or private persistence still fails acceptance.
