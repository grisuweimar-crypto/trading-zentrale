# Historical market data cache

The scanner currently refreshes Yahoo data live and derives `Trend200`, `RS3M`,
and risk metrics from that response. It does not use `score_history.csv` as a
price-history cache.

`scripts/prefetch_market_history.py` stores raw daily OHLCV observations in
`artifacts/market_data/yahoo_ohlcv.csv`. Rows are keyed by `date` and `symbol`;
existing rows are retained, duplicate keys are not added, and existing values
are not overwritten. Currency comes from the active master universe.

This cache is separate from scanner observations. It does not backfill or alter
`artifacts/snapshots/score_history.csv`.

The public analysis export is generated from this cache at
`artifacts/research/history_analysis.csv`. Its stable public URL is:

`https://raw.githubusercontent.com/grisuweimar-crypto/trading-zentrale/main/artifacts/research/history_analysis.csv`

The export currently contains raw market observations only. `observation_type`
and `data_source` identify the provenance; reconstructed scanner scores are not
claimed until they are separately calculated and reviewed.