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

The public analysis export combines all observed scanner rows from
`artifacts/snapshots/score_history.csv` with this cache at
`artifacts/research/history_analysis.csv`. Its stable public URL is:

`https://raw.githubusercontent.com/grisuweimar-crypto/trading-zentrale/refs/heads/main/artifacts/research/history_analysis.csv`

`observation_type=observed_scanner` and `data_source=scanner_run` identify real
scanner observations. `observation_type=market_data` and
`data_source=yahoo_ohlcv` identify raw market observations. The two types are
kept as separate rows even when their date and symbol overlap. Reconstructed
scanner scores are not claimed until they are separately calculated and
reviewed.