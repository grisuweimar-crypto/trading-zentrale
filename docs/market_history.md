# Historical market data cache

The production pipeline now discovers the full validated universe from
`latest_scanner.csv` and performs bootstrap/incremental updates before daily
research generation. See [Price Session Pipeline](price_session_pipeline.md)
for the authoritative workflow, validation, mapping, retry and coverage rules.

The scanner currently refreshes Yahoo data live and derives `Trend200`, `RS3M`,
and risk metrics from that response. It does not use `score_history.csv` as a
price-history cache.

`scripts/prefetch_market_history.py` stores raw daily OHLCV observations in
`artifacts/market_data/yahoo_ohlcv.csv`. Rows are keyed by `date` and `symbol`;
existing rows are retained, duplicate keys are not added, and existing values
are not overwritten. Currency comes from the active master universe.

This cache is separate from scanner observations. It does not backfill or alter
`artifacts/snapshots/score_history.csv`.

The validated analysis publisher stores new scanner observations at
`artifacts/research/history_analysis.csv` and prices separately at
`artifacts/research/price_backfill.csv`. The archive's stable public URL is:

`https://raw.githubusercontent.com/grisuweimar-crypto/trading-zentrale/refs/heads/main/artifacts/research/history_analysis.csv`

`observation_type=observed_scanner` and `data_source=scanner_run` identify real
scanner observations. Legacy `market_data` archive rows remain preserved but are
excluded from scanner views. New price rows use `observation_type=price_backfill`.
Scanner metrics must never be reconstructed from these prices. New downloads carry
their UTC retrieval time; unavailable legacy retrieval times remain blank.
See [research data architecture](research_data_architecture.md) for validation,
daily views, partial scans, and metadata.
