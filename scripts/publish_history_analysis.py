"""Publish the stable, public analysis CSV from scanner and market history."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from scanner.data.io.safe_csv import to_csv_safely


SCANNER_SOURCE = "artifacts/snapshots/score_history.csv"
MARKET_SOURCE = "artifacts/market_data/yahoo_ohlcv.csv"
TARGET = "artifacts/research/history_analysis.csv"
BASE_COLUMNS = ["date", "symbol", "currency", "open", "high", "low", "close", "volume"]
PROVENANCE_COLUMNS = ["observation_type", "data_source"]


def publish_history_analysis(root: Path) -> Path:
    target = root / TARGET
    scanner_path = root / SCANNER_SOURCE
    market_path = root / MARKET_SOURCE
    if not scanner_path.exists():
        raise FileNotFoundError(scanner_path)
    if not market_path.exists():
        raise FileNotFoundError(market_path)

    scanner = pd.read_csv(scanner_path, dtype=str, keep_default_na=False)
    if not {"date", "symbol"}.issubset(scanner.columns):
        raise ValueError("Scanner history requires date and symbol columns")
    scanner["observation_type"] = "observed_scanner"
    scanner["data_source"] = "scanner_run"

    market = pd.read_csv(market_path, dtype=str, keep_default_na=False)
    missing = [column for column in BASE_COLUMNS if column not in market.columns]
    if missing:
        raise ValueError(f"Market cache is missing columns: {missing}")
    market = market[BASE_COLUMNS].copy()
    market["observation_type"] = "market_data"
    market["data_source"] = "yahoo_ohlcv"
    market = market.drop_duplicates(["date", "symbol"], keep="first")

    columns = list(scanner.columns)
    columns += [column for column in market.columns if column not in columns]
    columns += [column for column in PROVENANCE_COLUMNS if column not in columns]
    data = pd.concat([scanner, market], ignore_index=True).reindex(columns=columns)
    data = data.sort_values(["date", "symbol", "observation_type"], kind="mergesort").reset_index(drop=True)

    target.parent.mkdir(parents=True, exist_ok=True)
    to_csv_safely(data, target, index=False)
    return target


def main() -> int:
    target = publish_history_analysis(Path(__file__).resolve().parents[1])
    print(f"Published: {target.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
