"""Publish the stable, public analysis CSV from the raw market-data cache."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from scanner.data.io.safe_csv import to_csv_safely


SOURCE = "artifacts/market_data/yahoo_ohlcv.csv"
TARGET = "artifacts/research/history_analysis.csv"
BASE_COLUMNS = ["date", "symbol", "currency", "open", "high", "low", "close", "volume"]
PUBLIC_COLUMNS = BASE_COLUMNS + ["observation_type", "data_source"]


def publish_history_analysis(root: Path) -> Path:
    source = root / SOURCE
    target = root / TARGET
    if not source.exists():
        raise FileNotFoundError(source)

    data = pd.read_csv(source, dtype={"date": str, "symbol": str, "currency": str})
    missing = [column for column in BASE_COLUMNS if column not in data.columns]
    if missing:
        raise ValueError(f"Market cache is missing columns: {missing}")
    data = data[BASE_COLUMNS].copy()
    data["observation_type"] = "market_data"
    data["data_source"] = "yahoo_ohlcv"
    data = data.drop_duplicates(["date", "symbol"], keep="first")
    data = data.sort_values(["symbol", "date"], kind="mergesort").reset_index(drop=True)

    target.parent.mkdir(parents=True, exist_ok=True)
    to_csv_safely(data[PUBLIC_COLUMNS], target, index=False)
    return target


def main() -> int:
    target = publish_history_analysis(Path(__file__).resolve().parents[1])
    print(f"Published: {target.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
