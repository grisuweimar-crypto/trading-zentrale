"""Fetch and persist historical Yahoo OHLCV data without touching scanner history."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import yfinance as yf

from scanner.data.io.paths import artifacts_dir
from scanner.data.io.safe_csv import to_csv_safely


DEFAULT_TICKERS = ("AVAV", "ROL")
DEFAULT_DAYS = 300
REQUIRED_COLUMNS = ["date", "symbol", "currency", "open", "high", "low", "close", "volume"]


def _currency_map(root: Path) -> dict[str, str]:
    path = root / "data" / "inputs" / "universe_master.csv"
    if not path.exists():
        return {}
    frame = pd.read_csv(path)
    symbols = frame.get("symbol", pd.Series(dtype="string")).astype(str).str.strip()
    currencies = frame.get("currency", pd.Series(dtype="string")).astype(str).str.strip()
    return {
        symbol: currency
        for symbol, currency in zip(symbols, currencies)
        if symbol and currency and str(currency).lower() != "nan"
    }


def _download(tickers: list[str]) -> pd.DataFrame:
    data = yf.download(
        tickers=tickers,
        period="2y",
        interval="1d",
        auto_adjust=False,
        group_by="column",
        threads=False,
        progress=False,
    )
    rows: list[pd.DataFrame] = []
    for ticker in tickers:
        if isinstance(data.columns, pd.MultiIndex):
            ticker_data = data.xs(ticker, axis=1, level=1, drop_level=True)
        else:
            ticker_data = data.copy()
        if ticker_data.empty:
            continue
        available = ticker_data.rename(columns=str.lower)
        needed = [c for c in ("open", "high", "low", "close", "volume") if c in available]
        if len(needed) != 5:
            continue
        row = available[needed].copy()
        row.insert(0, "symbol", ticker)
        row.index = pd.to_datetime(row.index).tz_localize(None).normalize()
        row.index.name = "date"
        rows.append(row.reset_index())
    if not rows:
        return pd.DataFrame(columns=["date", "symbol", "open", "high", "low", "close", "volume"])
    return pd.concat(rows, ignore_index=True)


def prefetch_history(
    root: Path,
    tickers: tuple[str, ...] = DEFAULT_TICKERS,
    minimum_days: int = DEFAULT_DAYS,
    output: Path | None = None,
) -> pd.DataFrame:
    if minimum_days < 1:
        raise ValueError("minimum_days must be positive")
    symbols = tuple(dict.fromkeys(s.strip() for s in tickers if s.strip()))
    if not symbols:
        raise ValueError("At least one ticker is required")

    target = output or artifacts_dir() / "market_data" / "yahoo_ohlcv.csv"
    target.parent.mkdir(parents=True, exist_ok=True)
    existing = pd.read_csv(target) if target.exists() else pd.DataFrame(columns=REQUIRED_COLUMNS)
    downloaded = _download(list(symbols))
    currencies = _currency_map(root)
    downloaded["currency"] = downloaded["symbol"].map(currencies).fillna("")
    downloaded = downloaded[REQUIRED_COLUMNS]

    def _keys(frame: pd.DataFrame) -> set[tuple[str, str]]:
        dates = pd.to_datetime(frame.get("date", pd.Series(dtype="object")), errors="coerce")
        symbols_frame = frame.get("symbol", pd.Series(dtype="object")).astype(str).str.strip()
        return {
            (timestamp.strftime("%Y-%m-%d"), symbol)
            for timestamp, symbol in zip(dates, symbols_frame)
            if pd.notna(timestamp) and symbol
        }

    existing_keys = _keys(existing)
    downloaded_keys = _keys(downloaded)
    if target.exists() and downloaded_keys.issubset(existing_keys):
        counts = existing.groupby("symbol").size()
        missing = {symbol: int(counts.get(symbol, 0)) for symbol in symbols if counts.get(symbol, 0) < minimum_days}
        if missing:
            raise RuntimeError(f"Insufficient historical rows: {missing}")
        return existing[existing["symbol"].isin(symbols)].copy()

    combined = pd.concat([existing, downloaded], ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"], errors="raise").dt.strftime("%Y-%m-%d")
    combined["symbol"] = combined["symbol"].astype(str).str.strip()
    combined = combined.drop_duplicates(subset=["date", "symbol"], keep="first")
    combined = combined.sort_values(["symbol", "date"], kind="mergesort").reset_index(drop=True)
    to_csv_safely(combined, target, index=False)

    counts = combined.groupby("symbol").size()
    missing = {symbol: int(counts.get(symbol, 0)) for symbol in symbols if counts.get(symbol, 0) < minimum_days}
    if missing:
        raise RuntimeError(f"Insufficient historical rows: {missing}")
    return combined[combined["symbol"].isin(symbols)].copy()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("tickers", nargs="*", default=list(DEFAULT_TICKERS))
    parser.add_argument("--minimum-days", type=int, default=DEFAULT_DAYS)
    args = parser.parse_args()
    frame = prefetch_history(Path(__file__).resolve().parents[1], tuple(args.tickers), args.minimum_days)
    print(frame.groupby("symbol").size().to_string())
    print(f"Saved: {(artifacts_dir() / 'market_data' / 'yahoo_ohlcv.csv').as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())