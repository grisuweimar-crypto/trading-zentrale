"""Refresh the existing canonical OHLCV cache BEFORE the scanner scores anything.

Uses the exact current input watchlist plus active master-universe identifiers.
Unlike prefetch_market_history's default discovery, this does not depend on
the not-yet-produced latest_scanner snapshot. No second price database.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scanner.data.enrich.yahoo_prices import _pick_symbol, ISIN_RE
from prefetch_market_history import prefetch_history


def symbols_before_scanner(root: Path) -> tuple[str, ...]:
    watch = root / "artifacts/watchlist/watchlist.csv"
    if not watch.is_file():
        watch = root / "data/inputs/watchlist.csv"
    if not watch.is_file():
        raise FileNotFoundError("scanner watchlist input missing")
    df = pd.read_csv(watch, low_memory=False)
    if df.empty:
        raise ValueError("empty scanner watchlist input")
    symbols = {_pick_symbol(row) for _, row in df.iterrows()}
    master = root / "data/inputs/universe_master.csv"
    if master.is_file():
        m = pd.read_csv(master, dtype=str, keep_default_na=False)
        if {"symbol", "active"}.issubset(m.columns):
            for _, row in m.iterrows():
                active = str(row["active"]).strip().lower() in {"1", "true", "yes", "on"}
                name = str(row["symbol"]).strip()
                if active and name and not ISIN_RE.fullmatch(name.upper()):
                    symbols.add(name)
    symbols.discard("")
    symbols.update(("SPY", "BTC-USD"))
    if len(symbols) < 2:
        raise ValueError("no resolvable scanner instruments")
    return tuple(sorted(symbols))


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    symbols = symbols_before_scanner(root)
    frame = prefetch_history(root, tickers=symbols, minimum_days=300)
    coverage = frame.attrs["coverage"]
    if frame.empty:
        raise ValueError("canonical_market_cache_empty_after_prefetch")
    print(json.dumps({
        "purpose": "closed_bar_pre_scoring",
        "source": "artifacts/market_data/yahoo_ohlcv.csv",
        "symbols_requested_including_benchmarks": len(symbols),
        "validated_rows": len(frame),
        "coverage_summary": {k:v for k,v in coverage.items() if k!="symbols"},
        "prior_scanner_snapshot_not_required": True,
    }, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
