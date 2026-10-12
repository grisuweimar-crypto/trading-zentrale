"""Public instrument coverage; no private portfolio records or investor attributes."""
from pathlib import Path

import pandas as pd

from scanner.app.build_watchlist import (
    _load_master_universe,
    _sync_watchlist_from_master,
    _dedupe_universe,
)
from scanner.data.schema.canonical import canonicalize_df

MASTER = Path(__file__).resolve().parents[1] / "data/inputs/universe_master.csv"

EXPECTED = {
    "EUNL.DE": ("IE00B4L5Y983", "etf", "EUR"),
    "VWCE.DE": ("IE00BK5BQT80", "etf", "EUR"),
    "BABA": ("US01609W1027", "stock", "USD"),
    "MANA-USD": ("", "crypto", "USD"),
    "AVAX-USD": ("", "crypto", "USD"),
}


def test_expanded_universe_instrument_identity_and_provider_symbols():
    master = _load_master_universe(MASTER)
    assert master is not None
    for symbol, (isin, kind, currency) in EXPECTED.items():
        rows = master.loc[master["symbol"].eq(symbol)]
        assert len(rows) == 1, symbol
        row = rows.iloc[0]
        assert str(row["isin"]) == isin
        assert str(row["asset_type"]) == kind
        assert str(row["currency"]) == currency


def test_new_instruments_survive_watchlist_sync_and_canonical_deduplication():
    master = _load_master_universe(MASTER)
    assert master is not None
    synced, _ = _sync_watchlist_from_master(
        pd.DataFrame(columns=["Ticker", "Symbol", "YahooSymbol", "ISIN", "Currency"]), master
    )
    canonical = canonicalize_df(synced)
    kept, _, _ = _dedupe_universe(canonical)
    assert len(kept) == 220
    for symbol in EXPECTED:
        rows = kept.loc[kept["YahooSymbol"].eq(symbol)]
        assert len(rows) == 1, symbol
    # ADR must be distinct from the home-market ordinary share.
    assert {"BABA", "9988.HK"} <= set(kept["YahooSymbol"])
    assert kept.loc[kept["YahooSymbol"].eq("BABA"), "ISIN"].iloc[0] == "US01609W1027"
