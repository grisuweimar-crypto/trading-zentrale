"""CY02-B01: independently documented dual Vale quote identities."""
from pathlib import Path
import pandas as pd
from scanner.app.build_watchlist import (
    _load_master_universe, _sync_watchlist_from_master, expected_master_universe_count,
)
from scanner.data.schema.canonical import canonicalize_df
from scanner.app.build_watchlist import _dedupe_universe

MASTER = Path(__file__).resolve().parents[1]/"data/inputs/universe_master.csv"
EXPECTED = {
    "VALE3.SA": ("BRVALEACNOR0", "BRL"),
    "VALE": ("US91912E1055", "USD"),
}


def test_master_contains_distinct_original_listing_and_adr():
    master = _load_master_universe(MASTER)
    assert master is not None
    rows = master[master["symbol"].isin(EXPECTED)]
    assert len(rows) == 2
    for symbol, (isin, ccy) in EXPECTED.items():
        r = rows[rows["symbol"]==symbol]
        assert len(r)==1
        assert r.iloc[0]["isin"] == isin
        assert r.iloc[0]["currency"] == ccy
    assert len(set(rows["isin"])) == 2


def test_separate_ticker_price_symbol_isin_quote_after_master_sync():
    master = _load_master_universe(MASTER)
    # Existing scanner DB can be stale and may contain only the previous VALE row.
    db = pd.DataFrame([dict(Ticker="VALE", Symbol="VALE", YahooSymbol="VALE",
                 Yahoo="VALE", ISIN="BRVALEACNOR0", Currency="BRL",
                 **{"Währung":"BRL"}, Name="VALE SA")])
    synced, _ = _sync_watchlist_from_master(db, master)
    for symbol, (isin, ccy) in EXPECTED.items():
        matches = synced[synced["YahooSymbol"] == symbol]
        assert len(matches)==1
        row = matches.iloc[0]
        assert row["ISIN"] == isin
        assert row["Currency"] == ccy
        assert row["Währung"] == ccy
        assert row["Ticker"] == symbol
    canonical = canonicalize_df(synced)
    kept, _, _ = _dedupe_universe(canonical)
    assert len(kept) == len(canonical) == 216
    assert expected_master_universe_count(MASTER) == 216
    assert len(set(kept["asset_id"])) == len(kept)


def test_us_adr_not_misinterpreted_as_br_ordinary_share():
    master = _load_master_universe(MASTER)
    adr = master[master["symbol"]=="VALE"].iloc[0]
    local = master[master["symbol"]=="VALE3.SA"].iloc[0]
    assert adr["isin"].startswith("US")
    assert local["isin"].startswith("BR")
    assert adr["currency"] != local["currency"]
