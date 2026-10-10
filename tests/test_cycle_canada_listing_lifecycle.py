"""CY02-B01: issuer/exchange-verified Canadian listing lifecycle."""
from pathlib import Path
import pandas as pd

from scanner.app.build_watchlist import (
    _load_master_universe, _sync_watchlist_from_master,
    expected_master_universe_count, _dedupe_universe,
)
from scanner.data.schema.canonical import canonicalize_df

MASTER = Path(__file__).resolve().parents[1] / "data/inputs/universe_master.csv"


def test_delisted_dolly_varden_preserved_as_inactive_not_fake_live_price():
    all_rows = pd.read_csv(MASTER, dtype=str, keep_default_na=False)
    old = all_rows[all_rows["symbol"] == "DV.V"]
    assert len(old) == 1
    assert old.iloc[0]["active"] == "0"
    assert old.iloc[0]["isin"] == "CA2568277834"
    assert "DV.V" not in set(_load_master_universe(MASTER)["symbol"])


def test_silver_tiger_exchange_migration_retains_cad_and_original_isin():
    all_rows = pd.read_csv(MASTER, dtype=str, keep_default_na=False)
    assert "SLVR.V" not in set(all_rows["symbol"])
    listed = all_rows[all_rows["symbol"] == "SLVR.TO"]
    assert len(listed) == 1
    assert listed.iloc[0]["active"] == "1"
    assert listed.iloc[0]["isin"] == "CA82831T1093"
    assert listed.iloc[0]["currency"] == "CAD"


def test_canonical_count_remains_215_after_venue_migration_and_delisting():
    # 216 after separately proved B3/NYSE identities, minus no-longer-tradable DV.V.
    assert expected_master_universe_count(MASTER) == 215


def test_sync_excludes_delisted_quote_and_does_not_reuse_old_ticker_symbol():
    seed = pd.DataFrame([dict(
        Ticker="SLVR.V", Symbol="SLVR.V", YahooSymbol="SLVR.V",
        Yahoo="SLVR.V", ISIN="CA82831T1093",
        Currency="CAD", **{"Währung": "CAD"}, Name="SILVER TIGER",
    ), dict(
        Ticker="DV.V", Symbol="DV.V", YahooSymbol="DV.V", Yahoo="DV.V",
        ISIN="CA2568277834", Currency="CAD",
        **{"Währung": "CAD"}, Name="DOLLY VARDEN",
    )])
    synced, _ = _sync_watchlist_from_master(seed, _load_master_universe(MASTER))
    assert "DV.V" not in set(synced["YahooSymbol"])
    assert "SLVR.V" not in set(synced["YahooSymbol"])
    migrated = synced[synced["YahooSymbol"]=="SLVR.TO"]
    assert len(migrated) == 1
    assert migrated.iloc[0]["ISIN"] == "CA82831T1093"
    assert migrated.iloc[0]["Currency"] == "CAD"
    canon = canonicalize_df(synced)
    kept, _, _ = _dedupe_universe(canon)
    assert len(kept) == 215
    assert "SLVR.TO" in set(kept["asset_id"])
    assert "DV.V" not in set(kept["asset_id"])
