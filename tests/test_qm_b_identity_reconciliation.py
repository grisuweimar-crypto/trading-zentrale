from __future__ import annotations

import csv
from pathlib import Path

from scanner.research.governance.qm_b_identity_reconciliation import (
    build_identity_reconciliation,
    is_valid_isin,
    stable_isin_instrument_id,
)


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def snapshot_overrides() -> dict[str, str]:
    legacy = (
        "Ticker,Name,ISIN,Symbol,YahooSymbol\n"
        "US0378331005,APPLE,US0378331005,US0378331005,AAPL\n"
        "BTC-USD,Bitcoin,,BTC-USD,BTC-EUR\n"
    )
    universe = (
        "active,symbol,name,isin,asset_type\n"
        "1,AAPL,Apple Inc.,US0378331005,stock\n"
        "1,BTC-USD,Bitcoin,,crypto\n"
    )
    return {
        "repo_watchlist_vnext_merge_20260215": legacy,
        "repo_universe_master_20260307": universe,
    }


def test_isin_validation_uses_checksum():
    assert is_valid_isin("US0378331005") is True
    assert is_valid_isin("CA00831V2057") is True
    assert is_valid_isin("FR0000120693") is True
    assert is_valid_isin("US0378331004") is False
    assert is_valid_isin("AAPL") is False
    assert stable_isin_instrument_id("US0378331005") == "urn:scanner:isin:US0378331005"


def test_historical_isin_and_symbol_are_reconciled_but_boundary_remains_pit_safe(tmp_path: Path):
    history = tmp_path / "history.csv"
    current = tmp_path / "universe.csv"
    write_csv(
        history,
        [
            {
                "date": "2026-02-10",
                "symbol": "US0378331005",
                "name": "APPLE",
                "observation_type": "observed_scanner",
                "data_source": "scanner_run",
            },
            {
                "date": "2026-03-01",
                "symbol": "AAPL",
                "name": "Apple Inc.",
                "observation_type": "observed_scanner",
                "data_source": "scanner_run",
            },
            {
                "date": "2026-03-08",
                "symbol": "AAPL",
                "name": "Apple Inc.",
                "observation_type": "observed_scanner",
                "data_source": "scanner_run",
            },
        ],
    )
    write_csv(
        current,
        [{"active": "1", "symbol": "AAPL", "isin": "US0378331005", "asset_type": "stock", "name": "Apple Inc."}],
    )
    payload = build_identity_reconciliation(
        history,
        current,
        repo_root=tmp_path,
        snapshot_overrides=snapshot_overrides(),
    )
    rows = {row["observed_identifier"]: row for row in payload["reconciled_identifiers"]}
    old = rows["US0378331005"]
    assert old["candidate_class"] == "HISTORICAL_ISIN_SNAPSHOT_MATCH"
    assert old["identity_status"] == "VERIFIED"
    assert old["pit_alias_available_from"] == "2026-02-16"
    assert old["pit_verified_observation_count"] == 0
    assert old["pit_unverified_observation_count"] == 1

    aapl = rows["AAPL"]
    assert aapl["candidate_class"] == "HISTORICAL_SYMBOL_SNAPSHOT_MATCH"
    assert aapl["identity_status"] == "VERIFIED"
    assert aapl["pit_alias_available_from"] == "2026-02-16"
    assert aapl["pit_verified_observation_count"] == 2
    assert aapl["pit_unverified_observation_count"] == 0
    assert aapl["strict_alias_promoted"] is False
    assert payload["strict_alias_ledger"] is False


def test_current_only_match_is_partial_and_cannot_become_historical_truth(tmp_path: Path):
    history = tmp_path / "history.csv"
    current = tmp_path / "universe.csv"
    write_csv(
        history,
        [{"date": "2026-09-01", "symbol": "NEW", "observation_type": "observed_scanner", "data_source": "scanner_run"}],
    )
    write_csv(
        current,
        [{"active": "1", "symbol": "NEW", "isin": "US0378331005", "asset_type": "stock", "name": "Current-only alias"}],
    )
    payload = build_identity_reconciliation(
        history,
        current,
        repo_root=tmp_path,
        snapshot_overrides={
            "repo_watchlist_vnext_merge_20260215": "Ticker,Name,ISIN,Symbol,YahooSymbol\nBTC-USD,Bitcoin,,BTC-USD,BTC-EUR\n",
            "repo_universe_master_20260307": "active,symbol,name,isin,asset_type\n1,BTC-USD,Bitcoin,,crypto\n",
        },
    )
    row = payload["reconciled_identifiers"][0]
    assert row["candidate_class"] == "CURRENT_SYMBOL_ONLY_MATCH"
    assert row["identity_status"] == "PARTIAL"
    assert row["pit_alias_available_from"] is None
    assert row["pit_verified_observation_count"] == 0
    assert payload["current_only_match_verifies_historical_identity"] is False


def test_crypto_base_is_candidate_family_not_stable_identity(tmp_path: Path):
    history = tmp_path / "history.csv"
    current = tmp_path / "universe.csv"
    write_csv(
        history,
        [
            {"date": "2026-02-10", "symbol": "BTC-USD", "observation_type": "observed_scanner", "data_source": "scanner_run"},
            {"date": "2026-03-01", "symbol": "CRYPTO:BTC", "observation_type": "observed_scanner", "data_source": "scanner_run"},
        ],
    )
    write_csv(current, [{"active": "1", "symbol": "BTC-USD", "isin": "", "asset_type": "crypto", "name": "Bitcoin"}])
    payload = build_identity_reconciliation(
        history,
        current,
        repo_root=tmp_path,
        snapshot_overrides=snapshot_overrides(),
    )
    rows = {row["observed_identifier"]: row for row in payload["reconciled_identifiers"]}
    assert rows["BTC-USD"]["candidate_class"] == "CRYPTO_BASE_LINEAGE"
    assert rows["CRYPTO:BTC"]["candidate_class"] == "CRYPTO_BASE_LINEAGE"
    assert rows["BTC-USD"]["identity_status"] == "PARTIAL"
    assert rows["BTC-USD"]["candidate_instrument_id"] == "candidate:crypto-family:BTC"
    assert rows["BTC-USD"]["strict_membership_promoted"] is False


def test_backfill_never_enters_identity_reconciliation(tmp_path: Path):
    history = tmp_path / "history.csv"
    current = tmp_path / "universe.csv"
    write_csv(
        history,
        [
            {"date": "2026-03-08", "symbol": "AAPL", "observation_type": "observed_scanner", "data_source": "scanner_run"},
            {"date": "2025-01-01", "symbol": "AAPL", "observation_type": "market_data", "data_source": "yahoo_ohlcv"},
        ],
    )
    write_csv(current, [{"active": "1", "symbol": "AAPL", "isin": "US0378331005", "asset_type": "stock", "name": "Apple"}])
    payload = build_identity_reconciliation(
        history,
        current,
        repo_root=tmp_path,
        snapshot_overrides=snapshot_overrides(),
    )
    aapl = payload["reconciled_identifiers"][0]
    assert aapl["scanner_observation_count"] == 1
    assert aapl["first_seen"] == "2026-03-08"
    assert payload["scanner_observation_count"] == 1


def test_snapshot_alias_seeds_are_pit_dated_and_candidate_only(tmp_path: Path):
    history = tmp_path / "history.csv"
    current = tmp_path / "universe.csv"
    write_csv(history, [{"date": "2026-03-08", "symbol": "AAPL", "observation_type": "observed_scanner", "data_source": "scanner_run"}])
    write_csv(current, [{"active": "1", "symbol": "AAPL", "isin": "US0378331005", "asset_type": "stock", "name": "Apple"}])
    payload = build_identity_reconciliation(
        history,
        current,
        repo_root=tmp_path,
        snapshot_overrides=snapshot_overrides(),
    )
    seeds = [row for row in payload["pit_alias_seeds"] if row["identifier_value"] == "AAPL"]
    assert seeds
    assert any(row["valid_from"] == "2026-03-08" for row in seeds)
    assert all(row["pit_verified"] is True for row in seeds)
    assert all(row["candidate_only"] is True for row in seeds)
    assert all(row["promotion_requires_interval_review"] is True for row in seeds)
