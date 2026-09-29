from __future__ import annotations

import csv
from pathlib import Path

from scanner.research.governance.qm_b_crypto_identifier import (
    build_crypto_identifier_timeline,
    parse_crypto_identifier,
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


def test_crypto_identifier_parser_is_conservative():
    assert parse_crypto_identifier("CRYPTO:BTC") == {"base": "BTC", "style": "INTERNAL_BASE", "quote": ""}
    assert parse_crypto_identifier("btc-usd") == {"base": "BTC", "style": "QUOTE_PAIR", "quote": "USD"}
    assert parse_crypto_identifier("ETH-EUR") == {"base": "ETH", "style": "QUOTE_PAIR", "quote": "EUR"}
    assert parse_crypto_identifier("SOL-USDT") == {"base": "SOL", "style": "QUOTE_PAIR", "quote": "USDT"}
    assert parse_crypto_identifier("BTC-GBP") is None
    assert parse_crypto_identifier("AAPL") is None
    assert parse_crypto_identifier("CRYPTO:") is None


def test_timeline_uses_scanner_observations_only_and_never_verifies_identity(tmp_path: Path):
    history = tmp_path / "history.csv"
    current = tmp_path / "universe.csv"
    write_csv(
        history,
        [
            {
                "date": "2026-02-10",
                "symbol": "BTC-EUR",
                "name": "Bitcoin",
                "currency": "EUR",
                "observation_type": "observed_scanner",
                "data_source": "scanner_run",
            },
            {
                "date": "2026-03-01",
                "symbol": "CRYPTO:BTC",
                "name": "Bitcoin",
                "currency": "USD",
                "observation_type": "observed_scanner",
                "data_source": "scanner_run",
            },
            {
                "date": "2026-02-01",
                "symbol": "BTC-USD",
                "name": "Bitcoin price backfill",
                "currency": "USD",
                "observation_type": "market_data",
                "data_source": "yahoo_ohlcv",
            },
        ],
    )
    write_csv(
        current,
        [
            {
                "active": "1",
                "symbol": "BTC-USD",
                "asset_type": "crypto",
            },
            {
                "active": "1",
                "symbol": "ETH-USD",
                "asset_type": "crypto",
            },
        ],
    )
    payload = build_crypto_identifier_timeline(history, current_universe_path=current)
    btc = next(row for row in payload["bases"] if row["base"] == "BTC")
    assert btc["historical_identifiers"] == ["BTC-EUR", "CRYPTO:BTC"]
    assert btc["current_crypto_symbols"] == ["BTC-USD"]
    assert btc["quote_pair_last_seen"] == "2026-02-10"
    assert btc["internal_identifier_first_seen"] == "2026-03-01"
    assert btc["lineage_status"] == "CANDIDATE"
    assert btc["stable_identity_verified"] is False
    assert btc["quote_pair_equivalence_verified"] is False
    assert payload["scanner_crypto_row_count"] == 2
    assert payload["stable_identity_verified"] is False
    assert payload["base_token_is_identity"] is False


def test_overlap_is_reported_not_silently_collapsed(tmp_path: Path):
    history = tmp_path / "history.csv"
    write_csv(
        history,
        [
            {
                "date": "2026-03-01",
                "symbol": "ETH-EUR",
                "observation_type": "observed_scanner",
                "data_source": "scanner_run",
            },
            {
                "date": "2026-03-01",
                "symbol": "CRYPTO:ETH",
                "observation_type": "observed_scanner",
                "data_source": "scanner_run",
            },
        ],
    )
    payload = build_crypto_identifier_timeline(history)
    eth = payload["bases"][0]
    assert eth["base"] == "ETH"
    assert eth["overlap_date_count"] == 1
    assert eth["overlap_dates"] == ["2026-03-01"]
    assert eth["lineage_status"] == "UNRESOLVED"


def test_current_non_crypto_pair_does_not_become_crypto_candidate(tmp_path: Path):
    history = tmp_path / "history.csv"
    current = tmp_path / "universe.csv"
    write_csv(
        history,
        [
            {
                "date": "2026-03-01",
                "symbol": "ADA-EUR",
                "observation_type": "observed_scanner",
                "data_source": "scanner_run",
            }
        ],
    )
    write_csv(
        current,
        [
            {"active": "1", "symbol": "ADA-USD", "asset_type": "stock"},
            {"active": "0", "symbol": "ADA-USD", "asset_type": "crypto"},
        ],
    )
    payload = build_crypto_identifier_timeline(history, current_universe_path=current)
    ada = payload["bases"][0]
    assert ada["current_crypto_symbols"] == []
    assert ada["lineage_status"] == "UNRESOLVED"
