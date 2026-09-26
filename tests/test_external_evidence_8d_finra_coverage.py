from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.finra_short_interest_coverage import (
    FinraShortInterestCoverageError,
    audit_finra_current_universe,
)


def _scanner(path: Path, rows: list[dict[str, str]]) -> Path:
    fieldnames = ["date", "symbol", "name", "currency", "as_of", "snapshot_id"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _finra_snapshot(path: Path, rows: list[dict], *, strict_pit: bool = True) -> Path:
    payload = {
        "schema_version": "external_evidence_8d_finra_short_interest_snapshot_v1",
        "published_at": "2026-09-24T20:40:00Z",
        "valid_from": "2026-09-26T10:00:00Z",
        "source_mode": "PROSPECTIVE_PUBLICATION_SNAPSHOT",
        "strict_pit_eligible": strict_pit,
        "rows": rows,
        "guards": {
            "market_outcomes_read": False,
            "market_direction_assigned": False,
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _finra_row(symbol: str, *, market: str = "N", settlement: str = "2026-09-15") -> dict:
    return {
        "symbol": symbol,
        "settlement_date": settlement,
        "market_class_code": market,
        "published_at": "2026-09-24T20:40:00Z",
        "valid_from": "2026-09-26T10:00:00Z",
        "strict_pit_eligible": True,
        "vintage_status": "PROSPECTIVE_SNAPSHOT_OBSERVED_AT_INGESTION",
        "revision_flag": None,
        "stock_split_flag": None,
        "feature_status": "KNOWN",
        "short_position_quantity": 1000,
        "short_position_change_quantity": 100,
        "short_position_change_percent": 11.11,
        "days_to_cover": 2.5,
        "market_direction": "UNASSIGNED",
    }


def test_coverage_uses_exact_symbol_only_and_keeps_missing_unknown(tmp_path: Path) -> None:
    scanner = _scanner(
        tmp_path / "scanner.csv",
        [
            {
                "date": "2026-09-25",
                "symbol": "AAPL",
                "name": "Apple",
                "currency": "USD",
                "as_of": "2026-09-25",
                "snapshot_id": "snap-1",
            },
            {
                "date": "2026-09-25",
                "symbol": "BMW.DE",
                "name": "BMW",
                "currency": "EUR",
                "as_of": "2026-09-25",
                "snapshot_id": "snap-1",
            },
        ],
    )
    finra = _finra_snapshot(tmp_path / "finra.json", [_finra_row("AAPL")])
    result = audit_finra_current_universe(scanner_path=scanner, finra_snapshot_path=finra)

    assert result["scanner_symbol_count"] == 2
    assert result["exact_known_symbol_count"] == 1
    assert result["exact_known_symbol_fraction"] == 0.5
    by_symbol = {row["symbol"]: row for row in result["rows"]}
    assert by_symbol["AAPL"]["coverage_status"] == "KNOWN_EXACT_SYMBOL"
    assert by_symbol["BMW.DE"]["coverage_status"] == "UNKNOWN_NOT_IN_FINRA_SNAPSHOT"
    assert by_symbol["BMW.DE"]["short_position_quantity"] is None
    assert result["guards"]["ticker_suffix_stripping_enabled"] is False
    assert result["guards"]["adr_substitution_enabled"] is False
    assert result["guards"]["missing_evidence_defaults_to_neutral"] is False
    assert result["guards"]["market_outcomes_read"] is False


def test_coverage_refuses_to_choose_between_multiple_exact_finra_rows(tmp_path: Path) -> None:
    scanner = _scanner(
        tmp_path / "scanner.csv",
        [
            {
                "date": "2026-09-25",
                "symbol": "XYZ",
                "name": "XYZ Inc",
                "currency": "USD",
                "as_of": "2026-09-25",
                "snapshot_id": "snap-1",
            }
        ],
    )
    finra = _finra_snapshot(
        tmp_path / "finra.json",
        [_finra_row("XYZ", market="N"), _finra_row("XYZ", market="O")],
    )
    result = audit_finra_current_universe(scanner_path=scanner, finra_snapshot_path=finra)
    row = result["rows"][0]
    assert row["coverage_status"] == "AMBIGUOUS_MULTIPLE_FINRA_ROWS"
    assert row["short_position_quantity"] is None
    assert row["candidate_market_class_codes"] == ["N", "O"]
    assert result["guards"]["ambiguous_market_class_auto_selection_enabled"] is False


def test_coverage_rejects_multi_period_finra_input(tmp_path: Path) -> None:
    scanner = _scanner(
        tmp_path / "scanner.csv",
        [
            {
                "date": "2026-09-25",
                "symbol": "AAPL",
                "name": "Apple",
                "currency": "USD",
                "as_of": "2026-09-25",
                "snapshot_id": "snap-1",
            }
        ],
    )
    finra = _finra_snapshot(
        tmp_path / "finra.json",
        [
            _finra_row("AAPL", settlement="2026-09-15"),
            _finra_row("MSFT", settlement="2026-08-31"),
        ],
    )
    with pytest.raises(FinraShortInterestCoverageError, match="exactly one FINRA settlement date"):
        audit_finra_current_universe(scanner_path=scanner, finra_snapshot_path=finra)


def test_coverage_rejects_duplicate_scanner_symbols(tmp_path: Path) -> None:
    scanner = _scanner(
        tmp_path / "scanner.csv",
        [
            {
                "date": "2026-09-25",
                "symbol": "AAPL",
                "name": "Apple",
                "currency": "USD",
                "as_of": "2026-09-25",
                "snapshot_id": "snap-1",
            },
            {
                "date": "2026-09-25",
                "symbol": "AAPL",
                "name": "Apple duplicate",
                "currency": "USD",
                "as_of": "2026-09-25",
                "snapshot_id": "snap-1",
            },
        ],
    )
    finra = _finra_snapshot(tmp_path / "finra.json", [_finra_row("AAPL")])
    with pytest.raises(FinraShortInterestCoverageError, match="duplicate symbols"):
        audit_finra_current_universe(scanner_path=scanner, finra_snapshot_path=finra)
