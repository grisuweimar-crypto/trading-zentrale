from __future__ import annotations

import pytest

from scanner.research.external_evidence.sec_insider_identity import (
    SecInsiderIdentityError,
    resolve_insider_asof_identity,
)


def _row(
    *,
    cik: str,
    symbol: str,
    accession: str,
    valid_from: str,
    strict_pit: bool = True,
) -> dict:
    return {
        "issuer_cik": cik,
        "issuer_trading_symbol_reported": symbol,
        "accession_number": accession,
        "valid_from": valid_from,
        "pit_status": "SAFE",
        "strict_pit_eligible": strict_pit,
    }


def _evidence(rows: list[dict]) -> dict:
    return {
        "schema_version": "external_evidence_8d_sec_insider_bulk_v1",
        "source_quarter": "2026Q2",
        "rows": rows,
        "guards": {"market_outcomes_read": False},
    }


def test_b5_does_not_retroject_future_symbol_cik_evidence() -> None:
    evidence = _evidence(
        [
            _row(
                cik="0000000001",
                symbol="AAA",
                accession="0000000001-26-000001",
                valid_from="2026-05-10T16:00:00-04:00",
            )
        ]
    )
    payload = resolve_insider_asof_identity(
        scanner_observations=[
            {"symbol": "AAA", "as_of": "2026-05-09T20:00:00+00:00"},
            {"symbol": "AAA", "as_of": "2026-05-11T20:00:00+00:00"},
        ],
        evidence_payloads=[evidence],
    )
    first, second = payload["rows"]
    assert first["identity_status"] == "UNKNOWN_NO_PIT_IDENTITY"
    assert first["issuer_cik"] is None
    assert second["identity_status"] == "VERIFIED_FAMILY_PIT_IDENTITY"
    assert second["issuer_cik"] == "0000000001"
    assert payload["guards"]["future_identity_evidence_used"] is False
    assert payload["guards"]["current_ticker_retrojection_enabled"] is False


def test_b5_symbol_change_stops_old_symbol_after_new_evidence() -> None:
    evidence = _evidence(
        [
            _row(
                cik="0000000001",
                symbol="OLD",
                accession="0000000001-26-000001",
                valid_from="2026-04-10T16:00:00-04:00",
            ),
            _row(
                cik="0000000001",
                symbol="NEW",
                accession="0000000001-26-000002",
                valid_from="2026-06-01T16:00:00-04:00",
            ),
        ]
    )
    payload = resolve_insider_asof_identity(
        scanner_observations=[
            {"symbol": "OLD", "as_of": "2026-05-01T20:00:00+00:00"},
            {"symbol": "OLD", "as_of": "2026-06-02T20:00:00+00:00"},
            {"symbol": "NEW", "as_of": "2026-06-02T20:00:00+00:00"},
        ],
        evidence_payloads=[evidence],
    )
    by_key = {(row["as_of_date"], row["symbol"]): row for row in payload["rows"]}
    assert by_key[("2026-05-01", "OLD")]["issuer_cik"] == "0000000001"
    assert by_key[("2026-06-02", "OLD")]["identity_status"] == "UNKNOWN_NO_PIT_IDENTITY"
    assert by_key[("2026-06-02", "NEW")]["issuer_cik"] == "0000000001"


def test_b5_multiple_ciks_for_same_latest_symbol_are_ambiguous() -> None:
    evidence = _evidence(
        [
            _row(
                cik="0000000001",
                symbol="DUP",
                accession="0000000001-26-000001",
                valid_from="2026-05-01T16:00:00-04:00",
            ),
            _row(
                cik="0000000002",
                symbol="DUP",
                accession="0000000002-26-000001",
                valid_from="2026-05-02T16:00:00-04:00",
            ),
        ]
    )
    payload = resolve_insider_asof_identity(
        scanner_observations=[{"symbol": "DUP", "as_of": "2026-05-03T20:00:00+00:00"}],
        evidence_payloads=[evidence],
    )
    row = payload["rows"][0]
    assert row["identity_status"] == "AMBIGUOUS_PIT_IDENTITY"
    assert row["issuer_cik"] is None
    assert row["source_coverage_status"] == "CONFLICTING_SOURCES"


def test_b5_same_cik_same_timestamp_conflicting_symbols_is_ambiguous() -> None:
    evidence = _evidence(
        [
            _row(
                cik="0000000001",
                symbol="AAA",
                accession="0000000001-26-000001",
                valid_from="2026-05-01T16:00:00-04:00",
            ),
            _row(
                cik="0000000001",
                symbol="BBB",
                accession="0000000001-26-000002",
                valid_from="2026-05-01T16:00:00-04:00",
            ),
        ]
    )
    payload = resolve_insider_asof_identity(
        scanner_observations=[{"symbol": "AAA", "as_of": "2026-05-02T20:00:00+00:00"}],
        evidence_payloads=[evidence],
    )
    assert payload["rows"][0]["identity_status"] == "AMBIGUOUS_PIT_IDENTITY"


def test_b5_rejects_date_only_asof_instead_of_inventing_intraday_time() -> None:
    evidence = _evidence(
        [
            _row(
                cik="0000000001",
                symbol="AAA",
                accession="0000000001-26-000001",
                valid_from="2026-05-01T16:00:00-04:00",
            )
        ]
    )
    with pytest.raises(SecInsiderIdentityError, match="timezone-aware"):
        resolve_insider_asof_identity(
            scanner_observations=[{"symbol": "AAA", "as_of": "2026-05-02"}],
            evidence_payloads=[evidence],
        )


def test_b5_ignores_non_strict_pit_identity_evidence() -> None:
    evidence = _evidence(
        [
            _row(
                cik="0000000001",
                symbol="AAA",
                accession="0000000001-26-000001",
                valid_from="2026-05-01T16:00:00-04:00",
                strict_pit=False,
            )
        ]
    )
    payload = resolve_insider_asof_identity(
        scanner_observations=[{"symbol": "AAA", "as_of": "2026-05-02T20:00:00+00:00"}],
        evidence_payloads=[evidence],
    )
    assert payload["identity_evidence_point_count"] == 0
    assert payload["rows"][0]["identity_status"] == "UNKNOWN_NO_PIT_IDENTITY"
