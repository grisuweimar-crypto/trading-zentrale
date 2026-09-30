from __future__ import annotations

from scanner.research.governance.qm_b_prospective_identity_match import (
    ProspectiveIdentityMatchError,
    match_listing_snapshot,
)


def snapshot(records):
    return {
        "snapshot_id": "qmbls_demo",
        "source_id": "nasdaq_symbol_directory",
        "valid_from": "2026-09-30T03:00:00+00:00",
        "historical_retrojection_permitted": False,
        "records": records,
    }


def test_unique_exact_symbol_with_valid_isin_matches():
    result = match_listing_snapshot(
        snapshot([{"source_symbol": "AAPL"}]),
        universe_rows=[
            {
                "active": "1",
                "symbol": "AAPL",
                "isin": "US0378331005",
                "asset_type": "stock",
                "name": "Apple Inc.",
            }
        ],
        universe_snapshot_id="u1",
        universe_observed_at="2026-09-30T03:05:00+00:00",
        universe_sha256="a" * 64,
    )
    row = result["matches"][0]
    assert row["match_status"] == "MATCHED"
    assert row["candidate_instrument_id"] == "urn:scanner:isin:US0378331005"
    assert row["identity_valid_from"] == "2026-09-30T03:05:00+00:00"
    assert row["historical_membership_verified"] is False
    assert row["tradability_verified"] is False
    assert row["project_investability_verified"] is False


def test_match_valid_from_never_precedes_later_input():
    result = match_listing_snapshot(
        snapshot([{"source_symbol": "AAPL"}]),
        universe_rows=[{"active": "1", "symbol": "AAPL", "isin": "US0378331005", "asset_type": "stock"}],
        universe_snapshot_id="u1",
        universe_observed_at="2026-09-30T02:00:00+00:00",
        universe_sha256="b" * 64,
    )
    assert result["identity_match_valid_from"] == "2026-09-30T03:00:00+00:00"


def test_no_symbol_normalization_or_suffix_guessing():
    result = match_listing_snapshot(
        snapshot([{"source_symbol": "BRK.B"}]),
        universe_rows=[{"active": "1", "symbol": "BRK-B", "isin": "US0846707026", "asset_type": "stock"}],
        universe_snapshot_id="u1",
        universe_observed_at="2026-09-30T03:00:00+00:00",
        universe_sha256="c" * 64,
    )
    assert result["matches"][0]["match_status"] == "UNMATCHED"


def test_source_provided_alias_may_match_exactly():
    result = match_listing_snapshot(
        snapshot([{"source_symbol": "BRK.B", "nasdaq_symbol": "BRK-B"}]),
        universe_rows=[{"active": "1", "symbol": "BRK-B", "isin": "US0846707026", "asset_type": "stock"}],
        universe_snapshot_id="u1",
        universe_observed_at="2026-09-30T03:00:00+00:00",
        universe_sha256="d" * 64,
    )
    assert result["matches"][0]["match_status"] == "MATCHED"


def test_multiple_distinct_isins_fail_ambiguous():
    result = match_listing_snapshot(
        snapshot([{"source_symbol": "ABC"}]),
        universe_rows=[
            {"active": "1", "symbol": "ABC", "isin": "US0378331005", "asset_type": "stock"},
            {"active": "1", "symbol": "ABC", "isin": "US0846707026", "asset_type": "stock"},
        ],
        universe_snapshot_id="u1",
        universe_observed_at="2026-09-30T03:00:00+00:00",
        universe_sha256="e" * 64,
    )
    assert result["matches"][0]["match_status"] == "AMBIGUOUS"
    assert result["matches"][0]["candidate_instrument_id"] is None


def test_duplicate_same_isin_requires_review():
    result = match_listing_snapshot(
        snapshot([{"source_symbol": "AAPL"}]),
        universe_rows=[
            {"active": "1", "symbol": "AAPL", "isin": "US0378331005", "asset_type": "stock"},
            {"active": "1", "symbol": "AAPL", "isin": "US0378331005", "asset_type": "stock"},
        ],
        universe_snapshot_id="u1",
        universe_observed_at="2026-09-30T03:00:00+00:00",
        universe_sha256="f" * 64,
    )
    row = result["matches"][0]
    assert row["match_status"] == "REVIEW_REQUIRED_DUPLICATE_MASTER_ROWS"
    assert row["candidate_instrument_id"] is None


def test_invalid_or_missing_isin_does_not_promote():
    result = match_listing_snapshot(
        snapshot([{"source_symbol": "ABC"}]),
        universe_rows=[{"active": "1", "symbol": "ABC", "isin": "NOTANISIN", "asset_type": "stock"}],
        universe_snapshot_id="u1",
        universe_observed_at="2026-09-30T03:00:00+00:00",
        universe_sha256="1" * 64,
    )
    assert result["matches"][0]["match_status"] == "UNMATCHED"


def test_inactive_or_crypto_master_rows_are_not_auto_matched():
    result = match_listing_snapshot(
        snapshot([{"source_symbol": "BTC-USD"}, {"source_symbol": "OLD"}]),
        universe_rows=[
            {"active": "1", "symbol": "BTC-USD", "isin": "", "asset_type": "crypto"},
            {"active": "0", "symbol": "OLD", "isin": "US0378331005", "asset_type": "stock"},
        ],
        universe_snapshot_id="u1",
        universe_observed_at="2026-09-30T03:00:00+00:00",
        universe_sha256="2" * 64,
    )
    assert [row["match_status"] for row in result["matches"]] == ["UNMATCHED", "UNMATCHED"]


def test_timezone_is_required():
    try:
        match_listing_snapshot(
            snapshot([{"source_symbol": "AAPL"}]),
            universe_rows=[{"active": "1", "symbol": "AAPL", "isin": "US0378331005", "asset_type": "stock"}],
            universe_snapshot_id="u1",
            universe_observed_at="2026-09-30T03:00:00",
            universe_sha256="3" * 64,
        )
    except ProspectiveIdentityMatchError as exc:
        assert "timezone_required" in str(exc)
    else:
        raise AssertionError("timezone-less universe observation must fail")


def test_retrojection_guard_is_required():
    bad = snapshot([{"source_symbol": "AAPL"}])
    bad["historical_retrojection_permitted"] = True
    try:
        match_listing_snapshot(
            bad,
            universe_rows=[{"active": "1", "symbol": "AAPL", "isin": "US0378331005", "asset_type": "stock"}],
            universe_snapshot_id="u1",
            universe_observed_at="2026-09-30T03:00:00+00:00",
            universe_sha256="4" * 64,
        )
    except ProspectiveIdentityMatchError as exc:
        assert "retrojection_guard" in str(exc)
    else:
        raise AssertionError("unsafe snapshot must fail")
