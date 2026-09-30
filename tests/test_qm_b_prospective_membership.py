from __future__ import annotations

import json
from pathlib import Path

import pytest

from scanner.research.governance.qm_b_prospective_membership import (
    ProspectiveMembershipError,
    append_membership_snapshot,
    build_membership_snapshot,
    membership_as_of,
    verify_membership_ledger,
)


APPLE_ISIN = "US0378331005"
TESLA_ISIN = "US88160R1014"
SHA_A = "a" * 64
SHA_B = "b" * 64


def row(*, active: str, symbol: str, isin: str, asset_type: str = "stock", name: str = "") -> dict[str, str]:
    return {
        "active": active,
        "symbol": symbol,
        "isin": isin,
        "asset_type": asset_type,
        "name": name,
        "country": "United States",
        "currency": "USD",
    }


def snapshot(rows, *, observed_at="2026-09-30T05:00:00Z", snapshot_id="universe_test_1", digest=SHA_A):
    return build_membership_snapshot(
        rows,
        universe_snapshot_id=snapshot_id,
        universe_observed_at=observed_at,
        universe_sha256=digest,
    )


def test_active_supported_security_becomes_positive_membership_evidence():
    payload = snapshot([row(active="1", symbol="AAPL", isin=APPLE_ISIN, name="Apple")])
    assert payload["stable_instrument_claim_count"] == 1
    claim = payload["claims"][0]
    assert claim["instrument_id"] == f"urn:scanner:isin:{APPLE_ISIN}"
    assert claim["membership_observation_status"] == "OBSERVED_IN_PROJECT_UNIVERSE"
    assert claim["positive_project_membership_verified"] is True
    assert claim["negative_out_of_scope_verified"] is False
    assert claim["membership_valid_from"] == "2026-09-30T05:00:00+00:00"
    assert claim["listing_status"] == "UNKNOWN"
    assert claim["tradability_status"] == "UNKNOWN"
    assert claim["project_investability_status"] == "UNKNOWN"
    assert payload["absence_interpreted_as_out_of_scope"] is False


def test_inactive_only_rows_are_not_promoted_to_out_of_scope():
    payload = snapshot([row(active="0", symbol="AAPL", isin=APPLE_ISIN)])
    claim = payload["claims"][0]
    assert claim["membership_observation_status"] == "EXPLICIT_INACTIVE_ROWS_ONLY"
    assert claim["positive_project_membership_verified"] is False
    assert claim["negative_out_of_scope_verified"] is False
    assert payload["negative_membership_promotion_performed"] is False


def test_duplicate_rows_same_isin_are_aggregated_and_flagged():
    payload = snapshot(
        [
            row(active="1", symbol="AAPL", isin=APPLE_ISIN),
            row(active="1", symbol="AAPL", isin=APPLE_ISIN),
            row(active="0", symbol="AAPL.OLD", isin=APPLE_ISIN),
        ]
    )
    assert len(payload["claims"]) == 1
    claim = payload["claims"][0]
    assert claim["active_row_count"] == 2
    assert claim["inactive_row_count"] == 1
    assert "MULTIPLE_ACTIVE_ROWS_SAME_ISIN" in claim["quality_flags"]
    assert "MULTIPLE_SYMBOLS_SAME_ISIN" in claim["quality_flags"]
    assert "MIXED_ACTIVE_AND_INACTIVE_ROWS_SAME_ISIN" in claim["quality_flags"]
    assert claim["membership_observation_status"] == "OBSERVED_IN_PROJECT_UNIVERSE"


def test_crypto_missing_isin_and_invalid_active_remain_unresolved():
    payload = snapshot(
        [
            row(active="1", symbol="BTC-USD", isin="", asset_type="crypto"),
            row(active="1", symbol="NOISIN", isin="", asset_type="stock"),
            row(active="maybe", symbol="TSLA", isin=TESLA_ISIN),
        ]
    )
    assert payload["stable_instrument_claim_count"] == 0
    assert payload["unresolved_status_counts"] == {
        "IDENTITY_UNRESOLVED_INVALID_OR_MISSING_ISIN": 1,
        "INVALID_ACTIVE_FLAG": 1,
        "UNSUPPORTED_ASSET_TYPE": 1,
    }


def test_observation_time_must_be_timezone_aware():
    with pytest.raises(ProspectiveMembershipError, match="timestamp_timezone_required"):
        snapshot([row(active="1", symbol="AAPL", isin=APPLE_ISIN)], observed_at="2026-09-30T05:00:00")


def test_ledger_append_verify_and_duplicate_rejection(tmp_path: Path):
    ledger = tmp_path / "membership.jsonl"
    first = snapshot([row(active="1", symbol="AAPL", isin=APPLE_ISIN)])
    state = append_membership_snapshot(ledger, first)
    assert state["valid"] is True
    assert state["event_count"] == 1
    assert len(state["head_hash"]) == 64
    with pytest.raises(ProspectiveMembershipError, match="duplicate_membership_snapshot"):
        append_membership_snapshot(ledger, first)


def test_ledger_detects_tampering(tmp_path: Path):
    ledger = tmp_path / "membership.jsonl"
    first = snapshot([row(active="1", symbol="AAPL", isin=APPLE_ISIN)])
    append_membership_snapshot(ledger, first)
    event = json.loads(ledger.read_text(encoding="utf-8"))
    event["snapshot"]["claims"][0]["active_symbols"] = ["FAKE"]
    ledger.write_text(json.dumps(event) + "\n", encoding="utf-8")
    with pytest.raises(ProspectiveMembershipError, match="ledger_event_hash_invalid"):
        verify_membership_ledger(ledger)


def test_as_of_uses_latest_snapshot_only_and_never_carries_positive_membership(tmp_path: Path):
    ledger = tmp_path / "membership.jsonl"
    first = snapshot(
        [row(active="1", symbol="AAPL", isin=APPLE_ISIN)],
        observed_at="2026-09-30T05:00:00Z",
        snapshot_id="u1",
        digest=SHA_A,
    )
    second = snapshot(
        [row(active="1", symbol="TSLA", isin=TESLA_ISIN)],
        observed_at="2026-10-01T05:00:00Z",
        snapshot_id="u2",
        digest=SHA_B,
    )
    append_membership_snapshot(ledger, first)
    append_membership_snapshot(ledger, second)

    apple = membership_as_of(
        ledger,
        instrument_id=f"urn:scanner:isin:{APPLE_ISIN}",
        as_of="2026-10-01T06:00:00Z",
    )
    assert apple["status"] == "UNKNOWN_ABSENT_FROM_LATEST_SNAPSHOT"
    assert apple["out_of_scope_verified"] is False
    assert apple["positive_membership_carried_forward"] is False
    assert apple["prior_positive_evidence"]["observed_at"] == "2026-09-30T05:00:00+00:00"

    tesla = membership_as_of(
        ledger,
        instrument_id=f"urn:scanner:isin:{TESLA_ISIN}",
        as_of="2026-10-01T06:00:00Z",
    )
    assert tesla["status"] == "OBSERVED_IN_PROJECT_UNIVERSE"
    assert tesla["out_of_scope_verified"] is False


def test_as_of_before_first_snapshot_is_unknown(tmp_path: Path):
    ledger = tmp_path / "membership.jsonl"
    first = snapshot([row(active="1", symbol="AAPL", isin=APPLE_ISIN)])
    append_membership_snapshot(ledger, first)
    result = membership_as_of(
        ledger,
        instrument_id=f"urn:scanner:isin:{APPLE_ISIN}",
        as_of="2026-09-29T23:59:59Z",
    )
    assert result["status"] == "UNKNOWN_NO_SNAPSHOT_AT_OR_BEFORE_QUERY"
    assert result["out_of_scope_verified"] is False
