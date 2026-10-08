from __future__ import annotations

import json
from pathlib import Path

import pytest

from scanner.research.governance.qm_b_market_tradability import (
    MarketTradabilityError,
    audit_membership_snapshot,
    evaluate_market_tradability,
)


def _contract(tmp_path: Path) -> Path:
    payload = json.loads(Path("configs/qm_b_market_tradability_v1.json").read_text(encoding="utf-8"))
    payload["registered_sources"] = ["test_feed"]
    path = tmp_path / "contract.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _row(status: str, *, venue: str = "XTEST", start: str = "2026-09-30T10:00:00+00:00", source: str = "test_feed") -> dict:
    return {
        "instrument_id": "urn:scanner:isin:US0378331005",
        "venue_id": venue,
        "status": status,
        "valid_from": start,
        "pit_verified": True,
        "source_id": source,
    }


def test_missing_evidence_is_unknown(tmp_path: Path) -> None:
    result = evaluate_market_tradability(
        instrument_id="urn:scanner:isin:US0378331005",
        as_of="2026-09-30T12:00:00+00:00",
        evidence_records=[],
        contract_path=_contract(tmp_path),
    )
    assert result["market_tradability_status"] == "UNKNOWN"
    assert result["strict_dimension_positive"] is False


def test_unregistered_source_fails_closed(tmp_path: Path) -> None:
    with pytest.raises(MarketTradabilityError, match="evidence_source_not_registered"):
        evaluate_market_tradability(
            instrument_id="urn:scanner:isin:US0378331005",
            as_of="2026-09-30T12:00:00+00:00",
            evidence_records=[_row("TRADABLE", source="unknown_feed")],
            contract_path=_contract(tmp_path),
        )


def test_future_evidence_cannot_back_project(tmp_path: Path) -> None:
    result = evaluate_market_tradability(
        instrument_id="urn:scanner:isin:US0378331005",
        as_of="2026-09-30T09:00:00+00:00",
        evidence_records=[_row("TRADABLE", start="2026-09-30T10:00:00+00:00")],
        contract_path=_contract(tmp_path),
    )
    assert result["market_tradability_status"] == "UNKNOWN"


def test_any_tradable_venue_is_positive(tmp_path: Path) -> None:
    rows = [_row("SUSPENDED", venue="XONE"), _row("TRADABLE", venue="XTWO")]
    result = evaluate_market_tradability(
        instrument_id="urn:scanner:isin:US0378331005",
        as_of="2026-09-30T12:00:00+00:00",
        evidence_records=rows,
        contract_path=_contract(tmp_path),
    )
    assert result["market_tradability_status"] == "TRADABLE"
    assert result["strict_dimension_positive"] is True


def test_suspended_when_no_tradable_and_any_suspended(tmp_path: Path) -> None:
    rows = [_row("NOT_TRADABLE", venue="XONE"), _row("SUSPENDED", venue="XTWO")]
    result = evaluate_market_tradability(
        instrument_id="urn:scanner:isin:US0378331005",
        as_of="2026-09-30T12:00:00+00:00",
        evidence_records=rows,
        contract_path=_contract(tmp_path),
    )
    assert result["market_tradability_status"] == "SUSPENDED"


def test_all_observed_venues_not_tradable_is_negative(tmp_path: Path) -> None:
    rows = [_row("NOT_TRADABLE", venue="XONE"), _row("NOT_TRADABLE", venue="XTWO")]
    result = evaluate_market_tradability(
        instrument_id="urn:scanner:isin:US0378331005",
        as_of="2026-09-30T12:00:00+00:00",
        evidence_records=rows,
        contract_path=_contract(tmp_path),
    )
    assert result["market_tradability_status"] == "NOT_TRADABLE"


def test_same_venue_latest_conflict_is_unknown(tmp_path: Path) -> None:
    rows = [_row("TRADABLE"), _row("SUSPENDED")]
    result = evaluate_market_tradability(
        instrument_id="urn:scanner:isin:US0378331005",
        as_of="2026-09-30T12:00:00+00:00",
        evidence_records=rows,
        contract_path=_contract(tmp_path),
    )
    assert result["market_tradability_status"] == "UNKNOWN"
    assert result["venues"]["XTEST"]["reason_codes"][0] == "LATEST_SAME_VENUE_CONFLICT"


def test_non_pit_record_is_not_positive(tmp_path: Path) -> None:
    row = _row("TRADABLE")
    row["pit_verified"] = False
    result = evaluate_market_tradability(
        instrument_id="urn:scanner:isin:US0378331005",
        as_of="2026-09-30T12:00:00+00:00",
        evidence_records=[row],
        contract_path=_contract(tmp_path),
    )
    assert result["market_tradability_status"] == "UNKNOWN"


def test_current_membership_audit_remains_unknown_without_registered_sources() -> None:
    latest = json.loads(Path("artifacts/research/qm/qm_b_membership/latest.json").read_text(encoding="utf-8"))
    snapshot = json.loads(Path(latest["normalized_snapshot_path"]).read_text(encoding="utf-8"))
    expected = len(snapshot["claims"])
    assert expected == snapshot["stable_instrument_claim_count"]
    result = audit_membership_snapshot(snapshot)
    assert result["instrument_count"] == expected
    assert result["registered_source_count"] == 0
    assert result["status_counts"] == {"UNKNOWN": expected}
    assert result["positive_tradability_count"] == 0
    assert result["current_gap_status"] == "BLOCKED_NO_MARKET_TRADABILITY_EVIDENCE"
