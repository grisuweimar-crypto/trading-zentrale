from __future__ import annotations

import json
from pathlib import Path

import pytest

from scanner.research.governance.qm_b_project_investability import (
    ProjectInvestabilityError,
    audit_membership_snapshot,
    evaluate_project_investability,
)


def rec(instrument_id: str, dimension: str, status: str, valid_from: str, *, pit: bool = True, source: str = "test"):
    return {
        "instrument_id": instrument_id,
        "dimension": dimension,
        "status": status,
        "valid_from": valid_from,
        "pit_verified": pit,
        "source_id": source,
    }


def all_positive(iid: str, t: str):
    return [
        rec(iid, "stable_identity", "VERIFIED", t),
        rec(iid, "project_membership", "OBSERVED_IN_PROJECT_UNIVERSE", t),
        rec(iid, "listing_state", "LISTED", t),
        rec(iid, "market_tradability", "TRADABLE", t),
        rec(iid, "execution_channel", "AVAILABLE", t),
        rec(iid, "project_restrictions", "CLEAR", t),
    ]


def test_all_required_dimensions_produce_investable_at_latest_boundary():
    iid = "urn:scanner:isin:US0378331005"
    rows = all_positive(iid, "2026-09-30T06:00:00Z")
    rows[0]["valid_from"] = "2026-09-30T05:00:00Z"
    rows[1]["valid_from"] = "2026-09-30T05:15:00Z"
    rows[2]["valid_from"] = "2026-09-30T05:30:00Z"
    result = evaluate_project_investability(instrument_id=iid, as_of="2026-09-30T07:00:00Z", evidence_records=rows)
    assert result["project_investability_status"] == "INVESTABLE"
    assert result["strict_universe_promotion_ready"] is True
    assert result["investability_valid_from"] == "2026-09-30T06:00:00+00:00"


def test_missing_dimension_fails_closed_to_unknown():
    iid = "urn:scanner:isin:US0378331005"
    rows = [row for row in all_positive(iid, "2026-09-30T06:00:00Z") if row["dimension"] != "execution_channel"]
    result = evaluate_project_investability(instrument_id=iid, as_of="2026-09-30T07:00:00Z", evidence_records=rows)
    assert result["project_investability_status"] == "UNKNOWN"
    assert result["strict_universe_promotion_ready"] is False
    assert result["dimensions"]["execution_channel"]["resolved_status"] == "UNKNOWN"


def test_hard_negative_is_not_investable_even_with_other_missing_dimensions():
    iid = "urn:scanner:isin:US0378331005"
    rows = [rec(iid, "market_tradability", "NOT_TRADABLE", "2026-09-30T06:00:00Z")]
    result = evaluate_project_investability(instrument_id=iid, as_of="2026-09-30T07:00:00Z", evidence_records=rows)
    assert result["project_investability_status"] == "NOT_INVESTABLE"
    assert "HARD_NEGATIVE:market_tradability" in result["reason_codes"]


def test_restriction_is_preserved_distinctly():
    iid = "urn:scanner:isin:US0378331005"
    rows = all_positive(iid, "2026-09-30T06:00:00Z")
    rows = [row for row in rows if row["dimension"] != "execution_channel"]
    rows.append(rec(iid, "execution_channel", "RESTRICTED", "2026-09-30T06:00:00Z"))
    result = evaluate_project_investability(instrument_id=iid, as_of="2026-09-30T07:00:00Z", evidence_records=rows)
    assert result["project_investability_status"] == "RESTRICTED"
    assert result["strict_universe_promotion_ready"] is False


def test_non_pit_evidence_is_ignored():
    iid = "urn:scanner:isin:US0378331005"
    rows = all_positive(iid, "2026-09-30T06:00:00Z")
    for row in rows:
        if row["dimension"] == "listing_state":
            row["pit_verified"] = False
    result = evaluate_project_investability(instrument_id=iid, as_of="2026-09-30T07:00:00Z", evidence_records=rows)
    assert result["project_investability_status"] == "UNKNOWN"
    assert result["dimensions"]["listing_state"]["resolved_status"] == "UNKNOWN"


def test_future_evidence_is_not_back_projected():
    iid = "urn:scanner:isin:US0378331005"
    rows = all_positive(iid, "2026-10-01T06:00:00Z")
    result = evaluate_project_investability(instrument_id=iid, as_of="2026-09-30T07:00:00Z", evidence_records=rows)
    assert result["project_investability_status"] == "UNKNOWN"
    assert all(row["resolved_status"] == "UNKNOWN" for row in result["dimensions"].values())


def test_conflicting_latest_evidence_fails_closed():
    iid = "urn:scanner:isin:US0378331005"
    rows = all_positive(iid, "2026-09-30T06:00:00Z")
    rows.append(rec(iid, "listing_state", "DELISTED", "2026-09-30T06:00:00Z", source="other"))
    result = evaluate_project_investability(instrument_id=iid, as_of="2026-09-30T07:00:00Z", evidence_records=rows)
    assert result["project_investability_status"] == "UNKNOWN"
    assert result["dimensions"]["listing_state"]["resolved_status"] == "CONFLICTING_EVIDENCE"


def test_invalid_dimension_status_is_rejected():
    iid = "urn:scanner:isin:US0378331005"
    with pytest.raises(ProjectInvestabilityError, match="evidence_status_invalid"):
        evaluate_project_investability(
            instrument_id=iid,
            as_of="2026-09-30T07:00:00Z",
            evidence_records=[rec(iid, "listing_state", "MAYBE", "2026-09-30T06:00:00Z")],
        )


def test_current_membership_snapshot_remains_unknown_without_listing_tradability_execution_or_restriction_evidence():
    latest = json.loads(Path("artifacts/research/qm/qm_b_membership/latest.json").read_text(encoding="utf-8"))
    snapshot = json.loads(Path(latest["normalized_snapshot_path"]).read_text(encoding="utf-8"))
    expected = len(snapshot["claims"])
    assert expected == snapshot["stable_instrument_claim_count"]
    result = audit_membership_snapshot(snapshot)
    assert result["instrument_count"] == expected
    assert result["status_counts"] == {"UNKNOWN": expected}
    assert result["strict_universe_promotion_ready_count"] == 0
    assert result["evidence_gap_counts"]["stable_identity"] == 0
    assert result["evidence_gap_counts"]["project_membership"] == 0
    assert result["evidence_gap_counts"]["listing_state"] == expected
    assert result["evidence_gap_counts"]["market_tradability"] == expected
    assert result["evidence_gap_counts"]["execution_channel"] == expected
    assert result["evidence_gap_counts"]["project_restrictions"] == expected
    assert result["historical_retrojection_permitted"] is False
