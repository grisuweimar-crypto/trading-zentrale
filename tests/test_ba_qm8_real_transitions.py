from __future__ import annotations

from scanner.research.governance.ba_qm8_end_to_end import EXPECTED_ERROR_CLASSES
from scanner.research.governance.ba_qm8_real_transitions import (
    audit_real_transitions,
)


def test_current_real_snapshot_executes_nine_of_ten_transitions_fail_closed() -> None:
    result = audit_real_transitions()

    assert result["status"] == "REAL_TRANSITIONS_PARTIAL_DATA_PROVENANCE_BLOCKED"
    assert result["transition_pass_count"] == 9
    assert result["transition_blocked_count"] == 1
    assert result["closure_eligible"] is False
    assert result["historical_lineage_backfilled"] is False
    assert result["missing_treated_as_neutral"] is False
    assert result["all_executed_transitions_cover_all_seven_classes"] is True

    blocked = result["blocked_transitions"]
    assert blocked == [
        {
            "from_stage": "DATA",
            "to_stage": "SCANNER",
            "reason": "scanner_input_provenance_missing",
            "historical_backfill_attempted": False,
        }
    ]

    pairs = [(r["from_stage"], r["to_stage"]) for r in result["receipts"]]
    assert pairs == [
        ("SCANNER", "SELECTION"),
        ("SELECTION", "TIMING"),
        ("TIMING", "PROBABILITY"),
        ("PROBABILITY", "RISK"),
        ("RISK", "CONFIDENCE"),
        ("CONFIDENCE", "LEARNING"),
        ("LEARNING", "ELLIOTT"),
        ("ELLIOTT", "DECISION_LAYER"),
        ("DECISION_LAYER", "EXTERNAL_EVIDENCE"),
    ]

    for receipt in result["receipts"]:
        assert receipt["status"] == "PASSED"
        assert set(receipt["checks"]) == set(EXPECTED_ERROR_CLASSES)
        assert all(item["passed"] is True for item in receipt["checks"].values())
        assert receipt["promotion_effect"] == "NONE"
        assert receipt["investment_logic_changed"] is False


def test_real_transition_claim_inventory_is_nonempty_for_core_families() -> None:
    result = audit_real_transitions()
    counts = result["family_claim_counts"]
    assert counts["selection"] > 0
    assert counts["timing"] > 0
    assert counts["probability"] > 0
    assert counts["risk"] > 0
    assert counts["confidence"] > 0
