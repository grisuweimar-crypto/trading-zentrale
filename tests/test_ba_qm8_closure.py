from __future__ import annotations

import scanner.research.governance.ba_qm8_closure as closure_module
from scanner.research.governance.ba_qm8_closure import evaluate_ba_qm8_closure


def test_ba_qm8_closure_gate_stays_pending_on_historical_snapshot() -> None:
    result = evaluate_ba_qm8_closure()

    assert result["status"] == "PENDING_PROSPECTIVE_SNAPSHOT"
    assert result["stage_bindings_complete"] is False
    assert result["real_transitions_complete"] is False
    assert result["transition_pass_count"] == 9
    assert result["transition_blocked_count"] == 1
    assert result["engineering_closure_eligible"] is False
    assert result["engineering_closure_performed"] is False

    assert "stage:DATA" in result["closure_blockers"]
    assert (
        "transition:DATA->SCANNER:scanner_input_provenance_missing"
        in result["closure_blockers"]
    )

    assert result["lag1_evidence_impact"] == "PROMOTION_BLOCKED"
    assert (
        result["lag1_effectiveness_verification"]
        == "PENDING_PROSPECTIVE_UNSPENT_EVIDENCE"
    )
    assert result["ba_qm8_may_release_lag1_block"] is False
    assert result["automatic_release_allowed"] is False
    assert result["empirical_validation_claimed"] is False
    assert result["empirical_promotion_performed"] is False


def test_ten_of_ten_makes_engineering_closure_eligible_but_never_releases_lag1(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        closure_module,
        "validate_foundation_file",
        lambda: {
            "engineering_status": "IN_PROGRESS",
            "closure_claimed": False,
            "evidence_impact": "PROMOTION_BLOCKED",
            "automatic_release_allowed": False,
        },
    )
    monkeypatch.setattr(
        closure_module,
        "validate_ba_qm7_closure_file",
        lambda: {
            "open_capa": {
                "finding_id": "QM-H-QMJ-PHASE1A-LAG1-001",
                "capa_id": "QM-H-CAPA-QMJ-PHASE1A-LAG1-001",
                "evidence_impact": "PROMOTION_BLOCKED",
                "effectiveness_verification": "PENDING_PROSPECTIVE_UNSPENT_EVIDENCE",
                "automatic_release_allowed": False,
            }
        },
    )
    monkeypatch.setattr(
        closure_module,
        "load_gate",
        lambda: {
            "blocked_evidence": {
                "evidence_impact": "PROMOTION_BLOCKED",
                "promotion_allowed": False,
            },
            "release_guard": {
                "automatic_release_from_promotion_block": False,
            },
        },
    )
    monkeypatch.setattr(closure_module, "validate_gate", lambda value: value)
    monkeypatch.setattr(
        closure_module,
        "audit_real_stage_bindings",
        lambda root: {
            "status": "REAL_STAGE_BINDINGS_PASS",
            "snapshot_id": "prospective-snapshot",
            "closure_eligible": True,
            "closure_blockers": [],
        },
    )
    monkeypatch.setattr(
        closure_module,
        "audit_real_transitions",
        lambda root: {
            "status": "REAL_TRANSITIONS_PASS",
            "transition_pass_count": 10,
            "transition_blocked_count": 0,
            "closure_eligible": True,
            "all_executed_transitions_cover_all_seven_classes": True,
            "blocked_transitions": [],
        },
    )

    result = evaluate_ba_qm8_closure(".")
    assert result["status"] == "ELIGIBLE_FOR_BA_QM8_ENGINEERING_CLOSURE"
    assert result["engineering_closure_eligible"] is True
    assert result["engineering_closure_performed"] is False
    assert result["lag1_evidence_impact"] == "PROMOTION_BLOCKED"
    assert result["ba_qm8_may_release_lag1_block"] is False
    assert result["automatic_release_allowed"] is False
    assert result["empirical_promotion_performed"] is False
