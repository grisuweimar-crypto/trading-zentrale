from __future__ import annotations

import scanner.research.governance.ba_qm8_closure as closure_module
from scanner.research.governance.ba_qm8_closure import evaluate_ba_qm8_closure
from scanner.research.governance.ba_qm8_real_stage_bindings import (
    audit_real_stage_bindings,
)


def _patch_static_governance(monkeypatch) -> None:
    monkeypatch.setattr(
        closure_module,
        "validate_foundation_file",
        lambda: {
            "engineering_status": "COMPLETE",
            "closure_claimed": True,
            "closure_snapshot_id": "historical-snapshot",
            "closure_snapshot_as_of": "2026-10-04",
            "closure_transition_pass_count": 10,
            "closure_transition_blocked_count": 0,
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


def test_ba_qm8_closure_gate_preserves_historical_receipt_and_audits_current_snapshot() -> None:
    live = audit_real_stage_bindings()
    result = evaluate_ba_qm8_closure()

    assert result["status"] == "BA_QM8_ENGINEERING_COMPLETE"
    assert result["snapshot_id"] == live["snapshot_id"]
    assert result["snapshot_as_of"] == live["snapshot_as_of"]
    assert (
        result["historical_closure_snapshot_id"]
        == "36cf527e-ca26-489f-aa55-9c7de3b4355b"
    )
    assert result["historical_closure_snapshot_as_of"] == "2026-10-04"
    assert result["historical_closure_evidence_immutable"] is True
    assert result["current_snapshot_audit_passed"] is True
    assert result["stage_bindings_complete"] is True
    assert result["real_transitions_complete"] is True
    assert result["transition_pass_count"] == 10
    assert result["transition_blocked_count"] == 0
    assert result["closure_blockers"] == []
    assert result["engineering_closure_eligible"] is True
    assert result["engineering_closure_performed"] is True

    assert result["lag1_evidence_impact"] == "PROMOTION_BLOCKED"
    assert (
        result["lag1_effectiveness_verification"]
        == "PENDING_PROSPECTIVE_UNSPENT_EVIDENCE"
    )
    assert result["ba_qm8_may_release_lag1_block"] is False
    assert result["automatic_release_allowed"] is False
    assert result["empirical_validation_claimed"] is False
    assert result["empirical_promotion_performed"] is False


def test_historical_closure_does_not_require_current_snapshot_identity(monkeypatch) -> None:
    _patch_static_governance(monkeypatch)
    monkeypatch.setattr(
        closure_module,
        "audit_real_stage_bindings",
        lambda root: {
            "status": "REAL_STAGE_BINDINGS_PASS",
            "snapshot_id": "current-snapshot",
            "snapshot_as_of": "2026-10-05",
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
    assert result["status"] == "BA_QM8_ENGINEERING_COMPLETE"
    assert result["snapshot_id"] == "current-snapshot"
    assert result["snapshot_as_of"] == "2026-10-05"
    assert result["historical_closure_snapshot_id"] == "historical-snapshot"
    assert result["historical_closure_snapshot_as_of"] == "2026-10-04"
    assert result["historical_closure_evidence_immutable"] is True
    assert result["current_snapshot_audit_passed"] is True
    assert result["engineering_closure_eligible"] is True
    assert result["engineering_closure_performed"] is True
    assert result["lag1_evidence_impact"] == "PROMOTION_BLOCKED"
    assert result["ba_qm8_may_release_lag1_block"] is False
    assert result["automatic_release_allowed"] is False


def test_historical_closure_survives_but_current_snapshot_failure_blocks_health(
    monkeypatch,
) -> None:
    _patch_static_governance(monkeypatch)
    monkeypatch.setattr(
        closure_module,
        "audit_real_stage_bindings",
        lambda root: {
            "status": "REAL_STAGE_BINDINGS_BLOCKED",
            "snapshot_id": "current-snapshot",
            "snapshot_as_of": "2026-10-05",
            "closure_eligible": False,
            "closure_blockers": ["snapshot_identity_mismatch"],
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
    assert result["status"] == "CURRENT_SNAPSHOT_AUDIT_BLOCKED"
    assert result["historical_closure_snapshot_id"] == "historical-snapshot"
    assert result["historical_closure_evidence_immutable"] is True
    assert result["engineering_closure_performed"] is True
    assert result["engineering_closure_eligible"] is False
    assert result["current_snapshot_audit_passed"] is False
    assert result["closure_blockers"] == ["stage:snapshot_identity_mismatch"]
    assert result["lag1_evidence_impact"] == "PROMOTION_BLOCKED"
    assert result["ba_qm8_may_release_lag1_block"] is False
