from __future__ import annotations

import copy

import pytest

from scanner.research.governance.qm_j_selection_freshness_gate import (
    SelectionFreshnessGateError,
    assert_evidence_promotion_blocked,
    load_gate,
    validate_gate,
)


FALSIFICATION_HASH = "a646d5eb4397f32d60f922ddd333e30cf61562cde8b5b1a6d1f791d3e380f483"


def test_freshness_gate_preserves_promotion_block_and_frozen_capa_identity() -> None:
    gate = load_gate()
    assert gate["finding_id"] == "QM-H-QMJ-PHASE1A-LAG1-001"
    assert gate["capa_id"] == "QM-H-CAPA-QMJ-PHASE1A-LAG1-001"
    assert gate["status"] == "IMPLEMENTED_GOVERNANCE_GUARD_PENDING_EFFECTIVENESS_VERIFICATION"
    assert gate["blocked_evidence"]["evidence_impact"] == "PROMOTION_BLOCKED"
    assert gate["blocked_evidence"]["promotion_allowed"] is False
    assert gate["boundaries"]["promotion_performed"] is False
    assert gate["future_promotion_requirements"]["prospective_unspent_evidence_required"] is True
    assert gate["future_promotion_requirements"]["historical_spent_evidence_may_verify_effectiveness"] is False

    result = assert_evidence_promotion_blocked(FALSIFICATION_HASH, gate)
    assert result["promotion_allowed"] is False
    assert result["effectiveness_verification_pending"] is True
    assert result["automatic_release_allowed"] is False
    assert result["capa_id"] == "QM-H-CAPA-QMJ-PHASE1A-LAG1-001"


def test_freshness_gate_rejects_capa_identity_drift() -> None:
    gate = load_gate()
    changed = copy.deepcopy(gate)
    changed["capa_id"] = "QM-H-CAPA-PHASE1A-LAG-FRESHNESS-001"
    with pytest.raises(SelectionFreshnessGateError, match="freshness_gate_capa_invalid"):
        validate_gate(changed)


def test_freshness_gate_rejects_automatic_release_or_promotion() -> None:
    gate = load_gate()
    changed = copy.deepcopy(gate)
    changed["release_guard"]["automatic_release_from_promotion_block"] = True
    with pytest.raises(SelectionFreshnessGateError, match="freshness_gate_automatic_release_forbidden"):
        validate_gate(changed)

    changed = copy.deepcopy(gate)
    changed["boundaries"]["promotion_performed"] = True
    with pytest.raises(SelectionFreshnessGateError, match="freshness_gate_boundary_violation:promotion_performed"):
        validate_gate(changed)


def test_freshness_gate_only_blocks_registered_falsification_result() -> None:
    with pytest.raises(SelectionFreshnessGateError, match="freshness_gate_result_hash_not_registered"):
        assert_evidence_promotion_blocked("0" * 64)
