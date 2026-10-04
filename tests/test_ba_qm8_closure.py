from __future__ import annotations

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
