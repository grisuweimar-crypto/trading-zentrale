from scanner.research.governance.qm_h_closure import validate_qm_h_closure


def test_qm_h_formal_closure_and_handoff_contract():
    result = validate_qm_h_closure()
    assert result["valid"] is True
    assert result["display_status"] == "QM-H COMPLETE — DEFECT / NEAR-MISS / CAPA CONTINUOUS CONTROL ACTIVE"
    assert result["finding_categories"] == ["DEFECT", "NEAR_MISS", "METHODOLOGY_FINDING", "EXTERNAL_EVIDENCE_GAP"]
    assert result["known_qm_b_external_blockers"] == ["EXECUTION_CHANNEL_EVIDENCE", "LISTING_EVIDENCE", "MARKET_TRADABILITY_EVIDENCE"]
    assert result["next_step_after_user_authorization"] == "BA-QM3 / QM-I"
    assert result["closure"]["scope_boundary"]["ba_qm3_implemented_by_this_closure"] is False
    assert result["closure"]["scope_boundary"]["qm_a_state_mutation_by_qm_h_permitted"] is False
    assert result["closure"]["scope_boundary"]["qm_b_engineering_reopened"] is False
