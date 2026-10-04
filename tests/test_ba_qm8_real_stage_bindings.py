from __future__ import annotations

from scanner.research.governance.ba_qm8_real_stage_bindings import (
    audit_real_stage_bindings,
)


def test_current_real_prospective_snapshot_is_fully_bound_without_invented_provenance() -> None:
    result = audit_real_stage_bindings()

    assert result["w10_status"] == "sealed"
    assert result["packet_count"] > 0
    assert result["claim_count"] >= result["packet_count"]
    assert result["family_claim_counts"]["selection"] == result["packet_count"]

    stages = result["stage_results"]
    assert stages["SCANNER"]["status"] == "PASS"
    assert stages["SELECTION"]["status"] == "PASS"
    assert stages["PROBABILITY"]["status"] != "FAIL"
    assert stages["RISK"]["status"] != "FAIL"
    assert stages["CONFIDENCE"]["status"] != "FAIL"
    assert stages["LEARNING"]["status"] != "FAIL"
    assert stages["ELLIOTT"]["status"] != "FAIL"
    assert stages["DECISION_LAYER"]["status"] == "PASS_PRIVACY_BOUNDARY"
    assert stages["EXTERNAL_EVIDENCE"]["status"] == "PASS_DISABLED_BOUNDARY"

    assert stages["DATA"]["status"] == "PASS"
    assert stages["DATA"]["gaps"] == []
    assert stages["DATA"]["evidence"]["scanner_input_provenance_present"] is True
    assert stages["DATA"]["evidence"]["historical_backfill"] is False
    assert stages["SCANNER"]["evidence"]["scanner_input_provenance_bound_in_w10"] is True
    assert result["closure_blockers"] == []
    assert result["closure_eligible"] is True
    assert result["status"] == "REAL_STAGE_BINDINGS_PASS"

    assert result["missing_treated_as_neutral"] is False
    assert result["guessed_lineage_added"] is False
    assert result["private_position_data_persisted"] is False
    assert result["external_evidence_productively_integrated"] is False


def test_expected_private_decision_runtime_boundary_is_not_misclassified_as_missing() -> None:
    result = audit_real_stage_bindings()
    decision = result["stage_results"]["DECISION_LAYER"]
    assert decision["expected_boundary"] is True
    assert decision["gaps"] == []
    assert decision["evidence"]["private_position_data_persisted"] is False
    assert decision["evidence"]["public_current_private_receipt_expected"] is False


def test_external_evidence_remains_disabled_after_decision_layer() -> None:
    result = audit_real_stage_bindings()
    external = result["stage_results"]["EXTERNAL_EVIDENCE"]
    assert external["expected_boundary"] is True
    assert external["evidence"]["phase8_activated_in_7a"] is False
    assert external["evidence"]["production_external_evidence_enabled"] is False
    assert external["evidence"]["phase7_integration_enabled"] is False
    assert external["evidence"]["may_generate_order"] is False
