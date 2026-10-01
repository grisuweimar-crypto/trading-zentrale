import copy
import json
from pathlib import Path

import pytest

from scanner.research.governance.qm_c6_closure import validate_ba_qm2_handoff
from scanner.research.governance.qm_h_closure import validate_qm_h_closure
from scanner.research.governance.qm_i_closure import EXPECTED_BA_STATUS, EXPECTED_DISPLAY_STATUS, validate_closure_file, validate_closure_manifest
from scanner.research.governance.qm_i_lineage import load_qm_i_contract

MANIFEST = Path("configs/ba_qm3_qm_i_closure_v1.json")
STANCE = Path("configs/decision_universal_stance_v1.json")


def load(path):
    return json.loads(path.read_text(encoding="utf-8"))


def inputs():
    return {
        "ba_qm2_handoff": validate_ba_qm2_handoff()["handoff"],
        "qm_h_closure": validate_qm_h_closure()["closure"],
        "lineage_contract": load_qm_i_contract(),
        "phase7_stance_contract": load(STANCE),
    }


def test_qm_i_and_ba_qm3_closure_is_valid_and_points_to_ba_qm4():
    payload = validate_closure_file()
    assert payload["display_status"] == EXPECTED_DISPLAY_STATUS
    assert payload["business_area_status"] == EXPECTED_BA_STATUS
    assert payload["engineering_status"] == "COMPLETE"
    assert payload["empirical_promotion_claimed"] is False
    assert payload["prerequisite"]["ba_qm2_handoff_schema"] == "ba_qm2_handoff_v2"
    assert payload["prerequisite"]["qm_h_closure_schema"] == "qm_h_closure_v1"
    assert payload["next_mandatory_work_package"].startswith("BA-QM4 / QM-D + QM-E")


def test_qm_i_requires_current_ba_qm2_and_active_qm_h():
    payload = load(MANIFEST)
    supplied = inputs()
    supplied["ba_qm2_handoff"] = copy.deepcopy(supplied["ba_qm2_handoff"])
    supplied["ba_qm2_handoff"]["schema_version"] = "ba_qm2_handoff_v1"
    with pytest.raises(ValueError, match="qm_i_ba_qm2_not_complete"):
        validate_closure_manifest(payload, **supplied)

    supplied = inputs()
    supplied["qm_h_closure"] = copy.deepcopy(supplied["qm_h_closure"])
    supplied["qm_h_closure"]["continuous_control_status"] = "INACTIVE"
    with pytest.raises(ValueError, match="qm_i_qm_h_not_active"):
        validate_closure_manifest(payload, **supplied)


def test_qm_i_missing_required_capability_fails_closed():
    payload = load(MANIFEST)
    payload["completed_capabilities"] = [x for x in payload["completed_capabilities"] if x != "common_ancestry_detector_with_paths"]
    with pytest.raises(ValueError, match="qm_i_capabilities_missing"):
        validate_closure_manifest(payload, **inputs())


def test_qm_i_cannot_override_upstream_authorities():
    payload = load(MANIFEST)
    payload["integration_contracts"] = copy.deepcopy(payload["integration_contracts"])
    payload["integration_contracts"]["qm_b"]["strict_promotion_blockers_overridden"] = True
    with pytest.raises(ValueError, match="qm_i_qm_b_integration_invalid"):
        validate_closure_manifest(payload, **inputs())

    payload = load(MANIFEST)
    payload["integration_contracts"] = copy.deepcopy(payload["integration_contracts"])
    payload["integration_contracts"]["qm_h"]["findings_closed_by_qm_i"] = True
    with pytest.raises(ValueError, match="qm_i_qm_h_integration_invalid"):
        validate_closure_manifest(payload, **inputs())


def test_qm_i_requires_current_qm_c_monitoring_identity_contract():
    payload = load(MANIFEST)
    supplied = inputs()
    supplied["lineage_contract"] = copy.deepcopy(supplied["lineage_contract"])
    supplied["lineage_contract"]["qm_c_integration"]["reuse_monitoring_plan_identity"] = False
    with pytest.raises(ValueError, match="qm_i_qm_c_contract_not_current"):
        validate_closure_manifest(payload, **supplied)


def test_qm_i_does_not_redefine_phase7_independence_semantics():
    payload = load(MANIFEST)
    supplied = inputs()
    supplied["phase7_stance_contract"] = copy.deepcopy(supplied["phase7_stance_contract"])
    supplied["phase7_stance_contract"]["guards"]["probability_is_not_independent_vote"] = False
    with pytest.raises(ValueError, match="qm_i_phase7_independence_semantics_changed"):
        validate_closure_manifest(payload, **supplied)


def test_qm_i_missing_lineage_may_not_be_relabelled_independent():
    payload = load(MANIFEST)
    payload["review_semantics"] = copy.deepcopy(payload["review_semantics"])
    payload["review_semantics"]["missing_lineage_proves_independence"] = True
    with pytest.raises(ValueError, match="qm_i_review_semantics_invalid:missing_lineage_proves_independence"):
        validate_closure_manifest(payload, **inputs())


def test_qm_i_cannot_claim_productive_change_or_ba_qm4_completion():
    payload = load(MANIFEST)
    payload["productive_integration_enabled"] = True
    with pytest.raises(ValueError, match="qm_i_scope_invalid"):
        validate_closure_manifest(payload, **inputs())

    payload = load(MANIFEST)
    payload["boundaries"] = copy.deepcopy(payload["boundaries"])
    payload["boundaries"]["ba_qm4_implemented_by_this_closure"] = True
    with pytest.raises(ValueError, match="qm_i_unsafe_boundary_enabled"):
        validate_closure_manifest(payload, **inputs())
