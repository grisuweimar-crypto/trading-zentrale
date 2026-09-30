import copy
import json
from pathlib import Path

import pytest

from scanner.research.governance.qm_b_closure import validate_closure_file as validate_qm_b_closure_file
from scanner.research.governance.qm_c4_closure import (
    EXPECTED_DISPLAY_STATUS,
    EXPECTED_HANDOFF_STATUS,
    validate_closure_file,
    validate_closure_manifest,
    validate_handoff_manifest,
)


CLOSURE = Path("configs/qm_c4_closure_v1.json")
HANDOFF = Path("configs/ba_qm2_handoff_v1.json")


def load(path):
    return json.loads(path.read_text(encoding="utf-8"))


def test_final_qm_c_closure_and_ba_qm2_handoff_are_valid():
    closure = validate_closure_file()
    assert closure["display_status"] == EXPECTED_DISPLAY_STATUS
    assert closure["qm_c_axis_status"] == "COMPLETE"
    assert closure["ba_qm2_handoff_ready"] is True
    assert closure["next_mandatory_work_package"] == "BA-QM3 / QM-I"
    handoff = load(HANDOFF)
    assert handoff["display_status"] == EXPECTED_HANDOFF_STATUS
    assert handoff["next_business_area"] == "BA-QM3"
    assert handoff["next_qm_axis"] == "QM-I"


def test_qm_c4_missing_capability_or_unsafe_boundary_fails():
    payload = load(CLOSURE)
    payload["completed_capabilities"] = [
        item for item in payload["completed_capabilities"]
        if item != "end_to_end_negative_result_regression_path"
    ]
    with pytest.raises(ValueError, match="qm_c4_capabilities_missing"):
        validate_closure_manifest(payload)

    payload = load(CLOSURE)
    payload["boundaries"] = copy.deepcopy(payload["boundaries"])
    payload["boundaries"]["empirical_promotion_performed"] = True
    with pytest.raises(ValueError, match="qm_c4_unsafe_boundary_enabled"):
        validate_closure_manifest(payload)


def test_qm_c4_cannot_hide_qm_b_external_blocker():
    payload = load(CLOSURE)
    payload["integration_contracts"] = copy.deepcopy(payload["integration_contracts"])
    payload["integration_contracts"]["qm_b"]["strict_historical_promotion_remains_blocked_by_external_evidence"] = False
    with pytest.raises(ValueError, match="qm_c4_qm_b_strict_blocker_guard_missing"):
        validate_closure_manifest(payload)


def test_ba_qm2_handoff_requires_exact_current_qm_b_blockers():
    closure = validate_closure_manifest(load(CLOSURE))
    qm_b = validate_qm_b_closure_file()
    handoff = load(HANDOFF)
    handoff["qm_b"] = copy.deepcopy(handoff["qm_b"])
    handoff["qm_b"]["external_blockers"] = ["LISTING_EVIDENCE"]
    with pytest.raises(ValueError, match="ba_qm2_handoff_qm_b_blockers_mismatch"):
        validate_handoff_manifest(handoff, qm_b_closure=qm_b, qm_c_closure=closure)


def test_ba_qm2_handoff_stable_ids_cannot_be_rekeyed_or_replaced_by_names():
    closure = validate_closure_manifest(load(CLOSURE))
    qm_b = validate_qm_b_closure_file()
    handoff = load(HANDOFF)
    handoff["lineage_contract_for_qm_i"] = copy.deepcopy(handoff["lineage_contract_for_qm_i"])
    handoff["lineage_contract_for_qm_i"]["qm_i_may_reference_qm_c_ids_without_rekeying"] = False
    with pytest.raises(ValueError, match="ba_qm2_handoff_qm_i_rekey_guard_missing"):
        validate_handoff_manifest(handoff, qm_b_closure=qm_b, qm_c_closure=closure)


def test_ba_qm2_handoff_does_not_pretend_strict_promotion_is_required_for_qm_i():
    closure = validate_closure_manifest(load(CLOSURE))
    qm_b = validate_qm_b_closure_file()
    handoff = load(HANDOFF)
    handoff["handoff_preconditions"] = copy.deepcopy(handoff["handoff_preconditions"])
    handoff["handoff_preconditions"]["strict_historical_promotion_required_for_starting_qm_i"] = True
    with pytest.raises(ValueError, match="ba_qm2_handoff_qm_i_must_not_depend_on_external_promotion"):
        validate_handoff_manifest(handoff, qm_b_closure=qm_b, qm_c_closure=closure)
