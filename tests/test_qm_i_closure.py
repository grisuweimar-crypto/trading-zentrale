import copy
import json
from pathlib import Path

import pytest

from scanner.research.governance.qm_c4_closure import validate_handoff_file as validate_ba_qm2_handoff_file
from scanner.research.governance.qm_i_closure import (
    EXPECTED_BA_STATUS,
    EXPECTED_DISPLAY_STATUS,
    validate_closure_file,
    validate_closure_manifest,
)
from scanner.research.governance.qm_i_lineage import load_qm_i_contract


MANIFEST = Path("configs/ba_qm3_qm_i_closure_v1.json")
STANCE = Path("configs/decision_universal_stance_v1.json")


def load(path):
    return json.loads(path.read_text(encoding="utf-8"))


def inputs():
    return {
        "ba_qm2_handoff": validate_ba_qm2_handoff_file(),
        "lineage_contract": load_qm_i_contract(),
        "phase7_stance_contract": load(STANCE),
    }


def test_qm_i_and_ba_qm3_closure_is_valid_and_keeps_qm_b_blockers_visible():
    payload = validate_closure_file()
    assert payload["display_status"] == EXPECTED_DISPLAY_STATUS
    assert payload["business_area_status"] == EXPECTED_BA_STATUS
    assert payload["engineering_status"] == "COMPLETE"
    assert payload["empirical_promotion_claimed"] is False
    assert payload["prerequisite"]["qm_b_strict_historical_promotion_required"] is False
    assert payload["prerequisite"]["qm_b_external_blockers_must_remain_visible"] is True
    assert payload["next_mandatory_work_package"].startswith("BA-QM4 / QM-D + QM-E")


def test_qm_i_missing_required_capability_fails_closed():
    payload = load(MANIFEST)
    payload["completed_capabilities"] = [
        item for item in payload["completed_capabilities"]
        if item != "common_ancestry_detector_with_paths"
    ]
    with pytest.raises(ValueError, match="qm_i_capabilities_missing"):
        validate_closure_manifest(payload, **inputs())


def test_qm_i_cannot_override_qm_b_or_rekey_qm_c():
    payload = load(MANIFEST)
    payload["integration_contracts"] = copy.deepcopy(payload["integration_contracts"])
    payload["integration_contracts"]["qm_b"]["strict_promotion_blockers_overridden"] = True
    with pytest.raises(ValueError, match="qm_i_qm_b_blocker_override_forbidden"):
        validate_closure_manifest(payload, **inputs())

    payload = load(MANIFEST)
    payload["integration_contracts"] = copy.deepcopy(payload["integration_contracts"])
    payload["integration_contracts"]["qm_c"]["stable_ids_reused_without_rekeying"] = False
    with pytest.raises(ValueError, match="qm_i_qm_c_integration_invalid"):
        validate_closure_manifest(payload, **inputs())


def test_qm_i_cannot_redefine_phase7_probability_confidence_or_timing_as_independent_votes():
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


def test_qm_i_cannot_claim_productive_change_or_empirical_promotion():
    for field, value in (
        ("productive_integration_enabled", True),
        ("empirical_promotion_claimed", True),
    ):
        payload = load(MANIFEST)
        payload[field] = value
        with pytest.raises(ValueError):
            validate_closure_manifest(payload, **inputs())

    payload = load(MANIFEST)
    payload["boundaries"] = copy.deepcopy(payload["boundaries"])
    payload["boundaries"]["scanner_weights_changed"] = True
    with pytest.raises(ValueError, match="qm_i_unsafe_boundary_enabled"):
        validate_closure_manifest(payload, **inputs())
