import copy
import json
from pathlib import Path

import pytest

from scanner.research.governance.qm_c3_closure import (
    EXPECTED_DISPLAY_STATUS,
    validate_closure_manifest,
)


MANIFEST = Path("configs/qm_c3_closure_v1.json")


def load_manifest():
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def test_qm_c3_closure_manifest_is_valid():
    payload = validate_closure_manifest(load_manifest())
    assert payload["display_status"] == EXPECTED_DISPLAY_STATUS
    assert payload["engineering_status"] == "COMPLETE"
    assert payload["empirical_promotion_claimed"] is False
    assert payload["next_mandatory_work_package"] == "QM-C4"


def test_qm_c3_missing_required_capability_fails():
    payload = load_manifest()
    payload["completed_capabilities"] = [
        item for item in payload["completed_capabilities"]
        if item != "predeclared_sequential_look_schedule"
    ]
    with pytest.raises(ValueError, match="qm_c3_capabilities_missing"):
        validate_closure_manifest(payload)


def test_qm_c3_cannot_claim_productive_or_empirical_promotion():
    for field, value in (
        ("productive_integration_enabled", True),
        ("empirical_promotion_claimed", True),
    ):
        payload = load_manifest()
        payload[field] = value
        with pytest.raises(ValueError):
            validate_closure_manifest(payload)


def test_qm_c3_cross_module_guards_are_mandatory():
    payload = load_manifest()
    payload["integration_contracts"] = copy.deepcopy(payload["integration_contracts"])
    payload["integration_contracts"]["qm_c2"]["every_family_member_must_be_confirmation_ready_at_control_freeze"] = False
    with pytest.raises(ValueError, match="qm_c3_qm_c2_readiness_guard_missing"):
        validate_closure_manifest(payload)

    payload = load_manifest()
    payload["integration_contracts"] = copy.deepcopy(payload["integration_contracts"])
    payload["integration_contracts"]["qm_a"]["later_looks_require_consumed_confirmatory_evidence_state"] = False
    with pytest.raises(ValueError, match="qm_c3_later_look_guard_missing"):
        validate_closure_manifest(payload)

    payload = load_manifest()
    payload["integration_contracts"] = copy.deepcopy(payload["integration_contracts"])
    payload["integration_contracts"]["qm_b"]["current_external_blockers_are_not_overridden"] = False
    with pytest.raises(ValueError, match="qm_c3_qm_b_blocker_override_forbidden"):
        validate_closure_manifest(payload)
