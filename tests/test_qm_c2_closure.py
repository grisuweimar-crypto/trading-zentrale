import copy
import json
from pathlib import Path

import pytest

from scanner.research.governance.qm_c2_closure import (
    EXPECTED_DISPLAY_STATUS,
    validate_closure_manifest,
)


MANIFEST = Path("configs/qm_c2_closure_v1.json")


def load_manifest():
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def test_qm_c2_closure_manifest_is_valid():
    payload = validate_closure_manifest(load_manifest())
    assert payload["display_status"] == EXPECTED_DISPLAY_STATUS
    assert payload["engineering_status"] == "COMPLETE"
    assert payload["empirical_promotion_claimed"] is False
    assert payload["next_mandatory_work_package"] == "QM-C3"


def test_qm_c2_missing_required_capability_fails():
    payload = load_manifest()
    payload["completed_capabilities"] = [
        item for item in payload["completed_capabilities"]
        if item != "end_to_end_confirmation_readiness_gate"
    ]
    with pytest.raises(ValueError, match="qm_c2_capabilities_missing"):
        validate_closure_manifest(payload)


def test_qm_c2_cannot_claim_productive_or_empirical_promotion():
    for field, value in (
        ("productive_integration_enabled", True),
        ("empirical_promotion_claimed", True),
    ):
        payload = load_manifest()
        payload[field] = value
        with pytest.raises(ValueError):
            validate_closure_manifest(payload)


def test_qm_c2_integration_guards_are_mandatory():
    payload = load_manifest()
    payload["integration_contracts"] = copy.deepcopy(payload["integration_contracts"])
    payload["integration_contracts"]["qm_c1"]["direct_hypothesis_freeze_alone_is_not_confirmation_ready"] = False
    with pytest.raises(ValueError, match="qm_c2_direct_freeze_bypass_guard_missing"):
        validate_closure_manifest(payload)

    payload = load_manifest()
    payload["integration_contracts"] = copy.deepcopy(payload["integration_contracts"])
    payload["integration_contracts"]["qm_b"]["current_external_blockers_are_not_overridden"] = False
    with pytest.raises(ValueError, match="qm_c2_qm_b_blocker_override_forbidden"):
        validate_closure_manifest(payload)
