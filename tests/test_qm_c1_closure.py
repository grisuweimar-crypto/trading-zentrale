import copy
import json
from pathlib import Path

import pytest

from scanner.research.governance.qm_c_closure import (
    EXPECTED_DISPLAY_STATUS,
    validate_closure_manifest,
)


MANIFEST = Path("configs/qm_c1_closure_v1.json")


def load_manifest():
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def test_qm_c1_manifest_closes_engineering_without_claiming_empirical_promotion():
    payload = validate_closure_manifest(load_manifest())
    assert payload["display_status"] == EXPECTED_DISPLAY_STATUS
    assert payload["engineering_status"] == "COMPLETE"
    assert payload["empirical_promotion_claimed"] is False
    assert payload["next_mandatory_work_package"] == "QM-C2"


def test_qm_c1_closure_requires_system_integrations():
    payload = load_manifest()
    payload["integration_contracts"] = copy.deepcopy(payload["integration_contracts"])
    payload["integration_contracts"]["qm_b"]["strict_universe_promotion_respected"] = False
    with pytest.raises(ValueError, match="qm_c1_qm_b_promotion_guard_missing"):
        validate_closure_manifest(payload)


def test_qm_c1_closure_cannot_enable_productive_or_decision_changes():
    payload = load_manifest()
    payload["boundaries"] = copy.deepcopy(payload["boundaries"])
    payload["boundaries"]["scanner_semantics_changed"] = True
    with pytest.raises(ValueError, match="qm_c1_unsafe_boundary_enabled"):
        validate_closure_manifest(payload)


def test_qm_c1_closure_requires_all_declared_capabilities():
    payload = load_manifest()
    payload["completed_capabilities"] = [
        item for item in payload["completed_capabilities"]
        if item != "qm_a_analysis_binding"
    ]
    with pytest.raises(ValueError, match="qm_c1_capabilities_missing"):
        validate_closure_manifest(payload)
