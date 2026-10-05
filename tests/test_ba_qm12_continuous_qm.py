from copy import deepcopy
import json
from pathlib import Path

import pytest

from scanner.research.governance.ba_qm12_continuous_qm import (
    BAQM12Error,
    LIFECYCLE,
    REQUIRED_CONTROLS,
    evaluate_continuous_qm,
    validate_contract,
)

ROOT = Path(__file__).resolve().parents[1]


def _contract():
    return json.loads((ROOT / "configs" / "ba_qm12_continuous_qm_v1.json").read_text(encoding="utf-8"))


def test_ba_qm12_current_system_is_continuous_qm_active():
    receipt = evaluate_continuous_qm(ROOT)
    assert receipt["engineering_status"] == "BA_QM12_ENGINEERING_COMPLETE"
    assert receipt["operating_status"] == "ACTIVE_CONTINUOUS_QM"
    assert receipt["required_control_count"] == 9
    assert tuple(receipt["required_controls_active"]) == REQUIRED_CONTROLS
    assert tuple(receipt["future_module_lifecycle"]) == LIFECYCLE
    assert receipt["lag1_evidence_impact"] == "PROMOTION_BLOCKED"
    assert receipt["ba_qm12_may_release_lag1_block"] is False
    assert receipt["w8_promotion_eligible"] is False
    assert receipt["snapshot_monitor"]["drift_monitoring"]["threshold_breach_claimed"] is False


def test_future_module_stage_skipping_cannot_be_enabled():
    value = deepcopy(_contract())
    value["lifecycle_guards"]["stage_skipping_allowed"] = True
    with pytest.raises(BAQM12Error, match="lifecycle_guard_invalid"):
        validate_contract(value)


def test_continuous_qm_cannot_auto_promote():
    value = deepcopy(_contract())
    value["monitoring_policy"]["automatic_promotion_allowed"] = True
    with pytest.raises(BAQM12Error, match="automatic_promotion_forbidden"):
        validate_contract(value)


def test_drift_threshold_may_not_be_invented_after_observation():
    value = deepcopy(_contract())
    value["monitoring_policy"]["drift_thresholds_may_not_be_invented"] = False
    with pytest.raises(BAQM12Error, match="drift_threshold_guard_missing"):
        validate_contract(value)
