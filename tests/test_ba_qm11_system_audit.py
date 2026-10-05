from pathlib import Path

import pytest

from scanner.research.governance.ba_qm11_system_audit import (
    BaQm11AuditError,
    EXPECTED_DIMENSIONS,
    audit_current_system,
    validate_contract,
)


ROOT = Path(__file__).resolve().parents[1]


def test_ba_qm11_current_system_closes_all_nine_dimensions():
    result = audit_current_system(ROOT)
    assert result["status"] == "BA_QM11_ENGINEERING_COMPLETE"
    assert result["result"] == "PASS"
    assert result["dimension_count"] == len(EXPECTED_DIMENSIONS) == 9
    assert result["dimensions_passed"] == 9
    assert result["finding_count"] == 2
    assert result["findings_closed_effective"] == 2
    assert result["w8_live_changed_action_count"] == 5
    assert result["w8_empirical_promotion_eligible"] is False
    assert result["lag1_evidence_impact"] == "PROMOTION_BLOCKED"
    assert result["ba_qm11_may_release_lag1_block"] is False
    assert result["next_mandatory_work_package"] == "BA-QM12 – Konsolidierung & produktiver QM-Betrieb"


def test_ba_qm11_contract_cannot_release_lag1():
    import json

    payload = json.loads((ROOT / "configs" / "ba_qm11_system_audit_v1.json").read_text())
    payload["independent_blocks"]["lag1"]["ba_qm11_may_release"] = True
    with pytest.raises(BaQm11AuditError, match="may_not_release_lag1"):
        validate_contract(payload)


def test_ba_qm11_contract_cannot_claim_decision_logic_change():
    import json

    payload = json.loads((ROOT / "configs" / "ba_qm11_system_audit_v1.json").read_text())
    payload["decision_logic_changed"] = True
    with pytest.raises(BaQm11AuditError, match="unsafe_true_or_missing:decision_logic_changed"):
        validate_contract(payload)


def test_ba_qm11_requires_every_system_dimension_to_pass():
    import json

    payload = json.loads((ROOT / "configs" / "ba_qm11_system_audit_v1.json").read_text())
    payload["dimensions"]["INFORMATION_FLOW"]["status"] = "FAIL"
    with pytest.raises(BaQm11AuditError, match="dimension_not_pass:INFORMATION_FLOW"):
        validate_contract(payload)
