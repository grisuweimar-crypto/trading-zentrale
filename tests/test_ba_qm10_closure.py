from __future__ import annotations

import scanner.research.governance.ba_qm10_closure as closure_module
from scanner.research.governance.ba_qm10_closure import evaluate_ba_qm10_closure
from scanner.research.governance.ba_qm10_production_operations import EXPECTED_CHECKS


def test_ba_qm10_closure_gate_stays_pending_until_all_capa_effectiveness_is_verified() -> None:
    result = evaluate_ba_qm10_closure()
    assert result["status"] == "PENDING_OPERATIONAL_EFFECTIVENESS"
    assert result["engineering_closure_eligible"] is False
    assert result["engineering_closure_performed"] is False
    assert result["closure_blockers"]
    assert any(value.startswith("finding:BA-QM10-F01:") for value in result["closure_blockers"])
    assert any(value.startswith("finding:BA-QM10-F04:") for value in result["closure_blockers"])
    assert any(value.startswith("risk:BA-QM10-R01:") for value in result["closure_blockers"])
    assert result["lag1_evidence_impact"] == "PROMOTION_BLOCKED"
    assert result["ba_qm10_may_release_lag1_block"] is False
    assert result["automatic_release_allowed"] is False
    assert result["empirical_promotion_performed"] is False


def test_all_operational_checks_may_close_baqm10_but_never_release_lag1(monkeypatch) -> None:
    contract = {
        "scope": "QUALITY_MANAGEMENT_ONLY",
        "current_assessment": {key: "PASS" for key in EXPECTED_CHECKS},
        "findings": [
            {"finding_id": f"BA-QM10-F0{index}", "state": "CLOSED_EFFECTIVE"}
            for index in range(1, 5)
        ],
        "static_risks_to_verify": [
            {"risk_id": "BA-QM10-R01", "state": "CLOSED_EFFECTIVE"}
        ],
        "dependencies": {
            "lag1_finding_id": "QM-H-QMJ-PHASE1A-LAG1-001",
            "lag1_capa_id": "QM-H-CAPA-QMJ-PHASE1A-LAG1-001",
        },
    }
    audit = {
        "scope": "QUALITY_MANAGEMENT_ONLY",
        "false_decision_observed": False,
        "all_observed_failures_fail_closed": True,
        "research_logic_changed": False,
        "decision_logic_changed": False,
        "investment_logic_changed": False,
        "execution_enabled": False,
        "lag1_evidence_impact": "PROMOTION_BLOCKED",
        "ba_qm10_may_release_lag1_block": False,
        "scanner_publication_race_risk_open": False,
        "open_finding_count": 0,
    }

    monkeypatch.setattr(closure_module, "load_contract", lambda path: contract)
    monkeypatch.setattr(
        closure_module.operations_module,
        "audit_current_operations",
        lambda root: audit,
    )

    result = evaluate_ba_qm10_closure(".")
    assert result["status"] == "ELIGIBLE_FOR_BA_QM10_ENGINEERING_CLOSURE"
    assert result["engineering_closure_eligible"] is True
    assert result["engineering_closure_performed"] is False
    assert result["closure_blockers"] == []
    assert result["lag1_evidence_impact"] == "PROMOTION_BLOCKED"
    assert result["ba_qm10_may_release_lag1_block"] is False
    assert result["automatic_release_allowed"] is False
    assert result["empirical_promotion_performed"] is False
