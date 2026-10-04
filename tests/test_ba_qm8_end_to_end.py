from __future__ import annotations

import copy
import json

import pytest

from scanner.research.governance.ba_qm8_end_to_end import (
    BAQM8AuditError,
    EXTERNAL_8C_COMPLETION_PATH,
    EXTERNAL_CONTRACT_PATH,
    load_contract,
    validate_contract,
    validate_dependencies,
    validate_foundation_file,
)
from scanner.research.governance.qm_i_lineage import load_qm_i_contract
from scanner.research.governance.qm_j_closure import (
    validate_closure_file as validate_ba_qm7_closure_file,
)
from scanner.research.governance.qm_j_selection_freshness_gate import load_gate


def _json(path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _dependencies() -> dict:
    return {
        "ba_qm7_closure": validate_ba_qm7_closure_file(),
        "freshness_gate": load_gate(),
        "qm_i_contract": load_qm_i_contract(),
        "external_contract": _json(EXTERNAL_CONTRACT_PATH),
        "external_8c_completion": _json(EXTERNAL_8C_COMPLETION_PATH),
    }


def test_ba_qm8_contract_foundation_binds_full_chain_without_claiming_closure() -> None:
    receipt = validate_foundation_file()
    assert receipt["status"] == "PASSED_CONTRACT_FOUNDATION"
    assert receipt["engineering_status"] == "IN_PROGRESS"
    assert receipt["transition_guard_status"] == "IMPLEMENTED_FAIL_CLOSED_ENGINE"
    assert receipt["real_stage_adapter_status"] == "PENDING"
    assert receipt["stage_count"] == 11
    assert receipt["transition_count"] == 10
    assert receipt["error_class_count"] == 7
    assert receipt["all_transitions_cover_all_error_classes"] is True
    assert receipt["qm_i_lineage_bound"] is True
    assert receipt["external_evidence_fail_closed"] is True
    assert receipt["evidence_impact"] == "PROMOTION_BLOCKED"
    assert receipt["effectiveness_verification_pending"] is True
    assert receipt["automatic_release_allowed"] is False
    assert receipt["empirical_promotion_performed"] is False
    assert receipt["closure_claimed"] is False


def test_contract_rejects_missing_or_reordered_stage() -> None:
    value = load_contract()
    changed = copy.deepcopy(value)
    changed["stage_chain"].pop(3)
    with pytest.raises(BAQM8AuditError, match="ba_qm8_stage_chain_invalid"):
        validate_contract(changed)

    changed = copy.deepcopy(value)
    changed["stage_chain"][1], changed["stage_chain"][2] = (
        changed["stage_chain"][2],
        changed["stage_chain"][1],
    )
    with pytest.raises(BAQM8AuditError, match="ba_qm8_stage_chain_invalid"):
        validate_contract(changed)


def test_contract_rejects_missing_error_class_on_any_transition() -> None:
    value = load_contract()
    changed = copy.deepcopy(value)
    changed["transition_contract"]["transitions"][4]["checks"].remove(
        "HIDDEN_EVIDENCE_REUSE"
    )
    with pytest.raises(
        BAQM8AuditError,
        match="ba_qm8_transition_error_coverage_invalid:PROBABILITY->RISK",
    ):
        validate_contract(changed)


def test_contract_rejects_neutral_missingness_or_missing_lineage() -> None:
    value = load_contract()
    changed = copy.deepcopy(value)
    changed["stage_chain"][2]["missing_policy"] = "NEUTRAL"
    with pytest.raises(
        BAQM8AuditError, match="ba_qm8_stage_missing_policy_not_fail_closed:SELECTION"
    ):
        validate_contract(changed)

    changed = copy.deepcopy(value)
    changed["stage_chain"][7]["lineage_required"] = False
    with pytest.raises(
        BAQM8AuditError, match="ba_qm8_stage_lineage_required:LEARNING"
    ):
        validate_contract(changed)


def test_contract_rejects_external_retrojection_or_decision_rewrite() -> None:
    value = load_contract()
    changed = copy.deepcopy(value)
    changed["external_evidence_contract"]["retrojection_forbidden"] = False
    with pytest.raises(
        BAQM8AuditError,
        match="ba_qm8_external_must_be_true:retrojection_forbidden",
    ):
        validate_contract(changed)

    changed = copy.deepcopy(value)
    changed["external_evidence_contract"]["may_rewrite_probability"] = True
    with pytest.raises(
        BAQM8AuditError,
        match="ba_qm8_external_must_be_false:may_rewrite_probability",
    ):
        validate_contract(changed)


def test_contract_rejects_green_audit_releasing_lag1_promotion_block() -> None:
    value = load_contract()
    changed = copy.deepcopy(value)
    changed["governance_guard"]["green_ba_qm8_may_release_promotion_block"] = True
    with pytest.raises(
        BAQM8AuditError,
        match="ba_qm8_governance_must_be_false:green_ba_qm8_may_release_promotion_block",
    ):
        validate_contract(changed)


def test_dependency_binding_rejects_ba_qm7_capa_release() -> None:
    contract = load_contract()
    dependencies = _dependencies()
    changed = copy.deepcopy(dependencies["ba_qm7_closure"])
    changed["open_capa"]["evidence_impact"] = "RELEASED"
    dependencies["ba_qm7_closure"] = changed

    with pytest.raises(
        BAQM8AuditError, match="ba_qm8_open_capa_promotion_block_missing"
    ):
        validate_dependencies(contract, **dependencies)


def test_dependency_binding_rejects_external_phase7_integration() -> None:
    contract = load_contract()
    dependencies = _dependencies()
    changed = copy.deepcopy(dependencies["external_8c_completion"])
    changed["hard_boundaries"]["phase7_integration_enabled"] = True
    dependencies["external_8c_completion"] = changed

    with pytest.raises(
        BAQM8AuditError,
        match="ba_qm8_external_8c_must_be_false:phase7_integration_enabled",
    ):
        validate_dependencies(contract, **dependencies)


def test_dependency_binding_rejects_missing_qm_i_independence_guard() -> None:
    contract = load_contract()
    dependencies = _dependencies()
    changed = copy.deepcopy(dependencies["qm_i_contract"])
    changed["principles"]["missing_lineage_is_not_independence"] = False
    dependencies["qm_i_contract"] = changed

    with pytest.raises(
        BAQM8AuditError,
        match="ba_qm8_qm_i_must_be_true:missing_lineage_is_not_independence",
    ):
        validate_dependencies(contract, **dependencies)


def test_contract_rejects_claiming_real_stage_adapter_before_it_exists() -> None:
    value = load_contract()
    changed = copy.deepcopy(value)
    changed["real_stage_adapter_status"] = "IMPLEMENTED"
    with pytest.raises(
        BAQM8AuditError, match="ba_qm8_real_stage_adapter_status_invalid"
    ):
        validate_contract(changed)
