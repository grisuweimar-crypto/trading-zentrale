from __future__ import annotations

import copy
import json
import subprocess
from pathlib import Path

from scanner.research.external_evidence.extended_stance_entry_gate_8i import (
    APPROVED_RECEIPT_STATE,
    EMPIRICAL_RECEIPT_SCHEMA,
    GATE_RESULT_SCHEMA,
    evaluate_entry_gate,
    validate_entry_gate_contract,
)


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "configs" / "external_evidence_8i_extended_stance_entry_gate_v1.json"
E_CONTRACT_PATH = ROOT / "configs" / "external_evidence_8i_reliability_extension_research_v1.json"
STANCE_PATH = ROOT / "configs" / "decision_universal_stance_v1.json"
TRANSITION_PATH = ROOT / "configs" / "decision_state_transition_v1.json"
PORTFOLIO_PATH = ROOT / "configs" / "decision_portfolio_action_v1.json"
REVIEW_PATH = ROOT / "artifacts" / "research" / "external_evidence_8i_evaluation_review_latest.json"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _git_blob_sha(path: str) -> str:
    return subprocess.check_output(["git", "rev-parse", f"HEAD:{path}"], cwd=ROOT, text=True).strip()


def _contracts() -> tuple[dict, dict, dict, dict, dict]:
    return _load(CONTRACT_PATH), _load(E_CONTRACT_PATH), _load(STANCE_PATH), _load(TRANSITION_PATH), _load(PORTFOLIO_PATH)


def _successful_review() -> dict:
    row = copy.deepcopy(_load(REVIEW_PATH))
    row["before_prospective_start"] = False
    row["blockers"] = []
    row["state"] = "TERMINAL_EMPIRICAL_REVIEW_COMPLETE"
    row["empirical_review_complete"] = True
    row["terminal_family_evaluation_complete"] = True
    row["terminal_outcome_gate_ready"] = True
    row["real_outcomes_opened"] = True
    row["real_outcome_values_read_by_review"] = True
    row["review_sha256"] = "f" * 64
    return row


def _successful_receipt() -> dict:
    return {
        "schema_version": EMPIRICAL_RECEIPT_SCHEMA,
        "phase": "8I-E",
        "state": APPROVED_RECEIPT_STATE,
        "manual_review_complete": True,
        "one_shot_prospective_family_consumed": True,
        "effect_size_and_uncertainty_reviewed_separately": True,
        "holm_family_reviewed": True,
        "failed_hypothesis_inversion_forbidden": True,
        "authorizes_8i_f_research_design_only": True,
        "authorizes_production": False,
        "authorizes_phase7_mutation": False,
        "authorizes_portfolio_action_change": False,
        "authorizes_orders_or_trades": False,
    }


def test_8i_f_entry_contract_is_fail_closed_and_parent_bound() -> None:
    contract, e_contract, stance, transition, portfolio = _contracts()
    validate_entry_gate_contract(contract, e_contract, stance, transition, portfolio)

    assert contract["phase"] == "8I-F"
    assert contract["status"] == "BLOCKED_WAITING_FOR_8I_E_EMPIRICAL_REVIEW"
    assert contract["scope"]["8i_f_definition"] == "EXTENDED_STANCE_PREREGISTRATION"
    assert contract["scope"]["portfolio_action_bridge_in_scope_now"] is False
    assert contract["extended_stance_enabled"] is False
    assert contract["portfolio_action_change_enabled"] is False
    assert contract["orders_or_trades_enabled"] is False
    assert contract["real_decision_outcome_read_allowed"] is False

    for parent in contract["parent_contracts"].values():
        assert _git_blob_sha(parent["path"]) == parent["git_blob_sha"]


def test_current_real_repository_state_cannot_enter_8i_f_rule_design() -> None:
    contract, e_contract, stance, transition, portfolio = _contracts()
    validate_entry_gate_contract(contract, e_contract, stance, transition, portfolio)
    review = _load(REVIEW_PATH)

    result = evaluate_entry_gate(contract, review)
    assert result["schema_version"] == GATE_RESULT_SCHEMA
    assert result["state"] == "BLOCKED_WAITING_FOR_8I_E_EMPIRICAL_REVIEW"
    assert result["eligible_to_begin_stance_preregistration"] is False
    assert "8I_E_EMPIRICAL_REVIEW_INCOMPLETE" in result["blockers"]
    assert "8I_E_TERMINAL_FAMILY_EVALUATION_INCOMPLETE" in result["blockers"]
    assert "8I_E_TERMINAL_OUTCOMES_NOT_OPENED" in result["blockers"]
    assert "MISSING_OR_INVALID_8I_E_SUCCESSFUL_EMPIRICAL_REVIEW_RECEIPT" in result["blockers"]
    assert result["extended_stance_enabled"] is False
    assert result["decision_effect"] == "NO_CHANGE_TO_PHASE7_DECISION"
    assert result["portfolio_action_change_authorized"] is False
    assert result["orders_or_trades_authorized"] is False
    assert result["real_outcome_values_read_by_8i_f_gate"] is False


def test_completed_8i_e_without_manual_success_receipt_still_fails_closed() -> None:
    contract, *_ = _contracts()
    result = evaluate_entry_gate(contract, _successful_review())
    assert result["eligible_to_begin_stance_preregistration"] is False
    assert result["blockers"] == ["MISSING_OR_INVALID_8I_E_SUCCESSFUL_EMPIRICAL_REVIEW_RECEIPT"]


def test_valid_synthetic_success_receipt_opens_only_research_design_gate() -> None:
    contract, *_ = _contracts()
    result = evaluate_entry_gate(contract, _successful_review(), _successful_receipt())
    assert result["state"] == "READY_FOR_8I_F_STANCE_PREREGISTRATION"
    assert result["eligible_to_begin_stance_preregistration"] is True
    assert result["blockers"] == []
    assert result["extended_stance_enabled"] is False
    assert result["phase7_mutation_authorized"] is False
    assert result["portfolio_action_change_authorized"] is False
    assert result["orders_or_trades_authorized"] is False
    assert result["real_outcome_values_read_by_8i_f_gate"] is False


def test_receipt_cannot_smuggle_production_or_portfolio_scope() -> None:
    contract, *_ = _contracts()
    receipt = _successful_receipt()
    receipt["authorizes_portfolio_action_change"] = True
    result = evaluate_entry_gate(contract, _successful_review(), receipt)
    assert result["eligible_to_begin_stance_preregistration"] is False
    assert "MISSING_OR_INVALID_8I_E_SUCCESSFUL_EMPIRICAL_REVIEW_RECEIPT" in result["blockers"]

    receipt = _successful_receipt()
    receipt["authorizes_orders_or_trades"] = True
    result = evaluate_entry_gate(contract, _successful_review(), receipt)
    assert result["eligible_to_begin_stance_preregistration"] is False


def test_phase7_portfolio_and_stance_boundaries_are_preserved() -> None:
    contract, _, stance, transition, portfolio = _contracts()
    guards = contract["pre_registration_guards"]
    assert guards["phase7_stance_must_remain_reconstructible"] is True
    assert guards["phase7_transition_must_remain_reconstructible"] is True
    assert guards["phase7_portfolio_action_must_remain_reconstructible"] is True
    assert guards["no_external_relation_to_portfolio_action_shortcut"] is True
    assert guards["external_only_cannot_create_portfolio_action"] is True
    assert stance["guards"]["portfolio_state_forbidden"] is True
    assert stance["guards"]["portfolio_action_forbidden"] is True
    assert transition["guards"]["no_portfolio_action"] is True
    assert portfolio["guards"]["no_broker_order_generation"] is True
    assert portfolio["guards"]["execution_allowed"] is False


def test_future_rule_design_consumes_8i_e_evidence_and_requires_fresh_validation() -> None:
    contract, *_ = _contracts()
    guards = contract["pre_registration_guards"]
    assert guards["future_8i_f_rule_design_must_mark_8i_e_evidence_spent_for_design"] is True
    assert guards["future_8i_f_validation_requires_fresh_evidence"] is True
    assert guards["no_failed_hypothesis_inversion"] is True
    assert contract["completion_gate"]["actual_8i_f_stance_preregistration_complete"] is False
    assert contract["completion_gate"]["8i_g_may_start_now"] is False
