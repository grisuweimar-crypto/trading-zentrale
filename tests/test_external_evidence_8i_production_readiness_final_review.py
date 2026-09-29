from __future__ import annotations

import copy
import json
import subprocess
from pathlib import Path

import pytest

from scanner.research.external_evidence.production_readiness_final_review_8i import (
    ALLOWED_FINAL_STATES,
    FINAL_APPROVED_STATE,
    FINAL_REVIEW_RECEIPT_SCHEMA,
    G_RECEIPT_SCHEMA,
    G_RECEIPT_STATE,
    GATE_RESULT_SCHEMA,
    ExternalEvidence8IFinalReviewError,
    evaluate_final_review_entry_gate,
    validate_final_review_contract,
    validate_final_review_receipt,
)


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "configs" / "external_evidence_8i_production_readiness_final_review_v1.json"
G_PATH = ROOT / "configs" / "external_evidence_8i_prospective_validation_entry_gate_v1.json"
P7_VALIDATION_PATH = ROOT / "configs" / "decision_validation_promotion_v1.json"
P7_PORTFOLIO_PATH = ROOT / "configs" / "decision_portfolio_action_v1.json"
A_PATH = ROOT / "configs" / "external_evidence_8i_decision_extension_contract_v1.json"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _git_blob_sha(path: str) -> str:
    return subprocess.check_output(["git", "rev-parse", f"HEAD:{path}"], cwd=ROOT, text=True).strip()


def _contracts() -> tuple[dict, dict, dict, dict, dict]:
    return _load(CONTRACT_PATH), _load(G_PATH), _load(P7_VALIDATION_PATH), _load(P7_PORTFOLIO_PATH), _load(A_PATH)


def _completed_g() -> dict:
    row = copy.deepcopy(_load(G_PATH))
    row["completion_gate"]["8i_g_validation_plan_frozen"] = True
    row["completion_gate"]["8i_g_prospective_validation_complete"] = True
    row["completion_gate"]["8i_g_holdout_complete"] = True
    row["completion_gate"]["8i_g_empirical_completion"] = True
    row["completion_gate"]["8i_h_may_start_now"] = True
    return row


def _g_receipt() -> dict:
    return {
        "schema_version": G_RECEIPT_SCHEMA,
        "phase": "8I-G",
        "state": G_RECEIPT_STATE,
        "stance_rule_id": "synthetic_8i_f_rule",
        "stance_spec_sha256": "a" * 64,
        "stance_preregistration_sha256": "b" * 64,
        "validation_plan_sha256": "c" * 64,
        "holdout_manifest_sha256": "d" * 64,
        "terminal_result_sha256": "e" * 64,
        "reviewed_at": "2027-01-01T00:00:00+00:00",
        "fresh_evidence_verified": True,
        "one_shot_holdout_consumed": True,
        "no_postfreeze_tuning": True,
        "effect_size_and_uncertainty_reviewed_separately": True,
        "multiplicity_review_complete": True,
        "failed_hypothesis_inversion_forbidden": True,
        "authorizes_8i_h_final_review_only": True,
        "authorizes_production": False,
        "authorizes_phase7_mutation": False,
        "authorizes_portfolio_action_change": False,
        "authorizes_orders_or_trades": False,
    }


def test_8i_h_contract_is_fail_closed_and_exactly_parent_bound() -> None:
    contract, g, p7v, p7p, a = _contracts()
    validate_final_review_contract(contract, g, p7v, p7p, a)
    assert contract["phase"] == "8I-H"
    assert contract["status"] == "BLOCKED_WAITING_FOR_8I_G_EMPIRICAL_COMPLETION"
    assert contract["productive_integration_enabled"] is False
    assert contract["phase7_mutation_enabled"] is False
    assert contract["portfolio_action_change_enabled"] is False
    assert contract["orders_or_trades_enabled"] is False
    for parent in contract["parent_contracts"].values():
        assert _git_blob_sha(parent["path"]) == parent["git_blob_sha"]


def test_current_real_repository_state_cannot_enter_final_review() -> None:
    contract, g, *_ = _contracts()
    result = evaluate_final_review_entry_gate(contract, g)
    assert result["schema_version"] == GATE_RESULT_SCHEMA
    assert result["state"] == "BLOCKED_WAITING_FOR_8I_G_EMPIRICAL_COMPLETION"
    assert result["eligible_to_begin_final_review"] is False
    assert "8I_G_VALIDATION_PLAN_NOT_FROZEN" in result["blockers"]
    assert "8I_G_PROSPECTIVE_VALIDATION_INCOMPLETE" in result["blockers"]
    assert "8I_G_HOLDOUT_INCOMPLETE" in result["blockers"]
    assert "8I_G_EMPIRICAL_COMPLETION_FALSE" in result["blockers"]
    assert "8I_G_HAS_NOT_RELEASED_8I_H" in result["blockers"]
    assert "MISSING_8I_G_EMPIRICAL_REVIEW_RECEIPT" in result["blockers"]
    assert result["productive_integration_enabled"] is False
    assert result["decision_effect"] == "NO_CHANGE_TO_PHASE7_DECISION"
    assert result["portfolio_action_change_authorized"] is False
    assert result["orders_or_trades_authorized"] is False


def test_synthetic_completed_g_plus_valid_receipt_opens_only_manual_final_review() -> None:
    contract, *_ = _contracts()
    result = evaluate_final_review_entry_gate(contract, _completed_g(), _g_receipt())
    assert result["state"] == "READY_FOR_8I_H_FINAL_REVIEW"
    assert result["eligible_to_begin_final_review"] is True
    assert result["blockers"] == []
    assert result["final_review_complete"] is False
    assert result["productive_integration_enabled"] is False
    assert result["phase7_mutation_authorized"] is False
    assert result["portfolio_action_change_authorized"] is False
    assert result["orders_or_trades_authorized"] is False
    assert result["separate_explicit_production_change_required"] is True
    assert result["real_outcome_values_read_by_8i_h_gate"] is False


def test_g_receipt_cannot_smuggle_production_scope() -> None:
    contract, *_ = _contracts()
    receipt = _g_receipt()
    receipt["authorizes_production"] = True
    result = evaluate_final_review_entry_gate(contract, _completed_g(), receipt)
    assert result["eligible_to_begin_final_review"] is False
    assert "8I_G_RECEIPT_FORBIDDEN_SCOPE:authorizes_production" in result["blockers"]


def test_final_review_decision_family_is_manual_and_nonactivating() -> None:
    contract, *_ = _contracts()
    assert tuple(contract["final_review_decision_model"]["allowed_states"]) == ALLOWED_FINAL_STATES
    assert contract["final_review_decision_model"]["automatic_state_change_allowed"] is False

    for state in ("CONTINUE_RESEARCH", "REJECTED", FINAL_APPROVED_STATE):
        receipt = {
            "schema_version": FINAL_REVIEW_RECEIPT_SCHEMA,
            "phase": "8I-H",
            "state": state,
            "manual_review_complete": True,
            "authorizes_production": False,
            "authorizes_phase7_mutation": False,
            "authorizes_portfolio_action_change": False,
            "authorizes_orders_or_trades": False,
            "requires_separate_explicit_production_change": state == FINAL_APPROVED_STATE,
        }
        result = validate_final_review_receipt(receipt)
        assert result["production_activation"] is False
        assert result["phase7_mutation"] is False
        assert result["portfolio_action_change"] is False
        assert result["orders_or_trades"] is False


def test_approved_final_review_still_requires_separate_change() -> None:
    receipt = {
        "schema_version": FINAL_REVIEW_RECEIPT_SCHEMA,
        "phase": "8I-H",
        "state": FINAL_APPROVED_STATE,
        "manual_review_complete": True,
        "authorizes_production": False,
        "authorizes_phase7_mutation": False,
        "authorizes_portfolio_action_change": False,
        "authorizes_orders_or_trades": False,
        "requires_separate_explicit_production_change": False,
    }
    with pytest.raises(ExternalEvidence8IFinalReviewError, match="8i_h_approval_requires_separate_change"):
        validate_final_review_receipt(receipt)


def test_phase7_and_execution_boundaries_remain_closed() -> None:
    contract, _, p7v, p7p, a = _contracts()
    assert p7v["promotion_policy"]["automatic_promotion_allowed"] is False
    assert p7v["promotion_policy"]["productive_integration_requires_separate_explicit_change_after_review"] is True
    assert p7p["guards"]["no_broker_order_generation"] is True
    assert p7p["guards"]["execution_allowed"] is False
    assert a["production_gate"]["state"] == "CLOSED"
    assert contract["production_change_boundary"]["8i_h_itself_may_modify_production_runtime"] is False
    assert contract["production_change_boundary"]["8i_h_itself_may_modify_phase7_contracts"] is False
    assert contract["production_change_boundary"]["8i_h_itself_may_enable_broker_execution"] is False
