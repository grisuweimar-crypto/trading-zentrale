from __future__ import annotations

import copy
import json
import subprocess
from pathlib import Path

import pytest

from scanner.research.external_evidence.prospective_validation_entry_gate_8i import (
    F_RECEIPT_SCHEMA,
    F_RECEIPT_STATE,
    GATE_RESULT_SCHEMA,
    ExternalEvidence8IValidationEntryGateError,
    evaluate_validation_entry_gate,
    validate_f_preregistration_receipt,
    validate_validation_entry_contract,
)


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "configs" / "external_evidence_8i_prospective_validation_entry_gate_v1.json"
F_PATH = ROOT / "configs" / "external_evidence_8i_extended_stance_entry_gate_v1.json"
P7_VALIDATION_PATH = ROOT / "configs" / "decision_validation_promotion_v1.json"
DATASET_PATH = ROOT / "configs" / "decision_research_dataset_v1.json"
A_PATH = ROOT / "configs" / "external_evidence_8i_decision_extension_contract_v1.json"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _git_blob_sha(path: str) -> str:
    return subprocess.check_output(["git", "rev-parse", f"HEAD:{path}"], cwd=ROOT, text=True).strip()


def _contracts() -> tuple[dict, dict, dict, dict, dict]:
    return _load(CONTRACT_PATH), _load(F_PATH), _load(P7_VALIDATION_PATH), _load(DATASET_PATH), _load(A_PATH)


def _released_f_contract() -> dict:
    row = copy.deepcopy(_load(F_PATH))
    row["completion_gate"]["actual_8i_f_stance_preregistration_complete"] = True
    row["completion_gate"]["empirical_8i_e_gate_open_now"] = True
    row["completion_gate"]["8i_g_may_start_now"] = True
    row["completion_gate"]["next_state"] = "8I-G_PROSPECTIVE_VALIDATION_PLAN"
    return row


def _valid_f_receipt() -> dict:
    return {
        "schema_version": F_RECEIPT_SCHEMA,
        "phase": "8I-F",
        "state": F_RECEIPT_STATE,
        "stance_rule_id": "synthetic_extended_stance_rule_v1",
        "stance_spec_version": "synthetic_spec_v1",
        "stance_spec_sha256": "a" * 64,
        "stance_preregistration_sha256": "b" * 64,
        "frozen_at": "2026-12-01T18:00:00+00:00",
        "8i_e_evidence_consumption_status": "spent_for_design",
        "authorizes_8i_g_validation_plan_only": True,
        "authorizes_production": False,
        "authorizes_phase7_mutation": False,
        "authorizes_portfolio_action_change": False,
        "authorizes_orders_or_trades": False,
    }


def test_8i_g_contract_is_fail_closed_and_exactly_parent_bound() -> None:
    contract, f_contract, p7_validation, dataset, decision_extension = _contracts()
    validate_validation_entry_contract(contract, f_contract, p7_validation, dataset, decision_extension)

    assert contract["phase"] == "8I-G"
    assert contract["status"] == "BLOCKED_WAITING_FOR_8I_F_STANCE_PREREGISTRATION"
    assert contract["scope"]["8i_g_definition"] == "PROSPECTIVE_VALIDATION_AND_HOLDOUT"
    assert contract["prospective_validation_started"] is False
    assert contract["holdout_opened"] is False
    assert contract["real_decision_outcome_read_allowed"] is False
    assert contract["extended_stance_enabled"] is False
    assert contract["portfolio_action_change_enabled"] is False
    assert contract["orders_or_trades_enabled"] is False

    for parent in contract["parent_contracts"].values():
        assert _git_blob_sha(parent["path"]) == parent["git_blob_sha"]


def test_current_real_repository_state_cannot_start_8i_g() -> None:
    contract, f_contract, *_ = _contracts()
    result = evaluate_validation_entry_gate(contract, f_contract)

    assert result["schema_version"] == GATE_RESULT_SCHEMA
    assert result["state"] == "BLOCKED_WAITING_FOR_8I_F_STANCE_PREREGISTRATION"
    assert result["eligible_to_freeze_8i_g_validation_plan"] is False
    assert "8I_F_STANCE_PREREGISTRATION_INCOMPLETE" in result["blockers"]
    assert "8I_F_HAS_NOT_RELEASED_8I_G" in result["blockers"]
    assert "MISSING_8I_F_STANCE_PREREGISTRATION_RECEIPT" in result["blockers"]
    assert result["validation_plan_frozen"] is False
    assert result["prospective_validation_started"] is False
    assert result["holdout_opened"] is False
    assert result["real_outcomes_opened"] is False
    assert result["real_outcome_values_read_by_8i_g_gate"] is False
    assert result["decision_effect"] == "NO_CHANGE_TO_PHASE7_DECISION"
    assert result["portfolio_action_change_authorized"] is False
    assert result["orders_or_trades_authorized"] is False


def test_released_8i_f_without_exact_receipt_still_fails_closed() -> None:
    contract, *_ = _contracts()
    result = evaluate_validation_entry_gate(contract, _released_f_contract())
    assert result["eligible_to_freeze_8i_g_validation_plan"] is False
    assert result["blockers"] == ["MISSING_8I_F_STANCE_PREREGISTRATION_RECEIPT"]


def test_valid_synthetic_f_handoff_opens_only_validation_plan_freeze_gate() -> None:
    contract, *_ = _contracts()
    receipt = _valid_f_receipt()
    result = evaluate_validation_entry_gate(contract, _released_f_contract(), receipt)

    assert result["state"] == "READY_TO_FREEZE_8I_G_VALIDATION_PLAN"
    assert result["eligible_to_freeze_8i_g_validation_plan"] is True
    assert result["blockers"] == []
    assert result["bound_8i_f_stance_rule_id"] == receipt["stance_rule_id"]
    assert result["bound_8i_f_stance_spec_sha256"] == receipt["stance_spec_sha256"]
    assert result["bound_8i_f_stance_preregistration_sha256"] == receipt["stance_preregistration_sha256"]
    assert result["fresh_evidence_boundary"] == "STRICTLY_AFTER:2026-12-01T18:00:00+00:00"
    assert result["validation_plan_frozen"] is False
    assert result["prospective_validation_started"] is False
    assert result["holdout_opened"] is False
    assert result["real_outcomes_opened"] is False
    assert result["extended_stance_enabled"] is False
    assert result["phase7_mutation_authorized"] is False
    assert result["portfolio_action_change_authorized"] is False
    assert result["orders_or_trades_authorized"] is False


def test_f_receipt_requires_spent_design_evidence_hashes_and_timezone() -> None:
    receipt = _valid_f_receipt()
    valid, errors = validate_f_preregistration_receipt(receipt)
    assert valid is True
    assert errors == []

    bad = copy.deepcopy(receipt)
    bad["8i_e_evidence_consumption_status"] = "fresh_for_8i_g"
    valid, errors = validate_f_preregistration_receipt(bad)
    assert valid is False
    assert "8I_E_EVIDENCE_NOT_MARKED_SPENT_FOR_DESIGN" in errors

    bad = copy.deepcopy(receipt)
    bad["stance_spec_sha256"] = "abc"
    valid, errors = validate_f_preregistration_receipt(bad)
    assert valid is False
    assert "INVALID_8I_F_RECEIPT_HASH:stance_spec_sha256" in errors

    bad = copy.deepcopy(receipt)
    bad["frozen_at"] = "2026-12-01T18:00:00"
    valid, errors = validate_f_preregistration_receipt(bad)
    assert valid is False
    assert "INVALID_8I_F_RECEIPT_FROZEN_AT" in errors


def test_f_receipt_cannot_smuggle_production_portfolio_or_order_scope() -> None:
    contract, *_ = _contracts()
    for key in ("authorizes_production", "authorizes_phase7_mutation", "authorizes_portfolio_action_change", "authorizes_orders_or_trades"):
        receipt = _valid_f_receipt()
        receipt[key] = True
        result = evaluate_validation_entry_gate(contract, _released_f_contract(), receipt)
        assert result["eligible_to_freeze_8i_g_validation_plan"] is False
        assert any(item.startswith("8I_F_RECEIPT_FORBIDDEN_SCOPE:") for item in result["blockers"])


def test_validation_and_holdout_methodology_is_frozen_before_any_outcome_read() -> None:
    contract, *_ = _contracts()
    guards = contract["validation_design_guards"]
    assert guards["validation_plan_must_be_frozen_before_any_8i_g_outcome_read"] is True
    assert guards["primary_metric_must_be_frozen_before_any_8i_g_outcome_read"] is True
    assert guards["horizon_family_must_be_frozen_before_any_8i_g_outcome_read"] is True
    assert guards["multiplicity_family_must_be_frozen_before_any_8i_g_outcome_read"] is True
    assert guards["fresh_evidence_start_must_be_after_8i_f_rule_freeze"] is True
    assert guards["backdating_fresh_evidence_start_forbidden"] is True
    assert guards["8i_e_design_evidence_is_not_fresh_8i_g_confirmation"] is True
    assert guards["validation_may_tune_rule"] is False
    assert guards["holdout_may_select_rule"] is False
    assert guards["repeated_interim_significance_looks_allowed"] is False
    assert guards["automatic_promotion_from_significance_forbidden"] is True
    assert guards["overlapping_forward_windows_are_iid"] is False

    floor = contract["inherited_statistical_floor"]
    assert floor["minimum_group_n"] == 30
    assert floor["minimum_temporal_support_regions"] == 2
    assert floor["block_length_rule"] == "2 x evaluated horizon sessions"
    assert floor["allowed_reference_horizons_sessions"] == [5, 20, 40, 60]

    holdout = contract["holdout_policy"]
    assert holdout["holdout_outcomes_open_once"] is True
    assert holdout["holdout_used_for_rule_selection"] is False
    assert holdout["holdout_used_for_threshold_tuning"] is False
    assert holdout["holdout_failure_may_be_relabelled_as_opposite_success"] is False
    assert holdout["holdout_success_is_automatic_production_promotion"] is False


def test_direct_external_or_validation_to_portfolio_action_remains_forbidden() -> None:
    contract, *_ = _contracts()
    guards = contract["decision_layer_guards"]
    assert guards["phase7_universal_stance_remains_reconstructible"] is True
    assert guards["phase7_state_transition_remains_reconstructible"] is True
    assert guards["phase7_portfolio_action_remains_reconstructible"] is True
    assert guards["8i_f_challenger_remains_separate_from_phase7_baseline"] is True
    assert guards["external_evidence_to_portfolio_action_shortcut_forbidden"] is True
    assert guards["validation_result_to_order_shortcut_forbidden"] is True
    assert guards["portfolio_action_change_requires_later_separate_gate"] is True
    assert guards["broker_execution_forbidden"] is True
    assert contract["completion_gate"]["8i_h_may_start_now"] is False


def test_contract_rejects_posthoc_or_automatic_promotion_drift() -> None:
    contract, f_contract, p7_validation, dataset, decision_extension = _contracts()

    bad = copy.deepcopy(contract)
    bad["validation_design_guards"]["holdout_may_select_rule"] = True
    with pytest.raises(ExternalEvidence8IValidationEntryGateError):
        validate_validation_entry_contract(bad, f_contract, p7_validation, dataset, decision_extension)

    bad = copy.deepcopy(contract)
    bad["validation_design_guards"]["automatic_promotion_from_significance_forbidden"] = False
    with pytest.raises(ExternalEvidence8IValidationEntryGateError):
        validate_validation_entry_contract(bad, f_contract, p7_validation, dataset, decision_extension)
