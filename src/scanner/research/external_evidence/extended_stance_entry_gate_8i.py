"""Phase 8I-F extended-stance preregistration entry gate.

This module does not define or activate an extended stance.  It only verifies
whether the frozen 8I-E prospective reliability research has completed the
manual empirical-review gate required before 8I-F rule design may begin.
"""
from __future__ import annotations

from hashlib import sha256
import json
from typing import Any, Mapping


CONTRACT_SCHEMA = "external_evidence_8i_extended_stance_entry_gate_v1"
REVIEW_STATUS_SCHEMA = "external_evidence_8i_reliability_review_status_v1"
EMPIRICAL_RECEIPT_SCHEMA = "external_evidence_8i_e_reliability_empirical_review_receipt_v1"
GATE_RESULT_SCHEMA = "external_evidence_8i_extended_stance_entry_gate_result_v1"
APPROVED_RECEIPT_STATE = "APPROVED_FOR_8I_F_RESEARCH_DESIGN_ONLY"


class ExternalEvidence8IStanceEntryGateError(ValueError):
    """Raised when the 8I-F entry boundary is malformed or widened."""


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def digest(value: object) -> str:
    return sha256(_stable_json(value).encode("utf-8")).hexdigest()


def validate_entry_gate_contract(
    contract: Mapping[str, Any],
    reliability_contract: Mapping[str, Any],
    phase7_stance_contract: Mapping[str, Any],
    phase7_transition_contract: Mapping[str, Any],
    phase7_portfolio_contract: Mapping[str, Any],
) -> None:
    if contract.get("schema_version") != CONTRACT_SCHEMA or contract.get("phase") != "8I-F":
        raise ExternalEvidence8IStanceEntryGateError("unsupported_8i_f_entry_gate_contract")
    if contract.get("research_only") is not True or contract.get("shadow_mode") is not True:
        raise ExternalEvidence8IStanceEntryGateError("8i_f_entry_gate_must_be_research_only_shadow")

    for key in (
        "productive_integration_enabled",
        "phase7_mutation_enabled",
        "extended_reliability_enabled",
        "extended_stance_enabled",
        "portfolio_action_change_enabled",
        "orders_or_trades_enabled",
        "real_decision_outcome_read_allowed",
    ):
        if contract.get(key) is not False:
            raise ExternalEvidence8IStanceEntryGateError(f"8i_f_boundary_must_remain_false:{key}")

    scope = contract.get("scope") or {}
    if scope.get("8i_f_definition") != "EXTENDED_STANCE_PREREGISTRATION":
        raise ExternalEvidence8IStanceEntryGateError("8i_f_definition_drift")
    if scope.get("entry_gate_only_until_8i_e_empirical_review_succeeds") is not True:
        raise ExternalEvidence8IStanceEntryGateError("8i_f_must_remain_entry_gate_only")
    for key in ("stance_rule_family_frozen_now", "stance_thresholds_frozen_now", "stance_weights_frozen_now"):
        if scope.get(key) is not False:
            raise ExternalEvidence8IStanceEntryGateError(f"premature_8i_f_rule_freeze:{key}")
    if scope.get("portfolio_action_bridge_in_scope_now") is not False:
        raise ExternalEvidence8IStanceEntryGateError("portfolio_action_bridge_must_remain_out_of_scope")
    if scope.get("portfolio_action_remains_downstream") is not True:
        raise ExternalEvidence8IStanceEntryGateError("portfolio_action_must_remain_downstream")
    if scope.get("phase7_universal_stance_remains_authoritative") is not True:
        raise ExternalEvidence8IStanceEntryGateError("phase7_stance_authority_required")

    if reliability_contract.get("schema_version") != "external_evidence_8i_reliability_extension_research_v1":
        raise ExternalEvidence8IStanceEntryGateError("8i_e_reliability_contract_required")
    completion = reliability_contract.get("completion_gate") or {}
    if completion.get("real_empirical_completion_requires_full_one_shot_prospective_family_evaluation") is not True:
        raise ExternalEvidence8IStanceEntryGateError("8i_e_one_shot_completion_gate_required")
    if completion.get("next_subblock_after_successful_empirical_review") != "8I-F_EXTENDED_STANCE_PREREGISTRATION":
        raise ExternalEvidence8IStanceEntryGateError("8i_e_to_8i_f_handoff_drift")

    if phase7_stance_contract.get("schema_version") != "decision_universal_stance_v1":
        raise ExternalEvidence8IStanceEntryGateError("phase7_universal_stance_contract_required")
    if phase7_stance_contract.get("guards", {}).get("portfolio_state_forbidden") is not True:
        raise ExternalEvidence8IStanceEntryGateError("phase7_stance_must_remain_portfolio_independent")
    if phase7_transition_contract.get("schema_version") != "decision_state_transition_v1":
        raise ExternalEvidence8IStanceEntryGateError("phase7_transition_contract_required")
    if phase7_portfolio_contract.get("schema_version") != "decision_portfolio_action_v1":
        raise ExternalEvidence8IStanceEntryGateError("phase7_portfolio_contract_required")
    if phase7_portfolio_contract.get("guards", {}).get("no_broker_order_generation") is not True:
        raise ExternalEvidence8IStanceEntryGateError("phase7_no_order_guard_required")

    requirements = contract.get("entry_requirements") or {}
    expected_true = (
        "8i_e_empirical_review_complete",
        "8i_e_terminal_family_evaluation_complete",
        "8i_e_one_shot_prospective_family_consumed",
        "successful_empirical_review_receipt_required",
        "manual_review_required",
        "effect_size_and_uncertainty_reviewed_separately",
        "holm_family_reviewed",
        "failed_hypothesis_inversion_forbidden",
    )
    for key in expected_true:
        if requirements.get(key) is not True:
            raise ExternalEvidence8IStanceEntryGateError(f"8i_f_entry_requirement_missing:{key}")
    if requirements.get("required_receipt_schema") != EMPIRICAL_RECEIPT_SCHEMA:
        raise ExternalEvidence8IStanceEntryGateError("8i_f_receipt_schema_drift")
    if requirements.get("required_receipt_state") != APPROVED_RECEIPT_STATE:
        raise ExternalEvidence8IStanceEntryGateError("8i_f_receipt_state_drift")
    for key in (
        "receipt_may_authorize_production",
        "receipt_may_authorize_phase7_mutation",
        "receipt_may_authorize_portfolio_action_change",
        "receipt_may_authorize_orders_or_trades",
    ):
        if requirements.get(key) is not False:
            raise ExternalEvidence8IStanceEntryGateError(f"8i_f_receipt_scope_must_remain_false:{key}")

    guards = contract.get("pre_registration_guards") or {}
    for key in (
        "no_stance_mapping_selected_before_entry_gate",
        "no_numeric_vote_counting",
        "no_weighted_super_score",
        "no_arbitrary_external_weights",
        "no_failed_hypothesis_inversion",
        "no_raw_external_component_to_stance_shortcut",
        "no_external_relation_to_portfolio_action_shortcut",
        "external_only_cannot_create_portfolio_action",
        "phase7_stance_must_remain_reconstructible",
        "phase7_transition_must_remain_reconstructible",
        "phase7_portfolio_action_must_remain_reconstructible",
        "future_8i_f_rule_design_must_mark_8i_e_evidence_spent_for_design",
        "future_8i_f_validation_requires_fresh_evidence",
    ):
        if guards.get(key) is not True:
            raise ExternalEvidence8IStanceEntryGateError(f"8i_f_guard_missing:{key}")


def _receipt_is_valid(receipt: Mapping[str, Any] | None) -> bool:
    if not isinstance(receipt, Mapping):
        return False
    if receipt.get("schema_version") != EMPIRICAL_RECEIPT_SCHEMA or receipt.get("phase") != "8I-E":
        return False
    if receipt.get("state") != APPROVED_RECEIPT_STATE:
        return False
    required_true = (
        "manual_review_complete",
        "one_shot_prospective_family_consumed",
        "effect_size_and_uncertainty_reviewed_separately",
        "holm_family_reviewed",
        "failed_hypothesis_inversion_forbidden",
        "authorizes_8i_f_research_design_only",
    )
    if any(receipt.get(key) is not True for key in required_true):
        return False
    forbidden_scope = (
        "authorizes_production",
        "authorizes_phase7_mutation",
        "authorizes_portfolio_action_change",
        "authorizes_orders_or_trades",
    )
    if any(receipt.get(key) is not False for key in forbidden_scope):
        return False
    return True


def evaluate_entry_gate(
    contract: Mapping[str, Any],
    review_status: Mapping[str, Any],
    empirical_review_receipt: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if review_status.get("schema_version") != REVIEW_STATUS_SCHEMA or review_status.get("phase") != "8I-E":
        raise ExternalEvidence8IStanceEntryGateError("8i_e_review_status_required")

    blockers: list[str] = []
    if review_status.get("empirical_review_complete") is not True:
        blockers.append("8I_E_EMPIRICAL_REVIEW_INCOMPLETE")
    if review_status.get("terminal_family_evaluation_complete") is not True:
        blockers.append("8I_E_TERMINAL_FAMILY_EVALUATION_INCOMPLETE")
    if review_status.get("real_outcomes_opened") is not True:
        blockers.append("8I_E_TERMINAL_OUTCOMES_NOT_OPENED")
    if not _receipt_is_valid(empirical_review_receipt):
        blockers.append("MISSING_OR_INVALID_8I_E_SUCCESSFUL_EMPIRICAL_REVIEW_RECEIPT")

    eligible = not blockers
    result = {
        "schema_version": GATE_RESULT_SCHEMA,
        "phase": "8I-F",
        "state": "READY_FOR_8I_F_STANCE_PREREGISTRATION" if eligible else "BLOCKED_WAITING_FOR_8I_E_EMPIRICAL_REVIEW",
        "eligible_to_begin_stance_preregistration": eligible,
        "blockers": blockers,
        "source_8i_e_review_sha256": review_status.get("review_sha256"),
        "extended_stance_enabled": False,
        "phase7_mutation_authorized": False,
        "decision_effect": "NO_CHANGE_TO_PHASE7_DECISION",
        "portfolio_action_change_authorized": False,
        "orders_or_trades_authorized": False,
        "real_outcome_values_read_by_8i_f_gate": False,
    }
    result["gate_sha256"] = digest(result)
    return result
