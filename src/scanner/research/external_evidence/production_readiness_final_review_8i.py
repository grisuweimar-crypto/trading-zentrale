"""Phase 8I-H production-readiness / final-review entry gate.

This module never activates production. It only verifies whether a completed
8I-G prospective validation and one-shot holdout may enter a manual final
review. Even an approved final review still requires a separate explicit
production-integration change.
"""
from __future__ import annotations

from hashlib import sha256
import json
from typing import Any, Mapping


CONTRACT_SCHEMA = "external_evidence_8i_production_readiness_final_review_v1"
G_CONTRACT_SCHEMA = "external_evidence_8i_prospective_validation_entry_gate_v1"
G_RECEIPT_SCHEMA = "external_evidence_8i_g_empirical_review_receipt_v1"
G_RECEIPT_STATE = "APPROVED_FOR_8I_H_FINAL_REVIEW_ONLY"
GATE_RESULT_SCHEMA = "external_evidence_8i_final_review_entry_gate_result_v1"
FINAL_REVIEW_RECEIPT_SCHEMA = "external_evidence_8i_h_final_review_receipt_v1"
FINAL_APPROVED_STATE = "APPROVED_FOR_SEPARATE_PRODUCTION_INTEGRATION_CHANGE_ONLY"
ALLOWED_FINAL_STATES = (
    "NOT_REVIEWED",
    "CONTINUE_RESEARCH",
    "REJECTED",
    FINAL_APPROVED_STATE,
)


class ExternalEvidence8IFinalReviewError(ValueError):
    """Raised when the 8I-H final-review boundary is malformed or widened."""


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def digest(value: object) -> str:
    return sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _is_sha256(value: object) -> bool:
    text = str(value or "")
    return len(text) == 64 and all(ch in "0123456789abcdef" for ch in text.lower())


def validate_final_review_contract(
    contract: Mapping[str, Any],
    g_contract: Mapping[str, Any],
    phase7_validation_contract: Mapping[str, Any],
    phase7_portfolio_contract: Mapping[str, Any],
    decision_extension_contract: Mapping[str, Any],
) -> None:
    if contract.get("schema_version") != CONTRACT_SCHEMA or contract.get("phase") != "8I-H":
        raise ExternalEvidence8IFinalReviewError("unsupported_8i_h_contract")
    if contract.get("research_only") is not True or contract.get("shadow_mode") is not True:
        raise ExternalEvidence8IFinalReviewError("8i_h_must_be_research_only_shadow")

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
            raise ExternalEvidence8IFinalReviewError(f"8i_h_boundary_must_remain_false:{key}")

    scope = contract.get("scope") or {}
    if scope.get("8i_h_definition") != "PRODUCTION_READINESS_AND_FINAL_REVIEW":
        raise ExternalEvidence8IFinalReviewError("8i_h_definition_drift")
    for key in (
        "automatic_production_promotion_allowed",
        "final_review_may_enable_production_directly",
        "portfolio_action_bridge_implementation_in_scope_now",
        "broker_execution_in_scope_now",
    ):
        if scope.get(key) is not False:
            raise ExternalEvidence8IFinalReviewError(f"8i_h_scope_leak:{key}")
    if scope.get("entry_gate_only_until_8i_g_empirical_completion") is not True:
        raise ExternalEvidence8IFinalReviewError("8i_h_entry_gate_required")
    if scope.get("separate_explicit_production_change_required_after_approval") is not True:
        raise ExternalEvidence8IFinalReviewError("8i_h_separate_production_change_required")

    if g_contract.get("schema_version") != G_CONTRACT_SCHEMA or g_contract.get("phase") != "8I-G":
        raise ExternalEvidence8IFinalReviewError("8i_g_contract_required")
    g_completion = g_contract.get("completion_gate") or {}
    if g_completion.get("entry_gate_technical_completion_is_not_empirical_8i_g_validation") is not True:
        raise ExternalEvidence8IFinalReviewError("8i_g_technical_not_empirical_guard_required")
    if g_contract.get("holdout_policy", {}).get("holdout_success_is_automatic_production_promotion") is not False:
        raise ExternalEvidence8IFinalReviewError("8i_g_holdout_may_not_auto_promote")

    if phase7_validation_contract.get("schema_version") != "decision_validation_promotion_v1":
        raise ExternalEvidence8IFinalReviewError("phase7_validation_contract_required")
    promotion = phase7_validation_contract.get("promotion_policy") or {}
    if promotion.get("automatic_promotion_allowed") is not False:
        raise ExternalEvidence8IFinalReviewError("phase7_auto_promotion_must_remain_false")
    if promotion.get("productive_integration_requires_separate_explicit_change_after_review") is not True:
        raise ExternalEvidence8IFinalReviewError("phase7_separate_change_boundary_required")

    if phase7_portfolio_contract.get("schema_version") != "decision_portfolio_action_v1":
        raise ExternalEvidence8IFinalReviewError("phase7_portfolio_contract_required")
    pguards = phase7_portfolio_contract.get("guards") or {}
    if pguards.get("no_broker_order_generation") is not True or pguards.get("execution_allowed") is not False:
        raise ExternalEvidence8IFinalReviewError("phase7_execution_boundary_drift")

    if decision_extension_contract.get("schema_version") != "external_evidence_8i_decision_extension_contract_v1":
        raise ExternalEvidence8IFinalReviewError("8i_a_contract_required")
    production_gate = decision_extension_contract.get("production_gate") or {}
    if production_gate.get("state") != "CLOSED":
        raise ExternalEvidence8IFinalReviewError("8i_a_production_gate_must_remain_closed")
    if production_gate.get("research_only_shadow_until_manual_final_review") is not True:
        raise ExternalEvidence8IFinalReviewError("8i_a_manual_final_review_required")

    handoff = contract.get("required_8i_g_handoff") or {}
    for key in (
        "8i_g_validation_plan_frozen",
        "8i_g_prospective_validation_complete",
        "8i_g_holdout_complete",
        "8i_g_empirical_completion",
        "8i_h_may_start_now",
        "empirical_review_receipt_required",
        "receipt_requires_fresh_evidence_verified",
        "receipt_requires_one_shot_holdout_consumed",
        "receipt_requires_no_postfreeze_tuning",
        "receipt_requires_effect_size_and_uncertainty_reviewed_separately",
        "receipt_requires_multiplicity_review_complete",
        "receipt_requires_failed_hypothesis_inversion_forbidden",
        "receipt_authorizes_only_8i_h_final_review",
    ):
        if handoff.get(key) is not True:
            raise ExternalEvidence8IFinalReviewError(f"8i_h_handoff_requirement_missing:{key}")
    if handoff.get("required_receipt_schema") != G_RECEIPT_SCHEMA:
        raise ExternalEvidence8IFinalReviewError("8i_h_g_receipt_schema_drift")
    if handoff.get("required_receipt_state") != G_RECEIPT_STATE:
        raise ExternalEvidence8IFinalReviewError("8i_h_g_receipt_state_drift")
    for key in (
        "receipt_may_authorize_production",
        "receipt_may_authorize_phase7_mutation",
        "receipt_may_authorize_portfolio_action_change",
        "receipt_may_authorize_orders_or_trades",
    ):
        if handoff.get(key) is not False:
            raise ExternalEvidence8IFinalReviewError(f"8i_h_g_receipt_scope_must_remain_false:{key}")

    decisions = contract.get("final_review_decision_model") or {}
    if tuple(decisions.get("allowed_states") or ()) != ALLOWED_FINAL_STATES:
        raise ExternalEvidence8IFinalReviewError("8i_h_final_state_family_drift")
    if decisions.get("default_state") != "NOT_REVIEWED":
        raise ExternalEvidence8IFinalReviewError("8i_h_default_state_drift")
    for key in (
        "automatic_state_change_allowed",
        "approval_is_production_activation",
        "approval_is_phase7_mutation",
        "approval_is_portfolio_action_change",
        "approval_is_order_authorization",
        "rejection_may_be_inverted_into_opposite_signal",
    ):
        if decisions.get(key) is not False:
            raise ExternalEvidence8IFinalReviewError(f"8i_h_final_decision_scope_leak:{key}")
    if decisions.get("approved_state_requires_separate_explicit_change") is not True:
        raise ExternalEvidence8IFinalReviewError("8i_h_approved_state_must_require_separate_change")


def validate_g_empirical_review_receipt(receipt: Mapping[str, Any] | None) -> tuple[bool, list[str]]:
    if not isinstance(receipt, Mapping):
        return False, ["MISSING_8I_G_EMPIRICAL_REVIEW_RECEIPT"]
    errors: list[str] = []
    if receipt.get("schema_version") != G_RECEIPT_SCHEMA or receipt.get("phase") != "8I-G":
        errors.append("INVALID_8I_G_RECEIPT_SCHEMA")
    if receipt.get("state") != G_RECEIPT_STATE:
        errors.append("INVALID_8I_G_RECEIPT_STATE")
    if not str(receipt.get("stance_rule_id") or "").strip():
        errors.append("MISSING_8I_G_RECEIPT_FIELD:stance_rule_id")
    for key in (
        "stance_spec_sha256",
        "stance_preregistration_sha256",
        "validation_plan_sha256",
        "holdout_manifest_sha256",
        "terminal_result_sha256",
    ):
        if not _is_sha256(receipt.get(key)):
            errors.append(f"INVALID_8I_G_RECEIPT_HASH:{key}")
    if not str(receipt.get("reviewed_at") or "").strip():
        errors.append("MISSING_8I_G_RECEIPT_FIELD:reviewed_at")
    for key in (
        "fresh_evidence_verified",
        "one_shot_holdout_consumed",
        "no_postfreeze_tuning",
        "effect_size_and_uncertainty_reviewed_separately",
        "multiplicity_review_complete",
        "failed_hypothesis_inversion_forbidden",
        "authorizes_8i_h_final_review_only",
    ):
        if receipt.get(key) is not True:
            errors.append(f"8I_G_RECEIPT_REQUIREMENT_MISSING:{key}")
    for key in (
        "authorizes_production",
        "authorizes_phase7_mutation",
        "authorizes_portfolio_action_change",
        "authorizes_orders_or_trades",
    ):
        if receipt.get(key) is not False:
            errors.append(f"8I_G_RECEIPT_FORBIDDEN_SCOPE:{key}")
    return not errors, errors


def evaluate_final_review_entry_gate(
    contract: Mapping[str, Any],
    g_contract: Mapping[str, Any],
    g_empirical_review_receipt: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if g_contract.get("schema_version") != G_CONTRACT_SCHEMA or g_contract.get("phase") != "8I-G":
        raise ExternalEvidence8IFinalReviewError("8i_g_contract_required")

    blockers: list[str] = []
    completion = g_contract.get("completion_gate") or {}
    for key, code in (
        ("8i_g_validation_plan_frozen", "8I_G_VALIDATION_PLAN_NOT_FROZEN"),
        ("8i_g_prospective_validation_complete", "8I_G_PROSPECTIVE_VALIDATION_INCOMPLETE"),
        ("8i_g_holdout_complete", "8I_G_HOLDOUT_INCOMPLETE"),
        ("8i_g_empirical_completion", "8I_G_EMPIRICAL_COMPLETION_FALSE"),
        ("8i_h_may_start_now", "8I_G_HAS_NOT_RELEASED_8I_H"),
    ):
        if completion.get(key) is not True:
            blockers.append(code)

    receipt_valid, receipt_errors = validate_g_empirical_review_receipt(g_empirical_review_receipt)
    if not receipt_valid:
        blockers.extend(receipt_errors)

    eligible = not blockers
    result = {
        "schema_version": GATE_RESULT_SCHEMA,
        "phase": "8I-H",
        "state": "READY_FOR_8I_H_FINAL_REVIEW" if eligible else "BLOCKED_WAITING_FOR_8I_G_EMPIRICAL_COMPLETION",
        "eligible_to_begin_final_review": eligible,
        "blockers": blockers,
        "final_review_complete": False,
        "final_review_state": "NOT_REVIEWED",
        "productive_integration_enabled": False,
        "phase7_mutation_authorized": False,
        "decision_effect": "NO_CHANGE_TO_PHASE7_DECISION",
        "portfolio_action_change_authorized": False,
        "orders_or_trades_authorized": False,
        "separate_explicit_production_change_required": True,
        "real_outcome_values_read_by_8i_h_gate": False,
    }
    result["gate_sha256"] = digest(result)
    return result


def validate_final_review_receipt(receipt: Mapping[str, Any]) -> dict[str, Any]:
    if receipt.get("schema_version") != FINAL_REVIEW_RECEIPT_SCHEMA or receipt.get("phase") != "8I-H":
        raise ExternalEvidence8IFinalReviewError("invalid_8i_h_final_review_receipt")
    state = str(receipt.get("state") or "")
    if state not in ALLOWED_FINAL_STATES:
        raise ExternalEvidence8IFinalReviewError("invalid_8i_h_final_review_state")
    if receipt.get("manual_review_complete") is not True:
        raise ExternalEvidence8IFinalReviewError("8i_h_manual_review_required")
    if receipt.get("authorizes_production") is not False:
        raise ExternalEvidence8IFinalReviewError("8i_h_receipt_may_not_activate_production")
    if receipt.get("authorizes_phase7_mutation") is not False:
        raise ExternalEvidence8IFinalReviewError("8i_h_receipt_may_not_mutate_phase7")
    if receipt.get("authorizes_portfolio_action_change") is not False:
        raise ExternalEvidence8IFinalReviewError("8i_h_receipt_may_not_change_portfolio_action")
    if receipt.get("authorizes_orders_or_trades") is not False:
        raise ExternalEvidence8IFinalReviewError("8i_h_receipt_may_not_authorize_execution")
    if state == FINAL_APPROVED_STATE and receipt.get("requires_separate_explicit_production_change") is not True:
        raise ExternalEvidence8IFinalReviewError("8i_h_approval_requires_separate_change")

    return {
        "state": state,
        "final_review_complete": True,
        "production_activation": False,
        "phase7_mutation": False,
        "portfolio_action_change": False,
        "orders_or_trades": False,
        "separate_explicit_production_change_required": state == FINAL_APPROVED_STATE,
    }
