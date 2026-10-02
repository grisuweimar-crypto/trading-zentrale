"""Phase 8I-G prospective-validation / holdout entry gate.

The module deliberately does not open outcomes, run validation, select a rule,
or alter Phase 7. It only verifies whether a future frozen 8I-F stance
preregistration is eligible to hand off into a separately frozen 8I-G
prospective-validation plan.
"""
from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from typing import Any, Mapping


CONTRACT_SCHEMA = "external_evidence_8i_prospective_validation_entry_gate_v1"
F_CONTRACT_SCHEMA = "external_evidence_8i_extended_stance_entry_gate_v1"
F_RECEIPT_SCHEMA = "external_evidence_8i_f_extended_stance_preregistration_receipt_v1"
F_RECEIPT_STATE = "FROZEN_FOR_8I_G_PROSPECTIVE_VALIDATION_ONLY"
GATE_RESULT_SCHEMA = "external_evidence_8i_prospective_validation_entry_gate_result_v1"


class ExternalEvidence8IValidationEntryGateError(ValueError):
    """Raised when the frozen 8I-G validation boundary is malformed or widened."""


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def digest(value: object) -> str:
    return sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _parse_aware_time(value: object) -> datetime | None:
    text = str(value or "").strip().replace("Z", "+00:00")
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed


def _is_sha256(value: object) -> bool:
    text = str(value or "")
    if len(text) != 64:
        return False
    return all(ch in "0123456789abcdef" for ch in text.lower())


def validate_validation_entry_contract(
    contract: Mapping[str, Any],
    f_contract: Mapping[str, Any],
    phase7_validation_contract: Mapping[str, Any],
    dataset_contract: Mapping[str, Any],
    decision_extension_contract: Mapping[str, Any],
) -> None:
    if contract.get("schema_version") != CONTRACT_SCHEMA or contract.get("phase") != "8I-G":
        raise ExternalEvidence8IValidationEntryGateError("unsupported_8i_g_entry_gate_contract")
    if contract.get("research_only") is not True or contract.get("shadow_mode") is not True:
        raise ExternalEvidence8IValidationEntryGateError("8i_g_must_be_research_only_shadow")

    for key in (
        "productive_integration_enabled",
        "phase7_mutation_enabled",
        "extended_reliability_enabled",
        "extended_stance_enabled",
        "portfolio_action_change_enabled",
        "orders_or_trades_enabled",
        "prospective_validation_started",
        "holdout_opened",
        "real_decision_outcome_read_allowed",
    ):
        if contract.get(key) is not False:
            raise ExternalEvidence8IValidationEntryGateError(f"8i_g_boundary_must_remain_false:{key}")

    scope = contract.get("scope") or {}
    if scope.get("8i_g_definition") != "PROSPECTIVE_VALIDATION_AND_HOLDOUT":
        raise ExternalEvidence8IValidationEntryGateError("8i_g_definition_drift")
    if scope.get("entry_gate_only_until_8i_f_stance_preregistration_complete") is not True:
        raise ExternalEvidence8IValidationEntryGateError("8i_g_entry_gate_only_required")
    for key in (
        "validation_plan_may_be_frozen_before_8i_f_completion",
        "real_validation_may_start_before_8i_f_completion",
        "holdout_may_open_before_frozen_validation_plan",
        "portfolio_action_bridge_in_scope",
        "production_promotion_in_scope",
    ):
        if scope.get(key) is not False:
            raise ExternalEvidence8IValidationEntryGateError(f"8i_g_scope_leak:{key}")

    if f_contract.get("schema_version") != F_CONTRACT_SCHEMA or f_contract.get("phase") != "8I-F":
        raise ExternalEvidence8IValidationEntryGateError("8i_f_entry_gate_contract_required")
    f_guards = f_contract.get("pre_registration_guards") or {}
    if f_guards.get("future_8i_f_validation_requires_fresh_evidence") is not True:
        raise ExternalEvidence8IValidationEntryGateError("8i_f_fresh_validation_handoff_required")
    if f_guards.get("future_8i_f_rule_design_must_mark_8i_e_evidence_spent_for_design") is not True:
        raise ExternalEvidence8IValidationEntryGateError("8i_f_evidence_consumption_handoff_required")

    if phase7_validation_contract.get("schema_version") != "decision_validation_promotion_v1":
        raise ExternalEvidence8IValidationEntryGateError("phase7_validation_contract_required")
    inherited = phase7_validation_contract.get("inherited_statistical_rules") or {}
    expected_inherited = {
        "minimum_group_n": 30,
        "minimum_temporal_support_regions": 2,
        "uncertainty": "circular_moving_observation_date_blocks",
        "block_length_rule": "2 x evaluated horizon sessions",
    }
    for key, expected in expected_inherited.items():
        if inherited.get(key) != expected:
            raise ExternalEvidence8IValidationEntryGateError(f"phase7_validation_rule_drift:{key}")
    if list(inherited.get("horizons_sessions") or ()) != [5, 20, 40, 60]:
        raise ExternalEvidence8IValidationEntryGateError("phase7_validation_horizon_family_drift")
    p7_guards = phase7_validation_contract.get("hard_guards") or {}
    for key in ("no_holdout_reuse_for_rule_selection", "no_overlapping_windows_as_iid", "no_weighted_super_score", "no_order_generation"):
        if p7_guards.get(key) is not True:
            raise ExternalEvidence8IValidationEntryGateError(f"phase7_validation_guard_missing:{key}")

    if dataset_contract.get("schema_version") != "decision_research_dataset_v1":
        raise ExternalEvidence8IValidationEntryGateError("decision_research_dataset_required")
    if list(dataset_contract.get("horizons_sessions") or ()) != [5, 20, 40, 60]:
        raise ExternalEvidence8IValidationEntryGateError("8i_g_dataset_horizon_drift")

    if decision_extension_contract.get("schema_version") != "external_evidence_8i_decision_extension_contract_v1":
        raise ExternalEvidence8IValidationEntryGateError("8i_a_decision_extension_contract_required")
    integration = decision_extension_contract.get("integration_order") or {}
    if integration.get("direct_external_to_portfolio_action_forbidden") is not True:
        raise ExternalEvidence8IValidationEntryGateError("8i_a_direct_external_to_action_guard_required")
    if integration.get("direct_external_to_order_or_trade_forbidden") is not True:
        raise ExternalEvidence8IValidationEntryGateError("8i_a_direct_external_to_order_guard_required")

    handoff = contract.get("required_8i_f_handoff") or {}
    if handoff.get("actual_8i_f_stance_preregistration_complete") is not True:
        raise ExternalEvidence8IValidationEntryGateError("8i_f_completion_required")
    if handoff.get("8i_g_may_start_now") is not True:
        raise ExternalEvidence8IValidationEntryGateError("8i_f_explicit_g_release_required")
    if handoff.get("frozen_stance_preregistration_receipt_required") is not True:
        raise ExternalEvidence8IValidationEntryGateError("8i_f_prereg_receipt_required")
    if handoff.get("required_receipt_schema") != F_RECEIPT_SCHEMA or handoff.get("required_receipt_state") != F_RECEIPT_STATE:
        raise ExternalEvidence8IValidationEntryGateError("8i_f_receipt_contract_drift")
    if handoff.get("required_8i_e_evidence_consumption_status") != "spent_for_design":
        raise ExternalEvidence8IValidationEntryGateError("8i_e_evidence_must_be_spent_for_f_design")
    for key in (
        "receipt_may_authorize_production",
        "receipt_may_authorize_phase7_mutation",
        "receipt_may_authorize_portfolio_action_change",
        "receipt_may_authorize_orders_or_trades",
    ):
        if handoff.get(key) is not False:
            raise ExternalEvidence8IValidationEntryGateError(f"8i_f_receipt_scope_must_remain_false:{key}")

    guards = contract.get("validation_design_guards") or {}
    required_true = (
        "exact_8i_f_rule_hash_must_be_bound_before_validation_plan_freeze",
        "validation_plan_must_be_frozen_before_any_8i_g_outcome_read",
        "primary_metric_must_be_frozen_before_any_8i_g_outcome_read",
        "horizon_family_must_be_frozen_before_any_8i_g_outcome_read",
        "multiplicity_family_must_be_frozen_before_any_8i_g_outcome_read",
        "fresh_evidence_start_must_be_after_8i_f_rule_freeze",
        "backdating_fresh_evidence_start_forbidden",
        "8i_e_design_evidence_is_not_fresh_8i_g_confirmation",
        "8g_or_8h_evidence_used_upstream_is_not_automatically_fresh_8i_g_confirmation",
        "failed_hypothesis_inversion_forbidden",
        "one_shot_holdout_required",
        "automatic_promotion_from_significance_forbidden",
        "effect_size_and_uncertainty_reported_separately",
        "snapshot_identity_required",
    )
    for key in required_true:
        if guards.get(key) is not True:
            raise ExternalEvidence8IValidationEntryGateError(f"8i_g_guard_missing:{key}")
    required_false = (
        "validation_may_tune_rule",
        "holdout_may_tune_rule",
        "holdout_may_select_rule",
        "validation_or_holdout_may_change_sign",
        "validation_or_holdout_may_change_threshold",
        "validation_or_holdout_may_change_horizon",
        "validation_or_holdout_may_change_variant",
        "repeated_interim_significance_looks_allowed",
        "overlapping_forward_windows_are_iid",
        "date_only_join_allowed",
    )
    for key in required_false:
        if guards.get(key) is not False:
            raise ExternalEvidence8IValidationEntryGateError(f"8i_g_guard_must_remain_false:{key}")

    holdout = contract.get("holdout_policy") or {}
    for key in (
        "holdout_exists_only_after_frozen_8i_g_validation_plan",
        "holdout_identity_must_be_frozen_before_outcome_read",
        "holdout_outcomes_open_once",
    ):
        if holdout.get(key) is not True:
            raise ExternalEvidence8IValidationEntryGateError(f"8i_g_holdout_guard_missing:{key}")
    for key in (
        "holdout_used_for_rule_selection",
        "holdout_used_for_threshold_tuning",
        "holdout_used_for_sign_tuning",
        "holdout_used_for_horizon_tuning",
        "holdout_used_for_variant_tuning",
        "holdout_failure_may_be_relabelled_as_opposite_success",
        "holdout_success_is_automatic_production_promotion",
    ):
        if holdout.get(key) is not False:
            raise ExternalEvidence8IValidationEntryGateError(f"8i_g_holdout_scope_leak:{key}")


def validate_f_preregistration_receipt(receipt: Mapping[str, Any] | None) -> tuple[bool, list[str]]:
    errors: list[str] = []
    if not isinstance(receipt, Mapping):
        return False, ["MISSING_8I_F_STANCE_PREREGISTRATION_RECEIPT"]
    if receipt.get("schema_version") != F_RECEIPT_SCHEMA or receipt.get("phase") != "8I-F":
        errors.append("INVALID_8I_F_RECEIPT_SCHEMA")
    if receipt.get("state") != F_RECEIPT_STATE:
        errors.append("INVALID_8I_F_RECEIPT_STATE")
    for key in ("stance_rule_id", "stance_spec_version"):
        if not str(receipt.get(key) or "").strip():
            errors.append(f"MISSING_8I_F_RECEIPT_FIELD:{key}")
    for key in ("stance_spec_sha256", "stance_preregistration_sha256"):
        if not _is_sha256(receipt.get(key)):
            errors.append(f"INVALID_8I_F_RECEIPT_HASH:{key}")
    if _parse_aware_time(receipt.get("frozen_at")) is None:
        errors.append("INVALID_8I_F_RECEIPT_FROZEN_AT")
    if receipt.get("8i_e_evidence_consumption_status") != "spent_for_design":
        errors.append("8I_E_EVIDENCE_NOT_MARKED_SPENT_FOR_DESIGN")
    if receipt.get("authorizes_8i_g_validation_plan_only") is not True:
        errors.append("8I_F_RECEIPT_DOES_NOT_AUTHORIZE_8I_G_PLAN_ONLY")
    for key in (
        "authorizes_production",
        "authorizes_phase7_mutation",
        "authorizes_portfolio_action_change",
        "authorizes_orders_or_trades",
    ):
        if receipt.get(key) is not False:
            errors.append(f"8I_F_RECEIPT_FORBIDDEN_SCOPE:{key}")
    return not errors, errors


def evaluate_validation_entry_gate(
    contract: Mapping[str, Any],
    f_contract: Mapping[str, Any],
    f_preregistration_receipt: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if f_contract.get("schema_version") != F_CONTRACT_SCHEMA or f_contract.get("phase") != "8I-F":
        raise ExternalEvidence8IValidationEntryGateError("8i_f_contract_required")

    blockers: list[str] = []
    completion = f_contract.get("completion_gate") or {}
    if completion.get("actual_8i_f_stance_preregistration_complete") is not True:
        blockers.append("8I_F_STANCE_PREREGISTRATION_INCOMPLETE")
    if completion.get("8i_g_may_start_now") is not True:
        blockers.append("8I_F_HAS_NOT_RELEASED_8I_G")

    receipt_valid, receipt_errors = validate_f_preregistration_receipt(f_preregistration_receipt)
    if not receipt_valid:
        blockers.extend(receipt_errors)

    eligible = not blockers
    frozen_at = None if not receipt_valid else str(f_preregistration_receipt.get("frozen_at"))
    result = {
        "schema_version": GATE_RESULT_SCHEMA,
        "phase": "8I-G",
        "state": "READY_TO_FREEZE_8I_G_VALIDATION_PLAN" if eligible else "BLOCKED_WAITING_FOR_8I_F_STANCE_PREREGISTRATION",
        "eligible_to_freeze_8i_g_validation_plan": eligible,
        "blockers": blockers,
        "bound_8i_f_stance_rule_id": None if not receipt_valid else f_preregistration_receipt.get("stance_rule_id"),
        "bound_8i_f_stance_spec_sha256": None if not receipt_valid else f_preregistration_receipt.get("stance_spec_sha256"),
        "bound_8i_f_stance_preregistration_sha256": None if not receipt_valid else f_preregistration_receipt.get("stance_preregistration_sha256"),
        "fresh_evidence_boundary": None if frozen_at is None else f"STRICTLY_AFTER:{frozen_at}",
        "validation_plan_frozen": False,
        "prospective_validation_started": False,
        "holdout_opened": False,
        "real_outcomes_opened": False,
        "real_outcome_values_read_by_8i_g_gate": False,
        "extended_stance_enabled": False,
        "phase7_mutation_authorized": False,
        "decision_effect": "NO_CHANGE_TO_PHASE7_DECISION",
        "portfolio_action_change_authorized": False,
        "orders_or_trades_authorized": False,
    }
    result["gate_sha256"] = digest(result)
    return result
