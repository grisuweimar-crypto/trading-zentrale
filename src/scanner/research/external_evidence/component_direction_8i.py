"""Phase 8I-D outcome-blind component direction state engine.

The engine maps the incremental prediction displacement of an exact frozen,
promoted model pair to POSITIVE / NEGATIVE / UNKNOWN.  It does not interpret
raw macro signs, read realized outcomes, aggregate components, change Phase 7,
or authorize portfolio/order effects.  Real execution remains closed; the
implementation is executable only for synthetic contract tests in 8I-D.
"""
from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
import math
import re
from typing import Any, Mapping

from scanner.research.external_evidence.decision_binding_8i import (
    BINDING_RESULT_SCHEMA,
    BOUND,
    digest as binding_digest,
    validate_binding_contract,
)


DIRECTION_CONTRACT_SCHEMA = "external_evidence_8i_component_direction_state_engine_v1"
DIRECTION_RESULT_SCHEMA = "external_evidence_8i_component_direction_state_v1"
AGGREGATION_CONTRACT_SCHEMA = "external_evidence_8i_external_aggregation_design_v1"

ADAPTER_ID = "incremental_prediction_delta_sign"
ADAPTER_VERSION = "v1"
ADAPTER_SEMANTICS = "MODEL_PAIR_INCREMENTAL_PREDICTION_DIRECTION_NOT_CAUSAL_FACTOR_EFFECT"

POSITIVE = "POSITIVE"
NEGATIVE = "NEGATIVE"
UNKNOWN = "UNKNOWN"
USABLE = "USABLE"
UNKNOWN_VALID = "UNKNOWN_VALID"
UNAVAILABLE = "UNAVAILABLE"

ALLOWED_COMPONENT_TYPES = {"8g_main_effect", "8h_interaction"}
ALLOWED_PREDICTION_STATUSES = {"AVAILABLE", "UNAVAILABLE"}
ALLOWED_PIT_STATUSES = {
    "PIT_ELIGIBLE",
    "PIT_INELIGIBLE_MISSING",
    "PIT_INELIGIBLE_STALE",
    "PIT_INELIGIBLE_MAPPING",
    "PIT_INELIGIBLE_SOURCE",
}
FORBIDDEN_INPUT_KEY_PARTS = (
    "observed_peer_excess",
    "realized_return",
    "adverse_excursion",
    "path_max_drawdown",
    "future_return",
    "forward_return",
    "forward_label",
    "loss_improvement",
    "p_value",
    "holm_adjusted_p",
    "confidence_interval",
    "phase7_stance",
    "universal_stance",
    "portfolio_action",
    "trade_decision",
    "order_instruction",
    "entry_price",
    "position_size",
    "target_weight",
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class ExternalEvidence8IDirectionError(ValueError):
    """Raised when the frozen 8I-D direction-state boundary is violated."""


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def digest(value: object) -> str:
    return sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _require_sha256(value: object, error: str) -> str:
    text = str(value or "").strip().lower()
    if not _SHA256_RE.fullmatch(text):
        raise ExternalEvidence8IDirectionError(error)
    return text


def _parse_time(value: object, error: str) -> datetime:
    text = str(value or "").strip().replace("Z", "+00:00")
    if not text:
        raise ExternalEvidence8IDirectionError(error)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ExternalEvidence8IDirectionError(error) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExternalEvidence8IDirectionError(error + "_timezone_required")
    return parsed


def _adapter_payload(contract: Mapping[str, Any]) -> dict[str, Any]:
    adapter = dict(contract.get("direction_adapter") or {})
    adapter.pop("direction_adapter_sha256", None)
    return adapter


def _verify_embedded_model(model: Mapping[str, Any], label: str) -> str:
    recorded = _require_sha256(model.get("model_sha256"), f"8i_d_{label}_model_sha256_required")
    payload = dict(model)
    payload.pop("model_sha256", None)
    if digest(payload) != recorded:
        raise ExternalEvidence8IDirectionError(f"8i_d_{label}_model_digest_mismatch")
    return recorded


def _verify_binding(binding: Mapping[str, Any]) -> str:
    if binding.get("schema_version") != BINDING_RESULT_SCHEMA:
        raise ExternalEvidence8IDirectionError("8i_d_binding_schema_mismatch")
    if binding.get("phase") != "8I-B" or binding.get("state") != BOUND:
        raise ExternalEvidence8IDirectionError("8i_d_component_not_bound_for_8i_research_only")
    recorded = _require_sha256(binding.get("binding_sha256"), "8i_d_binding_sha256_required")
    payload = dict(binding)
    payload.pop("binding_sha256", None)
    if binding_digest(payload) != recorded:
        raise ExternalEvidence8IDirectionError("8i_d_binding_digest_mismatch")
    for key in (
        "external_decision_influence_enabled",
        "decision_outcome_access_authorized",
        "phase7_mutation_authorized",
        "productive_integration_enabled",
        "portfolio_action_change_authorized",
        "orders_or_trades_authorized",
    ):
        if binding.get(key) is not False:
            raise ExternalEvidence8IDirectionError(f"8i_d_binding_scope_drift:{key}")
    return recorded


def validate_direction_contract(
    contract: Mapping[str, Any],
    binding_contract: Mapping[str, Any],
    aggregation_contract: Mapping[str, Any],
) -> None:
    if contract.get("schema_version") != DIRECTION_CONTRACT_SCHEMA or contract.get("phase") != "8I-D":
        raise ExternalEvidence8IDirectionError("unsupported_8i_d_contract")
    if contract.get("research_only") is not True or contract.get("shadow_mode") is not True:
        raise ExternalEvidence8IDirectionError("8i_d_must_be_research_only_shadow")
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
            raise ExternalEvidence8IDirectionError(f"8i_d_boundary_must_remain_false:{key}")

    validate_binding_contract(binding_contract)
    if aggregation_contract.get("schema_version") != AGGREGATION_CONTRACT_SCHEMA or aggregation_contract.get("phase") != "8I-C":
        raise ExternalEvidence8IDirectionError("8i_d_8i_c_contract_required")
    boundary = aggregation_contract.get("component_direction_boundary") or {}
    if boundary.get("real_direction_adapter_current_state") != "NOT_YET_FROZEN":
        raise ExternalEvidence8IDirectionError("8i_d_requires_8i_c_unfrozen_direction_adapter_entry_state")
    if boundary.get("real_component_direction_state_generation_belongs_to") != "8I-D_COMPONENT_DIRECTION_STATE_ENGINE_PREREGISTRATION":
        raise ExternalEvidence8IDirectionError("8i_c_does_not_delegate_direction_preregistration_to_8i_d")

    scope = contract.get("scope") or {}
    if scope.get("8i_d_is_preregistration_and_synthetic_engine_only") is not True:
        raise ExternalEvidence8IDirectionError("8i_d_scope_must_be_preregistration_only")
    if scope.get("synthetic_contract_execution_allowed") is not True:
        raise ExternalEvidence8IDirectionError("8i_d_synthetic_contract_execution_required")
    for key in (
        "real_bound_component_direction_generation_enabled_now",
        "aggregation_in_8i_d_allowed",
        "relation_to_phase7_in_8i_d_allowed",
        "reliability_recompute_in_8i_d_allowed",
        "stance_recompute_in_8i_d_allowed",
        "portfolio_action_recompute_in_8i_d_allowed",
    ):
        if scope.get(key) is not False:
            raise ExternalEvidence8IDirectionError(f"8i_d_scope_leak:{key}")

    adapter = contract.get("direction_adapter") or {}
    if adapter.get("direction_adapter_id") != ADAPTER_ID or adapter.get("direction_adapter_version") != ADAPTER_VERSION:
        raise ExternalEvidence8IDirectionError("8i_d_direction_adapter_identity_drift")
    if adapter.get("semantics") != ADAPTER_SEMANTICS:
        raise ExternalEvidence8IDirectionError("8i_d_direction_adapter_semantics_drift")
    adapter_sha = _require_sha256(adapter.get("direction_adapter_sha256"), "8i_d_direction_adapter_sha256_required")
    if digest(_adapter_payload(contract)) != adapter_sha:
        raise ExternalEvidence8IDirectionError("8i_d_direction_adapter_digest_mismatch")
    if adapter.get("target_template") != "peer_excess_{H}t":
        raise ExternalEvidence8IDirectionError("8i_d_target_template_drift")
    formulas = adapter.get("component_formulas") or {}
    if formulas != {
        "8g_main_effect": "challenger_prediction_minus_baseline_prediction",
        "8h_interaction": "challenger_prediction_minus_main_effects_baseline_prediction",
    }:
        raise ExternalEvidence8IDirectionError("8i_d_component_formula_drift")
    if adapter.get("zero_deadband") is not None or adapter.get("absolute_or_relative_threshold") is not None:
        raise ExternalEvidence8IDirectionError("8i_d_threshold_or_deadband_forbidden")
    for key in (
        "round_before_sign",
        "semantic_factor_sign_assignment",
        "raw_factor_value_sign_used",
        "coefficient_sign_used",
        "p_value_or_significance_used",
        "observed_outcome_used_at_runtime",
    ):
        if adapter.get(key) is not False:
            raise ExternalEvidence8IDirectionError(f"8i_d_forbidden_adapter_property:{key}")

    guards = contract.get("runtime_guards") or {}
    if any(value is not False for value in guards.values()):
        raise ExternalEvidence8IDirectionError("8i_d_runtime_guards_must_all_remain_false")


def current_direction_status(
    contract: Mapping[str, Any],
    binding_contract: Mapping[str, Any],
    aggregation_contract: Mapping[str, Any],
) -> dict[str, Any]:
    validate_direction_contract(contract, binding_contract, aggregation_contract)
    current = dict(contract.get("current_repository_state") or {})
    result = {
        "schema_version": DIRECTION_RESULT_SCHEMA,
        "phase": "8I-D",
        "state": str(current.get("state") or ""),
        "synthetic": False,
        "real_bound_component_ids": list(current.get("real_bound_component_ids") or ()),
        "direction_adapter_id": ADAPTER_ID,
        "direction_adapter_version": ADAPTER_VERSION,
        "direction_adapter_sha256": str(contract["direction_adapter"]["direction_adapter_sha256"]),
        "real_bound_component_direction_generation_authorized": False,
        "external_direction_state": "INSUFFICIENT_EXTERNAL",
        "external_evidence_state": "INSUFFICIENT_EXTERNAL",
        "decision_effect": "NO_CHANGE_TO_PHASE7_DECISION",
        "external_decision_influence_enabled": False,
        "phase7_mutation_authorized": False,
        "extended_reliability_enabled": False,
        "extended_stance_enabled": False,
        "portfolio_action_change_authorized": False,
        "orders_or_trades_authorized": False,
    }
    result["direction_status_sha256"] = digest(result)
    return result


def _reject_forbidden_keys(record: Mapping[str, Any]) -> None:
    for key in record:
        lowered = str(key).lower()
        if any(part in lowered for part in FORBIDDEN_INPUT_KEY_PARTS):
            raise ExternalEvidence8IDirectionError(f"8i_d_forbidden_outcome_or_decision_input:{key}")


def _model_pair_hashes(model_pair_artifact: Mapping[str, Any]) -> tuple[str, str, str]:
    baseline = model_pair_artifact.get("baseline_model")
    challenger = model_pair_artifact.get("challenger_model")
    if not isinstance(baseline, Mapping) or not isinstance(challenger, Mapping):
        raise ExternalEvidence8IDirectionError("8i_d_model_pair_requires_baseline_and_challenger_models")
    baseline_sha = _verify_embedded_model(baseline, "baseline")
    challenger_sha = _verify_embedded_model(challenger, "challenger")
    return baseline_sha, challenger_sha, digest(model_pair_artifact)


def derive_component_direction(
    *,
    contract: Mapping[str, Any],
    binding_contract: Mapping[str, Any],
    aggregation_contract: Mapping[str, Any],
    binding: Mapping[str, Any],
    prediction_record: Mapping[str, Any],
    model_pair_artifact: Mapping[str, Any],
    synthetic: bool,
) -> dict[str, Any]:
    """Derive one standardized direction state from a frozen prediction pair.

    Only synthetic execution is authorized by 8I-D.  The formula is fixed before
    any 8I reliability/decision outcome may be opened.
    """
    validate_direction_contract(contract, binding_contract, aggregation_contract)
    if synthetic is not True:
        raise ExternalEvidence8IDirectionError("8i_d_real_direction_generation_not_authorized")

    binding_sha = _verify_binding(binding)
    _reject_forbidden_keys(prediction_record)

    component_type = str(prediction_record.get("component_type") or "")
    component_id = str(prediction_record.get("component_id") or "")
    if component_type not in ALLOWED_COMPONENT_TYPES:
        raise ExternalEvidence8IDirectionError("8i_d_component_type_invalid")
    if component_type != str(binding.get("component_type") or "") or component_id != str(binding.get("component_id") or ""):
        raise ExternalEvidence8IDirectionError("8i_d_component_identity_mismatch")
    if str(prediction_record.get("binding_sha256") or "") != binding_sha:
        raise ExternalEvidence8IDirectionError("8i_d_prediction_binding_sha_mismatch")

    try:
        horizon = int(prediction_record.get("horizon_sessions"))
    except (TypeError, ValueError) as exc:
        raise ExternalEvidence8IDirectionError("8i_d_horizon_required") from exc
    if horizon != int(binding.get("horizon_sessions", -1)) or horizon <= 0:
        raise ExternalEvidence8IDirectionError("8i_d_horizon_binding_mismatch")
    snapshot_id = str(prediction_record.get("snapshot_id") or "").strip()
    symbol = str(prediction_record.get("symbol") or "").strip()
    if not snapshot_id or not symbol:
        raise ExternalEvidence8IDirectionError("8i_d_snapshot_symbol_identity_required")

    generated_at = _parse_time(prediction_record.get("generated_at"), "8i_d_generated_at_invalid")
    feature_valid_from = _parse_time(prediction_record.get("feature_valid_from"), "8i_d_feature_valid_from_invalid")
    if feature_valid_from > generated_at:
        raise ExternalEvidence8IDirectionError("8i_d_feature_valid_from_after_snapshot")
    binding_valid_from = _parse_time(binding.get("valid_from"), "8i_d_binding_valid_from_invalid")
    if binding_valid_from > generated_at:
        raise ExternalEvidence8IDirectionError("8i_d_binding_valid_from_after_snapshot")

    expected_target = f"peer_excess_{horizon}t"
    if str(prediction_record.get("target") or "") != expected_target:
        raise ExternalEvidence8IDirectionError("8i_d_prediction_target_mismatch")

    baseline_sha, challenger_sha, model_pair_sha = _model_pair_hashes(model_pair_artifact)
    bound_component_hash = _require_sha256(binding.get("component_artifact_hash"), "8i_d_bound_component_artifact_hash_required")
    if model_pair_sha != bound_component_hash:
        raise ExternalEvidence8IDirectionError("8i_d_model_pair_not_exact_bound_component_artifact")
    if _require_sha256(prediction_record.get("model_pair_artifact_sha256"), "8i_d_model_pair_artifact_sha256_required") != model_pair_sha:
        raise ExternalEvidence8IDirectionError("8i_d_model_pair_artifact_digest_mismatch")
    if _require_sha256(prediction_record.get("baseline_model_sha256"), "8i_d_baseline_model_sha256_required") != baseline_sha:
        raise ExternalEvidence8IDirectionError("8i_d_baseline_model_identity_mismatch")
    if _require_sha256(prediction_record.get("challenger_model_sha256"), "8i_d_challenger_model_sha256_required") != challenger_sha:
        raise ExternalEvidence8IDirectionError("8i_d_challenger_model_identity_mismatch")

    prediction_status = str(prediction_record.get("prediction_status") or "")
    if prediction_status not in ALLOWED_PREDICTION_STATUSES:
        raise ExternalEvidence8IDirectionError("8i_d_prediction_status_invalid")
    pit_status = str(prediction_record.get("input_pit_status") or "")
    if pit_status not in ALLOWED_PIT_STATUSES:
        raise ExternalEvidence8IDirectionError("8i_d_input_pit_status_invalid")

    baseline_value: float | None = None
    challenger_value: float | None = None
    delta: float | None = None
    direction: str | None = None
    observation_status: str
    reason: str

    if prediction_status == "UNAVAILABLE":
        reason = str(prediction_record.get("unavailable_reason") or "").strip()
        if not reason:
            raise ExternalEvidence8IDirectionError("8i_d_unavailable_prediction_reason_required")
        if prediction_record.get("baseline_prediction") is not None or prediction_record.get("challenger_prediction") is not None:
            raise ExternalEvidence8IDirectionError("8i_d_unavailable_prediction_must_not_carry_values")
        observation_status = UNAVAILABLE
    else:
        if pit_status != "PIT_ELIGIBLE":
            raise ExternalEvidence8IDirectionError("8i_d_available_prediction_requires_pit_eligible_input")
        try:
            baseline_value = float(prediction_record.get("baseline_prediction"))
            challenger_value = float(prediction_record.get("challenger_prediction"))
        except (TypeError, ValueError):
            baseline_value = None
            challenger_value = None
        if baseline_value is None or challenger_value is None or not math.isfinite(baseline_value) or not math.isfinite(challenger_value):
            observation_status = UNAVAILABLE
            direction = None
            delta = None
            reason = "NONFINITE_OR_INVALID_MODEL_PREDICTION"
        else:
            delta = challenger_value - baseline_value
            if not math.isfinite(delta):
                observation_status = UNAVAILABLE
                direction = None
                delta = None
                reason = "NONFINITE_INCREMENTAL_PREDICTION_DELTA"
            elif delta > 0.0:
                observation_status = USABLE
                direction = POSITIVE
                reason = "POSITIVE_INCREMENTAL_PREDICTION"
            elif delta < 0.0:
                observation_status = USABLE
                direction = NEGATIVE
                reason = "NEGATIVE_INCREMENTAL_PREDICTION"
            else:
                observation_status = UNKNOWN_VALID
                direction = UNKNOWN
                reason = "ZERO_INCREMENTAL_PREDICTION"

    result: dict[str, Any] = {
        "schema_version": DIRECTION_RESULT_SCHEMA,
        "phase": "8I-D",
        "state": "SYNTHETIC_DIRECTION_STATE_DERIVED",
        "synthetic": True,
        "binding": dict(binding),
        "snapshot_id": snapshot_id,
        "symbol": symbol,
        "horizon_sessions": horizon,
        "generated_at": generated_at.isoformat(),
        "component_observation_status": observation_status,
        "direction_state": direction,
        "direction_adapter_id": ADAPTER_ID,
        "direction_adapter_version": ADAPTER_VERSION,
        "direction_adapter_sha256": str(contract["direction_adapter"]["direction_adapter_sha256"]),
        "direction_valid_from": generated_at.isoformat(),
        "prediction_delta": delta,
        "baseline_model_sha256": baseline_sha,
        "challenger_model_sha256": challenger_sha,
        "model_pair_artifact_sha256": model_pair_sha,
        "feature_valid_from": feature_valid_from.isoformat(),
        "input_pit_status": pit_status,
        "reason_code": reason,
        "unavailable_reason": reason if observation_status == UNAVAILABLE else None,
        "decision_effect": "NO_CHANGE_TO_PHASE7_DECISION",
        "external_decision_influence_enabled": False,
        "phase7_mutation_authorized": False,
        "extended_reliability_enabled": False,
        "extended_stance_enabled": False,
        "portfolio_action_change_authorized": False,
        "orders_or_trades_authorized": False,
    }
    result["direction_state_sha256"] = digest(result)
    return result
