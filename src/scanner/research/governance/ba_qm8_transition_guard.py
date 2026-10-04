"""BA-QM8 transition-level fail-closed audit guards.

The guard evaluates one adjacent BA-QM8 information-path transition at a time.
It does not compute market evidence or decisions. It only verifies causality,
lineage disclosure, semantic boundaries, missingness and research-governance
controls before a transition may be marked audit-clean.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from scanner.research.governance.ba_qm8_end_to_end import (
    EXPECTED_ERROR_CLASSES,
    EXPECTED_TRANSITIONS,
    load_contract,
)


SCHEMA_VERSION = "ba_qm8_transition_observation_v1"
RECEIPT_SCHEMA_VERSION = "ba_qm8_transition_guard_receipt_v1"
_ALLOWED_MULTIPLICITY_STATES = {
    "NOT_APPLICABLE",
    "FROZEN_PREDECLARED",
    "PREDECLARED_SINGLE_PRIMARY",
}
_ALLOWED_DOUBLE_COUNTING_RESOLUTIONS = {
    "NOT_APPLICABLE",
    "NO_TRIGGER",
    "REVIEWED_NON_ADDITIVE",
}


class BAQM8TransitionError(ValueError):
    """Raised when an adjacent BA-QM8 transition is not fail-closed."""


def _utc(value: object, field: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise BAQM8TransitionError(f"{field}_required")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise BAQM8TransitionError(f"invalid_{field}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise BAQM8TransitionError(f"{field}_timezone_required")
    return parsed.astimezone(timezone.utc)


def _strings(value: object, field: str) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise BAQM8TransitionError(f"{field}_must_be_list")
    result = [str(item).strip() for item in value]
    if any(not item for item in result):
        raise BAQM8TransitionError(f"{field}_contains_blank")
    return result


def _stage_map(contract: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    stages = contract.get("stage_chain")
    if not isinstance(stages, list):
        raise BAQM8TransitionError("ba_qm8_stage_contract_missing")
    result: dict[str, Mapping[str, Any]] = {}
    for raw in stages:
        if not isinstance(raw, Mapping):
            raise BAQM8TransitionError("ba_qm8_stage_contract_invalid")
        result[str(raw.get("stage_id") or "")] = raw
    return result


def _require_bool(mapping: Mapping[str, Any], key: str) -> bool:
    value = mapping.get(key)
    if not isinstance(value, bool):
        raise BAQM8TransitionError(f"control_boolean_required:{key}")
    return value


def _record_check(
    checks: dict[str, dict[str, Any]],
    error_class: str,
    passed: bool,
    reason: str,
) -> None:
    checks[error_class] = {"passed": passed, "reason": reason}
    if not passed:
        raise BAQM8TransitionError(f"{error_class}:{reason}")


def validate_transition_observation(
    observation: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate one adjacent transition and return a sanitized guard receipt."""
    if not isinstance(observation, Mapping):
        raise BAQM8TransitionError("transition_observation_must_be_object")
    if observation.get("schema_version") != SCHEMA_VERSION:
        raise BAQM8TransitionError("transition_observation_schema_invalid")

    from_stage = str(observation.get("from_stage") or "")
    to_stage = str(observation.get("to_stage") or "")
    pair = (from_stage, to_stage)
    if pair not in EXPECTED_TRANSITIONS:
        raise BAQM8TransitionError(
            f"non_adjacent_or_unknown_transition:{from_stage}->{to_stage}"
        )
    transition_id = str(observation.get("transition_id") or "").strip()
    if not transition_id:
        raise BAQM8TransitionError("transition_id_required")

    source = observation.get("source")
    target = observation.get("target")
    controls = observation.get("controls")
    if not isinstance(source, Mapping) or not isinstance(target, Mapping):
        raise BAQM8TransitionError("source_and_target_required")
    if not isinstance(controls, Mapping):
        raise BAQM8TransitionError("transition_controls_required")

    foundation = dict(contract or load_contract())
    stages = _stage_map(foundation)
    source_spec = stages[from_stage]
    target_spec = stages[to_stage]

    source_role = str(source.get("semantic_role") or "")
    target_role = str(target.get("semantic_role") or "")
    source_snapshot = str(source.get("snapshot_id") or "").strip()
    target_snapshot = str(target.get("source_snapshot_id") or "").strip()
    source_as_of = _utc(source.get("as_of"), "source_as_of")
    source_available = _utc(source.get("available_from"), "source_available_from")
    target_as_of = _utc(target.get("as_of"), "target_as_of")
    target_available = _utc(target.get("available_from"), "target_available_from")

    source_evidence = _strings(source.get("material_evidence_ids"), "source.material_evidence_ids")
    target_evidence = _strings(target.get("material_evidence_ids"), "target.material_evidence_ids")
    declared_inputs = _strings(
        target.get("declared_input_evidence_ids"),
        "target.declared_input_evidence_ids",
    )
    native_inputs = _strings(
        target.get("target_native_evidence_ids"),
        "target.target_native_evidence_ids",
    )
    missing_inputs = _strings(
        controls.get("missing_input_ids"),
        "controls.missing_input_ids",
    )
    neutralized_missing = _strings(
        controls.get("neutralized_missing_input_ids"),
        "controls.neutralized_missing_input_ids",
    )
    retrojected = _strings(
        controls.get("retrojected_value_ids"),
        "controls.retrojected_value_ids",
    )

    checks: dict[str, dict[str, Any]] = {}

    leakage_ok = (
        source_as_of <= target_as_of
        and source_available <= target_available
    )
    _record_check(
        checks,
        "LEAKAGE",
        leakage_ok,
        "source_must_be_observable_before_target",
    )

    backdating_allowed = _require_bool(controls, "backdating_allowed")
    retrojection_ok = (
        not retrojected
        and backdating_allowed is False
        and source_as_of <= target_as_of
    )
    _record_check(
        checks,
        "RETROJECTION",
        retrojection_ok,
        "future_vintage_or_backdating_detected",
    )

    target_counts = Counter(target_evidence)
    duplicate_material = sorted(
        evidence_id for evidence_id, count in target_counts.items() if count > 1
    )
    resolution = str(controls.get("double_counting_resolution") or "")
    double_counting_ok = (
        not duplicate_material
        and resolution in _ALLOWED_DOUBLE_COUNTING_RESOLUTIONS
    )
    _record_check(
        checks,
        "DOUBLE_COUNTING",
        double_counting_ok,
        "duplicate_material_evidence_or_unresolved_ancestry_review",
    )

    semantic_contract_match = _require_bool(controls, "semantic_contract_match")
    declared_input_stage = str(target.get("declared_input_stage") or "")
    semantic_ok = (
        source_role == str(source_spec.get("semantic_role") or "")
        and target_role == str(target_spec.get("semantic_role") or "")
        and declared_input_stage == from_stage
        and semantic_contract_match is True
    )
    _record_check(
        checks,
        "SEMANTIC_DRIFT",
        semantic_ok,
        "stage_role_or_declared_input_semantics_changed",
    )

    missing_is_neutral = _require_bool(controls, "missing_is_neutral")
    missing_as_neutral_ok = (
        missing_is_neutral is False
        and not set(missing_inputs).intersection(neutralized_missing)
    )
    _record_check(
        checks,
        "MISSING_AS_NEUTRAL",
        missing_as_neutral_ok,
        "missing_input_was_neutralized",
    )

    multiplicity_applicable = _require_bool(controls, "multiplicity_applicable")
    multiplicity_state = str(controls.get("multiplicity_control_state") or "")
    multiplicity_ok = (
        multiplicity_state in _ALLOWED_MULTIPLICITY_STATES
        and (
            (multiplicity_applicable and multiplicity_state != "NOT_APPLICABLE")
            or (not multiplicity_applicable and multiplicity_state == "NOT_APPLICABLE")
        )
    )
    _record_check(
        checks,
        "UNCONTROLLED_MULTIPLICITY",
        multiplicity_ok,
        "multiplicity_required_but_not_predeclared_and_frozen",
    )

    if len(set(declared_inputs)) != len(declared_inputs):
        raise BAQM8TransitionError(
            "HIDDEN_EVIDENCE_REUSE:declared_input_evidence_ids_not_unique"
        )
    if len(set(native_inputs)) != len(native_inputs):
        raise BAQM8TransitionError(
            "HIDDEN_EVIDENCE_REUSE:target_native_evidence_ids_not_unique"
        )
    allowed_target_evidence = set(declared_inputs) | set(native_inputs)
    hidden = sorted(set(target_evidence) - allowed_target_evidence)
    reused_from_source = set(target_evidence).intersection(source_evidence)
    undeclared_reuse = sorted(reused_from_source - set(declared_inputs))
    hidden_reuse_ok = not hidden and not undeclared_reuse
    _record_check(
        checks,
        "HIDDEN_EVIDENCE_REUSE",
        hidden_reuse_ok,
        "target_contains_undeclared_or_hidden_material_evidence",
    )

    snapshot_binding_required = _require_bool(controls, "snapshot_binding_required")
    if snapshot_binding_required:
        if not source_snapshot or not target_snapshot:
            raise BAQM8TransitionError("snapshot_binding_identity_required")
        if source_snapshot != target_snapshot:
            raise BAQM8TransitionError("snapshot_binding_mismatch")

    if set(checks) != set(EXPECTED_ERROR_CLASSES):
        raise BAQM8TransitionError("transition_guard_error_class_coverage_incomplete")

    return {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "status": "PASSED",
        "transition_id": transition_id,
        "from_stage": from_stage,
        "to_stage": to_stage,
        "source_snapshot_id": source_snapshot or None,
        "target_source_snapshot_id": target_snapshot or None,
        "checks": checks,
        "all_required_error_classes_passed": True,
        "promotion_effect": "NONE",
        "investment_logic_changed": False,
    }
