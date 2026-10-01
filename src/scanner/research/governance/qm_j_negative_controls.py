"""QM-J / BA-QM7 negative controls and falsification harness.

Research-only. Produces isolated synthetic controls that must never replace or
mutate productive scanner, Decision-Layer, Watch, or execution artifacts.
"""
from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

SCHEMA_VERSION = "qm_j_negative_controls_v1"
CONTROL_ARTIFACT_SCHEMA = "qm_j_negative_control_artifact_v1"
EVALUATION_SCHEMA = "qm_j_falsification_evaluation_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_j_negative_controls_v1.json"

LEVEL_CONTROL_TYPES = {
    "DATA": {"SHIFTED_DATA"},
    "FEATURE": {"PERMUTED_FEATURE", "IRRELEVANT_FEATURE"},
    "RESEARCH": {"PSEUDO_SIGNAL", "PSEUDO_EVENT"},
    "DECISION_LAYER": {"PLACEBO_EVIDENCE"},
    "END_TO_END": {"DESTROYED_PREDICTIVE_INFORMATION"},
}
DIRECTIONS = {"HIGHER_IS_BETTER", "LOWER_IS_BETTER"}


class NegativeControlError(ValueError):
    """Raised when a negative-control or falsification invariant is violated."""


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def content_hash(value: Any) -> str:
    return sha256(_json(value).encode("utf-8")).hexdigest()


def _text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise NegativeControlError(f"value_required:{field}")
    return result


def _finite(value: Any, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise NegativeControlError(f"finite_number_required:{field}") from exc
    if not math.isfinite(number):
        raise NegativeControlError(f"finite_number_required:{field}")
    return number


def load_qm_j_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise NegativeControlError(f"qm_j_contract_unreadable:{target}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != SCHEMA_VERSION:
        raise NegativeControlError("qm_j_contract_schema_invalid")
    if payload.get("research_only") is not True:
        raise NegativeControlError("qm_j_must_be_research_only")
    if payload.get("productive_integration_enabled") is not False:
        raise NegativeControlError("qm_j_productive_integration_forbidden")
    if payload.get("execution_allowed") is not False:
        raise NegativeControlError("qm_j_execution_forbidden")
    configured = payload.get("negative_control_types")
    if not isinstance(configured, Mapping):
        raise NegativeControlError("qm_j_control_types_missing")
    for level, expected in LEVEL_CONTROL_TYPES.items():
        if set(configured.get(level) or []) != expected:
            raise NegativeControlError(f"qm_j_control_types_invalid:{level}")
    return payload


def _records(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes, bytearray)):
        raise NegativeControlError("records_sequence_required")
    if not records:
        raise NegativeControlError("records_nonempty_required")
    result: list[dict[str, Any]] = []
    for index, row in enumerate(records):
        if not isinstance(row, Mapping):
            raise NegativeControlError(f"record_object_required:{index}")
        result.append(deepcopy(dict(row)))
    return result


def _field_list(fields: Sequence[str], label: str) -> tuple[str, ...]:
    if not isinstance(fields, Sequence) or isinstance(fields, (str, bytes, bytearray)):
        raise NegativeControlError(f"{label}_sequence_required")
    normalized = tuple(str(field).strip() for field in fields if str(field).strip())
    if not normalized or len(set(normalized)) != len(normalized):
        raise NegativeControlError(f"{label}_nonempty_unique_required")
    return normalized


def _require_fields(rows: Sequence[Mapping[str, Any]], fields: Sequence[str], label: str) -> None:
    for index, row in enumerate(rows):
        missing = [field for field in fields if field not in row]
        if missing:
            raise NegativeControlError(f"{label}_fields_missing:{index}:" + ",".join(missing))


def _identity(row: Mapping[str, Any], fields: Sequence[str]) -> str:
    return _json([row[field] for field in fields])


def _build_artifact(
    *,
    level: str,
    control_type: str,
    source_snapshot_id: str,
    source_records: Sequence[Mapping[str, Any]],
    control_records: Sequence[Mapping[str, Any]],
    transform_definition: Mapping[str, Any],
) -> dict[str, Any]:
    if level not in LEVEL_CONTROL_TYPES or control_type not in LEVEL_CONTROL_TYPES[level]:
        raise NegativeControlError("negative_control_type_level_mismatch")
    original = _records(source_records)
    transformed = _records(control_records)
    if len(original) != len(transformed):
        raise NegativeControlError("negative_control_row_count_changed")
    artifact: dict[str, Any] = {
        "schema_version": CONTROL_ARTIFACT_SCHEMA,
        "module": "QM-J",
        "business_area": "BA-QM7",
        "level": level,
        "control_type": control_type,
        "source_snapshot_id": _text(source_snapshot_id, "source_snapshot_id"),
        "source_content_hash": content_hash(original),
        "transform_definition": deepcopy(dict(transform_definition)),
        "synthetic_control": True,
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "may_replace_productive_data": False,
        "may_enter_live_decision_path": False,
        "records": transformed,
    }
    artifact["control_content_hash"] = content_hash(transformed)
    body = dict(artifact)
    artifact["artifact_hash"] = content_hash(body)
    return validate_control_artifact(artifact)


def validate_control_artifact(artifact: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(artifact, Mapping) or artifact.get("schema_version") != CONTROL_ARTIFACT_SCHEMA:
        raise NegativeControlError("negative_control_artifact_schema_invalid")
    level = str(artifact.get("level") or "")
    control_type = str(artifact.get("control_type") or "")
    if control_type not in LEVEL_CONTROL_TYPES.get(level, set()):
        raise NegativeControlError("negative_control_type_level_mismatch")
    if artifact.get("synthetic_control") is not True or artifact.get("research_only") is not True:
        raise NegativeControlError("negative_control_must_be_synthetic_research_only")
    for field in (
        "productive_integration_enabled",
        "execution_allowed",
        "may_replace_productive_data",
        "may_enter_live_decision_path",
    ):
        if artifact.get(field) is not False:
            raise NegativeControlError(f"negative_control_boundary_invalid:{field}")
    rows = artifact.get("records")
    if not isinstance(rows, list):
        raise NegativeControlError("negative_control_records_list_required")
    if artifact.get("control_content_hash") != content_hash(rows):
        raise NegativeControlError("negative_control_content_hash_mismatch")
    body = dict(artifact)
    stored = str(body.pop("artifact_hash", "") or "")
    if stored != content_hash(body):
        raise NegativeControlError("negative_control_artifact_hash_mismatch")
    return deepcopy(dict(artifact))


def shifted_data_control(
    records: Sequence[Mapping[str, Any]],
    *,
    source_snapshot_id: str,
    value_fields: Sequence[str],
    entity_fields: Sequence[str],
    shift_by: int = 1,
) -> dict[str, Any]:
    rows = _records(records)
    values = _field_list(value_fields, "value_fields")
    entities = _field_list(entity_fields, "entity_fields")
    if not isinstance(shift_by, int) or shift_by < 1:
        raise NegativeControlError("shift_by_positive_integer_required")
    _require_fields(rows, (*values, *entities), "shifted_data")
    transformed = deepcopy(rows)
    groups: dict[str, list[int]] = {}
    for index, row in enumerate(rows):
        groups.setdefault(_identity(row, entities), []).append(index)
    for indices in groups.values():
        for position, target_index in enumerate(indices):
            source_position = position - shift_by
            for field in values:
                transformed[target_index][field] = (
                    deepcopy(rows[indices[source_position]][field]) if source_position >= 0 else None
                )
    return _build_artifact(
        level="DATA",
        control_type="SHIFTED_DATA",
        source_snapshot_id=source_snapshot_id,
        source_records=rows,
        control_records=transformed,
        transform_definition={
            "value_fields": list(values),
            "entity_fields": list(entities),
            "shift_by": shift_by,
            "uses_future_rows": False,
            "leading_missing_preserved_as_missing": True,
        },
    )


def _permutation_indices(rows: Sequence[Mapping[str, Any]], identity_fields: Sequence[str], seed: str) -> list[int]:
    keyed = [
        (content_hash({"seed": seed, "identity": _identity(row, identity_fields)}), index)
        for index, row in enumerate(rows)
    ]
    permutation = [index for _, index in sorted(keyed)]
    if len(permutation) > 1 and permutation == list(range(len(permutation))):
        permutation = permutation[1:] + permutation[:1]
    return permutation


def permuted_feature_control(
    records: Sequence[Mapping[str, Any]],
    *,
    source_snapshot_id: str,
    feature_fields: Sequence[str],
    identity_fields: Sequence[str],
    seed: str,
) -> dict[str, Any]:
    rows = _records(records)
    features = _field_list(feature_fields, "feature_fields")
    identities = _field_list(identity_fields, "identity_fields")
    _require_fields(rows, (*features, *identities), "permuted_feature")
    if len(rows) < 2:
        raise NegativeControlError("permutation_requires_two_records")
    seed = _text(seed, "seed")
    permutation = _permutation_indices(rows, identities, seed)
    transformed = deepcopy(rows)
    for target, source in enumerate(permutation):
        for field in features:
            transformed[target][field] = deepcopy(rows[source][field])
    return _build_artifact(
        level="FEATURE",
        control_type="PERMUTED_FEATURE",
        source_snapshot_id=source_snapshot_id,
        source_records=rows,
        control_records=transformed,
        transform_definition={
            "feature_fields": list(features),
            "identity_fields": list(identities),
            "seed": seed,
            "deterministic": True,
            "preserves_feature_marginals": True,
            "outcomes_used_for_permutation": False,
        },
    )


def irrelevant_feature_control(
    records: Sequence[Mapping[str, Any]],
    *,
    source_snapshot_id: str,
    identity_fields: Sequence[str],
    feature_name: str,
    seed: str,
) -> dict[str, Any]:
    rows = _records(records)
    identities = _field_list(identity_fields, "identity_fields")
    _require_fields(rows, identities, "irrelevant_feature")
    feature_name = _text(feature_name, "feature_name")
    if any(feature_name in row for row in rows):
        raise NegativeControlError("irrelevant_feature_must_not_overwrite_existing_field")
    seed = _text(seed, "seed")
    transformed = deepcopy(rows)
    for row in transformed:
        digest = content_hash({"seed": seed, "identity": _identity(row, identities), "kind": "irrelevant_feature"})
        row[feature_name] = int(digest[:13], 16) / float(16**13 - 1)
    return _build_artifact(
        level="FEATURE",
        control_type="IRRELEVANT_FEATURE",
        source_snapshot_id=source_snapshot_id,
        source_records=rows,
        control_records=transformed,
        transform_definition={
            "feature_name": feature_name,
            "identity_fields": list(identities),
            "seed": seed,
            "derived_from_outcome": False,
            "deterministic": True,
        },
    )


def pseudo_signal_control(
    records: Sequence[Mapping[str, Any]],
    *,
    source_snapshot_id: str,
    identity_fields: Sequence[str],
    signal_field: str,
    seed: str,
) -> dict[str, Any]:
    rows = _records(records)
    identities = _field_list(identity_fields, "identity_fields")
    _require_fields(rows, identities, "pseudo_signal")
    signal_field = _text(signal_field, "signal_field")
    if any(signal_field in row for row in rows):
        raise NegativeControlError("pseudo_signal_must_not_overwrite_existing_field")
    seed = _text(seed, "seed")
    states = ("NEGATIVE", "NEUTRAL", "POSITIVE")
    transformed = deepcopy(rows)
    for row in transformed:
        digest = content_hash({"seed": seed, "identity": _identity(row, identities), "kind": "pseudo_signal"})
        row[signal_field] = states[int(digest[:8], 16) % len(states)]
    return _build_artifact(
        level="RESEARCH",
        control_type="PSEUDO_SIGNAL",
        source_snapshot_id=source_snapshot_id,
        source_records=rows,
        control_records=transformed,
        transform_definition={
            "signal_field": signal_field,
            "identity_fields": list(identities),
            "seed": seed,
            "outcomes_used": False,
            "deterministic": True,
        },
    )


def pseudo_event_control(
    records: Sequence[Mapping[str, Any]],
    *,
    source_snapshot_id: str,
    identity_fields: Sequence[str],
    event_field: str,
    seed: str,
) -> dict[str, Any]:
    rows = _records(records)
    identities = _field_list(identity_fields, "identity_fields")
    _require_fields(rows, identities, "pseudo_event")
    event_field = _text(event_field, "event_field")
    if any(event_field in row for row in rows):
        raise NegativeControlError("pseudo_event_must_not_overwrite_existing_field")
    seed = _text(seed, "seed")
    transformed = deepcopy(rows)
    for row in transformed:
        digest = content_hash({"seed": seed, "identity": _identity(row, identities), "kind": "pseudo_event"})
        row[event_field] = bool(int(digest[:8], 16) % 5 == 0)
    return _build_artifact(
        level="RESEARCH",
        control_type="PSEUDO_EVENT",
        source_snapshot_id=source_snapshot_id,
        source_records=rows,
        control_records=transformed,
        transform_definition={
            "event_field": event_field,
            "identity_fields": list(identities),
            "seed": seed,
            "outcomes_used": False,
            "deterministic": True,
        },
    )


def placebo_evidence_control(
    records: Sequence[Mapping[str, Any]],
    *,
    source_snapshot_id: str,
    identity_fields: Sequence[str],
    evidence_field: str,
    seed: str,
) -> dict[str, Any]:
    rows = _records(records)
    identities = _field_list(identity_fields, "identity_fields")
    _require_fields(rows, identities, "placebo_evidence")
    evidence_field = _text(evidence_field, "evidence_field")
    if any(evidence_field in row for row in rows):
        raise NegativeControlError("placebo_evidence_must_not_overwrite_existing_field")
    seed = _text(seed, "seed")
    states = ("negative", "positive", "insufficient_evidence")
    transformed = deepcopy(rows)
    for row in transformed:
        digest = content_hash({"seed": seed, "identity": _identity(row, identities), "kind": "placebo_evidence"})
        row[evidence_field] = {
            "state": states[int(digest[:8], 16) % len(states)],
            "synthetic_control": True,
            "directional_vote_allowed": False,
            "productive_integration_enabled": False,
        }
    return _build_artifact(
        level="DECISION_LAYER",
        control_type="PLACEBO_EVIDENCE",
        source_snapshot_id=source_snapshot_id,
        source_records=rows,
        control_records=transformed,
        transform_definition={
            "evidence_field": evidence_field,
            "identity_fields": list(identities),
            "seed": seed,
            "may_change_universal_stance": False,
            "may_enter_live_decision_path": False,
        },
    )


def destroyed_information_control(
    records: Sequence[Mapping[str, Any]],
    *,
    source_snapshot_id: str,
    predictive_fields: Sequence[str],
    identity_fields: Sequence[str],
    protected_fields: Sequence[str],
    seed: str,
) -> dict[str, Any]:
    rows = _records(records)
    predictive = _field_list(predictive_fields, "predictive_fields")
    identities = _field_list(identity_fields, "identity_fields")
    protected = _field_list(protected_fields, "protected_fields")
    if set(predictive) & set(protected):
        raise NegativeControlError("predictive_fields_may_not_include_protected_fields")
    _require_fields(rows, (*predictive, *identities, *protected), "destroyed_information")
    if len(rows) < 2:
        raise NegativeControlError("destroyed_information_requires_two_records")
    seed = _text(seed, "seed")
    permutation = _permutation_indices(rows, identities, seed)
    transformed = deepcopy(rows)
    protected_before = [[deepcopy(row[field]) for field in protected] for row in rows]
    for target, source in enumerate(permutation):
        for field in predictive:
            transformed[target][field] = deepcopy(rows[source][field])
    protected_after = [[row[field] for field in protected] for row in transformed]
    if protected_before != protected_after:
        raise NegativeControlError("protected_fields_changed")
    return _build_artifact(
        level="END_TO_END",
        control_type="DESTROYED_PREDICTIVE_INFORMATION",
        source_snapshot_id=source_snapshot_id,
        source_records=rows,
        control_records=transformed,
        transform_definition={
            "predictive_fields": list(predictive),
            "identity_fields": list(identities),
            "protected_fields": list(protected),
            "seed": seed,
            "outcomes_used_for_destruction": False,
            "protected_fields_unchanged": True,
            "isolated_pipeline_required": True,
        },
    )


def _metric_result(raw: Mapping[str, Any], label: str) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise NegativeControlError(f"{label}_result_object_required")
    metric_name = _text(raw.get("metric_name"), f"{label}.metric_name")
    context_hash = _text(raw.get("comparison_context_hash"), f"{label}.comparison_context_hash").lower()
    if len(context_hash) != 64 or any(ch not in "0123456789abcdef" for ch in context_hash):
        raise NegativeControlError(f"{label}_comparison_context_hash_invalid")
    try:
        n = int(raw.get("n_observations"))
    except (TypeError, ValueError) as exc:
        raise NegativeControlError(f"{label}_n_observations_invalid") from exc
    if n <= 0:
        raise NegativeControlError(f"{label}_n_observations_invalid")
    return {
        "result_id": _text(raw.get("result_id"), f"{label}.result_id"),
        "metric_name": metric_name,
        "metric_value": _finite(raw.get("metric_value"), f"{label}.metric_value"),
        "comparison_context_hash": context_hash,
        "n_observations": n,
    }


def evaluate_falsification(
    *,
    plan: Mapping[str, Any],
    real_result: Mapping[str, Any],
    control_results: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if not isinstance(plan, Mapping):
        raise NegativeControlError("falsification_plan_object_required")
    normalized_plan = {
        "plan_id": _text(plan.get("plan_id"), "plan_id"),
        "plan_version": _text(plan.get("plan_version"), "plan_version"),
        "frozen_at": _text(plan.get("frozen_at"), "frozen_at"),
        "outcome_visibility_at_freeze": str(plan.get("outcome_visibility_at_freeze") or "").upper(),
        "primary_metric": _text(plan.get("primary_metric"), "primary_metric"),
        "metric_direction": str(plan.get("metric_direction") or "").upper(),
        "similarity_margin": _finite(plan.get("similarity_margin"), "similarity_margin"),
    }
    if normalized_plan["outcome_visibility_at_freeze"] != "NONE":
        raise NegativeControlError("falsification_plan_must_be_frozen_pre_outcome")
    if normalized_plan["metric_direction"] not in DIRECTIONS:
        raise NegativeControlError("metric_direction_invalid")
    if normalized_plan["similarity_margin"] < 0:
        raise NegativeControlError("similarity_margin_must_be_nonnegative")
    real = _metric_result(real_result, "real")
    if real["metric_name"] != normalized_plan["primary_metric"]:
        raise NegativeControlError("real_metric_mismatch_plan")
    if not isinstance(control_results, Sequence) or isinstance(control_results, (str, bytes, bytearray)) or not control_results:
        raise NegativeControlError("control_results_nonempty_sequence_required")

    controls = [_metric_result(item, f"control[{index}]") for index, item in enumerate(control_results)]
    triggers: list[dict[str, Any]] = []
    for control in controls:
        if control["metric_name"] != real["metric_name"]:
            raise NegativeControlError("control_metric_mismatch")
        if control["comparison_context_hash"] != real["comparison_context_hash"]:
            raise NegativeControlError("control_comparison_context_mismatch")
        if control["n_observations"] != real["n_observations"]:
            raise NegativeControlError("control_observation_count_mismatch")
        if normalized_plan["metric_direction"] == "HIGHER_IS_BETTER":
            favorable_gap = real["metric_value"] - control["metric_value"]
            control_better = control["metric_value"] > real["metric_value"]
        else:
            favorable_gap = control["metric_value"] - real["metric_value"]
            control_better = control["metric_value"] < real["metric_value"]
        similar_or_better = favorable_gap <= normalized_plan["similarity_margin"]
        if similar_or_better:
            triggers.append({
                "control_result_id": control["result_id"],
                "control_metric_value": control["metric_value"],
                "real_metric_value": real["metric_value"],
                "favorable_gap": favorable_gap,
                "similarity_margin": normalized_plan["similarity_margin"],
                "control_better_than_real": control_better,
            })

    status = (
        "PROMOTION_STOP_INVESTIGATION_CAPA_REQUIRED"
        if triggers
        else "NEGATIVE_CONTROLS_NOT_SIMILARLY_STRONG"
    )
    body = {
        "schema_version": EVALUATION_SCHEMA,
        "module": "QM-J",
        "business_area": "BA-QM7",
        "plan": normalized_plan,
        "real_result": real,
        "control_results": controls,
        "status": status,
        "triggered_controls": triggers,
        "promotion_blocked_by_qm_j": bool(triggers),
        "investigation_required": bool(triggers),
        "capa_required": bool(triggers),
        "promotion_performed": False,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "diagnostic_classes_if_triggered": [
            "LEAKAGE",
            "OVERFITTING",
            "MULTIPLE_TESTING",
            "DEPENDENCE_ERROR",
            "PIPELINE_BIAS",
        ] if triggers else [],
    }
    body["evaluation_id"] = content_hash(body)
    return body


def register_qm_h_falsification_finding(
    evaluation: Mapping[str, Any],
    *,
    ledger: Any,
    actor_id: str,
    actor_role: str,
) -> dict[str, Any]:
    if evaluation.get("schema_version") != EVALUATION_SCHEMA:
        raise NegativeControlError("falsification_evaluation_schema_invalid")
    if evaluation.get("status") != "PROMOTION_STOP_INVESTIGATION_CAPA_REQUIRED":
        raise NegativeControlError("qm_h_finding_requires_triggered_falsification")
    evaluation_id = _text(evaluation.get("evaluation_id"), "evaluation_id")
    finding_id = f"QM-H-QMJ-{evaluation_id[:12].upper()}"
    try:
        existing = ledger.get_finding(finding_id)
        return {
            "finding_id": finding_id,
            "already_registered": True,
            "status": existing.get("status"),
            "promotion_blocked": True,
            "capa_required": True,
        }
    except Exception:
        pass
    ledger.register_finding(
        finding_id=finding_id,
        category="METHODOLOGY_FINDING",
        title="QM-J negative control matched or exceeded real signal",
        description=(
            "A predeclared QM-J negative control achieved performance within the frozen "
            "similarity margin of the real result or exceeded it. Promotion must stop "
            "pending investigation and QM-H CAPA handling."
        ),
        source_reference=f"qm-j:{evaluation_id}",
        evidence_impact="PROMOTION_BLOCKED",
        identity_refs=[],
        actor_id=_text(actor_id, "actor_id"),
        actor_role=_text(actor_role, "actor_role"),
    )
    return {
        "finding_id": finding_id,
        "already_registered": False,
        "status": "OPEN",
        "promotion_blocked": True,
        "capa_required": True,
    }
