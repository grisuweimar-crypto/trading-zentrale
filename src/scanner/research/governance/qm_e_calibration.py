"""QM-E point-in-time probability-calibration audit."""
from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
import math
from typing import Any, Mapping, Sequence

import numpy as np

from scanner.research.governance.qm_c_analysis_plan import AnalysisPlanRegistry
from scanner.research.governance.qm_c_negative_results import NegativeResultRegistry
from scanner.research.governance.qm_d_dependence import load_qm_de_contract
from scanner.research.governance.qm_i_lineage import LineageRegistry


class CalibrationAuditError(ValueError):
    pass


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _hash(value: Any) -> str:
    return sha256(_json(value).encode("utf-8")).hexdigest()


def _text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise CalibrationAuditError(f"value_required:{field}")
    return result


def _timestamp(value: Any, field: str) -> datetime:
    text = _text(value, field)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CalibrationAuditError(f"timestamp_invalid:{field}:{text}") from exc
    if parsed.tzinfo is None:
        raise CalibrationAuditError(f"timestamp_timezone_required:{field}")
    return parsed


def _probability(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise CalibrationAuditError(f"probability_required:{field}")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise CalibrationAuditError(f"probability_required:{field}") from exc
    if not math.isfinite(number) or not 0.0 <= number <= 1.0:
        raise CalibrationAuditError(f"probability_out_of_range:{field}")
    return number


def _validate_identity(identity: Mapping[str, Any], *, analysis_plans: AnalysisPlanRegistry, results: NegativeResultRegistry, lineage: LineageRegistry) -> dict[str, str]:
    required = list(load_qm_de_contract()["identity_binding"]["required_audit_fields"]) + ["prediction_definition_hash", "label_definition_hash"]
    missing = [field for field in required if not str(identity.get(field) or "").strip()]
    if missing:
        raise CalibrationAuditError("audit_identity_missing:" + ",".join(missing))
    normalized = {field: str(identity[field]).strip() for field in required}
    _timestamp(normalized["audit_as_of"], "audit_identity.audit_as_of")
    plan = analysis_plans.get_plan(normalized["analysis_plan_id"], normalized["analysis_plan_version"])
    if plan["analysis_plan_hash"] != normalized["analysis_plan_hash"]:
        raise CalibrationAuditError("audit_identity_analysis_plan_hash_mismatch")
    result = results.get_result(normalized["result_id"], normalized["result_version"])
    if result["result_hash"] != normalized["result_hash"]:
        raise CalibrationAuditError("audit_identity_result_hash_mismatch")
    for field in ("analysis_plan_id", "analysis_plan_version", "analysis_plan_hash"):
        if result.get(field) != normalized[field]:
            raise CalibrationAuditError(f"audit_identity_result_plan_binding_mismatch:{field}")
    freeze = plan.get("freeze_context")
    if isinstance(freeze, Mapping) and freeze.get("dataset_snapshot_hash") is not None and str(freeze["dataset_snapshot_hash"]) != normalized["dataset_snapshot_hash"]:
        raise CalibrationAuditError("audit_identity_dataset_snapshot_hash_mismatch")
    if lineage.verify_integrity()["head_hash"] != normalized["lineage_registry_head_hash"]:
        raise CalibrationAuditError("audit_identity_lineage_head_hash_mismatch")
    return normalized


def _normalize(records: Sequence[Mapping[str, Any]], audit_as_of: datetime, lineage: LineageRegistry) -> list[dict[str, Any]]:
    contract = load_qm_de_contract()
    required = tuple(contract["calibration"]["required_prediction_fields"])
    allowed = set(contract["calibration"]["outcome_values"])
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw in enumerate(records):
        if not isinstance(raw, Mapping):
            raise CalibrationAuditError(f"prediction_pair_must_be_object:{index}")
        missing = [field for field in required if field not in raw]
        if missing:
            raise CalibrationAuditError(f"prediction_pair_fields_missing:{index}:" + ",".join(missing))
        prediction_id = _text(raw.get("prediction_id"), f"pairs[{index}].prediction_id")
        if prediction_id in seen:
            raise CalibrationAuditError(f"duplicate_prediction_id:{prediction_id}")
        seen.add(prediction_id)
        prediction_as_of = _timestamp(raw.get("prediction_as_of"), f"pairs[{index}].prediction_as_of")
        outcome_available_at = _timestamp(raw.get("outcome_available_at"), f"pairs[{index}].outcome_available_at")
        if prediction_as_of >= outcome_available_at:
            raise CalibrationAuditError(f"pit_prediction_must_precede_outcome:{prediction_id}")
        if outcome_available_at > audit_as_of:
            raise CalibrationAuditError(f"outcome_not_available_by_audit_as_of:{prediction_id}")
        outcome = int(raw.get("outcome")) if isinstance(raw.get("outcome"), bool) else raw.get("outcome")
        if outcome not in allowed:
            raise CalibrationAuditError(f"binary_outcome_required:{prediction_id}")
        node_id = _text(raw.get("lineage_node_id"), f"pairs[{index}].lineage_node_id")
        version_id = _text(raw.get("lineage_version_id"), f"pairs[{index}].lineage_version_id")
        node = lineage.get_node(node_id, version_id)
        rows.append({
            "prediction_id": prediction_id,
            "predicted_probability": _probability(raw.get("predicted_probability"), f"pairs[{index}].predicted_probability"),
            "outcome": int(outcome),
            "prediction_as_of": prediction_as_of,
            "outcome_available_at": outcome_available_at,
            "subgroup": str(raw.get("subgroup") or "").strip() or None,
            "time_block": str(raw.get("time_block") or "").strip() or None,
            "lineage_node_id": node_id,
            "lineage_version_id": version_id,
            "lineage_node_type": node["node_type"],
        })
    if not rows:
        raise CalibrationAuditError("calibration_audit_requires_prediction_pairs")
    return sorted(rows, key=lambda row: (row["prediction_as_of"], row["prediction_id"]))


def _hash_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [{
        "prediction_id": row["prediction_id"], "predicted_probability": row["predicted_probability"], "outcome": row["outcome"],
        "prediction_as_of": row["prediction_as_of"].isoformat(), "outcome_available_at": row["outcome_available_at"].isoformat(),
        "subgroup": row["subgroup"], "time_block": row["time_block"], "lineage_node_id": row["lineage_node_id"],
        "lineage_version_id": row["lineage_version_id"], "lineage_node_type": row["lineage_node_type"],
    } for row in rows]


def _sigmoid(values: np.ndarray) -> np.ndarray:
    result = np.empty_like(values, dtype=float)
    positive = values >= 0
    result[positive] = 1.0 / (1.0 + np.exp(-values[positive]))
    exp_values = np.exp(values[~positive])
    result[~positive] = exp_values / (1.0 + exp_values)
    return result


def _regression(probabilities: np.ndarray, outcomes: np.ndarray, minimum: int, epsilon: float) -> dict[str, Any]:
    if len(probabilities) < minimum:
        return {"status": "INSUFFICIENT_ROWS", "N": int(len(probabilities)), "minimum_rows": minimum, "intercept": None, "slope": None}
    if len(np.unique(outcomes)) < 2:
        return {"status": "INSUFFICIENT_OUTCOME_VARIATION", "N": int(len(probabilities)), "intercept": None, "slope": None}
    p = np.clip(probabilities, epsilon, 1.0 - epsilon)
    logits = np.log(p / (1.0 - p))
    x = np.column_stack([np.ones(len(logits)), logits])
    beta = np.asarray([0.0, 1.0])
    for _ in range(100):
        mu = _sigmoid(x @ beta)
        weights = np.clip(mu * (1.0 - mu), 1e-12, None)
        try:
            delta = np.linalg.solve(x.T @ (x * weights[:, None]), x.T @ (outcomes - mu))
        except np.linalg.LinAlgError:
            return {"status": "SINGULAR_INFORMATION_MATRIX", "N": int(len(probabilities)), "intercept": None, "slope": None}
        beta = beta + delta
        if not np.all(np.isfinite(beta)):
            return {"status": "NUMERICAL_FAILURE", "N": int(len(probabilities)), "intercept": None, "slope": None}
        if float(np.max(np.abs(delta))) < 1e-10:
            return {"status": "AVAILABLE", "N": int(len(probabilities)), "intercept": float(beta[0]), "slope": float(beta[1]), "ideal_intercept": 0.0, "ideal_slope": 1.0}
    return {"status": "NOT_CONVERGED", "N": int(len(probabilities)), "intercept": None, "slope": None}


def _bins(probabilities: np.ndarray, outcomes: np.ndarray, boundaries: Sequence[float]) -> list[dict[str, Any]]:
    edges = [float(value) for value in boundaries]
    if len(edges) < 2 or edges[0] != 0.0 or edges[-1] != 1.0 or any(not math.isfinite(value) for value in edges) or any(right <= left for left, right in zip(edges, edges[1:])):
        raise CalibrationAuditError("reliability_bin_boundaries_invalid")
    result = []
    for index, (left, right) in enumerate(zip(edges, edges[1:])):
        mask = (probabilities >= left) & ((probabilities <= right) if index == len(edges) - 2 else (probabilities < right))
        count = int(mask.sum())
        result.append({"lower": left, "upper": right, "upper_inclusive": index == len(edges) - 2, "N": count, "mean_predicted_probability": float(probabilities[mask].mean()) if count else None, "observed_rate": float(outcomes[mask].mean()) if count else None})
    return result


def _metrics(rows: Sequence[Mapping[str, Any]], bins: Sequence[float], minimum: int, epsilon: float) -> dict[str, Any]:
    p = np.asarray([row["predicted_probability"] for row in rows], dtype=float)
    y = np.asarray([row["outcome"] for row in rows], dtype=float)
    clipped = np.clip(p, epsilon, 1.0 - epsilon)
    return {
        "N": len(rows), "positive_outcomes": int(y.sum()), "mean_predicted_probability": float(p.mean()), "observed_rate": float(y.mean()),
        "BRIER_SCORE": float(np.mean((p - y) ** 2)),
        "LOG_LOSS": float(-np.mean(y * np.log(clipped) + (1.0 - y) * np.log(1.0 - clipped))),
        "CALIBRATION_REGRESSION": _regression(p, y, minimum, epsilon),
        "RELIABILITY_BINS": _bins(p, y, bins),
    }


def _groups(rows: Sequence[Mapping[str, Any]], field: str, bins: Sequence[float], minimum: int, epsilon: float) -> dict[str, Any]:
    missing = sum(row.get(field) is None for row in rows)
    groups = []
    for value in sorted({str(row[field]) for row in rows if row.get(field) is not None}):
        selected = [row for row in rows if str(row.get(field)) == value]
        groups.append({"group": value, "status": "AVAILABLE" if len(selected) >= minimum else "INSUFFICIENT_ROWS", "N": len(selected), "minimum_rows": minimum if len(selected) < minimum else None, "metrics": _metrics(selected, bins, minimum, epsilon) if len(selected) >= minimum else None})
    return {"field": field, "status": "UNKNOWN_MISSING_GROUP_METADATA" if missing else ("AVAILABLE" if groups else "NOT_APPLICABLE"), "missing_count": missing, "groups": groups}


def audit_calibration(records: Sequence[Mapping[str, Any]], *, identity: Mapping[str, Any], analysis_plans: AnalysisPlanRegistry, results: NegativeResultRegistry, lineage: LineageRegistry, reliability_bins: Sequence[float] | None = None) -> dict[str, Any]:
    contract = load_qm_de_contract()
    audit_identity = _validate_identity(identity, analysis_plans=analysis_plans, results=results, lineage=lineage)
    spec = contract["calibration"]
    rows = _normalize(records, _timestamp(audit_identity["audit_as_of"], "audit_identity.audit_as_of"), lineage)
    bins = list(reliability_bins or spec["default_reliability_bins"])
    minimum = int(spec["minimum_rows_for_calibration_regression"])
    subgroup_minimum = int(spec["minimum_rows_per_subgroup"])
    epsilon = float(spec["log_loss_clip_epsilon"])
    overall = _metrics(rows, bins, minimum, epsilon)
    subgroup = _groups(rows, "subgroup", bins, subgroup_minimum, epsilon)
    time_block = _groups(rows, "time_block", bins, subgroup_minimum, epsilon)
    unknown = any(item["status"].startswith("UNKNOWN") for item in (subgroup, time_block))
    parameters = {"reliability_bins": bins, "minimum_rows_for_calibration_regression": minimum, "minimum_rows_per_subgroup": subgroup_minimum, "log_loss_clip_epsilon": epsilon}
    return {
        "schema_version": "qm_e_calibration_audit_v1",
        "audit_identity": audit_identity,
        "audit_hash": _hash({"identity": audit_identity, "pairs": _hash_rows(rows), "parameters": parameters}),
        "audit_parameters": parameters,
        "status": "COMPLETE_WITH_UNKNOWN_COMPONENTS" if unknown else "COMPLETE",
        "pair_count": len(rows), "overall": overall, "subgroups": subgroup, "time_blocks": time_block,
        "lineage_node_types": sorted({str(row["lineage_node_type"]) for row in rows}),
        "automatic_recalibration_performed": False, "productive_probability_update_performed": False, "empirical_promotion_performed": False,
    }


def audit_phase2_bridge(report: Mapping[str, Any] | None) -> dict[str, Any]:
    bridge = load_qm_de_contract()["existing_phase2_bridge"]
    if report is None:
        return {"status": bridge["insufficient_status"], "aggregate_report_present": False, "recognized_block_method_present": False, "row_level_metrics_computed": False}
    recognized = bridge["recognized_block_method"] in _json(dict(report))
    return {"status": bridge["insufficient_status"], "aggregate_report_present": True, "recognized_block_method_present": bool(recognized), "iid_diagnostics_must_remain_diagnostics_only": bool(bridge["iid_diagnostics_must_remain_diagnostics_only"]), "row_level_metrics_computed": False, "aggregate_report_promoted_to_row_level_pairs": False}
