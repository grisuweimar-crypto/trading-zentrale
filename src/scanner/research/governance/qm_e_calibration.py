"""QM-E probability-calibration audit.

Research-only. Calibration is computed only from explicit point-in-time
prediction/outcome pairs bound to stable QM-C and QM-I identities. Aggregate
Phase-2 summaries are not silently reinterpreted as row-level predictions.
"""
from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from scanner.research.governance.qm_c_analysis_plan import AnalysisPlanRegistry
from scanner.research.governance.qm_c_results import ResultRegistry
from scanner.research.governance.qm_d_dependence import load_qm_de_contract
from scanner.research.governance.qm_i_lineage import LineageRegistry


class CalibrationAuditError(ValueError):
    """Raised when a QM-E calibration invariant is violated."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _hash(value: Any) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _nonblank(value: Any, *, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise CalibrationAuditError(f"value_required:{field}")
    return text


def _timestamp(value: Any, *, field: str) -> datetime:
    text = _nonblank(value, field=field)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CalibrationAuditError(f"timestamp_invalid:{field}:{text}") from exc
    if parsed.tzinfo is None:
        raise CalibrationAuditError(f"timestamp_timezone_required:{field}")
    return parsed


def _probability(value: Any, *, field: str) -> float:
    if isinstance(value, bool):
        raise CalibrationAuditError(f"probability_required:{field}")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise CalibrationAuditError(f"probability_required:{field}") from exc
    if not math.isfinite(parsed) or parsed < 0.0 or parsed > 1.0:
        raise CalibrationAuditError(f"probability_out_of_range:{field}")
    return parsed


def _validate_identity(
    identity: Mapping[str, Any],
    *,
    analysis_plans: AnalysisPlanRegistry,
    results: ResultRegistry,
    lineage: LineageRegistry,
) -> dict[str, str]:
    contract = load_qm_de_contract()
    required = list(contract["identity_binding"]["required_audit_fields"])
    required += ["prediction_definition_hash", "label_definition_hash"]
    missing = [field for field in required if not str(identity.get(field) or "").strip()]
    if missing:
        raise CalibrationAuditError("audit_identity_missing:" + ",".join(missing))
    normalized = {field: str(identity[field]).strip() for field in required}
    _timestamp(normalized["audit_as_of"], field="audit_identity.audit_as_of")

    plan = analysis_plans.get_plan(normalized["analysis_plan_id"], normalized["analysis_plan_version"])
    if plan["analysis_plan_hash"] != normalized["analysis_plan_hash"]:
        raise CalibrationAuditError("audit_identity_analysis_plan_hash_mismatch")

    result = results.get_result(normalized["result_id"], normalized["result_version"])
    if result["result_hash"] != normalized["result_hash"]:
        raise CalibrationAuditError("audit_identity_result_hash_mismatch")
    for field in ("analysis_plan_id", "analysis_plan_version", "analysis_plan_hash"):
        if result.get(field) != normalized[field]:
            raise CalibrationAuditError(f"audit_identity_result_plan_binding_mismatch:{field}")

    freeze_context = plan.get("freeze_context")
    if isinstance(freeze_context, Mapping) and freeze_context.get("dataset_snapshot_hash") is not None:
        if str(freeze_context["dataset_snapshot_hash"]) != normalized["dataset_snapshot_hash"]:
            raise CalibrationAuditError("audit_identity_dataset_snapshot_hash_mismatch")

    verification = lineage.verify_integrity()
    if verification["head_hash"] != normalized["lineage_registry_head_hash"]:
        raise CalibrationAuditError("audit_identity_lineage_head_hash_mismatch")
    return normalized


def _normalize_pairs(
    records: Sequence[Mapping[str, Any]],
    *,
    audit_as_of: datetime,
    lineage: LineageRegistry,
) -> list[dict[str, Any]]:
    contract = load_qm_de_contract()
    required = tuple(contract["calibration"]["required_prediction_fields"])
    allowed_outcomes = set(contract["calibration"]["outcome_values"])
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw in enumerate(records):
        if not isinstance(raw, Mapping):
            raise CalibrationAuditError(f"prediction_pair_must_be_object:{index}")
        missing = [field for field in required if field not in raw]
        if missing:
            raise CalibrationAuditError(f"prediction_pair_fields_missing:{index}:" + ",".join(missing))
        prediction_id = _nonblank(raw.get("prediction_id"), field=f"pairs[{index}].prediction_id")
        if prediction_id in seen:
            raise CalibrationAuditError(f"duplicate_prediction_id:{prediction_id}")
        seen.add(prediction_id)
        prediction_as_of = _timestamp(raw.get("prediction_as_of"), field=f"pairs[{index}].prediction_as_of")
        outcome_available_at = _timestamp(raw.get("outcome_available_at"), field=f"pairs[{index}].outcome_available_at")
        if prediction_as_of >= outcome_available_at:
            raise CalibrationAuditError(f"pit_prediction_must_precede_outcome:{prediction_id}")
        if outcome_available_at > audit_as_of:
            raise CalibrationAuditError(f"outcome_not_available_by_audit_as_of:{prediction_id}")
        outcome = raw.get("outcome")
        if isinstance(outcome, bool):
            outcome = int(outcome)
        if outcome not in allowed_outcomes:
            raise CalibrationAuditError(f"binary_outcome_required:{prediction_id}")
        node_id = _nonblank(raw.get("lineage_node_id"), field=f"pairs[{index}].lineage_node_id")
        version_id = _nonblank(raw.get("lineage_version_id"), field=f"pairs[{index}].lineage_version_id")
        node = lineage.get_node(node_id, version_id)
        rows.append({
            "prediction_id": prediction_id,
            "predicted_probability": _probability(raw.get("predicted_probability"), field=f"pairs[{index}].predicted_probability"),
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


def _sigmoid(values: np.ndarray) -> np.ndarray:
    positive = values >= 0
    out = np.empty_like(values, dtype=float)
    out[positive] = 1.0 / (1.0 + np.exp(-values[positive]))
    exp_values = np.exp(values[~positive])
    out[~positive] = exp_values / (1.0 + exp_values)
    return out


def _calibration_regression(
    probabilities: np.ndarray,
    outcomes: np.ndarray,
    *,
    minimum_rows: int,
    epsilon: float,
) -> dict[str, Any]:
    if len(probabilities) < minimum_rows:
        return {
            "status": "INSUFFICIENT_ROWS",
            "N": int(len(probabilities)),
            "minimum_rows": int(minimum_rows),
            "intercept": None,
            "slope": None,
        }
    if len(np.unique(outcomes)) < 2:
        return {
            "status": "INSUFFICIENT_OUTCOME_VARIATION",
            "N": int(len(probabilities)),
            "intercept": None,
            "slope": None,
        }

    clipped = np.clip(probabilities, epsilon, 1.0 - epsilon)
    logits = np.log(clipped / (1.0 - clipped))
    x = np.column_stack([np.ones(len(logits), dtype=float), logits])
    beta = np.asarray([0.0, 1.0], dtype=float)
    converged = False
    for _ in range(100):
        eta = x @ beta
        mu = _sigmoid(eta)
        weights = np.clip(mu * (1.0 - mu), 1e-12, None)
        information = x.T @ (x * weights[:, None])
        score = x.T @ (outcomes - mu)
        try:
            delta = np.linalg.solve(information, score)
        except np.linalg.LinAlgError:
            return {
                "status": "SINGULAR_INFORMATION_MATRIX",
                "N": int(len(probabilities)),
                "intercept": None,
                "slope": None,
            }
        beta_next = beta + delta
        if not np.all(np.isfinite(beta_next)):
            return {
                "status": "NUMERICAL_FAILURE",
                "N": int(len(probabilities)),
                "intercept": None,
                "slope": None,
            }
        beta = beta_next
        if float(np.max(np.abs(delta))) < 1e-10:
            converged = True
            break
    if not converged:
        return {
            "status": "NOT_CONVERGED",
            "N": int(len(probabilities)),
            "intercept": None,
            "slope": None,
        }
    return {
        "status": "AVAILABLE",
        "N": int(len(probabilities)),
        "intercept": float(beta[0]),
        "slope": float(beta[1]),
        "ideal_intercept": 0.0,
        "ideal_slope": 1.0,
    }


def _reliability_bins(
    probabilities: np.ndarray,
    outcomes: np.ndarray,
    boundaries: Sequence[float],
) -> list[dict[str, Any]]:
    edges = [float(value) for value in boundaries]
    if len(edges) < 2 or edges[0] != 0.0 or edges[-1] != 1.0 or any(
        not math.isfinite(value) for value in edges
    ) or any(right <= left for left, right in zip(edges, edges[1:])):
        raise CalibrationAuditError("reliability_bin_boundaries_invalid")
    bins: list[dict[str, Any]] = []
    for index, (left, right) in enumerate(zip(edges, edges[1:])):
        if index == len(edges) - 2:
            mask = (probabilities >= left) & (probabilities <= right)
        else:
            mask = (probabilities >= left) & (probabilities < right)
        count = int(mask.sum())
        bins.append({
            "lower": left,
            "upper": right,
            "upper_inclusive": index == len(edges) - 2,
            "N": count,
            "mean_predicted_probability": float(probabilities[mask].mean()) if count else None,
            "observed_rate": float(outcomes[mask].mean()) if count else None,
        })
    return bins


def _metrics(rows: Sequence[Mapping[str, Any]], *, bins: Sequence[float], minimum_rows: int, epsilon: float) -> dict[str, Any]:
    probabilities = np.asarray([row["predicted_probability"] for row in rows], dtype=float)
    outcomes = np.asarray([row["outcome"] for row in rows], dtype=float)
    clipped = np.clip(probabilities, epsilon, 1.0 - epsilon)
    brier = float(np.mean((probabilities - outcomes) ** 2))
    log_loss = float(-np.mean(outcomes * np.log(clipped) + (1.0 - outcomes) * np.log(1.0 - clipped)))
    return {
        "N": int(len(rows)),
        "positive_outcomes": int(outcomes.sum()),
        "mean_predicted_probability": float(probabilities.mean()),
        "observed_rate": float(outcomes.mean()),
        "BRIER_SCORE": brier,
        "LOG_LOSS": log_loss,
        "CALIBRATION_REGRESSION": _calibration_regression(
            probabilities, outcomes, minimum_rows=minimum_rows, epsilon=epsilon
        ),
        "RELIABILITY_BINS": _reliability_bins(probabilities, outcomes, bins),
    }


def _group_audit(
    rows: Sequence[Mapping[str, Any]],
    field: str,
    *,
    bins: Sequence[float],
    minimum_rows: int,
    epsilon: float,
) -> dict[str, Any]:
    missing = sum(row.get(field) is None for row in rows)
    values = sorted({str(row[field]) for row in rows if row.get(field) is not None})
    groups: list[dict[str, Any]] = []
    for value in values:
        selected = [row for row in rows if str(row.get(field)) == value]
        if len(selected) < minimum_rows:
            groups.append({
                "group": value,
                "status": "INSUFFICIENT_ROWS",
                "N": len(selected),
                "minimum_rows": minimum_rows,
                "metrics": None,
            })
        else:
            groups.append({
                "group": value,
                "status": "AVAILABLE",
                "N": len(selected),
                "metrics": _metrics(selected, bins=bins, minimum_rows=minimum_rows, epsilon=epsilon),
            })
    return {
        "field": field,
        "status": "UNKNOWN_MISSING_GROUP_METADATA" if missing else ("AVAILABLE" if groups else "NOT_APPLICABLE"),
        "missing_count": missing,
        "groups": groups,
    }


def audit_calibration(
    records: Sequence[Mapping[str, Any]],
    *,
    identity: Mapping[str, Any],
    analysis_plans: AnalysisPlanRegistry,
    results: ResultRegistry,
    lineage: LineageRegistry,
    reliability_bins: Sequence[float] | None = None,
) -> dict[str, Any]:
    """Audit calibration without retraining, reselection or productive changes."""
    contract = load_qm_de_contract()
    audit_identity = _validate_identity(
        identity,
        analysis_plans=analysis_plans,
        results=results,
        lineage=lineage,
    )
    spec = contract["calibration"]
    audit_as_of = _timestamp(audit_identity["audit_as_of"], field="audit_identity.audit_as_of")
    rows = _normalize_pairs(records, audit_as_of=audit_as_of, lineage=lineage)
    bins = list(reliability_bins or spec["default_reliability_bins"])
    minimum_rows = int(spec["minimum_rows_for_calibration_regression"])
    subgroup_minimum = int(spec["minimum_rows_per_subgroup"])
    epsilon = float(spec["log_loss_clip_epsilon"])
    overall = _metrics(rows, bins=bins, minimum_rows=minimum_rows, epsilon=epsilon)
    subgroup = _group_audit(
        rows, "subgroup", bins=bins, minimum_rows=subgroup_minimum, epsilon=epsilon
    )
    time_block = _group_audit(
        rows, "time_block", bins=bins, minimum_rows=subgroup_minimum, epsilon=epsilon
    )
    unknown_components = any(
        item["status"].startswith("UNKNOWN") for item in (subgroup, time_block)
    )
    return {
        "schema_version": "qm_e_calibration_audit_v1",
        "audit_identity": audit_identity,
        "audit_hash": _hash({
            "identity": audit_identity,
            "prediction_ids": [row["prediction_id"] for row in rows],
        }),
        "status": "COMPLETE_WITH_UNKNOWN_COMPONENTS" if unknown_components else "COMPLETE",
        "pair_count": len(rows),
        "overall": overall,
        "subgroups": subgroup,
        "time_blocks": time_block,
        "lineage_node_types": sorted({str(row["lineage_node_type"]) for row in rows}),
        "automatic_recalibration_performed": False,
        "productive_probability_update_performed": False,
        "empirical_promotion_performed": False,
    }


def audit_phase2_bridge(report: Mapping[str, Any] | None) -> dict[str, Any]:
    """Classify what existing Phase-2 aggregate calibration evidence can support.

    Existing block-bootstrap diagnostics are acknowledged, but an aggregate
    report without row-level PIT prediction/outcome pairs cannot produce QM-E
    Brier, log-loss or calibration-regression metrics.
    """
    contract = load_qm_de_contract()
    bridge = contract["existing_phase2_bridge"]
    if report is None:
        return {
            "status": bridge["insufficient_status"],
            "aggregate_report_present": False,
            "recognized_block_method_present": False,
            "row_level_metrics_computed": False,
        }
    text = _canonical_json(dict(report))
    recognized = bridge["recognized_block_method"] in text
    return {
        "status": bridge["insufficient_status"],
        "aggregate_report_present": True,
        "recognized_block_method_present": bool(recognized),
        "iid_diagnostics_must_remain_diagnostics_only": bool(bridge["iid_diagnostics_must_remain_diagnostics_only"]),
        "row_level_metrics_computed": False,
        "aggregate_report_promoted_to_row_level_pairs": False,
    }
