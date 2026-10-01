"""QM-D dependence, effective-N and robustness audit."""
from __future__ import annotations

from collections import Counter
from datetime import datetime
from hashlib import sha256
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from scanner.research.governance.qm_c_analysis_plan import AnalysisPlanRegistry
from scanner.research.governance.qm_c_negative_results import NegativeResultRegistry
from scanner.research.governance.qm_i_lineage import LineageRegistry

DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_de_dependence_calibration_v1.json"


class DependenceAuditError(ValueError):
    pass


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _hash(value: Any) -> str:
    return sha256(_json(value).encode("utf-8")).hexdigest()


def _text(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise DependenceAuditError(f"value_required:{field}")
    return text


def _finite(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise DependenceAuditError(f"finite_number_required:{field}")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise DependenceAuditError(f"finite_number_required:{field}") from exc
    if not math.isfinite(number):
        raise DependenceAuditError(f"finite_number_required:{field}")
    return number


def _timestamp(value: Any, field: str) -> datetime:
    text = _text(value, field)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DependenceAuditError(f"timestamp_invalid:{field}:{text}") from exc
    if parsed.tzinfo is None:
        raise DependenceAuditError(f"timestamp_timezone_required:{field}")
    return parsed


def load_qm_de_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DependenceAuditError(f"qm_de_contract_unreadable:{target}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != "qm_de_dependence_calibration_v1":
        raise DependenceAuditError("qm_de_contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False or payload.get("execution_allowed") is not False:
        raise DependenceAuditError("qm_de_contract_scope_invalid")
    return payload


def _validate_identity(identity: Mapping[str, Any], *, analysis_plans: AnalysisPlanRegistry, results: NegativeResultRegistry, lineage: LineageRegistry) -> dict[str, str]:
    required = list(load_qm_de_contract()["identity_binding"]["required_audit_fields"])
    missing = [field for field in required if not str(identity.get(field) or "").strip()]
    if missing:
        raise DependenceAuditError("audit_identity_missing:" + ",".join(missing))
    normalized = {field: str(identity[field]).strip() for field in required}
    _timestamp(normalized["audit_as_of"], "audit_identity.audit_as_of")
    plan = analysis_plans.get_plan(normalized["analysis_plan_id"], normalized["analysis_plan_version"])
    if plan["analysis_plan_hash"] != normalized["analysis_plan_hash"]:
        raise DependenceAuditError("audit_identity_analysis_plan_hash_mismatch")
    result = results.get_result(normalized["result_id"], normalized["result_version"])
    if result["result_hash"] != normalized["result_hash"]:
        raise DependenceAuditError("audit_identity_result_hash_mismatch")
    for field in ("analysis_plan_id", "analysis_plan_version", "analysis_plan_hash"):
        if result.get(field) != normalized[field]:
            raise DependenceAuditError(f"audit_identity_result_plan_binding_mismatch:{field}")
    freeze = plan.get("freeze_context")
    if isinstance(freeze, Mapping) and freeze.get("dataset_snapshot_hash") is not None and str(freeze["dataset_snapshot_hash"]) != normalized["dataset_snapshot_hash"]:
        raise DependenceAuditError("audit_identity_dataset_snapshot_hash_mismatch")
    if lineage.verify_integrity()["head_hash"] != normalized["lineage_registry_head_hash"]:
        raise DependenceAuditError("audit_identity_lineage_head_hash_mismatch")
    return normalized


def _normalize(records: Sequence[Mapping[str, Any]], lineage: LineageRegistry) -> list[dict[str, Any]]:
    required = tuple(load_qm_de_contract()["dependence"]["required_observation_fields"])
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw in enumerate(records):
        if not isinstance(raw, Mapping):
            raise DependenceAuditError(f"observation_must_be_object:{index}")
        missing = [field for field in required if field not in raw]
        if missing:
            raise DependenceAuditError(f"observation_fields_missing:{index}:" + ",".join(missing))
        observation_id = _text(raw.get("observation_id"), f"observations[{index}].observation_id")
        if observation_id in seen:
            raise DependenceAuditError(f"duplicate_observation_id:{observation_id}")
        seen.add(observation_id)
        observed_at = _timestamp(raw.get("observed_at"), f"observations[{index}].observed_at")
        interval_start = _timestamp(raw.get("interval_start"), f"observations[{index}].interval_start")
        interval_end = _timestamp(raw.get("interval_end"), f"observations[{index}].interval_end")
        if interval_end <= interval_start:
            raise DependenceAuditError(f"observation_interval_invalid:{observation_id}")
        node_id = _text(raw.get("lineage_node_id"), f"observations[{index}].lineage_node_id")
        version_id = _text(raw.get("lineage_version_id"), f"observations[{index}].lineage_version_id")
        lineage.get_node(node_id, version_id)
        rows.append({
            "observation_id": observation_id,
            "symbol": _text(raw.get("symbol"), f"observations[{index}].symbol"),
            "observed_at": observed_at,
            "value": _finite(raw.get("value"), f"observations[{index}].value"),
            "interval_start": interval_start,
            "interval_end": interval_end,
            "sector": str(raw.get("sector") or "").strip() or None,
            "time_block": str(raw.get("time_block") or "").strip() or None,
            "lineage_node_id": node_id,
            "lineage_version_id": version_id,
        })
    if not rows:
        raise DependenceAuditError("dependence_audit_requires_observations")
    return sorted(rows, key=lambda row: (row["observed_at"], row["symbol"], row["observation_id"]))


def _hash_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [{
        "observation_id": row["observation_id"], "symbol": row["symbol"],
        "observed_at": row["observed_at"].isoformat(), "value": row["value"],
        "interval_start": row["interval_start"].isoformat(), "interval_end": row["interval_end"].isoformat(),
        "sector": row["sector"], "time_block": row["time_block"],
        "lineage_node_id": row["lineage_node_id"], "lineage_version_id": row["lineage_version_id"],
    } for row in rows]


def _cluster(rows: Sequence[Mapping[str, Any]], field: str) -> dict[str, Any]:
    missing = sum(not row.get(field) for row in rows)
    if missing:
        return {"status": "UNKNOWN_MISSING_CLUSTER_METADATA", "cluster_field": field, "missing_count": missing, "N_raw": len(rows), "N_eff": None}
    counts = Counter(str(row[field]) for row in rows)
    n = len(rows)
    denom = float(sum(size * size for size in counts.values()))
    return {"status": "AVAILABLE", "cluster_field": field, "cluster_count": len(counts), "cluster_sizes": dict(sorted(counts.items())), "N_raw": n, "N_eff": float(n * n / denom) if denom else None, "interpretation": "conservative effective cluster count under full within-cluster dependence; not a universal adjusted sample size"}


def _ar1(rows: Sequence[Mapping[str, Any]], minimum: int) -> dict[str, Any]:
    by_symbol: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        by_symbol.setdefault(str(row["symbol"]), []).append(row)
    estimates: list[tuple[str, float, int]] = []
    for symbol, values in sorted(by_symbol.items()):
        ordered = sorted(values, key=lambda row: row["observed_at"])
        if len(ordered) < minimum:
            continue
        x = np.asarray([row["value"] for row in ordered], dtype=float)
        if np.std(x[:-1]) <= 0 or np.std(x[1:]) <= 0:
            continue
        rho = float(np.corrcoef(x[:-1], x[1:])[0, 1])
        if math.isfinite(rho):
            estimates.append((symbol, max(-0.999999, min(0.999999, rho)), len(x) - 1))
    if not estimates:
        return {"status": "INSUFFICIENT_DATA", "N_raw": len(rows), "N_eff": None, "eligible_symbols": 0, "minimum_rows_per_symbol": minimum}
    pairs = sum(weight for _, _, weight in estimates)
    rho = sum(value * weight for _, value, weight in estimates) / pairs
    n = float(len(rows))
    n_eff = max(1.0, min(n, n * (1.0 - rho) / (1.0 + rho)))
    return {"status": "AVAILABLE", "N_raw": len(rows), "N_eff": n_eff, "pooled_lag1_autocorrelation": rho, "eligible_symbols": len(estimates), "symbol_estimates": [{"symbol": symbol, "lag1_autocorrelation": value, "pair_count": weight} for symbol, value, weight in estimates], "interpretation": "AR(1)-style diagnostic; not a universal N_eff"}


def _overlap(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    pairs = sum(left["interval_start"] < right["interval_end"] and right["interval_start"] < left["interval_end"] for i, left in enumerate(rows) for right in rows[i + 1:])
    n = len(rows)
    concurrency = 1.0 + 2.0 * pairs / n
    return {"status": "AVAILABLE", "N_raw": n, "N_eff": max(1.0, min(float(n), n / concurrency)), "overlapping_pair_count": int(pairs), "mean_pairwise_concurrency_proxy": concurrency, "interpretation": "overlap-concurrency proxy only; not a universal N_eff"}


def _leave_one(rows: Sequence[Mapping[str, Any]], field: str, minimum: int) -> dict[str, Any]:
    if any(not row.get(field) for row in rows):
        return {"status": "UNKNOWN_MISSING_CLUSTER_METADATA", "cluster_field": field, "estimates": []}
    groups = sorted({str(row[field]) for row in rows})
    if len(groups) < minimum:
        return {"status": "INSUFFICIENT_CLUSTERS", "cluster_field": field, "cluster_count": len(groups), "estimates": []}
    overall = float(np.mean([row["value"] for row in rows]))
    estimates = [{"omitted": group, "mean": float(np.mean([row["value"] for row in rows if str(row[field]) != group])), "N": sum(str(row[field]) != group for row in rows)} for group in groups]
    means = [row["mean"] for row in estimates]
    sign_stable = all(value > 0 for value in means) if overall > 0 else all(value < 0 for value in means) if overall < 0 else all(value == 0 for value in means)
    return {"status": "AVAILABLE", "cluster_field": field, "cluster_count": len(groups), "overall_mean": overall, "leave_one_out_min": min(means), "leave_one_out_max": max(means), "sign_stable": sign_stable, "estimates": estimates}


def _bootstrap(rows: Sequence[Mapping[str, Any]], field: str, reps: int, seed: int) -> dict[str, Any]:
    if reps <= 0:
        raise DependenceAuditError("bootstrap_reps_must_be_positive")
    if any(not row.get(field) for row in rows):
        return {"status": "UNKNOWN_MISSING_CLUSTER_METADATA", "cluster_field": field, "interval_95": None}
    groups: dict[str, list[float]] = {}
    for row in rows:
        groups.setdefault(str(row[field]), []).append(float(row["value"]))
    names = sorted(groups)
    if len(names) < 2:
        return {"status": "INSUFFICIENT_CLUSTERS", "cluster_field": field, "cluster_count": len(names), "interval_95": None}
    rng = np.random.default_rng(seed)
    means = []
    for _ in range(reps):
        chosen = rng.choice(names, size=len(names), replace=True)
        means.append(float(np.mean([value for name in chosen for value in groups[str(name)]])))
    low, high = np.quantile(np.asarray(means), [0.025, 0.975])
    return {"status": "AVAILABLE", "cluster_field": field, "cluster_count": len(names), "bootstrap_reps": reps, "seed": seed, "mean_estimate": float(np.mean([row["value"] for row in rows])), "interval_95": [float(low), float(high)]}


def audit_dependence(records: Sequence[Mapping[str, Any]], *, identity: Mapping[str, Any], analysis_plans: AnalysisPlanRegistry, results: NegativeResultRegistry, lineage: LineageRegistry, bootstrap_reps: int | None = None, random_seed: int = 20261001) -> dict[str, Any]:
    contract = load_qm_de_contract()
    audit_identity = _validate_identity(identity, analysis_plans=analysis_plans, results=results, lineage=lineage)
    rows = _normalize(records, lineage)
    spec = contract["dependence"]
    reps = int(bootstrap_reps if bootstrap_reps is not None else spec["default_bootstrap_reps"])
    if reps <= 0:
        raise DependenceAuditError("bootstrap_reps_must_be_positive")
    diagnostics = {
        "RAW_N": {"status": "AVAILABLE", "N_raw": len(rows), "N_eff": float(len(rows))},
        "POOLED_WITHIN_SYMBOL_AR1": _ar1(rows, int(spec["minimum_ar1_observations_per_symbol"])),
        "SYMBOL_CLUSTER_CONCENTRATION": _cluster(rows, "symbol"),
        "SECTOR_CLUSTER_CONCENTRATION": _cluster(rows, "sector"),
        "TIME_BLOCK_CLUSTER_CONCENTRATION": _cluster(rows, "time_block"),
        "OVERLAP_CONCURRENCY_PROXY": _overlap(rows),
    }
    robustness = {
        "LEAVE_ONE_SYMBOL_OUT": _leave_one(rows, "symbol", int(spec["minimum_cluster_count_for_leave_one_out"])),
        "LEAVE_ONE_SECTOR_OUT": _leave_one(rows, "sector", int(spec["minimum_cluster_count_for_leave_one_out"])),
        "LEAVE_ONE_TIME_BLOCK_OUT": _leave_one(rows, "time_block", int(spec["minimum_cluster_count_for_leave_one_out"])),
        "SYMBOL_CLUSTER_BOOTSTRAP": _bootstrap(rows, "symbol", reps, random_seed),
        "TIME_BLOCK_BOOTSTRAP": _bootstrap(rows, "time_block", reps, random_seed + 1),
    }
    unknown = any(str(value.get("status", "")).startswith("UNKNOWN") for value in [*diagnostics.values(), *robustness.values()])
    audit_parameters = {"bootstrap_reps": reps, "random_seed": int(random_seed), "time_block_bootstrap_seed": int(random_seed) + 1}
    return {
        "schema_version": "qm_d_dependence_audit_v1",
        "audit_identity": audit_identity,
        "audit_hash": _hash({"identity": audit_identity, "observations": _hash_rows(rows), "parameters": audit_parameters}),
        "audit_parameters": audit_parameters,
        "status": "COMPLETE_WITH_UNKNOWN_COMPONENTS" if unknown else "COMPLETE",
        "N_raw": len(rows),
        "effective_n_diagnostics": diagnostics,
        "robustness": robustness,
        "single_universal_N_eff_selected": False,
        "productive_change_performed": False,
    }
