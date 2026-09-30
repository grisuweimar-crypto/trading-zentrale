"""QM-D dependence, effective-N and robustness audit.

Research-only. The audit reports multiple explicitly labelled effective-information
diagnostics; it never collapses them into one supposedly universal N_eff. Missing
cluster metadata remains UNKNOWN, and every observation must resolve to an
existing QM-I lineage node/version.
"""
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
from scanner.research.governance.qm_c_results import ResultRegistry
from scanner.research.governance.qm_i_lineage import LineageRegistry


DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_de_dependence_calibration_v1.json"


class DependenceAuditError(ValueError):
    """Raised when a QM-D dependence-audit invariant is violated."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _hash(value: Any) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _nonblank(value: Any, *, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise DependenceAuditError(f"value_required:{field}")
    return text


def _finite(value: Any, *, field: str) -> float:
    if isinstance(value, bool):
        raise DependenceAuditError(f"finite_number_required:{field}")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise DependenceAuditError(f"finite_number_required:{field}") from exc
    if not math.isfinite(result):
        raise DependenceAuditError(f"finite_number_required:{field}")
    return result


def _timestamp(value: Any, *, field: str) -> datetime:
    text = _nonblank(value, field=field)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DependenceAuditError(f"timestamp_invalid:{field}:{text}") from exc
    if parsed.tzinfo is None:
        raise DependenceAuditError(f"timestamp_timezone_required:{field}")
    return parsed


def load_qm_de_contract(path: str | Path | None = None) -> dict[str, Any]:
    contract_path = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(contract_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DependenceAuditError(f"qm_de_contract_unreadable:{contract_path}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != "qm_de_dependence_calibration_v1":
        raise DependenceAuditError("qm_de_contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise DependenceAuditError("qm_de_contract_scope_invalid")
    return payload


def _validate_identity(
    identity: Mapping[str, Any],
    *,
    analysis_plans: AnalysisPlanRegistry,
    results: ResultRegistry,
    lineage: LineageRegistry,
    calibration: bool = False,
) -> dict[str, str]:
    contract = load_qm_de_contract()
    required = list(contract["identity_binding"]["required_audit_fields"])
    if calibration:
        required += ["prediction_definition_hash", "label_definition_hash"]
    missing = [field for field in required if not str(identity.get(field) or "").strip()]
    if missing:
        raise DependenceAuditError("audit_identity_missing:" + ",".join(missing))
    normalized = {field: str(identity[field]).strip() for field in required}

    plan = analysis_plans.get_plan(normalized["analysis_plan_id"], normalized["analysis_plan_version"])
    if plan["analysis_plan_hash"] != normalized["analysis_plan_hash"]:
        raise DependenceAuditError("audit_identity_analysis_plan_hash_mismatch")
    result = results.get_result(normalized["result_id"], normalized["result_version"])
    if result["result_hash"] != normalized["result_hash"]:
        raise DependenceAuditError("audit_identity_result_hash_mismatch")
    verification = lineage.verify_integrity()
    if verification["head_hash"] != normalized["lineage_registry_head_hash"]:
        raise DependenceAuditError("audit_identity_lineage_head_hash_mismatch")
    return normalized


def _normalize_observations(
    records: Sequence[Mapping[str, Any]],
    *,
    lineage: LineageRegistry,
) -> list[dict[str, Any]]:
    contract = load_qm_de_contract()
    required = tuple(contract["dependence"]["required_observation_fields"])
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw in enumerate(records):
        if not isinstance(raw, Mapping):
            raise DependenceAuditError(f"observation_must_be_object:{index}")
        missing = [field for field in required if field not in raw]
        if missing:
            raise DependenceAuditError(f"observation_fields_missing:{index}:" + ",".join(missing))
        observation_id = _nonblank(raw.get("observation_id"), field=f"observations[{index}].observation_id")
        if observation_id in seen:
            raise DependenceAuditError(f"duplicate_observation_id:{observation_id}")
        seen.add(observation_id)
        observed_at = _timestamp(raw.get("observed_at"), field=f"observations[{index}].observed_at")
        interval_start = _timestamp(raw.get("interval_start"), field=f"observations[{index}].interval_start")
        interval_end = _timestamp(raw.get("interval_end"), field=f"observations[{index}].interval_end")
        if interval_end <= interval_start:
            raise DependenceAuditError(f"observation_interval_invalid:{observation_id}")
        lineage_node_id = _nonblank(raw.get("lineage_node_id"), field=f"observations[{index}].lineage_node_id")
        lineage_version_id = _nonblank(raw.get("lineage_version_id"), field=f"observations[{index}].lineage_version_id")
        lineage.get_node(lineage_node_id, lineage_version_id)
        normalized.append(
            {
                "observation_id": observation_id,
                "symbol": _nonblank(raw.get("symbol"), field=f"observations[{index}].symbol"),
                "observed_at": observed_at,
                "value": _finite(raw.get("value"), field=f"observations[{index}].value"),
                "interval_start": interval_start,
                "interval_end": interval_end,
                "sector": str(raw.get("sector") or "").strip() or None,
                "time_block": str(raw.get("time_block") or "").strip() or None,
                "lineage_node_id": lineage_node_id,
                "lineage_version_id": lineage_version_id,
            }
        )
    if not normalized:
        raise DependenceAuditError("dependence_audit_requires_observations")
    return sorted(normalized, key=lambda row: (row["observed_at"], row["symbol"], row["observation_id"]))


def _cluster_concentration(rows: Sequence[Mapping[str, Any]], field: str) -> dict[str, Any]:
    missing = [row["observation_id"] for row in rows if not row.get(field)]
    if missing:
        return {
            "status": "UNKNOWN_MISSING_CLUSTER_METADATA",
            "cluster_field": field,
            "missing_count": len(missing),
            "N_raw": len(rows),
            "N_eff": None,
        }
    counts = Counter(str(row[field]) for row in rows)
    n = len(rows)
    denom = float(sum(size * size for size in counts.values()))
    n_eff = float(n * n / denom) if denom else None
    return {
        "status": "AVAILABLE",
        "cluster_field": field,
        "cluster_count": len(counts),
        "cluster_sizes": dict(sorted(counts.items())),
        "N_raw": n,
        "N_eff": n_eff,
        "interpretation": "conservative effective cluster count under full within-cluster dependence; not a universal adjusted sample size",
    }


def _pooled_symbol_ar1(rows: Sequence[Mapping[str, Any]], min_rows: int) -> dict[str, Any]:
    by_symbol: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        by_symbol.setdefault(str(row["symbol"]), []).append(row)
    estimates: list[tuple[str, float, int]] = []
    for symbol, values in sorted(by_symbol.items()):
        ordered = sorted(values, key=lambda row: row["observed_at"])
        if len(ordered) < min_rows:
            continue
        x = np.asarray([row["value"] for row in ordered], dtype=float)
        left, right = x[:-1], x[1:]
        if np.std(left) <= 0 or np.std(right) <= 0:
            continue
        rho = float(np.corrcoef(left, right)[0, 1])
        if math.isfinite(rho):
            estimates.append((symbol, max(-0.999999, min(0.999999, rho)), len(left)))
    if not estimates:
        return {
            "status": "INSUFFICIENT_DATA",
            "N_raw": len(rows),
            "N_eff": None,
            "eligible_symbols": 0,
            "minimum_rows_per_symbol": min_rows,
        }
    total_pairs = sum(weight for _, _, weight in estimates)
    pooled_rho = sum(rho * weight for _, rho, weight in estimates) / total_pairs
    raw_n = float(len(rows))
    n_eff = raw_n * (1.0 - pooled_rho) / (1.0 + pooled_rho)
    n_eff = max(1.0, min(raw_n, n_eff))
    return {
        "status": "AVAILABLE",
        "N_raw": len(rows),
        "N_eff": float(n_eff),
        "pooled_lag1_autocorrelation": float(pooled_rho),
        "eligible_symbols": len(estimates),
        "symbol_estimates": [
            {"symbol": symbol, "lag1_autocorrelation": rho, "pair_count": pairs}
            for symbol, rho, pairs in estimates
        ],
        "interpretation": "AR(1)-style diagnostic based on pooled within-symbol lag-1 correlation; clipped to [1,N_raw] and not a universal N_eff",
    }


def _overlap_proxy(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    overlap_pairs = 0
    for i, left in enumerate(rows):
        for right in rows[i + 1 :]:
            if left["interval_start"] < right["interval_end"] and right["interval_start"] < left["interval_end"]:
                overlap_pairs += 1
    n = len(rows)
    mean_concurrency = 1.0 + (2.0 * overlap_pairs / n)
    n_eff = max(1.0, min(float(n), float(n / mean_concurrency)))
    return {
        "status": "AVAILABLE",
        "N_raw": n,
        "N_eff": n_eff,
        "overlapping_pair_count": overlap_pairs,
        "mean_pairwise_concurrency_proxy": mean_concurrency,
        "interpretation": "overlap-concurrency proxy only; it flags information reuse from overlapping evaluation windows and is not a universal N_eff",
    }


def _leave_one_out(rows: Sequence[Mapping[str, Any]], field: str, minimum_clusters: int) -> dict[str, Any]:
    if any(not row.get(field) for row in rows):
        return {"status": "UNKNOWN_MISSING_CLUSTER_METADATA", "cluster_field": field, "estimates": []}
    groups = sorted({str(row[field]) for row in rows})
    if len(groups) < minimum_clusters:
        return {"status": "INSUFFICIENT_CLUSTERS", "cluster_field": field, "cluster_count": len(groups), "estimates": []}
    overall = float(np.mean([row["value"] for row in rows]))
    estimates = []
    for group in groups:
        kept = [row["value"] for row in rows if str(row[field]) != group]
        if not kept:
            continue
        estimates.append({"omitted": group, "mean": float(np.mean(kept)), "N": len(kept)})
    means = [row["mean"] for row in estimates]
    if overall > 0:
        sign_stable = all(value > 0 for value in means)
    elif overall < 0:
        sign_stable = all(value < 0 for value in means)
    else:
        sign_stable = all(value == 0 for value in means)
    return {
        "status": "AVAILABLE",
        "cluster_field": field,
        "cluster_count": len(groups),
        "overall_mean": overall,
        "leave_one_out_min": min(means) if means else None,
        "leave_one_out_max": max(means) if means else None,
        "sign_stable": sign_stable,
        "estimates": estimates,
    }


def _cluster_bootstrap(
    rows: Sequence[Mapping[str, Any]],
    field: str,
    *,
    reps: int,
    seed: int,
) -> dict[str, Any]:
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
    means: list[float] = []
    for _ in range(reps):
        chosen = rng.choice(names, size=len(names), replace=True)
        values = [value for name in chosen for value in groups[str(name)]]
        means.append(float(np.mean(values)))
    low, high = np.quantile(np.asarray(means, dtype=float), [0.025, 0.975])
    return {
        "status": "AVAILABLE",
        "cluster_field": field,
        "cluster_count": len(names),
        "bootstrap_reps": reps,
        "seed": seed,
        "mean_estimate": float(np.mean([row["value"] for row in rows])),
        "interval_95": [float(low), float(high)],
    }


def audit_dependence(
    records: Sequence[Mapping[str, Any]],
    *,
    identity: Mapping[str, Any],
    analysis_plans: AnalysisPlanRegistry,
    results: ResultRegistry,
    lineage: LineageRegistry,
    bootstrap_reps: int | None = None,
    random_seed: int = 20261001,
) -> dict[str, Any]:
    """Run the declared QM-D dependence/robustness audit.

    The result intentionally exposes several method-specific N_eff diagnostics
    rather than choosing one number as truth.
    """
    contract = load_qm_de_contract()
    audit_identity = _validate_identity(
        identity,
        analysis_plans=analysis_plans,
        results=results,
        lineage=lineage,
    )
    rows = _normalize_observations(records, lineage=lineage)
    spec = contract["dependence"]
    reps = int(bootstrap_reps if bootstrap_reps is not None else spec["default_bootstrap_reps"])
    if reps <= 0:
        raise DependenceAuditError("bootstrap_reps_must_be_positive")

    symbol_cluster = _cluster_concentration(rows, "symbol")
    sector_cluster = _cluster_concentration(rows, "sector")
    time_cluster = _cluster_concentration(rows, "time_block")
    diagnostics = {
        "RAW_N": {"status": "AVAILABLE", "N_raw": len(rows), "N_eff": float(len(rows))},
        "POOLED_WITHIN_SYMBOL_AR1": _pooled_symbol_ar1(rows, int(spec["minimum_ar1_observations_per_symbol"])),
        "SYMBOL_CLUSTER_CONCENTRATION": symbol_cluster,
        "SECTOR_CLUSTER_CONCENTRATION": sector_cluster,
        "TIME_BLOCK_CLUSTER_CONCENTRATION": time_cluster,
        "OVERLAP_CONCURRENCY_PROXY": _overlap_proxy(rows),
    }
    robustness = {
        "LEAVE_ONE_SYMBOL_OUT": _leave_one_out(rows, "symbol", int(spec["minimum_cluster_count_for_leave_one_out"])),
        "LEAVE_ONE_SECTOR_OUT": _leave_one_out(rows, "sector", int(spec["minimum_cluster_count_for_leave_one_out"])),
        "LEAVE_ONE_TIME_BLOCK_OUT": _leave_one_out(rows, "time_block", int(spec["minimum_cluster_count_for_leave_one_out"])),
        "SYMBOL_CLUSTER_BOOTSTRAP": _cluster_bootstrap(rows, "symbol", reps=reps, seed=random_seed),
        "TIME_BLOCK_BOOTSTRAP": _cluster_bootstrap(rows, "time_block", reps=reps, seed=random_seed + 1),
    }
    return {
        "schema_version": "qm_d_dependence_audit_v1",
        "audit_identity": audit_identity,
        "audit_hash": _hash({"identity": audit_identity, "observations": [row["observation_id"] for row in rows]}),
        "status": "COMPLETE_WITH_UNKNOWN_COMPONENTS" if any(
            value.get("status", "").startswith("UNKNOWN") for value in [*diagnostics.values(), *robustness.values()]
        ) else "COMPLETE",
        "N_raw": len(rows),
        "effective_n_diagnostics": diagnostics,
        "robustness": robustness,
        "single_universal_N_eff_selected": False,
        "productive_change_performed": False,
    }
