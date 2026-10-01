"""QM-F / BA-QM5 stateful Decision-Layer incremental ablation.

Research-only. Evaluates frozen B0-B6 shadow policy paths without changing Phase-7I
conclusions, productive Decision-Layer semantics, portfolio actions, or execution.
"""
from __future__ import annotations

from hashlib import sha256
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

from scanner.research.governance.qm_i_lineage import LineageRegistry

SCHEMA_VERSION = "qm_f_decision_ablation_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_f_decision_ablation_v1.json"
POLICIES = ("B0_FLAT", "B0_LONG", "B1", "B2", "B3", "B4", "B5", "B6")
ESTIMANDS = {"REALIZED_POLICY_VALUE", "NET_RETURN_DELTA", "TURNOVER_DELTA", "MAX_DRAWDOWN_DELTA"}


class DecisionAblationError(ValueError):
    pass


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def content_hash(value: Any) -> str:
    return sha256(_json(value).encode("utf-8")).hexdigest()


def _finite(value: object, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise DecisionAblationError(f"invalid_{field}") from exc
    if not math.isfinite(number):
        raise DecisionAblationError(f"invalid_{field}")
    return number


def load_qm_f_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DecisionAblationError(f"qm_f_contract_unreadable:{target}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != SCHEMA_VERSION:
        raise DecisionAblationError("qm_f_contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise DecisionAblationError("qm_f_contract_scope_invalid")
    if payload.get("execution_allowed") is not False or payload.get("must_not_relabel_phase7i_validation") is not True:
        raise DecisionAblationError("qm_f_contract_boundary_invalid")
    if tuple(payload.get("candidate_ladder") or ()) != POLICIES:
        raise DecisionAblationError("qm_f_candidate_ladder_invalid")
    return payload


def _validate_lineage_ref(ref: Mapping[str, Any], registry: LineageRegistry) -> tuple[str, str, str]:
    node_id = str(ref.get("node_id") or "").strip()
    version_id = str(ref.get("version_id") or "").strip()
    expected_hash = str(ref.get("content_hash") or "").strip().lower()
    if not node_id or not version_id or len(expected_hash) != 64:
        raise DecisionAblationError("lineage_ref_identity_invalid")
    node = registry.get_node(node_id, version_id)
    if node.get("content_hash") != expected_hash:
        raise DecisionAblationError(f"lineage_ref_hash_mismatch:{node_id}::{version_id}")
    if node.get("lineage_complete") is not True:
        raise DecisionAblationError(f"lineage_ref_incomplete:{node_id}::{version_id}")
    return node_id, version_id, expected_hash


def canonical_lineage(refs: Sequence[Mapping[str, Any]], *, registry: LineageRegistry, ignored_refs: Sequence[Mapping[str, Any]] = ()) -> tuple[tuple[str, str, str], ...]:
    ignored = {(str(ref.get("node_id") or "").strip(), str(ref.get("version_id") or "").strip()) for ref in ignored_refs}
    result: list[tuple[str, str, str]] = []
    seen: set[tuple[str, str]] = set()
    for ref in refs:
        identity = _validate_lineage_ref(ref, registry)
        key = (identity[0], identity[1])
        if key in seen:
            raise DecisionAblationError(f"duplicate_lineage_ref:{identity[0]}::{identity[1]}")
        seen.add(key)
        if key not in ignored:
            result.append(identity)
    for ref in ignored_refs:
        identity = _validate_lineage_ref(ref, registry)
        if (identity[0], identity[1]) not in seen:
            raise DecisionAblationError(f"ignored_lineage_ref_not_in_full_set:{identity[0]}::{identity[1]}")
    return tuple(sorted(result))


def validate_b5_b6_lineage_equivalence(*, b5_refs: Sequence[Mapping[str, Any]], b6_refs: Sequence[Mapping[str, Any]], b6_elliott_adjustment_refs: Sequence[Mapping[str, Any]], registry: LineageRegistry) -> dict[str, Any]:
    if not b6_elliott_adjustment_refs:
        raise DecisionAblationError("b6_elliott_adjustment_lineage_required")
    b5_core = canonical_lineage(b5_refs, registry=registry)
    b6_core = canonical_lineage(b6_refs, registry=registry, ignored_refs=b6_elliott_adjustment_refs)
    if b5_core != b6_core:
        raise DecisionAblationError("b5_b6_core_lineage_not_equivalent")
    return {
        "status": "EQUIVALENT_AFTER_REGISTERED_ELLIOTT_REMOVAL",
        "b5_core_hash": content_hash(b5_core),
        "b6_core_hash": content_hash(b6_core),
        "elliott_adjustment_count": len(b6_elliott_adjustment_refs),
    }


def _validate_step(step: Mapping[str, Any], policy_id: str, index: int) -> dict[str, Any]:
    if not isinstance(step, Mapping):
        raise DecisionAblationError(f"step_not_object:{index}")
    if step.get("policy_id") != policy_id:
        raise DecisionAblationError(f"step_policy_mismatch:{index}")
    symbol = str(step.get("symbol") or "").strip()
    as_of = str(step.get("as_of") or "").strip()
    snapshot_id = str(step.get("source_snapshot_id") or "").strip()
    if not symbol or not as_of or not snapshot_id:
        raise DecisionAblationError(f"step_identity_missing:{index}")
    exposure_before = _finite(step.get("exposure_before"), f"exposure_before:{index}")
    exposure_after = _finite(step.get("exposure_after"), f"exposure_after:{index}")
    if not (0.0 <= exposure_before <= 1.0 and 0.0 <= exposure_after <= 1.0):
        raise DecisionAblationError(f"exposure_out_of_range:{index}")
    asset_return = _finite(step.get("asset_return"), f"asset_return:{index}")
    cost_bps = _finite(step.get("transaction_cost_bps"), f"transaction_cost_bps:{index}")
    if cost_bps < 0:
        raise DecisionAblationError(f"negative_transaction_cost:{index}")
    eligible = step.get("eligible")
    tradeable = step.get("tradeable")
    action_available = step.get("action_available")
    if not all(isinstance(v, bool) for v in (eligible, tradeable, action_available)):
        raise DecisionAblationError(f"step_boolean_guard_invalid:{index}")
    if (not eligible or not tradeable or not action_available) and exposure_after != exposure_before:
        raise DecisionAblationError(f"state_change_when_action_unavailable:{index}")
    if policy_id == "B0_FLAT" and (exposure_before != 0.0 or exposure_after != 0.0):
        raise DecisionAblationError("b0_flat_must_remain_flat")
    if policy_id == "B0_LONG" and (exposure_before != 1.0 or exposure_after != 1.0):
        raise DecisionAblationError("b0_long_must_remain_long")
    return {**dict(step), "symbol": symbol, "as_of": as_of, "source_snapshot_id": snapshot_id, "exposure_before": exposure_before, "exposure_after": exposure_after, "asset_return": asset_return, "transaction_cost_bps": cost_bps, "eligible": eligible, "tradeable": tradeable, "action_available": action_available}


def evaluate_policy_path(*, policy_id: str, steps: Sequence[Mapping[str, Any]], estimand: str, initial_exposure: float, benchmark_returns: Sequence[float] | None = None) -> dict[str, Any]:
    if policy_id not in POLICIES:
        raise DecisionAblationError("unsupported_policy_id")
    if estimand not in ESTIMANDS:
        raise DecisionAblationError("unsupported_estimand")
    initial = _finite(initial_exposure, "initial_exposure")
    if not 0.0 <= initial <= 1.0:
        raise DecisionAblationError("initial_exposure_out_of_range")
    if not steps:
        raise DecisionAblationError("policy_path_steps_required")
    if estimand == "REALIZED_POLICY_VALUE" and benchmark_returns is None:
        raise DecisionAblationError("realized_policy_value_requires_explicit_benchmark")
    if benchmark_returns is not None and len(benchmark_returns) != len(steps):
        raise DecisionAblationError("benchmark_length_mismatch")
    validated = [_validate_step(step, policy_id, i) for i, step in enumerate(steps)]
    prior = initial
    wealth = 1.0
    benchmark_wealth = 1.0
    peak = 1.0
    max_drawdown = 0.0
    turnover = 0.0
    rows: list[dict[str, Any]] = []
    for i, step in enumerate(validated):
        if abs(step["exposure_before"] - prior) > 1e-12:
            raise DecisionAblationError(f"policy_path_discontinuity:{i}")
        turnover_i = abs(step["exposure_after"] - step["exposure_before"])
        cost = turnover_i * step["transaction_cost_bps"] / 10000.0
        gross = step["exposure_after"] * step["asset_return"]
        net = gross - cost
        wealth *= 1.0 + net
        if wealth <= 0:
            raise DecisionAblationError(f"nonpositive_policy_wealth:{i}")
        turnover += turnover_i
        peak = max(peak, wealth)
        max_drawdown = min(max_drawdown, wealth / peak - 1.0)
        if benchmark_returns is not None:
            br = _finite(benchmark_returns[i], f"benchmark_return:{i}")
            benchmark_wealth *= 1.0 + br
            if benchmark_wealth <= 0:
                raise DecisionAblationError(f"nonpositive_benchmark_wealth:{i}")
        rows.append({"as_of": step["as_of"], "symbol": step["symbol"], "source_snapshot_id": step["source_snapshot_id"], "exposure_before": step["exposure_before"], "exposure_after": step["exposure_after"], "turnover": turnover_i, "gross_return": gross, "cost_return": cost, "net_return": net, "wealth": wealth})
        prior = step["exposure_after"]
    return {
        "schema_version": SCHEMA_VERSION,
        "policy_id": policy_id,
        "estimand": estimand,
        "initial_exposure": initial,
        "final_exposure": prior,
        "n_steps": len(rows),
        "path_divergence_modeled": True,
        "stateful_policy_evaluation": True,
        "claim_level_pairing_used_after_divergence": False,
        "metrics": {"cumulative_net_return": wealth - 1.0, "turnover": turnover, "max_drawdown": max_drawdown, "benchmark_cumulative_return": None if benchmark_returns is None else benchmark_wealth - 1.0, "excess_vs_benchmark": None if benchmark_returns is None else wealth - benchmark_wealth},
        "path": rows,
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
    }


def _step_comparison_key(step: Mapping[str, Any]) -> tuple[str, str, str]:
    return str(step["as_of"]), str(step["symbol"]), str(step["source_snapshot_id"])


def validate_b5_b6_comparability(*, b5_steps: Sequence[Mapping[str, Any]], b6_steps: Sequence[Mapping[str, Any]], initial_exposure_b5: float, initial_exposure_b6: float) -> dict[str, Any]:
    if abs(float(initial_exposure_b5) - float(initial_exposure_b6)) > 1e-12:
        raise DecisionAblationError("b5_b6_starting_state_mismatch")
    if len(b5_steps) != len(b6_steps) or not b5_steps:
        raise DecisionAblationError("b5_b6_path_grid_mismatch")
    for i, (raw5, raw6) in enumerate(zip(b5_steps, b6_steps)):
        s5 = _validate_step(raw5, "B5", i)
        s6 = _validate_step(raw6, "B6", i)
        if _step_comparison_key(s5) != _step_comparison_key(s6):
            raise DecisionAblationError(f"b5_b6_observation_grid_mismatch:{i}")
        for field in ("eligible", "tradeable", "action_available", "transaction_cost_bps", "asset_return"):
            if s5[field] != s6[field]:
                raise DecisionAblationError(f"b5_b6_comparison_input_mismatch:{field}:{i}")
    return {"status": "COMPARABLE_INPUTS", "same_starting_state": True, "same_observation_grid": True, "same_eligibility": True, "same_tradeability": True, "same_action_availability": True, "same_execution_cost_model": True, "same_realized_asset_returns": True, "path_divergence_permitted": True}


def compare_policy_results(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, Any]:
    for value in (left, right):
        if value.get("schema_version") != SCHEMA_VERSION or value.get("stateful_policy_evaluation") is not True:
            raise DecisionAblationError("invalid_policy_result")
    if left.get("estimand") != right.get("estimand"):
        raise DecisionAblationError("policy_estimand_mismatch")
    lm = left.get("metrics") or {}
    rm = right.get("metrics") or {}
    return {"left_policy_id": left["policy_id"], "right_policy_id": right["policy_id"], "estimand": left["estimand"], "incremental_cumulative_net_return": float(rm["cumulative_net_return"]) - float(lm["cumulative_net_return"]), "incremental_turnover": float(rm["turnover"]) - float(lm["turnover"]), "incremental_max_drawdown": float(rm["max_drawdown"]) - float(lm["max_drawdown"]), "interpretation": "RESEARCH_ONLY_INCREMENTAL_ABLATION", "phase7i_relabelled": False, "promotion_claimed": False}
