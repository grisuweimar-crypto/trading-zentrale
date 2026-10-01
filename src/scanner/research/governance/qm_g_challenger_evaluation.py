"""QM-G frozen-core versus challenger stateful shadow evaluation.

Research-only. Both arms are evaluated through the existing QM-F B6 stateful
policy engine. The frozen core arm and challenger shadow arm must share all
exogenous comparison inputs; only the registered challenger lineage node may be
added to the challenger arm. No winner or promotion is declared here.
"""
from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from scanner.research.governance.qm_f_decision_ablation import (
    canonical_lineage,
    compare_policy_results,
    evaluate_policy_path,
)
from scanner.research.governance.qm_g_elliott_challengers import ElliottChallengerRegistry
from scanner.research.governance.qm_i_lineage import LineageRegistry

SCHEMA_VERSION = "qm_g_challenger_evaluation_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_g_challenger_evaluation_v1.json"


class ChallengerEvaluationError(ValueError):
    pass


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _hash(value: Any) -> str:
    return sha256(_json(value).encode("utf-8")).hexdigest()


def load_challenger_evaluation_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ChallengerEvaluationError(f"challenger_evaluation_contract_unreadable:{target}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != SCHEMA_VERSION:
        raise ChallengerEvaluationError("challenger_evaluation_contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise ChallengerEvaluationError("challenger_evaluation_scope_invalid")
    if payload.get("execution_allowed") is not False:
        raise ChallengerEvaluationError("challenger_evaluation_execution_scope_invalid")
    return payload


def _arm_step(raw: Mapping[str, Any], index: int) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise ChallengerEvaluationError(f"shadow_step_not_object:{index}")
    required = (
        "symbol", "as_of", "source_snapshot_id", "exposure_before", "exposure_after",
        "asset_return", "transaction_cost_bps", "eligible", "tradeable", "action_available",
    )
    missing = [field for field in required if field not in raw]
    if missing:
        raise ChallengerEvaluationError(f"shadow_step_fields_missing:{index}:" + ",".join(missing))
    return {"policy_id": "B6", **{field: raw[field] for field in required}}


def validate_core_challenger_comparability(
    *,
    core_steps: Sequence[Mapping[str, Any]],
    challenger_steps: Sequence[Mapping[str, Any]],
    initial_exposure_core: float,
    initial_exposure_challenger: float,
) -> dict[str, Any]:
    if abs(float(initial_exposure_core) - float(initial_exposure_challenger)) > 1e-12:
        raise ChallengerEvaluationError("core_challenger_starting_state_mismatch")
    if not core_steps or len(core_steps) != len(challenger_steps):
        raise ChallengerEvaluationError("core_challenger_path_grid_mismatch")
    fields = (
        "symbol", "as_of", "source_snapshot_id", "asset_return", "transaction_cost_bps",
        "eligible", "tradeable", "action_available",
    )
    for index, (core_raw, challenger_raw) in enumerate(zip(core_steps, challenger_steps)):
        core = _arm_step(core_raw, index)
        challenger = _arm_step(challenger_raw, index)
        for field in fields:
            if core[field] != challenger[field]:
                raise ChallengerEvaluationError(f"core_challenger_comparison_input_mismatch:{field}:{index}")
    return {
        "status": "COMPARABLE_INPUTS",
        "same_starting_state": True,
        "same_observation_grid": True,
        "same_eligibility": True,
        "same_tradeability": True,
        "same_action_availability": True,
        "same_execution_cost_model": True,
        "same_realized_asset_returns": True,
        "path_divergence_permitted": True,
    }


def _ref_key(ref: Mapping[str, Any]) -> tuple[str, str]:
    return str(ref.get("node_id") or "").strip(), str(ref.get("version_id") or "").strip()


def validate_core_challenger_lineage(
    *,
    challenger_record: Mapping[str, Any],
    core_refs: Sequence[Mapping[str, Any]],
    challenger_refs: Sequence[Mapping[str, Any]],
    lineage_registry: LineageRegistry,
) -> dict[str, Any]:
    binding = challenger_record.get("lineage_binding")
    if not isinstance(binding, Mapping) or not isinstance(binding.get("challenger"), Mapping):
        raise ChallengerEvaluationError("registered_challenger_lineage_binding_required")
    adjustment = dict(binding["challenger"])
    adjustment_key = _ref_key(adjustment)
    core_keys = {_ref_key(ref) for ref in core_refs}
    challenger_keys = {_ref_key(ref) for ref in challenger_refs}
    if adjustment_key in core_keys:
        raise ChallengerEvaluationError("challenger_adjustment_present_in_frozen_core_lineage")
    if adjustment_key not in challenger_keys:
        raise ChallengerEvaluationError("registered_challenger_adjustment_missing_from_shadow_lineage")
    try:
        core = canonical_lineage(core_refs, registry=lineage_registry)
        shadow = canonical_lineage(
            challenger_refs,
            registry=lineage_registry,
            ignored_refs=[adjustment],
        )
    except Exception as exc:
        raise ChallengerEvaluationError("core_challenger_lineage_validation_failed") from exc
    if core != shadow:
        raise ChallengerEvaluationError("core_challenger_lineage_not_equivalent_after_registered_delta")
    return {
        "status": "EQUIVALENT_AFTER_REGISTERED_CHALLENGER_REMOVAL",
        "core_lineage_hash": _hash(core),
        "challenger_core_lineage_hash": _hash(shadow),
        "registered_challenger_delta": adjustment,
        "unregistered_lineage_delta_allowed": False,
    }


def evaluate_challenger_policy_increment(
    *,
    challenger_id: str,
    challenger_version: str,
    readiness: Mapping[str, Any],
    challenger_registry: ElliottChallengerRegistry,
    lineage_registry: LineageRegistry,
    core_steps: Sequence[Mapping[str, Any]],
    challenger_steps: Sequence[Mapping[str, Any]],
    core_lineage_refs: Sequence[Mapping[str, Any]],
    challenger_lineage_refs: Sequence[Mapping[str, Any]],
    initial_exposure_core: float,
    initial_exposure_challenger: float,
    estimand: str,
    benchmark_returns: Sequence[float] | None = None,
    contract_path: str | Path | None = None,
) -> dict[str, Any]:
    """Evaluate frozen B6 core versus one registered challenger shadow arm."""
    contract = load_challenger_evaluation_contract(contract_path)
    record = challenger_registry.get_challenger(challenger_id, challenger_version)
    if readiness.get("schema_version") != "qm_g_elliott_challenger_readiness_v1" or readiness.get("valid") is not True:
        raise ChallengerEvaluationError("valid_qm_g_readiness_required")
    if readiness.get("challenger_id") != challenger_id or readiness.get("challenger_version") != challenger_version:
        raise ChallengerEvaluationError("challenger_readiness_identity_mismatch")
    if readiness.get("challenger_version_hash") != record.get("challenger_version_hash"):
        raise ChallengerEvaluationError("challenger_readiness_hash_mismatch")
    if readiness.get("qm_a_evidence_state") != "FROZEN_FOR_CONFIRMATION":
        raise ChallengerEvaluationError("challenger_evaluation_requires_frozen_unspent_evidence")
    if readiness.get("productive_integration_enabled") is not False or readiness.get("execution_allowed") is not False:
        raise ChallengerEvaluationError("challenger_readiness_scope_violation")

    boundaries = record.get("boundaries")
    if not isinstance(boundaries, Mapping) or boundaries.get("sidecar_evidence_only") is not True:
        raise ChallengerEvaluationError("registered_challenger_must_remain_sidecar")
    for field in (
        "elliott_core_changed", "hard_rules_changed", "universal_stance_changed",
        "w6_interface_replaced", "w8_action_matrix_replaced",
        "portfolio_action_directly_changed", "order_or_execution_generated",
        "productive_promotion_performed",
    ):
        if boundaries.get(field) is not False:
            raise ChallengerEvaluationError(f"registered_challenger_boundary_violation:{field}")

    comparability = validate_core_challenger_comparability(
        core_steps=core_steps,
        challenger_steps=challenger_steps,
        initial_exposure_core=initial_exposure_core,
        initial_exposure_challenger=initial_exposure_challenger,
    )
    lineage = validate_core_challenger_lineage(
        challenger_record=record,
        core_refs=core_lineage_refs,
        challenger_refs=challenger_lineage_refs,
        lineage_registry=lineage_registry,
    )

    core_result = evaluate_policy_path(
        policy_id="B6",
        steps=[_arm_step(step, index) for index, step in enumerate(core_steps)],
        estimand=estimand,
        initial_exposure=initial_exposure_core,
        benchmark_returns=benchmark_returns,
    )
    challenger_result = evaluate_policy_path(
        policy_id="B6",
        steps=[_arm_step(step, index) for index, step in enumerate(challenger_steps)],
        estimand=estimand,
        initial_exposure=initial_exposure_challenger,
        benchmark_returns=benchmark_returns,
    )
    comparison = compare_policy_results(core_result, challenger_result)

    result = {
        "schema_version": SCHEMA_VERSION,
        "challenger_id": challenger_id,
        "challenger_version": challenger_version,
        "challenger_version_hash": record["challenger_version_hash"],
        "arms": {
            "core": contract["arms"]["core"],
            "challenger": contract["arms"]["challenger"],
            "qm_f_policy_engine_id": "B6",
        },
        "estimand": estimand,
        "comparability": comparability,
        "lineage": lineage,
        "core_result": core_result,
        "challenger_result": challenger_result,
        "incremental_comparison": comparison,
        "stateful_policy_evaluation": True,
        "claim_level_pairing_used_after_divergence": False,
        "winner_declared": False,
        "promotion_claimed": False,
        "automatic_promotion_allowed": False,
        "elliott_core_changed": False,
        "universal_stance_changed": False,
        "w6_interface_changed": False,
        "w8_action_matrix_changed": False,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "research_only": True,
    }
    result["evaluation_hash"] = _hash(result)
    return result
