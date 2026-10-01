"""Fail-closed closure validator for BA-QM5 / QM-F."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from scanner.research.governance.qm_de_closure import validate_closure_file as validate_ba_qm4_closure_file
from scanner.research.governance.qm_f_decision_ablation import load_qm_f_contract

EXPECTED_QM_F_STATUS = "QM-F COMPLETE — STATEFUL DECISION ABLATION GOVERNANCE ACTIVE"
EXPECTED_BA_STATUS = "BA-QM5 COMPLETE — DECISION ABLATION READY FOR ELLIOTT CHALLENGER GOVERNANCE"
DEFAULT_MANIFEST_PATH = Path(__file__).resolve().parents[4] / "configs" / "ba_qm5_qm_f_closure_v1.json"

_REQUIRED_CAPABILITIES = {
    "b0_flat_and_long_separated",
    "b1_to_b6_candidate_ladder_frozen",
    "stateful_policy_path_evaluation",
    "path_divergence_modelled",
    "claim_level_pairing_blocked_after_divergence",
    "turnover_cost_accounting",
    "explicit_policy_value_benchmark_requirement",
    "b5_b6_common_start_state_guard",
    "b5_b6_common_observation_grid_guard",
    "b5_b6_eligibility_guard",
    "b5_b6_tradeability_guard",
    "b5_b6_action_availability_guard",
    "b5_b6_execution_cost_guard",
    "b5_b6_qm_i_core_lineage_equivalence",
    "registered_elliott_delta_only",
    "phase7i_non_relabelling_guard",
    "research_only_no_execution",
}
_EXPECTED_BOUNDARIES = {
    "phase7i_validation_relabelled",
    "scanner_weights_changed",
    "selection_or_timing_retrained",
    "probability_or_confidence_retrained",
    "risk_or_elliott_retrained",
    "decision_layer_semantics_changed",
    "portfolio_action_changed",
    "orders_generated",
    "productive_integration_enabled",
    "empirical_promotion_performed",
}


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"ba_qm5_closure_unreadable:{path}") from exc
    if not isinstance(payload, dict):
        raise ValueError("ba_qm5_closure_must_be_object")
    return payload


def validate_closure_manifest(payload: Mapping[str, Any], *, ba_qm4_closure: Mapping[str, Any], qm_f_contract: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise ValueError("ba_qm5_closure_must_be_object")
    result = dict(payload)
    if result.get("schema_version") != "ba_qm5_qm_f_closure_v1":
        raise ValueError("ba_qm5_closure_schema_invalid")
    if result.get("business_area") != "BA-QM5" or result.get("qm_axis") != "QM-F":
        raise ValueError("ba_qm5_identity_invalid")
    if result.get("display_status_qm_f") != EXPECTED_QM_F_STATUS or result.get("business_area_status") != EXPECTED_BA_STATUS:
        raise ValueError("ba_qm5_display_status_invalid")
    if result.get("engineering_status") != "COMPLETE":
        raise ValueError("ba_qm5_engineering_status_invalid")
    if result.get("research_only") is not True or result.get("productive_integration_enabled") is not False or result.get("execution_allowed") is not False:
        raise ValueError("ba_qm5_scope_invalid")
    if result.get("empirical_promotion_claimed") is not False:
        raise ValueError("ba_qm5_must_not_claim_empirical_promotion")

    if ba_qm4_closure.get("schema_version") != "ba_qm4_qm_de_closure_v1" or ba_qm4_closure.get("engineering_status") != "COMPLETE":
        raise ValueError("ba_qm5_ba_qm4_not_complete")
    if ba_qm4_closure.get("next_mandatory_work_package") != "BA-QM5 / QM-F — Decision Ablation":
        raise ValueError("ba_qm5_ba_qm4_handoff_mismatch")
    if qm_f_contract.get("schema_version") != "qm_f_decision_ablation_v1":
        raise ValueError("ba_qm5_contract_invalid")

    principles = qm_f_contract.get("principles") or {}
    for field in (
        "primary_estimand_declared_before_evaluation",
        "stateful_policy_evaluation_required",
        "claim_level_pairing_insufficient_after_path_divergence",
        "b5_b6_common_starting_state_required",
        "b5_b6_same_eligibility_required",
        "b5_b6_same_tradeability_required",
        "b5_b6_same_action_availability_required",
        "b5_b6_same_execution_cost_model_required",
        "b5_b6_qm_i_core_lineage_equivalence_required",
        "b6_registered_elliott_adjustment_is_only_allowed_lineage_delta",
        "path_divergence_must_be_modeled",
        "execution_remains_disabled_until_separate_promotion",
    ):
        if principles.get(field) is not True:
            raise ValueError(f"ba_qm5_principle_guard_missing:{field}")

    missing = sorted(_REQUIRED_CAPABILITIES - set(result.get("completed_capabilities") or []))
    if missing:
        raise ValueError("ba_qm5_capabilities_missing:" + ",".join(missing))

    integrations = result.get("integration_contracts")
    if not isinstance(integrations, Mapping):
        raise ValueError("ba_qm5_integrations_missing")
    if integrations.get("ba_qm4_closure_required") is not True or integrations.get("qm_i_lineage_required") is not True or integrations.get("qm_b_external_blockers_preserved") is not True:
        raise ValueError("ba_qm5_integration_guard_invalid")

    boundaries = result.get("boundaries")
    if not isinstance(boundaries, Mapping) or set(boundaries) != _EXPECTED_BOUNDARIES:
        raise ValueError("ba_qm5_boundary_keys_mismatch")
    if any(value is not False for value in boundaries.values()):
        raise ValueError("ba_qm5_boundary_values_must_be_false")
    if result.get("next_mandatory_work_package") != "QM-G — Elliott Challenger Registry":
        raise ValueError("ba_qm5_next_work_package_invalid")
    return result


def validate_closure_file(path: str | Path | None = None) -> dict[str, Any]:
    ba_qm4 = validate_ba_qm4_closure_file()
    contract = load_qm_f_contract()
    payload = _read_json(Path(path) if path is not None else DEFAULT_MANIFEST_PATH)
    return validate_closure_manifest(payload, ba_qm4_closure=ba_qm4, qm_f_contract=contract)
