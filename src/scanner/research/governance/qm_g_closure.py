"""Fail-closed closure validator for BA-QM6 / QM-G."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from scanner.research.governance.qm_f_closure import (
    validate_closure_file as validate_ba_qm5_closure_file,
)
from scanner.research.governance.qm_g_challenger_evaluation import (
    load_challenger_evaluation_contract,
)
from scanner.research.governance.qm_g_elliott_challengers import load_qm_g_contract
from scanner.research.governance.qm_g_scenario_stability import (
    load_scenario_stability_contract,
)

EXPECTED_QM_G_STATUS = "QM-G COMPLETE — ELLIOTT CHALLENGER RESEARCH GOVERNANCE ACTIVE"
EXPECTED_BA_STATUS = "BA-QM6 COMPLETE — ELLIOTT CHALLENGER RESEARCH READY FOR NEGATIVE CONTROLS"
NEXT_WORK_PACKAGE = "BA-QM7 / QM-J — Negative Controls & Falsifikation"
DEFAULT_MANIFEST_PATH = Path(__file__).resolve().parents[4] / "configs" / "ba_qm6_qm_g_closure_v1.json"

_REQUIRED_CAPABILITIES = {
    "append_only_versioned_challenger_registry",
    "frozen_elliott_core_binding",
    "qm_c_hypothesis_plan_multiplicity_binding",
    "qm_a_evidence_state_binding",
    "pit_contract_and_pre_outcome_count_freeze",
    "retroactive_count_fitting_forbidden",
    "qm_i_core_and_challenger_lineage_binding",
    "stateful_qm_f_incremental_evaluation_route",
    "scenario_stability_executable_feature",
    "scenario_stability_strict_primary_scenario_id_identity",
    "scenario_stability_missing_is_insufficient",
    "scenario_stability_full_source_lineage",
    "frozen_core_vs_challenger_shadow_evaluator",
    "same_exogenous_comparison_inputs_guard",
    "registered_challenger_lineage_delta_only",
    "winner_and_automatic_promotion_forbidden",
    "w6_and_w8_boundaries_preserved",
    "research_only_no_execution",
}
_EXPECTED_BOUNDARIES = {
    "elliott_core_changed",
    "elliott_hard_rules_changed",
    "historical_count_retrofitted",
    "universal_stance_changed",
    "w6_interface_changed",
    "w8_action_matrix_changed",
    "portfolio_action_semantics_changed",
    "orders_generated",
    "execution_enabled",
    "productive_integration_enabled",
    "empirical_promotion_performed",
}
_EXPECTED_INTEGRATIONS = {
    "ba_qm5_closure_required",
    "qm_c_governance_required",
    "qm_a_evidence_lifecycle_required",
    "qm_i_lineage_required",
    "qm_f_stateful_ablation_required",
    "qm_b_external_blockers_preserved",
}
_EXECUTABLE = {"Scenario Stability"}


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"ba_qm6_closure_unreadable:{path}") from exc
    if not isinstance(payload, dict):
        raise ValueError("ba_qm6_closure_must_be_object")
    return payload


def validate_closure_manifest(
    payload: Mapping[str, Any],
    *,
    ba_qm5_closure: Mapping[str, Any],
    registry_contract: Mapping[str, Any],
    scenario_contract: Mapping[str, Any],
    evaluation_contract: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise ValueError("ba_qm6_closure_must_be_object")
    result = dict(payload)
    if result.get("schema_version") != "ba_qm6_qm_g_closure_v1":
        raise ValueError("ba_qm6_closure_schema_invalid")
    if result.get("business_area") != "BA-QM6" or result.get("qm_axis") != "QM-G":
        raise ValueError("ba_qm6_identity_invalid")
    if result.get("display_status_qm_g") != EXPECTED_QM_G_STATUS or result.get("business_area_status") != EXPECTED_BA_STATUS:
        raise ValueError("ba_qm6_display_status_invalid")
    if result.get("engineering_status") != "COMPLETE":
        raise ValueError("ba_qm6_engineering_status_invalid")
    if result.get("empirical_validation_status") != "NOT_ESTABLISHED":
        raise ValueError("ba_qm6_empirical_status_must_remain_not_established")
    if result.get("research_only") is not True or result.get("productive_integration_enabled") is not False or result.get("execution_allowed") is not False:
        raise ValueError("ba_qm6_scope_invalid")
    if result.get("empirical_promotion_claimed") is not False:
        raise ValueError("ba_qm6_must_not_claim_empirical_promotion")

    if ba_qm5_closure.get("schema_version") != "ba_qm5_qm_f_closure_v1" or ba_qm5_closure.get("engineering_status") != "COMPLETE":
        raise ValueError("ba_qm6_ba_qm5_not_complete")
    if ba_qm5_closure.get("next_mandatory_work_package") != "QM-G — Elliott Challenger Registry":
        raise ValueError("ba_qm6_ba_qm5_handoff_mismatch")

    contracts = (
        (registry_contract, "qm_g_elliott_challenger_registry_v1"),
        (scenario_contract, "qm_g_scenario_stability_v1"),
        (evaluation_contract, "qm_g_challenger_evaluation_v1"),
    )
    for contract, schema in contracts:
        if contract.get("schema_version") != schema:
            raise ValueError(f"ba_qm6_contract_invalid:{schema}")
        if contract.get("research_only") is not True or contract.get("productive_integration_enabled") is not False or contract.get("execution_allowed") is not False:
            raise ValueError(f"ba_qm6_contract_scope_invalid:{schema}")

    documented = set(registry_contract.get("documented_challenger_examples") or [])
    if len(documented) != 12 or "Scenario Stability" not in documented:
        raise ValueError("ba_qm6_documented_challenger_catalog_invalid")
    executable = set(result.get("executable_challengers") or [])
    remaining = set(result.get("remaining_documented_candidates") or [])
    if executable != _EXECUTABLE:
        raise ValueError("ba_qm6_executable_challenger_set_invalid")
    if executable & remaining or executable | remaining != documented:
        raise ValueError("ba_qm6_candidate_partition_invalid")
    if result.get("all_documented_challengers_empirically_validated") is not False:
        raise ValueError("ba_qm6_must_not_claim_all_challengers_validated")
    if result.get("promoted_challengers") not in ([], ()):
        raise ValueError("ba_qm6_promoted_challengers_must_be_empty")

    feature = scenario_contract.get("feature") or {}
    if feature.get("identity_field") != "primary_scenario.scenario_id":
        raise ValueError("ba_qm6_scenario_identity_contract_invalid")
    if feature.get("comparison") != "STRICT_EQUALITY_BETWEEN_CONSECUTIVE_AVAILABLE_OUTPUTS":
        raise ValueError("ba_qm6_scenario_comparison_contract_invalid")
    if feature.get("threshold_allowed") is not False or feature.get("smoothing_allowed") is not False:
        raise ValueError("ba_qm6_scenario_fitted_rule_forbidden")
    if feature.get("missing_state") != "INSUFFICIENT_EVIDENCE":
        raise ValueError("ba_qm6_scenario_missing_semantics_invalid")

    principles = evaluation_contract.get("principles") or {}
    for field in (
        "valid_qm_g_readiness_required",
        "qm_a_evidence_must_be_frozen_unspent",
        "same_starting_state_required",
        "same_observation_grid_required",
        "same_eligibility_required",
        "same_tradeability_required",
        "same_action_availability_required",
        "same_execution_cost_model_required",
        "same_realized_asset_returns_required",
        "stateful_policy_evaluation_required",
        "claim_level_pairing_after_divergence_forbidden",
        "registered_challenger_lineage_node_is_only_allowed_delta",
        "winner_not_declared_by_engine",
        "automatic_promotion_forbidden",
    ):
        if principles.get(field) is not True:
            raise ValueError(f"ba_qm6_evaluation_principle_guard_missing:{field}")

    missing = sorted(_REQUIRED_CAPABILITIES - set(result.get("completed_capabilities") or []))
    if missing:
        raise ValueError("ba_qm6_capabilities_missing:" + ",".join(missing))

    integrations = result.get("integration_contracts")
    if not isinstance(integrations, Mapping) or set(integrations) != _EXPECTED_INTEGRATIONS:
        raise ValueError("ba_qm6_integration_keys_mismatch")
    if any(value is not True for value in integrations.values()):
        raise ValueError("ba_qm6_integration_guards_must_be_true")

    boundaries = result.get("boundaries")
    if not isinstance(boundaries, Mapping) or set(boundaries) != _EXPECTED_BOUNDARIES:
        raise ValueError("ba_qm6_boundary_keys_mismatch")
    if any(value is not False for value in boundaries.values()):
        raise ValueError("ba_qm6_boundary_values_must_be_false")
    if result.get("next_mandatory_work_package") != NEXT_WORK_PACKAGE:
        raise ValueError("ba_qm6_next_work_package_invalid")
    return result


def validate_closure_file(path: str | Path | None = None) -> dict[str, Any]:
    ba_qm5 = validate_ba_qm5_closure_file()
    registry = load_qm_g_contract()
    scenario = load_scenario_stability_contract()
    evaluation = load_challenger_evaluation_contract()
    payload = _read_json(Path(path) if path is not None else DEFAULT_MANIFEST_PATH)
    return validate_closure_manifest(
        payload,
        ba_qm5_closure=ba_qm5,
        registry_contract=registry,
        scenario_contract=scenario,
        evaluation_contract=evaluation,
    )
