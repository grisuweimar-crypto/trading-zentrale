"""Fail-closed closure validator for BA-QM4 / QM-D + QM-E."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from scanner.research.governance.qm_d_dependence import load_qm_de_contract
from scanner.research.governance.qm_i_closure import validate_closure_file as validate_ba_qm3_closure_file

EXPECTED_QM_D_STATUS = "QM-D COMPLETE — DEPENDENCE AND EFFECTIVE-N AUDIT ACTIVE"
EXPECTED_QM_E_STATUS = "QM-E COMPLETE — POINT-IN-TIME CALIBRATION AUDIT ACTIVE"
EXPECTED_BA_STATUS = "BA-QM4 COMPLETE — DEPENDENCE AND CALIBRATION QM READY FOR DECISION ABLATION"
DEFAULT_MANIFEST_PATH = Path(__file__).resolve().parents[4] / "configs" / "ba_qm4_qm_de_closure_v1.json"

_REQUIRED_CAPABILITIES = {
    "method_labelled_effective_n_diagnostics", "raw_n_separated_from_effective_n",
    "within_symbol_ar1_diagnostic", "symbol_cluster_concentration", "sector_cluster_concentration",
    "time_block_cluster_concentration", "overlap_concurrency_proxy", "leave_one_symbol_out",
    "leave_one_sector_out", "leave_one_time_block_out", "symbol_cluster_bootstrap", "time_block_bootstrap",
    "missing_cluster_metadata_unknown_state", "qm_i_lineage_node_validation",
    "qm_c_plan_and_result_identity_binding", "explicit_audit_as_of", "pit_prediction_outcome_pair_validation",
    "outcome_maturity_gate", "brier_score", "log_loss", "calibration_intercept", "calibration_slope",
    "reliability_bins", "subgroup_calibration_with_minimum_n", "time_block_calibration_with_minimum_n",
    "phase2_block_bootstrap_bridge", "aggregate_phase2_not_relabelled_as_row_level_calibration",
    "no_automatic_recalibration", "no_productive_probability_update", "cross_qm_regression_tests",
}
_EXPECTED_QM_B_BLOCKERS = {"LISTING_EVIDENCE", "MARKET_TRADABILITY_EVIDENCE", "EXECUTION_CHANNEL_EVIDENCE"}
_EXPECTED_BOUNDARIES = {
    "selection_or_timing_retrained", "probability_or_confidence_retrained", "risk_or_elliott_retrained",
    "scanner_weights_changed", "decision_layer_semantics_changed", "productive_calibration_changed",
    "portfolio_action_changed", "orders_generated", "empirical_promotion_performed",
}


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"ba_qm4_closure_unreadable:{path}") from exc
    if not isinstance(payload, dict):
        raise ValueError("ba_qm4_closure_must_be_object")
    return payload


def validate_closure_manifest(payload: Mapping[str, Any], *, ba_qm3_closure: Mapping[str, Any], qm_de_contract: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise ValueError("ba_qm4_closure_must_be_object")
    result = dict(payload)
    if result.get("schema_version") != "ba_qm4_qm_de_closure_v1":
        raise ValueError("ba_qm4_closure_schema_invalid")
    if result.get("business_area") != "BA-QM4" or result.get("qm_axes") != ["QM-D", "QM-E"]:
        raise ValueError("ba_qm4_identity_invalid")
    if result.get("display_status_qm_d") != EXPECTED_QM_D_STATUS:
        raise ValueError("qm_d_display_status_invalid")
    if result.get("display_status_qm_e") != EXPECTED_QM_E_STATUS:
        raise ValueError("qm_e_display_status_invalid")
    if result.get("business_area_status") != EXPECTED_BA_STATUS:
        raise ValueError("ba_qm4_display_status_invalid")
    if result.get("engineering_status") != "COMPLETE":
        raise ValueError("ba_qm4_engineering_status_invalid")
    if result.get("research_only") is not True or result.get("productive_integration_enabled") is not False:
        raise ValueError("ba_qm4_scope_invalid")
    if result.get("empirical_promotion_claimed") is not False:
        raise ValueError("ba_qm4_must_not_claim_empirical_promotion")

    if ba_qm3_closure.get("schema_version") != "ba_qm3_qm_i_closure_v1" or ba_qm3_closure.get("engineering_status") != "COMPLETE":
        raise ValueError("ba_qm4_ba_qm3_not_complete")
    if ba_qm3_closure.get("next_mandatory_work_package") != "BA-QM4 / QM-D + QM-E — Dependence, Effective N & Calibration":
        raise ValueError("ba_qm4_ba_qm3_handoff_mismatch")

    if qm_de_contract.get("schema_version") != "qm_de_dependence_calibration_v1":
        raise ValueError("ba_qm4_contract_invalid")
    principles = qm_de_contract.get("principles") or {}
    for field in ("qm_i_lineage_identity_is_reused", "qm_c_analysis_and_result_identity_is_reused", "no_single_universal_effective_n", "calibration_requires_point_in_time_prediction_outcome_pairs"):
        if principles.get(field) is not True:
            raise ValueError(f"ba_qm4_principle_guard_missing:{field}")

    missing_capabilities = sorted(_REQUIRED_CAPABILITIES - set(result.get("completed_capabilities") or []))
    if missing_capabilities:
        raise ValueError("ba_qm4_capabilities_missing:" + ",".join(missing_capabilities))

    integrations = result.get("integration_contracts")
    if not isinstance(integrations, Mapping):
        raise ValueError("ba_qm4_integrations_missing")
    qc, qi, p2 = integrations.get("qm_c"), integrations.get("qm_i"), integrations.get("phase2_probability")
    if (
        not isinstance(qc, Mapping)
        or qc.get("analysis_plan_ids_reused") is not True
        or qc.get("analysis_plan_hash_reused") is not True
        or qc.get("result_ids_reused") is not True
        or qc.get("result_hash_reused") is not True
        or qc.get("result_plan_binding_checked_in_qm_e") is not True
    ):
        raise ValueError("ba_qm4_qm_c_integration_invalid")
    if (
        not isinstance(qi, Mapping)
        or qi.get("closure_schema") != "ba_qm3_qm_i_closure_v1"
        or qi.get("lineage_registry_head_hash_required") is not True
        or qi.get("observation_and_prediction_lineage_nodes_must_exist") is not True
        or qi.get("missing_lineage_fails_closed") is not True
    ):
        raise ValueError("ba_qm4_qm_i_integration_invalid")
    if not isinstance(p2, Mapping) or p2.get("module") != "scanner.reports.probability_calibration" or p2.get("existing_moving_block_bootstrap_recognized") is not True or p2.get("iid_diagnostics_remain_diagnostics_only") is not True or p2.get("aggregate_report_is_row_level_calibration_data") is not False:
        raise ValueError("ba_qm4_phase2_bridge_invalid")

    dependence, calibration = result.get("dependence_scope"), result.get("calibration_scope")
    if not isinstance(dependence, Mapping) or not isinstance(calibration, Mapping):
        raise ValueError("ba_qm4_scope_sections_missing")
    if dependence.get("single_universal_effective_n_selected") is not False or dependence.get("overlapping_forward_windows_treated_as_independent") is not False or dependence.get("cluster_metadata_missing_treated_as_neutral") is not False or dependence.get("robustness_reselects_hypotheses") is not False:
        raise ValueError("ba_qm4_dependence_boundary_violation")
    for field in ("requires_row_level_prediction_outcome_pairs", "prediction_as_of_precedes_outcome_available_at", "outcome_available_by_audit_as_of", "prediction_definition_hash_required", "label_definition_hash_required", "empty_bins_are_not_imputed", "small_subgroups_are_not_promoted"):
        if calibration.get(field) is not True:
            raise ValueError(f"ba_qm4_calibration_guard_missing:{field}")

    qm_b = result.get("qm_b_constraints")
    if not isinstance(qm_b, Mapping) or qm_b.get("strict_historical_promotion_status") != "BLOCKED_EXTERNAL_EVIDENCE":
        raise ValueError("ba_qm4_qm_b_status_changed")
    if set(qm_b.get("external_blockers") or []) != _EXPECTED_QM_B_BLOCKERS:
        raise ValueError("ba_qm4_qm_b_blockers_changed")
    if qm_b.get("blockers_overridden") is not False or qm_b.get("qm_b_engineering_reopened") is not False:
        raise ValueError("ba_qm4_qm_b_boundary_violation")

    boundaries = result.get("boundaries")
    if not isinstance(boundaries, Mapping):
        raise ValueError("ba_qm4_boundaries_missing")
    if set(boundaries) != _EXPECTED_BOUNDARIES:
        raise ValueError("ba_qm4_boundary_keys_mismatch")
    if any(value is not False for value in boundaries.values()):
        raise ValueError("ba_qm4_boundary_values_must_be_false")
    if result.get("next_mandatory_work_package") != "BA-QM5 / QM-F — Decision Ablation":
        raise ValueError("ba_qm4_next_work_package_invalid")
    return result


def validate_closure_file(path: str | Path | None = None) -> dict[str, Any]:
    ba_qm3 = validate_ba_qm3_closure_file()
    contract = load_qm_de_contract()
    payload = _read_json(Path(path) if path is not None else DEFAULT_MANIFEST_PATH)
    return validate_closure_manifest(payload, ba_qm3_closure=ba_qm3, qm_de_contract=contract)
