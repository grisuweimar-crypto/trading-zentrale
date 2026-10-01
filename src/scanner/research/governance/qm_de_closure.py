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
    "method_labelled_effective_n_diagnostics",
    "raw_n_separated_from_effective_n",
    "within_symbol_ar1_diagnostic",
    "symbol_cluster_concentration",
    "sector_cluster_concentration",
    "time_block_cluster_concentration",
    "overlap_concurrency_proxy",
    "leave_one_symbol_out",
    "leave_one_sector_out",
    "leave_one_time_block_out",
    "symbol_cluster_bootstrap",
    "time_block_bootstrap",
    "missing_cluster_metadata_unknown_state",
    "qm_i_lineage_node_validation",
    "qm_c_plan_and_result_identity_binding",
    "explicit_audit_as_of",
    "pit_prediction_outcome_pair_validation",
    "outcome_maturity_gate",
    "brier_score",
    "log_loss",
    "calibration_intercept",
    "calibration_slope",
    "reliability_bins",
    "subgroup_calibration_with_minimum_n",
    "time_block_calibration_with_minimum_n",
    "phase2_block_bootstrap_bridge",
    "aggregate_phase2_not_relabelled_as_row_level_calibration",
    "no_automatic_recalibration",
    "no_productive_probability_update",
    "cross_qm_regression_tests",
}

_EXPECTED_QM_B_BLOCKERS = {
    "LISTING_EVIDENCE",
    "MARKET_TRADABILITY_EVIDENCE",
    "EXECUTION_CHANNEL_EVIDENCE",
}


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"ba_qm4_closure_unreadable:{path}") from exc
    if not isinstance(payload, dict):
        raise ValueError("ba_qm4_closure_must_be_object")
    return payload


def validate_closure_manifest(
    payload: Mapping[str, Any],
    *,
    ba_qm3_closure: Mapping[str, Any],
    qm_de_contract: Mapping[str, Any],
) -> dict[str, Any]:
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

    if ba_qm3_closure.get("schema_version") != "ba_qm3_qm_i_closure_v1":
        raise ValueError("ba_qm4_ba_qm3_schema_invalid")
    if ba_qm3_closure.get("engineering_status") != "COMPLETE":
        raise ValueError("ba_qm4_ba_qm3_not_complete")
    if qm_de_contract.get("schema_version") != "qm_de_dependence_calibration_v1":
        raise ValueError("ba_qm4_contract_invalid")
    if qm_de_contract.get("principles", {}).get("qm_i_lineage_identity_is_reused") is not True:
        raise ValueError("ba_qm4_qm_i_binding_guard_missing")
    if qm_de_contract.get("principles", {}).get("qm_c_analysis_and_result_identity_is_reused") is not True:
        raise ValueError("ba_qm4_qm_c_binding_guard_missing")
    if qm_de_contract.get("principles", {}).get("no_single_universal_effective_n") is not True:
        raise ValueError("ba_qm4_effective_n_guard_missing")
    if qm_de_contract.get("principles", {}).get("calibration_requires_point_in_time_prediction_outcome_pairs") is not True:
        raise ValueError("ba_qm4_calibration_pit_guard_missing")

    capabilities = set(result.get("completed_capabilities") or [])
    missing = sorted(_REQUIRED_CAPABILITIES - capabilities)
    if missing:
        raise ValueError("ba_qm4_capabilities_missing:" + ",".join(missing))

    integrations = result.get("integration_contracts")
    if not isinstance(integrations, Mapping):
        raise ValueError("ba_qm4_integrations_missing")
    qc = integrations.get("qm_c")
    qi = integrations.get("qm_i")
    p2 = integrations.get("phase2_probability")
    if not isinstance(qc, Mapping):
        raise ValueError("ba_qm4_qm_c_integration_missing")
    if qc.get("analysis_plan_ids_reused") is not True or qc.get("result_ids_reused") is not True:
        raise ValueError("ba_qm4_qm_c_identity_reuse_invalid")
    if qc.get("result_plan_binding_checked_in_qm_e") is not True:
        raise ValueError("ba_qm4_qm_e_result_plan_guard_missing")
    if not isinstance(qi, Mapping) or qi.get("closure_schema") != "ba_qm3_qm_i_closure_v1":
        raise ValueError("ba_qm4_qm_i_integration_invalid")
    if qi.get("lineage_registry_head_hash_required") is not True or qi.get("missing_lineage_fails_closed") is not True:
        raise ValueError("ba_qm4_qm_i_lineage_guards_invalid")
    if not isinstance(p2, Mapping) or p2.get("module") != "scanner.reports.probability_calibration":
        raise ValueError("ba_qm4_phase2_bridge_invalid")
    if p2.get("existing_moving_block_bootstrap_recognized") is not True:
        raise ValueError("ba_qm4_phase2_block_method_missing")
    if p2.get("iid_diagnostics_remain_diagnostics_only") is not True:
        raise ValueError("ba_qm4_phase2_iid_guard_missing")
    if p2.get("aggregate_report_is_row_level_calibration_data") is not False:
        raise ValueError("ba_qm4_phase2_aggregate_relabel_forbidden")

    dependence = result.get("dependence_scope")
    calibration = result.get("calibration_scope")
    if not isinstance(dependence, Mapping) or not isinstance(calibration, Mapping):
        raise ValueError("ba_qm4_scope_sections_missing")
    if dependence.get("single_universal_effective_n_selected") is not False:
        raise ValueError("ba_qm4_universal_effective_n_forbidden")
    if dependence.get("overlapping_forward_windows_treated_as_independent") is not False:
        raise ValueError("ba_qm4_overlap_independence_forbidden")
    if dependence.get("cluster_metadata_missing_treated_as_neutral") is not False:
        raise ValueError("ba_qm4_missing_cluster_neutral_forbidden")
    for field in (
        "requires_row_level_prediction_outcome_pairs",
        "prediction_as_of_precedes_outcome_available_at",
        "outcome_available_by_audit_as_of",
        "prediction_definition_hash_required",
        "label_definition_hash_required",
        "empty_bins_are_not_imputed",
        "small_subgroups_are_not_promoted",
    ):
        if calibration.get(field) is not True:
            raise ValueError(f"ba_qm4_calibration_guard_missing:{field}")

    qm_b = result.get("qm_b_constraints")
    if not isinstance(qm_b, Mapping):
        raise ValueError("ba_qm4_qm_b_constraints_missing")
    if qm_b.get("strict_historical_promotion_status") != "BLOCKED_EXTERNAL_EVIDENCE":
        raise ValueError("ba_qm4_qm_b_status_changed")
    if set(qm_b.get("external_blockers") or []) != _EXPECTED_QM_B_BLOCKERS:
        raise ValueError("ba_qm4_qm_b_blockers_changed")
    if qm_b.get("blockers_overridden") is not False or qm_b.get("qm_b_engineering_reopened") is not False:
        raise ValueError("ba_qm4_qm_b_boundary_violation")

    boundaries = result.get("boundaries")
    if not isinstance(boundaries, Mapping):
        raise ValueError("ba_qm4_boundaries_missing")
    unsafe = sorted(key for key, value in boundaries.items() if value is True)
    if unsafe:
        raise ValueError("ba_qm4_unsafe_boundary_enabled:" + ",".join(unsafe))
    if result.get("next_mandatory_work_package") != "BA-QM5 / QM-F — Decision Ablation":
        raise ValueError("ba_qm4_next_work_package_invalid")
    return result


def validate_closure_file(path: str | Path | None = None) -> dict[str, Any]:
    ba_qm3 = validate_ba_qm3_closure_file()
    contract = load_qm_de_contract()
    manifest_path = Path(path) if path is not None else DEFAULT_MANIFEST_PATH
    payload = _read_json(manifest_path)
    return validate_closure_manifest(payload, ba_qm3_closure=ba_qm3, qm_de_contract=contract)
