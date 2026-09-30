"""Fail-closed validator for the QM-C3 closure manifest."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


EXPECTED_DISPLAY_STATUS = "QM-C3 COMPLETE — MULTIPLICITY AND SEQUENTIAL MONITORING GATES ACTIVE"
DEFAULT_MANIFEST_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_c3_closure_v1.json"

_REQUIRED_CAPABILITIES = {
    "versioned_hypothesis_family_control_plan",
    "immutable_control_plan_hash",
    "explicit_successor_control_plan_versioning",
    "exact_qm_c1_hypothesis_family_binding",
    "exact_qm_c2_analysis_plan_binding",
    "predeclared_multiplicity_strategy",
    "fwer_fdr_and_custom_strategy_contracts",
    "predeclared_sequential_look_schedule",
    "out_of_order_and_unplanned_look_rejection",
    "predeclared_early_stop_enforcement",
    "qm_a_evidence_state_guard_between_looks",
    "qm_b_fail_closed_binding_via_qm_c2",
    "end_to_end_evaluation_readiness_gate",
    "append_only_hash_chained_monitoring_history",
    "integration_regression_tests",
}


def validate_closure_manifest(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise ValueError("qm_c3_closure_must_be_object")
    result = dict(payload)
    if result.get("schema_version") != "qm_c3_closure_v1":
        raise ValueError("qm_c3_closure_schema_invalid")
    if result.get("work_package") != "QM-C3":
        raise ValueError("qm_c3_work_package_invalid")
    if result.get("display_status") != EXPECTED_DISPLAY_STATUS:
        raise ValueError("qm_c3_display_status_invalid")
    if result.get("engineering_status") != "COMPLETE":
        raise ValueError("qm_c3_engineering_status_invalid")
    if result.get("empirical_promotion_claimed") is not False:
        raise ValueError("qm_c3_must_not_claim_empirical_promotion")
    if result.get("research_only") is not True or result.get("productive_integration_enabled") is not False:
        raise ValueError("qm_c3_scope_invalid")

    capabilities = set(result.get("completed_capabilities") or [])
    missing = sorted(_REQUIRED_CAPABILITIES - capabilities)
    if missing:
        raise ValueError("qm_c3_capabilities_missing:" + ",".join(missing))

    integrations = result.get("integration_contracts")
    if not isinstance(integrations, Mapping):
        raise ValueError("qm_c3_integrations_missing")
    c1 = integrations.get("qm_c1")
    c2 = integrations.get("qm_c2")
    qa = integrations.get("qm_a")
    qb = integrations.get("qm_b")
    if not isinstance(c1, Mapping) or c1.get("contract_schema") != "qm_c_hypothesis_registry_v1":
        raise ValueError("qm_c3_qm_c1_binding_invalid")
    if c1.get("family_id_must_match") is not True or c1.get("hypothesis_version_hash_must_match") is not True:
        raise ValueError("qm_c3_qm_c1_guards_missing")
    if not isinstance(c2, Mapping) or c2.get("contract_schema") != "qm_c_analysis_plan_v1":
        raise ValueError("qm_c3_qm_c2_binding_invalid")
    if c2.get("analysis_plan_hash_must_match") is not True:
        raise ValueError("qm_c3_qm_c2_plan_hash_guard_missing")
    if c2.get("every_family_member_must_be_confirmation_ready_at_control_freeze") is not True:
        raise ValueError("qm_c3_qm_c2_readiness_guard_missing")
    if not isinstance(qa, Mapping) or qa.get("contract_schema") != "qm_a_research_governance_v1":
        raise ValueError("qm_c3_qm_a_binding_invalid")
    if qa.get("first_look_requires_frozen_confirmation_state") is not True:
        raise ValueError("qm_c3_first_look_guard_missing")
    if qa.get("later_looks_require_consumed_confirmatory_evidence_state") is not True:
        raise ValueError("qm_c3_later_look_guard_missing")
    if not isinstance(qb, Mapping) or qb.get("closure_schema") != "qm_b_closure_v1":
        raise ValueError("qm_c3_qm_b_binding_invalid")
    if qb.get("binding_reused_through_qm_c2") is not True:
        raise ValueError("qm_c3_qm_b_reuse_guard_missing")
    if qb.get("current_external_blockers_are_not_overridden") is not True:
        raise ValueError("qm_c3_qm_b_blocker_override_forbidden")

    boundaries = result.get("boundaries")
    if not isinstance(boundaries, Mapping):
        raise ValueError("qm_c3_boundaries_missing")
    unsafe_true = [key for key, value in boundaries.items() if value is True]
    if unsafe_true:
        raise ValueError("qm_c3_unsafe_boundary_enabled:" + ",".join(sorted(unsafe_true)))
    if result.get("next_mandatory_work_package") != "QM-C4":
        raise ValueError("qm_c3_next_work_package_invalid")
    return result


def validate_closure_file(path: str | Path | None = None) -> dict[str, Any]:
    manifest_path = Path(path) if path is not None else DEFAULT_MANIFEST_PATH
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"qm_c3_closure_unreadable:{manifest_path}") from exc
    return validate_closure_manifest(payload)
