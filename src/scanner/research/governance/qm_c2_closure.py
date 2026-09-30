"""Fail-closed validator for the QM-C2 closure manifest."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


EXPECTED_DISPLAY_STATUS = "QM-C2 COMPLETE — ANALYSIS PLAN FREEZE GATE ACTIVE"
DEFAULT_MANIFEST_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_c2_closure_v1.json"

_REQUIRED_CAPABILITIES = {
    "versioned_analysis_plan_registry",
    "immutable_analysis_plan_hash",
    "explicit_successor_plan_versioning",
    "primary_estimand_and_metric_contract",
    "population_universe_window_outcome_contract",
    "exclusion_and_sensitivity_contract",
    "preconfirmation_plan_freeze",
    "immutable_freeze_context",
    "qm_c1_exact_hypothesis_binding",
    "qm_a_full_identity_binding",
    "qm_b_strict_universe_fail_closed_binding",
    "end_to_end_confirmation_readiness_gate",
    "integration_regression_tests",
}


def validate_closure_manifest(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise ValueError("qm_c2_closure_must_be_object")
    result = dict(payload)
    if result.get("schema_version") != "qm_c2_closure_v1":
        raise ValueError("qm_c2_closure_schema_invalid")
    if result.get("work_package") != "QM-C2":
        raise ValueError("qm_c2_work_package_invalid")
    if result.get("display_status") != EXPECTED_DISPLAY_STATUS:
        raise ValueError("qm_c2_display_status_invalid")
    if result.get("engineering_status") != "COMPLETE":
        raise ValueError("qm_c2_engineering_status_invalid")
    if result.get("empirical_promotion_claimed") is not False:
        raise ValueError("qm_c2_must_not_claim_empirical_promotion")
    if result.get("research_only") is not True or result.get("productive_integration_enabled") is not False:
        raise ValueError("qm_c2_scope_invalid")

    capabilities = set(result.get("completed_capabilities") or [])
    missing = sorted(_REQUIRED_CAPABILITIES - capabilities)
    if missing:
        raise ValueError("qm_c2_capabilities_missing:" + ",".join(missing))

    integrations = result.get("integration_contracts")
    if not isinstance(integrations, Mapping):
        raise ValueError("qm_c2_integrations_missing")
    qm_c1 = integrations.get("qm_c1")
    qm_a = integrations.get("qm_a")
    qm_b = integrations.get("qm_b")
    if not isinstance(qm_c1, Mapping) or qm_c1.get("contract_schema") != "qm_c_hypothesis_registry_v1":
        raise ValueError("qm_c2_qm_c1_binding_invalid")
    if qm_c1.get("hypothesis_version_hash_must_match") is not True:
        raise ValueError("qm_c2_hypothesis_hash_guard_missing")
    if qm_c1.get("direct_hypothesis_freeze_alone_is_not_confirmation_ready") is not True:
        raise ValueError("qm_c2_direct_freeze_bypass_guard_missing")

    if not isinstance(qm_a, Mapping) or qm_a.get("contract_schema") != "qm_a_research_governance_v1":
        raise ValueError("qm_c2_qm_a_binding_invalid")
    for field in (
        "hypothesis_version_hash_must_match",
        "analysis_plan_hash_must_match",
        "all_freeze_identity_fields_must_match",
    ):
        if qm_a.get(field) is not True:
            raise ValueError(f"qm_c2_qm_a_guard_missing:{field}")

    if not isinstance(qm_b, Mapping) or qm_b.get("closure_schema") != "qm_b_closure_v1":
        raise ValueError("qm_c2_qm_b_binding_invalid")
    if qm_b.get("strict_universe_promotion_respected") is not True:
        raise ValueError("qm_c2_qm_b_promotion_guard_missing")
    if qm_b.get("current_external_blockers_are_not_overridden") is not True:
        raise ValueError("qm_c2_qm_b_blocker_override_forbidden")

    boundaries = result.get("boundaries")
    if not isinstance(boundaries, Mapping):
        raise ValueError("qm_c2_boundaries_missing")
    unsafe_true = [key for key, value in boundaries.items() if value is True]
    if unsafe_true:
        raise ValueError("qm_c2_unsafe_boundary_enabled:" + ",".join(sorted(unsafe_true)))
    if result.get("next_mandatory_work_package") != "QM-C3":
        raise ValueError("qm_c2_next_work_package_invalid")
    return result


def validate_closure_file(path: str | Path | None = None) -> dict[str, Any]:
    manifest_path = Path(path) if path is not None else DEFAULT_MANIFEST_PATH
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"qm_c2_closure_unreadable:{manifest_path}") from exc
    return validate_closure_manifest(payload)
