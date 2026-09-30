"""Fail-closed validator for the QM-C1 closure manifest."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


EXPECTED_DISPLAY_STATUS = "QM-C1 COMPLETE — VERSIONED HYPOTHESIS REGISTRY ACTIVE"
DEFAULT_MANIFEST_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_c1_closure_v1.json"

_REQUIRED_CAPABILITIES = {
    "stable_hypothesis_id",
    "explicit_hypothesis_version",
    "immutable_semantic_version_hash",
    "append_only_hash_chained_registry",
    "explicit_successor_versioning",
    "discovery_confirmation_separation",
    "rejected_and_retired_retention",
    "qm_a_analysis_binding",
    "qm_b_strict_universe_fail_closed_binding",
    "integration_regression_tests",
}


def validate_closure_manifest(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise ValueError("qm_c1_closure_must_be_object")
    result = dict(payload)
    if result.get("schema_version") != "qm_c1_closure_v1":
        raise ValueError("qm_c1_closure_schema_invalid")
    if result.get("work_package") != "QM-C1":
        raise ValueError("qm_c1_work_package_invalid")
    if result.get("display_status") != EXPECTED_DISPLAY_STATUS:
        raise ValueError("qm_c1_display_status_invalid")
    if result.get("engineering_status") != "COMPLETE":
        raise ValueError("qm_c1_engineering_status_invalid")
    if result.get("empirical_promotion_claimed") is not False:
        raise ValueError("qm_c1_must_not_claim_empirical_promotion")
    if result.get("research_only") is not True or result.get("productive_integration_enabled") is not False:
        raise ValueError("qm_c1_scope_invalid")

    capabilities = set(result.get("completed_capabilities") or [])
    missing = sorted(_REQUIRED_CAPABILITIES - capabilities)
    if missing:
        raise ValueError("qm_c1_capabilities_missing:" + ",".join(missing))

    integrations = result.get("integration_contracts")
    if not isinstance(integrations, Mapping):
        raise ValueError("qm_c1_integrations_missing")
    qm_a = integrations.get("qm_a")
    qm_b = integrations.get("qm_b")
    if not isinstance(qm_a, Mapping) or qm_a.get("contract_schema") != "qm_a_research_governance_v1":
        raise ValueError("qm_c1_qm_a_binding_invalid")
    if qm_a.get("shared_identity_field") != "hypothesis_version_hash":
        raise ValueError("qm_c1_qm_a_identity_field_invalid")
    if not isinstance(qm_b, Mapping) or qm_b.get("closure_schema") != "qm_b_closure_v1":
        raise ValueError("qm_c1_qm_b_binding_invalid")
    if qm_b.get("strict_universe_promotion_respected") is not True:
        raise ValueError("qm_c1_qm_b_promotion_guard_missing")
    if qm_b.get("current_external_blockers_are_not_overridden") is not True:
        raise ValueError("qm_c1_qm_b_blocker_override_forbidden")

    boundaries = result.get("boundaries")
    if not isinstance(boundaries, Mapping):
        raise ValueError("qm_c1_boundaries_missing")
    unsafe_true = [key for key, value in boundaries.items() if value is True]
    if unsafe_true:
        raise ValueError("qm_c1_unsafe_boundary_enabled:" + ",".join(sorted(unsafe_true)))
    if result.get("next_mandatory_work_package") != "QM-C2":
        raise ValueError("qm_c1_next_work_package_invalid")
    return result


def validate_closure_file(path: str | Path | None = None) -> dict[str, Any]:
    manifest_path = Path(path) if path is not None else DEFAULT_MANIFEST_PATH
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"qm_c1_closure_unreadable:{manifest_path}") from exc
    return validate_closure_manifest(payload)
