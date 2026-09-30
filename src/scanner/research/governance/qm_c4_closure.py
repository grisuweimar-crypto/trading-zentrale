"""Fail-closed validators for final QM-C closure and the BA-QM2 handoff."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from scanner.research.governance.qm_b_closure import validate_closure_file as validate_qm_b_closure_file
from scanner.research.governance.qm_c_closure import validate_closure_file as validate_qm_c1_closure_file
from scanner.research.governance.qm_c2_closure import validate_closure_file as validate_qm_c2_closure_file
from scanner.research.governance.qm_c3_closure import validate_closure_file as validate_qm_c3_closure_file


EXPECTED_DISPLAY_STATUS = "QM-C COMPLETE — VERSIONED RESEARCH DISCIPLINE CHAIN ACTIVE"
EXPECTED_HANDOFF_STATUS = "BA-QM2 GOVERNANCE COMPLETE — READY FOR QM-I LINEAGE; QM-B STRICT PROMOTION REMAINS EXTERNALLY BLOCKED"
DEFAULT_MANIFEST_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_c4_closure_v1.json"
DEFAULT_HANDOFF_PATH = Path(__file__).resolve().parents[4] / "configs" / "ba_qm2_handoff_v1.json"

_REQUIRED_CAPABILITIES = {
    "symmetric_positive_negative_inconclusive_result_retention",
    "rejected_and_retired_without_evaluation_retention",
    "immutable_versioned_result_registry",
    "exact_confirmatory_c1_c2_c3_qm_a_result_binding",
    "confirmatory_result_requires_consumed_evidence",
    "deterministic_cross_id_duplicate_hypothesis_audit",
    "no_fuzzy_or_automatic_duplicate_merging",
    "full_qm_c1_to_c4_integrity_audit",
    "full_qm_c_cross_reference_audit",
    "end_to_end_negative_result_regression_path",
    "stable_identity_chain_for_qm_i",
    "ba_qm2_handoff_manifest",
}

_EXPECTED_IDENTITY_CHAIN = [
    "hypothesis_id+hypothesis_version+hypothesis_version_hash",
    "analysis_plan_id+analysis_plan_version+analysis_plan_hash",
    "control_plan_id+control_plan_version+control_plan_hash",
    "result_id+result_version+result_hash",
]

_EXPECTED_QM_B_BLOCKERS = {
    "LISTING_EVIDENCE",
    "MARKET_TRADABILITY_EVIDENCE",
    "EXECUTION_CHANNEL_EVIDENCE",
}


def _load(path: Path, *, error_prefix: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"{error_prefix}_unreadable:{path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{error_prefix}_must_be_object")
    return payload


def validate_closure_manifest(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise ValueError("qm_c4_closure_must_be_object")
    result = dict(payload)
    if result.get("schema_version") != "qm_c4_closure_v1":
        raise ValueError("qm_c4_closure_schema_invalid")
    if result.get("work_package") != "QM-C4" or result.get("qm_axis") != "QM-C":
        raise ValueError("qm_c4_identity_invalid")
    if result.get("display_status") != EXPECTED_DISPLAY_STATUS:
        raise ValueError("qm_c4_display_status_invalid")
    if result.get("engineering_status") != "COMPLETE" or result.get("qm_c_axis_status") != "COMPLETE":
        raise ValueError("qm_c4_completion_status_invalid")
    if result.get("empirical_promotion_claimed") is not False:
        raise ValueError("qm_c4_must_not_claim_empirical_promotion")
    if result.get("research_only") is not True or result.get("productive_integration_enabled") is not False:
        raise ValueError("qm_c4_scope_invalid")

    completed = result.get("completed_work_packages")
    if not isinstance(completed, Mapping) or set(completed) != {"QM-C1", "QM-C2", "QM-C3", "QM-C4"}:
        raise ValueError("qm_c4_completed_work_packages_invalid")
    if completed.get("QM-C1") != "QM-C1 COMPLETE — VERSIONED HYPOTHESIS REGISTRY ACTIVE":
        raise ValueError("qm_c4_qm_c1_status_invalid")
    if completed.get("QM-C2") != "QM-C2 COMPLETE — ANALYSIS PLAN FREEZE GATE ACTIVE":
        raise ValueError("qm_c4_qm_c2_status_invalid")
    if completed.get("QM-C3") != "QM-C3 COMPLETE — MULTIPLICITY AND SEQUENTIAL MONITORING GATES ACTIVE":
        raise ValueError("qm_c4_qm_c3_status_invalid")
    if completed.get("QM-C4") != EXPECTED_DISPLAY_STATUS:
        raise ValueError("qm_c4_self_status_invalid")

    capabilities = set(result.get("completed_capabilities") or [])
    missing = sorted(_REQUIRED_CAPABILITIES - capabilities)
    if missing:
        raise ValueError("qm_c4_capabilities_missing:" + ",".join(missing))

    integrations = result.get("integration_contracts")
    if not isinstance(integrations, Mapping):
        raise ValueError("qm_c4_integrations_missing")
    required_schemas = {
        "qm_a": "qm_a_research_governance_v1",
        "qm_b": "qm_b_closure_v1",
        "qm_c1": "qm_c_hypothesis_registry_v1",
        "qm_c2": "qm_c_analysis_plan_v1",
        "qm_c3": "qm_c_multiplicity_monitoring_v1",
        "qm_c4": "qm_c_results_audit_v1",
    }
    for key, schema in required_schemas.items():
        binding = integrations.get(key)
        if not isinstance(binding, Mapping) or binding.get("contract_schema") != schema and binding.get("closure_schema") != schema:
            raise ValueError(f"qm_c4_integration_schema_invalid:{key}")
    if integrations["qm_a"].get("confirmatory_results_require_consumed_evidence_state") is not True:
        raise ValueError("qm_c4_qm_a_consumption_guard_missing")
    if integrations["qm_b"].get("strict_historical_promotion_remains_blocked_by_external_evidence") is not True:
        raise ValueError("qm_c4_qm_b_strict_blocker_guard_missing")
    if integrations["qm_b"].get("external_blockers_do_not_reopen_qm_b_engineering") is not True:
        raise ValueError("qm_c4_qm_b_reopen_guard_missing")

    if result.get("stable_identity_chain") != _EXPECTED_IDENTITY_CHAIN:
        raise ValueError("qm_c4_stable_identity_chain_invalid")
    if result.get("ba_qm2_handoff_ready") is not True:
        raise ValueError("qm_c4_ba_qm2_handoff_not_ready")
    if result.get("next_mandatory_work_package") != "BA-QM3 / QM-I":
        raise ValueError("qm_c4_next_work_package_invalid")

    boundaries = result.get("boundaries")
    if not isinstance(boundaries, Mapping):
        raise ValueError("qm_c4_boundaries_missing")
    unsafe_true = [key for key, value in boundaries.items() if value is True]
    if unsafe_true:
        raise ValueError("qm_c4_unsafe_boundary_enabled:" + ",".join(sorted(unsafe_true)))
    return result


def validate_handoff_manifest(payload: Mapping[str, Any], *, qm_b_closure: Mapping[str, Any], qm_c_closure: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise ValueError("ba_qm2_handoff_must_be_object")
    result = dict(payload)
    if result.get("schema_version") != "ba_qm2_handoff_v1" or result.get("business_area") != "BA-QM2":
        raise ValueError("ba_qm2_handoff_schema_invalid")
    if result.get("display_status") != EXPECTED_HANDOFF_STATUS:
        raise ValueError("ba_qm2_handoff_display_status_invalid")
    if result.get("engineering_governance_status") != "COMPLETE":
        raise ValueError("ba_qm2_handoff_engineering_status_invalid")
    if result.get("strict_historical_promotion_status") != "BLOCKED_EXTERNAL_EVIDENCE":
        raise ValueError("ba_qm2_handoff_strict_status_invalid")
    if result.get("empirical_promotion_claimed") is not False:
        raise ValueError("ba_qm2_handoff_must_not_claim_empirical_promotion")

    qb = result.get("qm_b")
    if not isinstance(qb, Mapping) or qb.get("closure_schema") != "qm_b_closure_v1":
        raise ValueError("ba_qm2_handoff_qm_b_binding_invalid")
    if qb.get("display_status") != qm_b_closure.get("display_status"):
        raise ValueError("ba_qm2_handoff_qm_b_status_mismatch")
    blocker_ids = set(qb.get("external_blockers") or [])
    actual_blockers = {item.get("id") for item in qm_b_closure.get("external_blockers", []) if isinstance(item, Mapping)}
    if blocker_ids != _EXPECTED_QM_B_BLOCKERS or actual_blockers != _EXPECTED_QM_B_BLOCKERS:
        raise ValueError("ba_qm2_handoff_qm_b_blockers_mismatch")
    if qb.get("external_blockers_reopen_engineering") is not False:
        raise ValueError("ba_qm2_handoff_must_not_reopen_qm_b")

    qc = result.get("qm_c")
    if not isinstance(qc, Mapping) or qc.get("closure_schema") != "qm_c4_closure_v1":
        raise ValueError("ba_qm2_handoff_qm_c_binding_invalid")
    if qc.get("display_status") != qm_c_closure.get("display_status"):
        raise ValueError("ba_qm2_handoff_qm_c_status_mismatch")
    if qc.get("work_packages_complete") != ["QM-C1", "QM-C2", "QM-C3", "QM-C4"]:
        raise ValueError("ba_qm2_handoff_qm_c_packages_invalid")

    identities = result.get("stable_lineage_identities")
    if not isinstance(identities, Mapping):
        raise ValueError("ba_qm2_handoff_stable_identities_missing")
    expected_identity_fields = {
        "hypothesis": ["hypothesis_id", "hypothesis_version", "hypothesis_version_hash"],
        "analysis_plan": ["analysis_plan_id", "analysis_plan_version", "analysis_plan_hash"],
        "multiplicity_monitoring_control": ["control_plan_id", "control_plan_version", "control_plan_hash"],
        "result": ["result_id", "result_version", "result_hash"],
        "qm_a_analysis": ["analysis_id", "version_id", "identity_hash"],
        "qm_b_universe": ["universe_ledger_version", "instrument_master_version"],
    }
    if dict(identities) != expected_identity_fields:
        raise ValueError("ba_qm2_handoff_stable_identities_invalid")

    lineage = result.get("lineage_contract_for_qm_i")
    if not isinstance(lineage, Mapping):
        raise ValueError("ba_qm2_handoff_lineage_contract_missing")
    if lineage.get("qm_i_may_reference_qm_c_ids_without_rekeying") is not True:
        raise ValueError("ba_qm2_handoff_qm_i_rekey_guard_missing")
    if lineage.get("mutable_names_may_not_replace_stable_ids") is not True:
        raise ValueError("ba_qm2_handoff_mutable_name_guard_missing")
    if lineage.get("historical_pit_constraints_from_qm_b_remain_authoritative") is not True:
        raise ValueError("ba_qm2_handoff_qm_b_authority_missing")
    if lineage.get("evidence_consumption_state_from_qm_a_remains_authoritative") is not True:
        raise ValueError("ba_qm2_handoff_qm_a_authority_missing")

    preconditions = result.get("handoff_preconditions")
    if not isinstance(preconditions, Mapping):
        raise ValueError("ba_qm2_handoff_preconditions_missing")
    required_true = (
        "qm_b_engineering_complete",
        "qm_c_engineering_complete",
        "qm_c_stable_ids_and_versions_available",
        "qm_b_stable_identity_and_universe_versions_available",
        "external_qm_b_blockers_must_remain_visible_in_lineage",
    )
    if any(preconditions.get(field) is not True for field in required_true):
        raise ValueError("ba_qm2_handoff_precondition_missing")
    if preconditions.get("strict_historical_promotion_required_for_starting_qm_i") is not False:
        raise ValueError("ba_qm2_handoff_qm_i_must_not_depend_on_external_promotion")
    if result.get("next_business_area") != "BA-QM3" or result.get("next_qm_axis") != "QM-I":
        raise ValueError("ba_qm2_handoff_next_axis_invalid")
    return result


def validate_closure_file(path: str | Path | None = None, handoff_path: str | Path | None = None) -> dict[str, Any]:
    # Revalidate every predecessor closure instead of trusting copied status text.
    qm_b = validate_qm_b_closure_file()
    validate_qm_c1_closure_file()
    validate_qm_c2_closure_file()
    validate_qm_c3_closure_file()
    closure_path = Path(path) if path is not None else DEFAULT_MANIFEST_PATH
    handoff_manifest_path = Path(handoff_path) if handoff_path is not None else DEFAULT_HANDOFF_PATH
    closure = validate_closure_manifest(_load(closure_path, error_prefix="qm_c4_closure"))
    validate_handoff_manifest(
        _load(handoff_manifest_path, error_prefix="ba_qm2_handoff"),
        qm_b_closure=qm_b,
        qm_c_closure=closure,
    )
    return closure


def validate_handoff_file(path: str | Path | None = None) -> dict[str, Any]:
    qm_b = validate_qm_b_closure_file()
    closure = validate_closure_manifest(_load(DEFAULT_MANIFEST_PATH, error_prefix="qm_c4_closure"))
    handoff_path = Path(path) if path is not None else DEFAULT_HANDOFF_PATH
    return validate_handoff_manifest(
        _load(handoff_path, error_prefix="ba_qm2_handoff"),
        qm_b_closure=qm_b,
        qm_c_closure=closure,
    )
