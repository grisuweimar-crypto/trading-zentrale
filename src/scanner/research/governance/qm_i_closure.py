"""Fail-closed closure validator for BA-QM3 / QM-I."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from scanner.research.governance.qm_c4_closure import validate_handoff_file as validate_ba_qm2_handoff_file
from scanner.research.governance.qm_i_lineage import load_qm_i_contract


EXPECTED_DISPLAY_STATUS = "QM-I COMPLETE — TYPED EVIDENCE LINEAGE AND DOUBLE-COUNTING REVIEW ACTIVE"
EXPECTED_BA_STATUS = "BA-QM3 COMPLETE — EVIDENCE LINEAGE READY FOR DEPENDENCE AND CALIBRATION QM"
DEFAULT_MANIFEST_PATH = Path(__file__).resolve().parents[4] / "configs" / "ba_qm3_qm_i_closure_v1.json"
DEFAULT_STANCE_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "decision_universal_stance_v1.json"

_REQUIRED_CAPABILITIES = {
    "typed_versioned_provenance_graph",
    "append_only_hash_chained_lineage_registry",
    "raw_source_ids",
    "feature_ids",
    "indicator_ids",
    "score_ids",
    "derived_metric_ids",
    "claim_ids",
    "calibration_ids",
    "decision_ids",
    "watch_ids",
    "qm_c_stable_identity_reuse",
    "independence_claim_registry",
    "common_ancestry_detector_with_paths",
    "direct_dependency_detector",
    "incomplete_lineage_unknown_state",
    "double_counting_review_trigger",
    "independence_claim_contradiction_detection",
    "material_lineage_cycle_rejection",
    "phase7_claim_and_claim_ref_integration",
    "phase7_decision_lineage_integration",
    "phase7_native_watch_id_reuse",
    "end_to_end_raw_feature_indicator_score_claim_calibration_decision_watch_test",
    "cross_qm_regression_tests",
}

_EXPECTED_QM_B_BLOCKERS = {
    "LISTING_EVIDENCE",
    "MARKET_TRADABILITY_EVIDENCE",
    "EXECUTION_CHANNEL_EVIDENCE",
}


def _read_json(path: Path, *, error_prefix: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"{error_prefix}_unreadable:{path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{error_prefix}_must_be_object")
    return payload


def validate_closure_manifest(
    payload: Mapping[str, Any],
    *,
    ba_qm2_handoff: Mapping[str, Any],
    lineage_contract: Mapping[str, Any],
    phase7_stance_contract: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise ValueError("qm_i_closure_must_be_object")
    result = dict(payload)
    if result.get("schema_version") != "ba_qm3_qm_i_closure_v1":
        raise ValueError("qm_i_closure_schema_invalid")
    if result.get("business_area") != "BA-QM3" or result.get("qm_axis") != "QM-I":
        raise ValueError("qm_i_closure_identity_invalid")
    if result.get("display_status") != EXPECTED_DISPLAY_STATUS:
        raise ValueError("qm_i_display_status_invalid")
    if result.get("business_area_status") != EXPECTED_BA_STATUS:
        raise ValueError("ba_qm3_display_status_invalid")
    if result.get("engineering_status") != "COMPLETE":
        raise ValueError("qm_i_engineering_status_invalid")
    if result.get("empirical_promotion_claimed") is not False:
        raise ValueError("qm_i_must_not_claim_empirical_promotion")
    if result.get("research_only") is not True or result.get("productive_integration_enabled") is not False:
        raise ValueError("qm_i_scope_invalid")

    prerequisite = result.get("prerequisite")
    if not isinstance(prerequisite, Mapping):
        raise ValueError("qm_i_prerequisite_missing")
    if prerequisite.get("ba_qm2_handoff_schema") != "ba_qm2_handoff_v1":
        raise ValueError("qm_i_ba_qm2_schema_invalid")
    if prerequisite.get("qm_b_and_qm_c_stable_ids_required") is not True:
        raise ValueError("qm_i_stable_id_prerequisite_missing")
    if prerequisite.get("qm_b_strict_historical_promotion_required") is not False:
        raise ValueError("qm_i_must_not_require_qm_b_external_promotion")
    if prerequisite.get("qm_b_external_blockers_must_remain_visible") is not True:
        raise ValueError("qm_i_qm_b_blocker_visibility_missing")

    if ba_qm2_handoff.get("schema_version") != "ba_qm2_handoff_v1":
        raise ValueError("qm_i_actual_ba_qm2_handoff_invalid")
    if ba_qm2_handoff.get("engineering_governance_status") != "COMPLETE":
        raise ValueError("qm_i_ba_qm2_not_complete")
    if ba_qm2_handoff.get("strict_historical_promotion_status") != "BLOCKED_EXTERNAL_EVIDENCE":
        raise ValueError("qm_i_ba_qm2_strict_status_changed")
    blockers = set((ba_qm2_handoff.get("qm_b") or {}).get("external_blockers") or [])
    if blockers != _EXPECTED_QM_B_BLOCKERS:
        raise ValueError("qm_i_qm_b_external_blockers_mismatch")

    if lineage_contract.get("schema_version") != "qm_i_evidence_lineage_v1":
        raise ValueError("qm_i_lineage_contract_invalid")
    if lineage_contract.get("principles", {}).get("stable_upstream_ids_must_be_reused") is not True:
        raise ValueError("qm_i_stable_upstream_id_guard_missing")
    if lineage_contract.get("principles", {}).get("missing_lineage_is_not_independence") is not True:
        raise ValueError("qm_i_missing_lineage_guard_missing")
    required_types = {"RAW_SOURCE", "FEATURE", "INDICATOR", "SCORE", "CLAIM", "CALIBRATION", "DECISION", "WATCH"}
    actual_types = set((lineage_contract.get("node") or {}).get("node_types") or [])
    if not required_types.issubset(actual_types):
        raise ValueError("qm_i_required_lineage_node_types_missing")

    capabilities = set(result.get("completed_capabilities") or [])
    missing = sorted(_REQUIRED_CAPABILITIES - capabilities)
    if missing:
        raise ValueError("qm_i_capabilities_missing:" + ",".join(missing))

    integrations = result.get("integration_contracts")
    if not isinstance(integrations, Mapping):
        raise ValueError("qm_i_integrations_missing")
    qa = integrations.get("qm_a")
    qb = integrations.get("qm_b")
    qc = integrations.get("qm_c")
    p7 = integrations.get("phase7")
    if not isinstance(qa, Mapping) or qa.get("contract_schema") != "qm_a_research_governance_v1" or qa.get("evidence_consumption_authority_preserved") is not True:
        raise ValueError("qm_i_qm_a_integration_invalid")
    if not isinstance(qb, Mapping) or qb.get("closure_schema") != "qm_b_closure_v1" or qb.get("pit_authority_preserved") is not True:
        raise ValueError("qm_i_qm_b_integration_invalid")
    if qb.get("strict_promotion_blockers_overridden") is not False:
        raise ValueError("qm_i_qm_b_blocker_override_forbidden")
    if not isinstance(qc, Mapping) or qc.get("handoff_schema") != "ba_qm2_handoff_v1" or qc.get("stable_ids_reused_without_rekeying") is not True:
        raise ValueError("qm_i_qm_c_integration_invalid")
    if not isinstance(p7, Mapping) or p7.get("input_contract_schema") != "decision_layer_input_contract_v1":
        raise ValueError("qm_i_phase7_integration_invalid")
    if p7.get("relation_graph_semantics_replaced") is not False:
        raise ValueError("qm_i_must_not_replace_phase7_relation_semantics")
    if p7.get("claim_ids_reused") is not True or p7.get("claim_ref_lineage_recorded") is not True:
        raise ValueError("qm_i_phase7_claim_lineage_guards_missing")
    if p7.get("decision_ids_guessed_when_absent") is not False or p7.get("watch_id_reused") is not True:
        raise ValueError("qm_i_phase7_id_guards_invalid")

    stance_guards = phase7_stance_contract.get("guards")
    if not isinstance(stance_guards, Mapping):
        raise ValueError("qm_i_phase7_stance_guards_missing")
    required_phase7_guards = (
        "probability_is_not_independent_vote",
        "confidence_is_not_independent_vote",
        "same_family_timing_is_correlated_not_independent",
    )
    if any(stance_guards.get(field) is not True for field in required_phase7_guards):
        raise ValueError("qm_i_phase7_independence_semantics_changed")

    review = result.get("review_semantics")
    if not isinstance(review, Mapping):
        raise ValueError("qm_i_review_semantics_missing")
    expected_review = {
        "common_ancestry_is_automatic_defect": False,
        "common_ancestry_requires_review": True,
        "missing_lineage_proves_independence": False,
        "direct_reference_is_independent_confirmation": False,
        "automatic_weight_change_allowed": False,
        "automatic_scanner_or_decision_change_allowed": False,
    }
    for field, expected in expected_review.items():
        if review.get(field) is not expected:
            raise ValueError(f"qm_i_review_semantics_invalid:{field}")

    boundaries = result.get("boundaries")
    if not isinstance(boundaries, Mapping):
        raise ValueError("qm_i_boundaries_missing")
    unsafe_true = [key for key, value in boundaries.items() if value is True]
    if unsafe_true:
        raise ValueError("qm_i_unsafe_boundary_enabled:" + ",".join(sorted(unsafe_true)))
    if result.get("next_mandatory_work_package") != "BA-QM4 / QM-D + QM-E — Dependence, Effective N & Calibration":
        raise ValueError("qm_i_next_work_package_invalid")
    return result


def validate_closure_file(path: str | Path | None = None) -> dict[str, Any]:
    ba_qm2 = validate_ba_qm2_handoff_file()
    lineage_contract = load_qm_i_contract()
    phase7_stance = _read_json(DEFAULT_STANCE_CONTRACT_PATH, error_prefix="phase7_stance_contract")
    manifest_path = Path(path) if path is not None else DEFAULT_MANIFEST_PATH
    payload = _read_json(manifest_path, error_prefix="qm_i_closure")
    return validate_closure_manifest(
        payload,
        ba_qm2_handoff=ba_qm2,
        lineage_contract=lineage_contract,
        phase7_stance_contract=phase7_stance,
    )
