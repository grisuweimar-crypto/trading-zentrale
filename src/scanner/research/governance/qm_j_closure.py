"""Fail-closed closure validator for BA-QM7 / QM-J.

Engineering closure does not mean empirical validation. A triggered QM-J control
must remain promotion-blocked through its QM-H/CAPA lifecycle even when BA-QM7
engineering coverage is complete.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from scanner.research.governance.qm_g_closure import (
    validate_closure_file as validate_ba_qm6_closure_file,
)
from scanner.research.governance.qm_h_capa import CapaLedger
from scanner.research.governance.qm_j_negative_controls import load_qm_j_contract
from scanner.research.governance.qm_j_selection_freshness_gate import load_gate

EXPECTED_QM_J_STATUS = "QM-J COMPLETE — MULTI-LEVEL NEGATIVE CONTROLS & FALSIFICATION ACTIVE"
EXPECTED_BA_STATUS = "BA-QM7 COMPLETE — FALSIFICATION COVERAGE COMPLETE; SELECTION FRESHNESS CAPA PRESERVED"
NEXT_WORK_PACKAGE = "BA-QM8 – End-to-End Scanner Audit"
DEFAULT_MANIFEST_PATH = Path(__file__).resolve().parents[4] / "configs" / "ba_qm7_qm_j_closure_v1.json"
QM_DIR = Path(__file__).resolve().parents[4] / "artifacts" / "research" / "qm"

EXPECTED_HASHES = {
    "feature": "8fca2c6b4619720ab8f21929c843bc42c889f6e17067061f06d622639a660b6d",
    "data_research": "a646d5eb4397f32d60f922ddd333e30cf61562cde8b5b1a6d1f791d3e380f483",
    "decision_e2e": "6d124f53d28b2ab9d6cf5778b4d0b793e418fdfd791b71bf2e83b834ad105936",
    "investigation": "27ae7c66a2e931a5008301d88f96b73539917fe6798852218a846455d7817a23",
}
EXPECTED_LEVELS = {"DATA", "FEATURE", "RESEARCH", "DECISION_LAYER", "END_TO_END"}
REQUIRED_CAPABILITIES = {
    "five_level_negative_control_harness",
    "data_shifted_past_only_control",
    "feature_permutation_null_distribution",
    "research_pseudo_signal_null_distribution",
    "decision_placebo_evidence_isolation",
    "end_to_end_destroyed_predictive_information_attack",
    "matched_grid_comparison_for_phase1a",
    "outcome_blind_control_generation",
    "reproducible_control_hashes_and_result_hashes",
    "promotion_stop_on_similarly_strong_control",
    "qm_h_finding_and_capa_escalation",
    "post_trigger_root_cause_investigation",
    "fail_closed_selection_freshness_guard",
    "promotion_block_preserved_pending_prospective_effectiveness",
    "no_negative_control_result_auto_promotes_system",
}
EXPECTED_INTEGRATIONS = {
    "ba_qm6_closure_required",
    "qm_a_governance_preserved",
    "qm_b_historical_taxonomy_integrity_preserved",
    "qm_h_capa_lifecycle_required_for_trigger",
    "w11_integrity_interpretation_only",
    "open_promotion_block_must_survive_ba_qm7_closure",
}
EXPECTED_BOUNDARIES = {
    "history_or_prices_mutated",
    "scanner_logic_changed",
    "scanner_weights_changed",
    "selection_signal_changed",
    "timing_signal_changed",
    "decision_semantics_changed",
    "depot_watch_semantics_changed",
    "productive_artifacts_replaced",
    "orders_generated",
    "execution_enabled",
    "productive_integration_enabled",
    "empirical_promotion_performed",
    "open_capa_closed_by_engineering_closure",
}


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"ba_qm7_closure_input_unreadable:{path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"ba_qm7_closure_input_must_be_object:{path}")
    return payload


def _validate_research_only(payload: Mapping[str, Any], label: str) -> None:
    if payload.get("research_only") is not True:
        raise ValueError(f"ba_qm7_research_only_required:{label}")
    if payload.get("productive_integration_enabled") is not False:
        raise ValueError(f"ba_qm7_productive_integration_forbidden:{label}")
    if payload.get("execution_allowed") is not False:
        raise ValueError(f"ba_qm7_execution_forbidden:{label}")
    if payload.get("promotion_performed") is not False:
        raise ValueError(f"ba_qm7_promotion_forbidden:{label}")


def validate_closure_manifest(
    payload: Mapping[str, Any],
    *,
    ba_qm6_closure: Mapping[str, Any],
    qm_j_contract: Mapping[str, Any],
    feature_result: Mapping[str, Any],
    data_research_result: Mapping[str, Any],
    decision_e2e_result: Mapping[str, Any],
    investigation_result: Mapping[str, Any],
    freshness_gate: Mapping[str, Any],
    capa_finding: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise ValueError("ba_qm7_closure_must_be_object")
    result = dict(payload)
    if result.get("schema_version") != "ba_qm7_qm_j_closure_v1":
        raise ValueError("ba_qm7_closure_schema_invalid")
    if result.get("business_area") != "BA-QM7" or result.get("qm_axis") != "QM-J":
        raise ValueError("ba_qm7_identity_invalid")
    if result.get("display_status_qm_j") != EXPECTED_QM_J_STATUS or result.get("business_area_status") != EXPECTED_BA_STATUS:
        raise ValueError("ba_qm7_display_status_invalid")
    if result.get("engineering_status") != "COMPLETE" or result.get("negative_control_coverage_status") != "COMPLETE":
        raise ValueError("ba_qm7_engineering_or_coverage_incomplete")
    if result.get("empirical_validation_status") != "NOT_ESTABLISHED":
        raise ValueError("ba_qm7_must_not_claim_empirical_validation")
    if result.get("research_only") is not True or result.get("productive_integration_enabled") is not False or result.get("execution_allowed") is not False:
        raise ValueError("ba_qm7_scope_invalid")
    if result.get("empirical_promotion_claimed") is not False:
        raise ValueError("ba_qm7_must_not_claim_promotion")
    if result.get("all_negative_controls_passed") is not False:
        raise ValueError("ba_qm7_must_preserve_triggered_control")
    if result.get("triggered_negative_controls") != ["PHASE1A_DATA_LAG1_SHIFTED_SCORE"]:
        raise ValueError("ba_qm7_triggered_control_set_invalid")

    if ba_qm6_closure.get("schema_version") != "ba_qm6_qm_g_closure_v1" or ba_qm6_closure.get("engineering_status") != "COMPLETE":
        raise ValueError("ba_qm7_ba_qm6_not_complete")
    if ba_qm6_closure.get("next_mandatory_work_package") != "BA-QM7 / QM-J — Negative Controls & Falsifikation":
        raise ValueError("ba_qm7_ba_qm6_handoff_mismatch")
    if qm_j_contract.get("schema_version") != "qm_j_negative_controls_v1":
        raise ValueError("ba_qm7_qm_j_contract_invalid")

    if feature_result.get("schema_version") != "qm_j_phase1a_selection_falsification_result_v1" or feature_result.get("result_hash") != EXPECTED_HASHES["feature"]:
        raise ValueError("ba_qm7_feature_result_invalid")
    _validate_research_only(feature_result, "feature")
    feature_eval = feature_result.get("falsification_evaluation") or {}
    if feature_eval.get("status") != "NEGATIVE_CONTROLS_NOT_SIMILARLY_STRONG" or feature_eval.get("triggered_controls") not in ([], ()):
        raise ValueError("ba_qm7_feature_control_unexpected_trigger")

    if data_research_result.get("schema_version") != "qm_j_phase1a_data_research_falsification_result_v1" or data_research_result.get("result_hash") != EXPECTED_HASHES["data_research"]:
        raise ValueError("ba_qm7_data_research_result_invalid")
    _validate_research_only(data_research_result, "data_research")
    data_eval = (data_research_result.get("data_control") or {}).get("evaluation") or {}
    research_eval = (data_research_result.get("research_control") or {}).get("evaluation") or {}
    if data_eval.get("triggered") is not True or data_eval.get("status") != "PROMOTION_STOP_INVESTIGATION_CAPA_REQUIRED":
        raise ValueError("ba_qm7_data_trigger_not_preserved")
    if data_eval.get("promotion_blocked_by_qm_j") is not True or data_eval.get("capa_required") is not True:
        raise ValueError("ba_qm7_data_trigger_governance_missing")
    if research_eval.get("triggered") is not False or research_eval.get("status") != "NEGATIVE_CONTROLS_NOT_SIMILARLY_STRONG":
        raise ValueError("ba_qm7_research_control_invalid")

    if decision_e2e_result.get("schema_version") != "qm_j_decision_e2e_falsification_result_v1" or decision_e2e_result.get("result_hash") != EXPECTED_HASHES["decision_e2e"]:
        raise ValueError("ba_qm7_decision_e2e_result_invalid")
    _validate_research_only(decision_e2e_result, "decision_e2e")
    placebo = decision_e2e_result.get("placebo_sidecar") or {}
    destroyed = decision_e2e_result.get("destroyed_information") or {}
    if placebo.get("zero_decision_effect") is not True or placebo.get("triggered") is not False or placebo.get("w11_status") != "passed":
        raise ValueError("ba_qm7_decision_placebo_invalid")
    if destroyed.get("effective") is not True or destroyed.get("triggered") is not False or destroyed.get("w11_status") != "passed":
        raise ValueError("ba_qm7_destroyed_information_control_invalid")
    if int(destroyed.get("affected_universal_stance_change_count") or 0) <= 0 or int(destroyed.get("portfolio_action_change_count") or 0) <= 0:
        raise ValueError("ba_qm7_e2e_pipeline_not_sensitive_to_destroyed_information")
    if decision_e2e_result.get("w11_acceptance_interpretation") != "INTEGRITY_ONLY_NOT_PREDICTIVE_VALIDATION":
        raise ValueError("ba_qm7_w11_interpretation_invalid")

    if investigation_result.get("schema_version") != "qm_j_phase1a_lag1_investigation_v1" or investigation_result.get("result_hash") != EXPECTED_HASHES["investigation"]:
        raise ValueError("ba_qm7_investigation_result_invalid")
    if investigation_result.get("finding_id") != "QM-H-QMJ-PHASE1A-LAG1-001":
        raise ValueError("ba_qm7_investigation_finding_invalid")
    guard = investigation_result.get("interpretation_guard") or {}
    if guard.get("descriptive_only") is not True or guard.get("post_trigger") is not True or guard.get("may_not_be_used_as_confirmation") is not True:
        raise ValueError("ba_qm7_post_trigger_investigation_guard_invalid")
    if investigation_result.get("root_cause_assigned_by_code") is not False or investigation_result.get("promotion_performed") is not False:
        raise ValueError("ba_qm7_investigation_overreach")

    if freshness_gate.get("finding_id") != "QM-H-QMJ-PHASE1A-LAG1-001" or freshness_gate.get("capa_id") != "QM-H-CAPA-QMJ-PHASE1A-LAG1-001":
        raise ValueError("ba_qm7_freshness_gate_identity_invalid")
    if freshness_gate.get("status") != "IMPLEMENTED_GOVERNANCE_GUARD_PENDING_EFFECTIVENESS_VERIFICATION":
        raise ValueError("ba_qm7_freshness_gate_status_invalid")
    if (freshness_gate.get("blocked_evidence") or {}).get("promotion_allowed") is not False:
        raise ValueError("ba_qm7_freshness_promotion_block_missing")
    if (freshness_gate.get("release_guard") or {}).get("automatic_release_from_promotion_block") is not False:
        raise ValueError("ba_qm7_automatic_release_forbidden")

    if capa_finding.get("finding_id") != "QM-H-QMJ-PHASE1A-LAG1-001" or capa_finding.get("status") != "IMPLEMENTED":
        raise ValueError("ba_qm7_qm_h_capa_not_implemented")
    if capa_finding.get("capa_id") != "QM-H-CAPA-QMJ-PHASE1A-LAG1-001" or capa_finding.get("evidence_impact") != "PROMOTION_BLOCKED":
        raise ValueError("ba_qm7_qm_h_capa_identity_or_impact_invalid")

    levels = result.get("control_levels")
    if not isinstance(levels, Mapping) or set(levels) != EXPECTED_LEVELS:
        raise ValueError("ba_qm7_control_level_coverage_invalid")
    if (levels.get("DATA") or {}).get("result") != "TRIGGERED_PROMOTION_STOP_INVESTIGATION_CAPA":
        raise ValueError("ba_qm7_manifest_data_trigger_missing")
    if (levels.get("FEATURE") or {}).get("result") != "NEGATIVE_CONTROLS_NOT_SIMILARLY_STRONG":
        raise ValueError("ba_qm7_manifest_feature_result_invalid")
    if (levels.get("RESEARCH") or {}).get("result") != "NEGATIVE_CONTROLS_NOT_SIMILARLY_STRONG":
        raise ValueError("ba_qm7_manifest_research_result_invalid")
    if (levels.get("DECISION_LAYER") or {}).get("result") != "ZERO_DECISION_EFFECT":
        raise ValueError("ba_qm7_manifest_decision_result_invalid")
    if (levels.get("END_TO_END") or {}).get("result") != "PIPELINE_REACTED_TO_DESTROYED_INFORMATION_WITHOUT_STRUCTURAL_BREAK":
        raise ValueError("ba_qm7_manifest_e2e_result_invalid")

    open_capa = result.get("open_capa")
    if not isinstance(open_capa, Mapping):
        raise ValueError("ba_qm7_open_capa_required")
    if open_capa.get("finding_id") != capa_finding.get("finding_id") or open_capa.get("capa_id") != capa_finding.get("capa_id"):
        raise ValueError("ba_qm7_open_capa_identity_mismatch")
    if open_capa.get("status") != "IMPLEMENTED" or open_capa.get("evidence_impact") != "PROMOTION_BLOCKED":
        raise ValueError("ba_qm7_open_capa_state_invalid")
    if open_capa.get("effectiveness_verification") != "PENDING_PROSPECTIVE_UNSPENT_EVIDENCE":
        raise ValueError("ba_qm7_open_capa_effectiveness_state_invalid")
    if open_capa.get("closure_does_not_close_capa") is not True or open_capa.get("automatic_release_allowed") is not False:
        raise ValueError("ba_qm7_open_capa_release_guard_invalid")

    missing = sorted(REQUIRED_CAPABILITIES - set(result.get("completed_capabilities") or []))
    if missing:
        raise ValueError("ba_qm7_capabilities_missing:" + ",".join(missing))
    integrations = result.get("integration_contracts")
    if not isinstance(integrations, Mapping) or set(integrations) != EXPECTED_INTEGRATIONS or any(value is not True for value in integrations.values()):
        raise ValueError("ba_qm7_integration_guards_invalid")
    boundaries = result.get("boundaries")
    if not isinstance(boundaries, Mapping) or set(boundaries) != EXPECTED_BOUNDARIES or any(value is not False for value in boundaries.values()):
        raise ValueError("ba_qm7_boundary_guards_invalid")
    if result.get("next_mandatory_work_package") != NEXT_WORK_PACKAGE:
        raise ValueError("ba_qm7_next_work_package_invalid")
    return result


def validate_closure_file(path: str | Path | None = None) -> dict[str, Any]:
    ba_qm6 = validate_ba_qm6_closure_file()
    qm_j_contract = load_qm_j_contract()
    feature = _read_json(QM_DIR / "qm_j_phase1a_selection_falsification.json")
    data_research = _read_json(QM_DIR / "qm_j_phase1a_data_research_falsification.json")
    decision_e2e = _read_json(QM_DIR / "qm_j_decision_e2e_falsification.json")
    investigation = _read_json(QM_DIR / "qm_j_phase1a_lag1_investigation.json")
    freshness = load_gate()
    ledger = CapaLedger(QM_DIR / "qm_h_capa_ledger.jsonl")
    verification = ledger.verify_integrity()
    if verification.get("valid") is not True:
        raise ValueError("ba_qm7_qm_h_ledger_invalid")
    finding = ledger.get_finding("QM-H-QMJ-PHASE1A-LAG1-001")
    manifest = _read_json(Path(path) if path is not None else DEFAULT_MANIFEST_PATH)
    return validate_closure_manifest(
        manifest,
        ba_qm6_closure=ba_qm6,
        qm_j_contract=qm_j_contract,
        feature_result=feature,
        data_research_result=data_research,
        decision_e2e_result=decision_e2e,
        investigation_result=investigation,
        freshness_gate=freshness,
        capa_finding=finding,
    )
