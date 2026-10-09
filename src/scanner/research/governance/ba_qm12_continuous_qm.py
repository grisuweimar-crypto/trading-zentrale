"""BA-QM12 permanent Continuous-QM operating contract.

This module consolidates already implemented governance and monitoring controls.
It observes current artifacts and existing audit surfaces; it does not change
scanner, research, Decision-Layer, portfolio-action or execution semantics.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Mapping

from scanner.research.governance.ba_qm10_production_operations import audit_current_operations
from scanner.research.governance.qm_h_capa import CapaLedger
from scanner.research.governance.qm_j_phase1a_lag1_effectiveness import (
    load_plan as load_lag1_effectiveness_plan,
)

ROOT = Path(__file__).resolve().parents[4]
CONTRACT_PATH = ROOT / "configs" / "ba_qm12_continuous_qm_v1.json"
REQUIRED_CONTROLS = (
    "QM_A_GOVERNANCE",
    "QM_H_CAPA",
    "QM_I_LINEAGE",
    "NEGATIVE_CONTROLS",
    "REGRESSION_TESTS",
    "COVERAGE_MONITORING",
    "DRIFT_MONITORING",
    "CALIBRATION_MONITORING",
    "PRODUCTION_HEALTH",
)
LIFECYCLE = (
    "DESIGN",
    "QM_CONTRACT",
    "TEST",
    "RESEARCH",
    "VALIDATION",
    "PROMOTION",
    "PRODUCTION",
    "CONTINUOUS_QM",
)

MASTERPLAN_RESIDUAL_IDS = (
    "W6_ELLIOTT_LINEAGE",
    "PHASE1A_LAG1_CAPA",
    "W8_EMPIRICAL_UTILITY",
    "BA_QM2_EXTERNAL_HISTORICAL_INTEGRITY",
    "BA_QM6_EMPIRICAL_VALIDATION",
    "BA_QM7_EMPIRICAL_VALIDATION",
    "DECISION_LAYER_EMPIRICAL_PROMOTION",
    "PHASE8_EXTERNAL_EVIDENCE",
)


class BAQM12Error(ValueError):
    pass


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BAQM12Error(f"json_unreadable:{path}") from exc
    if not isinstance(value, dict):
        raise BAQM12Error(f"json_object_required:{path}")
    return value


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise BAQM12Error(f"mapping_required:{field}")
    return value


def validate_contract(value: Mapping[str, Any]) -> dict[str, Any]:
    if value.get("schema_version") != "ba_qm12_continuous_qm_v1":
        raise BAQM12Error("schema_invalid")
    if value.get("business_area") != "BA-QM12":
        raise BAQM12Error("business_area_invalid")
    if value.get("engineering_status") != "COMPLETE":
        raise BAQM12Error("engineering_not_complete")
    if value.get("operating_status") != "ACTIVE_CONTINUOUS_QM":
        raise BAQM12Error("continuous_qm_not_active")
    if value.get("scope") != "QUALITY_MANAGEMENT_ONLY":
        raise BAQM12Error("scope_invalid")
    for field in (
        "research_logic_changed",
        "decision_logic_changed",
        "investment_logic_changed",
        "execution_enabled",
        "empirical_promotion_performed",
    ):
        if value.get(field) is not False:
            raise BAQM12Error(f"unsafe_true_or_missing:{field}")

    controls = _mapping(value.get("continuous_controls"), "continuous_controls")
    if tuple(controls.keys()) != REQUIRED_CONTROLS:
        raise BAQM12Error("continuous_control_set_invalid")
    for key in REQUIRED_CONTROLS:
        row = _mapping(controls[key], f"control:{key}")
        if row.get("required") is not True or not str(row.get("authority") or "").strip():
            raise BAQM12Error(f"continuous_control_binding_invalid:{key}")

    if tuple(value.get("future_module_lifecycle") or ()) != LIFECYCLE:
        raise BAQM12Error("future_module_lifecycle_invalid")
    guards = _mapping(value.get("lifecycle_guards"), "lifecycle_guards")
    required_false = (
        "stage_skipping_allowed",
        "engineering_completion_equals_empirical_validation",
        "validation_equals_promotion",
        "promotion_equals_production",
        "production_without_continuous_qm_allowed",
        "missing_monitoring_is_neutral",
    )
    if any(guards.get(field) is not False for field in required_false):
        raise BAQM12Error("lifecycle_guard_invalid")

    finding_policy = _mapping(value.get("finding_lifecycle_policy"), "finding_lifecycle_policy")
    expected_states = ("OPEN", "CAPA_IMPLEMENTED_PENDING_VERIFICATION", "CLOSED_EFFECTIVE", "REOPENED")
    if tuple(finding_policy.get("states") or ()) != expected_states:
        raise BAQM12Error("finding_lifecycle_states_invalid")
    for field in (
        "closure_requires_effectiveness_evidence",
        "recurrence_requires_reopen_or_linked_new_finding",
        "closed_finding_may_not_hide_current_failure",
    ):
        if finding_policy.get(field) is not True:
            raise BAQM12Error(f"finding_lifecycle_guard_missing:{field}")
    reopen_triggers = tuple(finding_policy.get("reopen_triggers") or ())
    if reopen_triggers != (
        "CONTROL_REGRESSION",
        "RECURRENCE",
        "EFFECTIVENESS_EVIDENCE_INVALIDATED",
        "CURRENT_FAIL_CLOSED_GUARD_FAILURE",
    ):
        raise BAQM12Error("finding_reopen_triggers_invalid")
    escalation = _mapping(finding_policy.get("escalation"), "finding_lifecycle_policy.escalation")
    if not tuple(escalation.get("required_for") or ()):
        raise BAQM12Error("finding_escalation_triggers_missing")
    if escalation.get("automatic_semantic_change_allowed") is not False:
        raise BAQM12Error("finding_escalation_semantic_change_forbidden")
    if escalation.get("automatic_promotion_allowed") is not False:
        raise BAQM12Error("finding_escalation_auto_promotion_forbidden")

    policy = _mapping(value.get("monitoring_policy"), "monitoring_policy")
    if policy.get("scheduled_continuous_qm_required") is not True:
        raise BAQM12Error("scheduled_continuous_qm_required")
    if policy.get("drift_thresholds_may_not_be_invented") is not True:
        raise BAQM12Error("drift_threshold_guard_missing")
    if policy.get("drift_without_preregistered_threshold_is_report_only") is not True:
        raise BAQM12Error("drift_report_only_guard_missing")
    if policy.get("automatic_research_or_decision_change_allowed") is not False:
        raise BAQM12Error("automatic_semantic_change_forbidden")
    if policy.get("automatic_promotion_allowed") is not False:
        raise BAQM12Error("automatic_promotion_forbidden")
    completion = _mapping(value.get("masterplan_completion_gate"), "masterplan_completion_gate")
    if completion.get("status") != "ACTIVE":
        raise BAQM12Error("masterplan_completion_gate_not_active")
    if completion.get("full_completion_claim_requires_zero_blocking_residuals") is not True:
        raise BAQM12Error("masterplan_zero_residual_gate_missing")
    if completion.get("engineering_completion_is_full_masterplan_completion") is not False:
        raise BAQM12Error("engineering_may_not_equal_masterplan_completion")
    tracked = _mapping(completion.get("tracked_residuals"), "masterplan_completion_gate.tracked_residuals")
    if tuple(tracked.keys()) != MASTERPLAN_RESIDUAL_IDS:
        raise BAQM12Error("masterplan_residual_set_invalid")
    return dict(value)


def _validated_numeric_score_coverage(
    validation: Mapping[str, Any], *, symbol_count: int
) -> dict[str, Any]:
    """Independently enforce the scanner's observed, published completeness policy.

    An upstream 'ok' flag does not exempt BA-QM12 from checking the numeric
    score count against that snapshot's authoritative policy threshold.
    """
    policy = _mapping(validation.get("policy"), "history_metadata.validation.policy")
    threshold = policy.get("min_score_ratio")
    if (
        type(threshold) not in (int, float)
        or not math.isfinite(threshold)
        or not 0 < threshold <= 1
    ):
        raise BAQM12Error("scanner_score_ratio_policy_invalid")

    numeric = validation.get("numeric_score_count")
    if type(numeric) is not int or not 0 <= numeric <= symbol_count:
        raise BAQM12Error("scanner_numeric_score_count_invalid")

    required_numeric = math.ceil(symbol_count * threshold)
    if numeric < required_numeric:
        raise BAQM12Error("scanner_numeric_score_coverage_below_threshold")
    return {
        "numeric_score_count": numeric,
        "numeric_score_ratio": numeric / symbol_count,
        "min_score_ratio": threshold,
        "required_numeric_score_count": required_numeric,
        "numeric_score_coverage_status": "PASS",
    }


def _snapshot_monitor(root: Path) -> dict[str, Any]:
    metadata = _read(root / "artifacts" / "research" / "history_metadata.json")
    calibration = _read(root / "artifacts" / "research" / "probability_calibration_2.json")
    runtime = _read(root / "artifacts" / "research" / "watch_runtime" / "public_long_reference.json")

    snapshot_id = str(metadata.get("snapshot_id") or "")
    if not snapshot_id:
        raise BAQM12Error("snapshot_id_missing")
    validation = _mapping(metadata.get("validation"), "history_metadata.validation")
    if validation.get("status") != "ok" or metadata.get("latest_run_complete") is not True:
        raise BAQM12Error("current_scanner_snapshot_not_valid")

    required = int(validation.get("required_symbol_count") or 0)
    symbols = int(validation.get("symbol_count") or 0)
    if required <= 0 or symbols != required:
        raise BAQM12Error("scanner_coverage_incomplete")
    score_coverage = _validated_numeric_score_coverage(validation, symbol_count=symbols)

    price = _mapping(metadata.get("price_coverage"), "price_coverage")
    price_required = int(price.get("required_symbol_count") or 0)
    covered = int(price.get("covered_symbol_count") or 0)
    unavailable = int(price.get("unavailable_symbol_count") or 0)
    if price_required != required or covered + unavailable != price_required:
        raise BAQM12Error("price_coverage_accounting_invalid")

    source = _mapping(calibration.get("source"), "calibration.source")
    calibration_current = str(source.get("snapshot_id") or "") == snapshot_id
    if not calibration_current:
        raise BAQM12Error("calibration_snapshot_stale")

    diagnostics = _mapping(runtime.get("diagnostics"), "runtime.diagnostics")
    runtime_current = str(diagnostics.get("snapshot_id") or "") == snapshot_id
    if not runtime_current:
        raise BAQM12Error("runtime_snapshot_stale")
    runtime_rows = int(runtime.get("row_count") or 0)
    runtime_bundles = int(diagnostics.get("bundle_count") or 0)
    if runtime_rows != symbols or runtime_bundles != symbols:
        raise BAQM12Error("runtime_symbol_coverage_mismatch")
    if list(diagnostics.get("missing_current_packet_symbols") or []):
        raise BAQM12Error("runtime_missing_current_packets")
    if diagnostics.get("private_position_data_persisted") is not False:
        raise BAQM12Error("runtime_private_position_data_persisted")
    if diagnostics.get("scanner_scalar_fallback_used") is not False:
        raise BAQM12Error("runtime_scanner_fallback_used")

    return {
        "snapshot_id": snapshot_id,
        "as_of": metadata.get("as_of"),
        "coverage": {
            "required_symbol_count": required,
            "symbol_count": symbols,
            **score_coverage,
            "price_covered_symbol_count": covered,
            "price_unavailable_symbol_count": unavailable,
        },
        "drift_monitoring": {
            "status": "OBSERVED_REPORT_ONLY_NO_PREREGISTERED_THRESHOLD",
            "baseline_symbol_count": validation.get("baseline_symbol_count"),
            "current_symbol_count": symbols,
            "price_median_sessions": price.get("median_sessions"),
            "price_min_sessions": price.get("min_sessions"),
            "threshold_breach_claimed": False,
        },
        "calibration": {
            "snapshot_current": True,
            "source_as_of": source.get("as_of"),
        },
        "runtime": {
            "snapshot_current": True,
            "row_count": runtime_rows,
            "bundle_count": runtime_bundles,
            "missing_current_packet_count": 0,
            "private_position_data_persisted": False,
            "scanner_scalar_fallback_used": False,
        },
    }


def _masterplan_residual_monitor(
    root: Path,
    *,
    qmi: Mapping[str, Any],
    qmj: Mapping[str, Any],
    w8: Mapping[str, Any],
) -> dict[str, Any]:
    lag1_plan = load_lag1_effectiveness_plan(
        root / "configs" / "qm_j_phase1a_lag1_effectiveness_v1.json",
        root / "configs" / "qm_j_phase1a_lag1_effectiveness_freeze_v1.json",
    )
    phase7 = _mapping(qmi.get("phase7_integration"), "qmi.phase7_integration")
    w6_complete = all(
        phase7.get(field) is True
        for field in (
            "w6_exact_prospective_raw_source_lineage_supported",
            "w6_exact_prospective_feature_lineage_supported",
            "w6_validation_lineage_supported",
            "w6_uncertainty_lineage_supported",
            "w6_legacy_missing_provenance_remains_incomplete",
        )
    )

    capa = _mapping(qmj.get("open_capa"), "qmj.open_capa")
    lag1_effectiveness = str(capa.get("effectiveness_verification") or "UNKNOWN")
    lag1_blocked = (
        capa.get("evidence_impact") == "PROMOTION_BLOCKED"
        and lag1_effectiveness != "VERIFIED_EFFECTIVE"
    )

    w8_validation = _mapping(w8.get("validation"), "w8.validation")
    w8_governance = _mapping(w8.get("governance"), "w8.governance")
    w8_open = (
        w8_validation.get("empirically_validated") is not True
        or w8_validation.get("promotion_eligible") is not True
    )

    ba_qm2 = _read(root / "configs" / "ba_qm2_handoff_v2.json")
    blockers = list(_mapping(ba_qm2.get("qm_b"), "ba_qm2.qm_b").get("external_blockers") or [])
    ba_qm2_open = ba_qm2.get("strict_historical_promotion_status") != "PROMOTED"

    ba_qm6 = _read(root / "configs" / "ba_qm6_qm_g_closure_v1.json")
    ba_qm7 = _read(root / "configs" / "ba_qm7_qm_j_closure_v1.json")
    ba_qm6_open = ba_qm6.get("empirical_validation_status") != "ESTABLISHED"
    ba_qm7_open = ba_qm7.get("empirical_validation_status") != "ESTABLISHED"

    decision = _read(root / "configs" / "decision_validation_promotion_v1.json")
    decision_open = decision.get("productive_integration_enabled") is not True

    external = _read(root / "configs" / "external_evidence_8_contract_v1.json")
    external_quarantined = external.get("status") != "productive_promoted"

    residuals = {
        "W6_ELLIOTT_LINEAGE": {
            "status": (
                "PROSPECTIVE_RAW_FEATURE_VALIDATION_UNCERTAINTY_LINEAGE_COMPLETE"
                if w6_complete
                else "UPSTREAM_LINEAGE_INCOMPLETE"
            ),
            "blocking_masterplan_completion": not w6_complete,
            "legacy_backfill_policy": "MISSING_PROVENANCE_REMAINS_EXPLICIT_NOT_GUESSED",
        },
        "PHASE1A_LAG1_CAPA": {
            "status": lag1_effectiveness,
            "blocking_masterplan_completion": lag1_blocked,
            "evidence_impact": capa.get("evidence_impact"),
            "finding_id": capa.get("finding_id"),
            "capa_id": capa.get("capa_id"),
            "prospective_plan_validated": True,
            "eligible_observation_from": lag1_plan.eligible_observation_from,
            "primary_horizon_sessions": lag1_plan.horizon_sessions,
            "automatic_release_allowed": False,
        },
        "W8_EMPIRICAL_UTILITY": {
            "status": "RESEARCH_REQUIRED" if w8_open else "ESTABLISHED",
            "blocking_masterplan_completion": w8_open,
            "prospective_unspent_from": w8_governance.get("prospective_unspent_from"),
        },
        "BA_QM2_EXTERNAL_HISTORICAL_INTEGRITY": {
            "status": ba_qm2.get("strict_historical_promotion_status"),
            "blocking_masterplan_completion": ba_qm2_open,
            "external_blockers": blockers,
        },
        "BA_QM6_EMPIRICAL_VALIDATION": {
            "status": ba_qm6.get("empirical_validation_status"),
            "blocking_masterplan_completion": ba_qm6_open,
        },
        "BA_QM7_EMPIRICAL_VALIDATION": {
            "status": ba_qm7.get("empirical_validation_status"),
            "blocking_masterplan_completion": ba_qm7_open,
        },
        "DECISION_LAYER_EMPIRICAL_PROMOTION": {
            "status": "NOT_ESTABLISHED" if decision_open else "ESTABLISHED",
            "blocking_masterplan_completion": decision_open,
            "productive_integration_enabled": decision.get("productive_integration_enabled"),
            "execution_allowed": decision.get("execution_allowed"),
        },
        "PHASE8_EXTERNAL_EVIDENCE": {
            "status": (
                "QUARANTINED_PENDING_SEPARATE_PROMOTION"
                if external_quarantined
                else "SEPARATELY_PROMOTED"
            ),
            "blocking_masterplan_completion": False,
            "canonical_decision_path_integration_required": False,
        },
    }
    blocking = [
        residual_id
        for residual_id, row in residuals.items()
        if row["blocking_masterplan_completion"] is True
    ]
    return {
        "residuals": residuals,
        "blocking_residual_ids": blocking,
        "blocking_residual_count": len(blocking),
        "masterplan_end_state_complete": len(blocking) == 0,
        "full_completion_claim_allowed": len(blocking) == 0,
    }


def evaluate_continuous_qm(root: str | Path = ROOT) -> dict[str, Any]:
    root = Path(root).resolve()
    contract = validate_contract(_read(root / "configs" / "ba_qm12_continuous_qm_v1.json"))

    predecessor = _read(root / "configs" / "ba_qm11_system_audit_v1.json")
    if predecessor.get("status") != "COMPLETE" or predecessor.get("result") != "PASS":
        raise BAQM12Error("ba_qm11_not_closed_pass")

    qma = _read(root / "configs" / "qm_a_research_governance_v1.json")
    if qma.get("status") != "active_research_governance":
        raise BAQM12Error("qm_a_not_active")
    qmh = _read(root / "configs" / "qm_h_capa_v1.json")
    if qmh.get("continuous_control") is not True:
        raise BAQM12Error("qm_h_not_continuous")
    qmi = _read(root / "configs" / "qm_i_evidence_lineage_v1.json")
    if qmi.get("status") != "active_research_governance":
        raise BAQM12Error("qm_i_not_active")
    qmj = _read(root / "configs" / "ba_qm7_qm_j_closure_v1.json")
    if qmj.get("negative_control_coverage_status") != "COMPLETE":
        raise BAQM12Error("negative_controls_not_complete")

    ledger = CapaLedger(root / "artifacts" / "research" / "qm" / "qm_h_capa_ledger.jsonl")
    lag1 = ledger.get_finding("QM-H-QMJ-PHASE1A-LAG1-001")
    if lag1.get("evidence_impact") != "PROMOTION_BLOCKED":
        raise BAQM12Error("lag1_promotion_block_lost")

    w8 = _read(root / "configs" / "decision_depot_action_policy_v1.json")
    w8_validation = _mapping(w8.get("validation"), "w8.validation")
    if w8_validation.get("promotion_eligible") is not False:
        raise BAQM12Error("w8_unexpectedly_promotion_eligible")

    masterplan = _masterplan_residual_monitor(root, qmi=qmi, qmj=qmj, w8=w8)
    snapshot = _snapshot_monitor(root)
    production = audit_current_operations(root)
    if production.get("status") != "BA_QM10_ENGINEERING_COMPLETE":
        raise BAQM12Error("production_health_not_complete")
    if production.get("all_required_guards_passed") is not True:
        raise BAQM12Error("production_health_guard_failure")
    if production.get("false_decision_observed") is not False:
        raise BAQM12Error("production_false_decision_observed")

    workflow_path = root / ".github" / "workflows" / "ba_qm12_continuous_qm.yml"
    workflow_text = workflow_path.read_text(encoding="utf-8")
    if "schedule:" not in workflow_text or "workflow_dispatch:" not in workflow_text:
        raise BAQM12Error("continuous_qm_workflow_not_scheduled")
    if "tests/test_ba_qm12_continuous_qm.py" not in workflow_text:
        raise BAQM12Error("continuous_qm_regression_not_bound")

    return {
        "schema_version": "ba_qm12_continuous_qm_receipt_v1",
        "engineering_status": "BA_QM12_ENGINEERING_COMPLETE",
        "operating_status": "ACTIVE_CONTINUOUS_QM",
        "required_control_count": len(REQUIRED_CONTROLS),
        "required_controls_active": list(REQUIRED_CONTROLS),
        "finding_lifecycle_status": "ACTIVE_FAIL_CLOSED",
        "finding_reopen_triggers": list(contract["finding_lifecycle_policy"]["reopen_triggers"]),
        "finding_escalation_required_for": list(contract["finding_lifecycle_policy"]["escalation"]["required_for"]),
        "future_module_lifecycle": list(LIFECYCLE),
        "snapshot_monitor": snapshot,
        "production_health_status": production["status"],
        "lag1_evidence_impact": "PROMOTION_BLOCKED",
        "ba_qm12_may_release_lag1_block": False,
        "w8_promotion_eligible": False,
        "masterplan_residual_monitor": masterplan,
        "masterplan_end_state_complete": masterplan["masterplan_end_state_complete"],
        "full_masterplan_completion_claim_allowed": masterplan["full_completion_claim_allowed"],
        "research_logic_changed": False,
        "decision_logic_changed": False,
        "investment_logic_changed": False,
        "execution_enabled": False,
        "empirical_promotion_performed": False,
    }
