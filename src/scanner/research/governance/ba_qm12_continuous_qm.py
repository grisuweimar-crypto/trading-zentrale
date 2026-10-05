"""BA-QM12 permanent Continuous-QM operating contract.

This module consolidates already implemented governance and monitoring controls.
It observes current artifacts and existing audit surfaces; it does not change
scanner, research, Decision-Layer, portfolio-action or execution semantics.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from scanner.research.governance.ba_qm10_production_operations import audit_current_operations
from scanner.research.governance.qm_h_capa import CapaLedger

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
    return dict(value)


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
    numeric = int(validation.get("numeric_score_count") or 0)
    if required <= 0 or symbols != required:
        raise BAQM12Error("scanner_coverage_incomplete")

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
            "numeric_score_count": numeric,
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
        "future_module_lifecycle": list(LIFECYCLE),
        "snapshot_monitor": snapshot,
        "production_health_status": production["status"],
        "lag1_evidence_impact": "PROMOTION_BLOCKED",
        "ba_qm12_may_release_lag1_block": False,
        "w8_promotion_eligible": False,
        "research_logic_changed": False,
        "decision_logic_changed": False,
        "investment_logic_changed": False,
        "execution_enabled": False,
        "empirical_promotion_performed": False,
    }
