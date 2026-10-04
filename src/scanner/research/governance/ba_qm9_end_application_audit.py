"""BA-QM9 pure quality-management audit for the existing Depot-Watch end application.

No Depot-Watch, Decision-Layer, scanner, action or execution logic is implemented
here. The module only validates the BA-QM9 audit contract and inspects the
current public Research/Decision/runtime boundaries.

Private Depot snapshots are deliberately not loaded from the repository. Their
end-application behavior is exercised by controlled manipulation tests.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from scanner.reports.daily_research import validate_daily_research
from scanner.research.decision_layer.w10_orchestration import validate_sealed_manifest
from scanner.research.decision_layer.watch_runtime import (
    WatchRuntimeError,
    validate_runtime_manifest,
)
from scanner.research.governance.ba_qm8_closure import evaluate_ba_qm8_closure


SCHEMA_VERSION = "ba_qm9_end_application_audit_v1"
_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_CONTRACT_PATH = _ROOT / "configs" / "ba_qm9_end_application_audit_v1.json"
DEFAULT_W10_PATH = _ROOT / "artifacts" / "research" / "decision_snapshot_w10.json"
DEFAULT_RUNTIME_MANIFEST = (
    _ROOT / "artifacts" / "research" / "watch_runtime" / "manifest.json"
)

EXPECTED_PATH = (
    "RESEARCH_SNAPSHOT",
    "DECISION_BUNDLE",
    "DEPOT_SNAPSHOT",
    "WERTPAPIERDEPOT_WATCH",
)
EXPECTED_CHECKS = (
    "IDENTITY",
    "TIME",
    "COMPLETENESS",
    "POSITION_STATE",
    "ADD_CAPACITY",
    "ACTION_SEMANTICS",
    "MISSING_EVIDENCE",
    "STALE_DATA",
    "FAIL_CLOSED",
    "EXPLANATION",
    "REPRODUCIBILITY",
)


class BAQM9AuditError(ValueError):
    """Raised when the BA-QM9 quality contract becomes permissive or ambiguous."""


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BAQM9AuditError(f"ba_qm9_input_unreadable:{path}") from exc
    if not isinstance(value, dict):
        raise BAQM9AuditError(f"ba_qm9_input_must_be_object:{path}")
    return value


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise BAQM9AuditError(f"ba_qm9_mapping_required:{field}")
    return value


def _require_true(value: Mapping[str, Any], key: str, label: str) -> None:
    if value.get(key) is not True:
        raise BAQM9AuditError(f"{label}_must_be_true:{key}")


def _require_false(value: Mapping[str, Any], key: str, label: str) -> None:
    if value.get(key) is not False:
        raise BAQM9AuditError(f"{label}_must_be_false:{key}")


def load_contract(path: str | Path = DEFAULT_CONTRACT_PATH) -> dict[str, Any]:
    return validate_contract(_read_json(Path(path)))


def validate_contract(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise BAQM9AuditError("ba_qm9_contract_must_be_object")
    result = dict(value)

    if result.get("schema_version") != SCHEMA_VERSION:
        raise BAQM9AuditError("ba_qm9_schema_invalid")
    if result.get("business_area") != "BA-QM9":
        raise BAQM9AuditError("ba_qm9_business_area_invalid")
    if result.get("name") != "Wertpapierdepot-Watch Audit":
        raise BAQM9AuditError("ba_qm9_name_invalid")
    if result.get("status") != "COMPLETE":
        raise BAQM9AuditError("ba_qm9_status_invalid")
    if result.get("engineering_status") != "COMPLETE":
        raise BAQM9AuditError("ba_qm9_engineering_status_invalid")
    if result.get("scope") != "QUALITY_MANAGEMENT_ONLY":
        raise BAQM9AuditError("ba_qm9_scope_must_be_quality_management_only")
    for key in (
        "product_logic_changed",
        "investment_logic_changed",
        "execution_enabled",
        "empirical_promotion_performed",
    ):
        _require_false(result, key, "ba_qm9")
    _require_true(result, "closure_claimed", "ba_qm9")

    if tuple(result.get("audit_path") or ()) != EXPECTED_PATH:
        raise BAQM9AuditError("ba_qm9_audit_path_invalid")
    if tuple(result.get("required_checks") or ()) != EXPECTED_CHECKS:
        raise BAQM9AuditError("ba_qm9_required_checks_invalid")

    masterplan = _mapping(result.get("masterplan_guard"), "masterplan_guard")
    _require_false(
        masterplan,
        "missing_decision_state_may_use_old_scanner_heuristic",
        "ba_qm9_masterplan",
    )

    checks = _mapping(result.get("check_contract"), "check_contract")
    if tuple(checks.keys()) != EXPECTED_CHECKS:
        raise BAQM9AuditError("ba_qm9_check_contract_coverage_invalid")

    must_true: dict[str, tuple[str, ...]] = {
        "IDENTITY": (
            "same_research_snapshot_required",
            "exact_symbol_identity_required",
            "fuzzy_symbol_substitution_forbidden",
            "decision_position_context_identity_required",
        ),
        "TIME": (
            "future_position_snapshot_forbidden",
            "decision_as_of_must_match_authoritative_snapshot",
            "later_evidence_backdating_forbidden",
        ),
        "COMPLETENESS": (
            "one_watch_row_per_position_required",
            "duplicate_position_symbols_forbidden",
            "watch_status_must_match_available_rows",
            "missing_bundle_must_remain_visible",
        ),
        "POSITION_STATE": (
            "supplied_depot_snapshot_is_authoritative",
            "model_portfolio_substitution_forbidden",
            "legacy_holdings_substitution_forbidden",
            "position_state_may_not_rewrite_universal_stance",
        ),
        "ADD_CAPACITY": (
            "capacity_only_from_position_snapshot",
            "contradictory_capacity_rejected",
            "unknown_capacity_remains_unknown",
        ),
        "ACTION_SEMANTICS": (
            "watch_presents_existing_decision",
            "watch_may_not_recompute_universal_stance",
            "watch_may_not_recompute_transition",
            "watch_may_not_change_portfolio_action",
            "review_state_is_not_order",
            "execution_forbidden",
        ),
        "MISSING_EVIDENCE": (
            "missing_decision_bundle_produces_unavailable_row",
            "unavailable_row_exposes_no_decision",
            "scanner_scalar_fallback_forbidden",
            "missing_may_not_be_neutralized",
        ),
        "STALE_DATA": (
            "snapshot_mismatch_rejected",
            "as_of_mismatch_rejected",
            "stale_runtime_transport_rejected",
            "undefined_wall_clock_age_may_not_be_claimed_fresh",
        ),
        "FAIL_CLOSED": (
            "invalid_decision_bundle_becomes_unavailable",
            "ambiguous_duplicate_positions_fail_hard",
            "position_context_mismatch_becomes_unavailable",
            "forbidden_execution_fields_fail_hard",
        ),
        "EXPLANATION": (
            "explanation_id_preserved",
            "missing_or_limited_evidence_preserved",
            "decision_change_triggers_preserved",
            "reliability_not_interpreted_as_success_probability",
        ),
        "REPRODUCIBILITY": (
            "canonical_watch_id_required",
            "same_inputs_same_watch_id_required",
            "post_build_tampering_detected",
            "w10_snapshot_identity_required",
        ),
    }
    for dimension, keys in must_true.items():
        block = _mapping(checks.get(dimension), dimension)
        for key in keys:
            _require_true(block, key, f"ba_qm9_{dimension.lower()}")

    stale = _mapping(checks["STALE_DATA"], "STALE_DATA")
    _require_false(
        stale,
        "arbitrary_wall_clock_threshold_invented",
        "ba_qm9_stale_data",
    )

    finding_policy = _mapping(
        result.get("current_finding_policy"), "current_finding_policy"
    )
    for key in (
        "stale_runtime_transport_is_ba_qm9_safety_test",
        "stale_runtime_transport_operational_cause_routes_to_ba_qm10",
        "stale_runtime_may_not_generate_watch",
    ):
        _require_true(finding_policy, key, "ba_qm9_finding_policy")

    dependencies = _mapping(result.get("dependencies"), "dependencies")
    _require_true(
        dependencies,
        "ba_qm8_engineering_complete_required",
        "ba_qm9_dependencies",
    )
    if dependencies.get("lag1_finding_id") != "QM-H-QMJ-PHASE1A-LAG1-001":
        raise BAQM9AuditError("ba_qm9_lag1_finding_invalid")
    if dependencies.get("lag1_capa_id") != "QM-H-CAPA-QMJ-PHASE1A-LAG1-001":
        raise BAQM9AuditError("ba_qm9_lag1_capa_invalid")
    if dependencies.get("lag1_evidence_impact") != "PROMOTION_BLOCKED":
        raise BAQM9AuditError("ba_qm9_lag1_block_missing")
    _require_false(
        dependencies,
        "ba_qm9_may_release_lag1_block",
        "ba_qm9_dependencies",
    )


    if result.get("closure_required_check_count") != len(EXPECTED_CHECKS):
        raise BAQM9AuditError("ba_qm9_closure_check_count_invalid")
    if result.get("closure_required_checks_passed") is not True:
        raise BAQM9AuditError("ba_qm9_closure_checks_not_passed")
    if int(result.get("closure_manipulation_and_regression_tests_passed") or 0) < 1:
        raise BAQM9AuditError("ba_qm9_closure_test_receipt_missing")
    for key in (
        "closure_product_logic_changed",
        "closure_investment_logic_changed",
        "closure_execution_enabled",
        "closure_empirical_promotion_performed",
    ):
        _require_false(result, key, "ba_qm9_closure")
    if not str(result.get("closure_snapshot_id") or "").strip():
        raise BAQM9AuditError("ba_qm9_closure_snapshot_id_required")
    if not str(result.get("closure_snapshot_as_of") or "").strip():
        raise BAQM9AuditError("ba_qm9_closure_snapshot_as_of_required")
    findings = result.get("open_operational_findings")
    if not isinstance(findings, list):
        raise BAQM9AuditError("ba_qm9_open_operational_findings_must_be_list")
    for finding in findings:
        if not isinstance(finding, Mapping):
            raise BAQM9AuditError("ba_qm9_operational_finding_must_be_object")
        if finding.get("blocks_ba_qm9_engineering_closure") is not False:
            raise BAQM9AuditError("ba_qm9_operational_finding_must_not_silently_block_closed_audit")
        if finding.get("decision_safety_effect") != "NO_FALSE_WATCH_RUNTIME_ACCEPTED":
            raise BAQM9AuditError("ba_qm9_operational_finding_safety_state_invalid")
        if finding.get("next_owner") != "BA-QM10 – Produktions- und Betriebs-QM":
            raise BAQM9AuditError("ba_qm9_operational_finding_owner_invalid")
    if result.get("next_mandatory_work_package") != "BA-QM10 – Produktions- und Betriebs-QM":
        raise BAQM9AuditError("ba_qm9_next_work_package_invalid")

    return result


def audit_current_public_boundaries(root: str | Path = _ROOT) -> dict[str, Any]:
    """Inspect current public inputs without touching private Depot data."""
    root = Path(root).resolve()
    contract = load_contract(root / "configs" / "ba_qm9_end_application_audit_v1.json")

    ba_qm8 = evaluate_ba_qm8_closure(root)
    if ba_qm8.get("status") != "BA_QM8_ENGINEERING_COMPLETE":
        raise BAQM9AuditError("ba_qm9_requires_completed_ba_qm8")
    if ba_qm8.get("engineering_closure_performed") is not True:
        raise BAQM9AuditError("ba_qm9_requires_performed_ba_qm8_closure")
    if ba_qm8.get("lag1_evidence_impact") != "PROMOTION_BLOCKED":
        raise BAQM9AuditError("ba_qm9_requires_preserved_lag1_block")

    daily = validate_daily_research(root)
    snapshot_id = str(daily.get("snapshot_id") or "")
    w10 = validate_sealed_manifest(
        _read_json(root / "artifacts" / "research" / "decision_snapshot_w10.json"),
        expected_snapshot_id=snapshot_id,
    )

    runtime_path = root / "artifacts" / "research" / "watch_runtime" / "manifest.json"
    runtime_state = "MISSING_FAIL_CLOSED"
    runtime_reason = "runtime_manifest_missing"
    runtime_snapshot_id = None
    runtime_accepted = False
    finding: dict[str, Any] | None = None

    if runtime_path.exists():
        runtime = _read_json(runtime_path)
        runtime_snapshot_id = str(runtime.get("snapshot_id") or "")
        try:
            validate_runtime_manifest(
                runtime,
                w10_manifest=w10,
                expected_snapshot_id=snapshot_id,
            )
        except WatchRuntimeError as exc:
            runtime_state = "STALE_OR_INVALID_FAIL_CLOSED"
            runtime_reason = str(exc)
            finding = {
                "finding_id": "BA-QM9-F01",
                "category": "STALE_RUNTIME_TRANSPORT",
                "state": runtime_state,
                "reason": runtime_reason,
                "current_snapshot_id": snapshot_id,
                "runtime_snapshot_id": runtime_snapshot_id,
                "decision_safety_effect": "NO_FALSE_WATCH_RUNTIME_ACCEPTED",
                "operational_followup_owner": "BA-QM10",
            }
        else:
            runtime_state = "CURRENT_VALID"
            runtime_reason = "validated_against_current_w10"
            runtime_accepted = True

    if runtime_snapshot_id and runtime_snapshot_id != snapshot_id and runtime_accepted:
        raise BAQM9AuditError("stale_runtime_was_silently_accepted")

    return {
        "schema_version": "ba_qm9_current_public_boundary_receipt_v1",
        "status": "PASSED_PUBLIC_BOUNDARY_AUDIT",
        "scope": "QUALITY_MANAGEMENT_ONLY",
        "snapshot_id": snapshot_id,
        "snapshot_as_of": daily.get("as_of"),
        "w10_status": w10.get("status"),
        "w10_snapshot_id": w10.get("snapshot_id"),
        "runtime_transport_state": runtime_state,
        "runtime_transport_reason": runtime_reason,
        "runtime_snapshot_id": runtime_snapshot_id,
        "runtime_transport_accepted": runtime_accepted,
        "stale_runtime_silently_accepted": False,
        "private_depot_data_loaded": False,
        "product_logic_changed": False,
        "investment_logic_changed": False,
        "execution_enabled": False,
        "lag1_finding_id": contract["dependencies"]["lag1_finding_id"],
        "lag1_capa_id": contract["dependencies"]["lag1_capa_id"],
        "lag1_evidence_impact": "PROMOTION_BLOCKED",
        "ba_qm9_may_release_lag1_block": False,
        "operational_finding": finding,
    }



def evaluate_ba_qm9_closure(root: str | Path = _ROOT) -> dict[str, Any]:
    """Validate BA-QM9 closure against the current public boundaries.

    The controlled private-Depot manipulation suite is executed by CI. This
    runtime closure check binds the recorded closure to the live authoritative
    Research/W10 snapshot and verifies that stale runtime transport is either
    current-valid or explicitly rejected fail-closed.
    """
    root = Path(root).resolve()
    contract = load_contract(root / "configs" / "ba_qm9_end_application_audit_v1.json")
    public = audit_current_public_boundaries(root)

    if str(contract.get("closure_snapshot_id") or "") != str(public.get("snapshot_id") or ""):
        raise BAQM9AuditError("ba_qm9_closure_snapshot_identity_mismatch")
    if str(contract.get("closure_snapshot_as_of") or "") != str(public.get("snapshot_as_of") or ""):
        raise BAQM9AuditError("ba_qm9_closure_snapshot_as_of_mismatch")
    if public.get("w10_status") != "sealed":
        raise BAQM9AuditError("ba_qm9_requires_sealed_w10")
    if public.get("w10_snapshot_id") != public.get("snapshot_id"):
        raise BAQM9AuditError("ba_qm9_w10_snapshot_mismatch")
    if public.get("stale_runtime_silently_accepted") is not False:
        raise BAQM9AuditError("ba_qm9_stale_runtime_silently_accepted")
    if public.get("runtime_transport_accepted") is False and public.get(
        "runtime_transport_state"
    ) not in {"STALE_OR_INVALID_FAIL_CLOSED", "MISSING_FAIL_CLOSED"}:
        raise BAQM9AuditError("ba_qm9_runtime_fail_closed_state_invalid")
    if public.get("lag1_evidence_impact") != "PROMOTION_BLOCKED":
        raise BAQM9AuditError("ba_qm9_lag1_block_not_preserved")

    return {
        "schema_version": "ba_qm9_end_application_closure_receipt_v1",
        "status": "BA_QM9_ENGINEERING_COMPLETE",
        "scope": "QUALITY_MANAGEMENT_ONLY",
        "snapshot_id": public["snapshot_id"],
        "snapshot_as_of": public["snapshot_as_of"],
        "required_check_count": len(EXPECTED_CHECKS),
        "all_required_checks_passed": True,
        "manipulation_and_regression_tests_passed": contract[
            "closure_manipulation_and_regression_tests_passed"
        ],
        "runtime_transport_state": public["runtime_transport_state"],
        "runtime_transport_accepted": public["runtime_transport_accepted"],
        "stale_runtime_silently_accepted": False,
        "open_operational_findings": contract["open_operational_findings"],
        "product_logic_changed": False,
        "investment_logic_changed": False,
        "execution_enabled": False,
        "empirical_promotion_performed": False,
        "lag1_finding_id": public["lag1_finding_id"],
        "lag1_capa_id": public["lag1_capa_id"],
        "lag1_evidence_impact": "PROMOTION_BLOCKED",
        "ba_qm9_may_release_lag1_block": False,
        "next_mandatory_work_package": "BA-QM10 – Produktions- und Betriebs-QM",
    }
