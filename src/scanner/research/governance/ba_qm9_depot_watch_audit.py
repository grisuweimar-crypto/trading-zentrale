"""BA-QM9 Depot-Watch audit contract and dependency validator.

BA-QM9 may be engineered in parallel while BA-QM8 awaits a prospective
snapshot, but BA-QM9 engineering closure is not allowed before BA-QM8 closure.
This module adds no investment logic.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from scanner.research.governance.ba_qm8_closure import evaluate_ba_qm8_closure


SCHEMA_VERSION = "ba_qm9_depot_watch_audit_v1"
_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_CONTRACT_PATH = _ROOT / "configs" / "ba_qm9_depot_watch_audit_v1.json"

EXPECTED_PATH = [
    "RESEARCH_SNAPSHOT",
    "DECISION_BUNDLE",
    "DEPOT_SNAPSHOT",
    "WERTPAPIERDEPOT_WATCH",
]
EXPECTED_CHECKS = [
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
]
EXPECTED_FORBIDDEN = {
    "replace_missing_decision_state_with_scanner_score",
    "replace_missing_decision_state_with_r_code",
    "replace_missing_decision_state_with_old_scanner_heuristic",
    "fuzzy_match_position_symbol",
    "infer_add_capacity_from_market_signal",
    "treat_unknown_add_capacity_as_available",
    "turn_review_state_into_broker_order",
    "persist_private_position_book_to_public_repository",
    "claim_undefined_depot_age_is_fresh",
}


class BAQM9AuditError(ValueError):
    pass


def load_contract(path: str | Path = DEFAULT_CONTRACT_PATH) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BAQM9AuditError("ba_qm9_contract_unreadable") from exc
    if not isinstance(value, dict):
        raise BAQM9AuditError("ba_qm9_contract_must_be_object")
    return value


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise BAQM9AuditError(f"ba_qm9_mapping_required:{field}")
    return value


def _must_true(value: Mapping[str, Any], key: str, prefix: str) -> None:
    if value.get(key) is not True:
        raise BAQM9AuditError(f"{prefix}_must_be_true:{key}")


def _must_false(value: Mapping[str, Any], key: str, prefix: str) -> None:
    if value.get(key) is not False:
        raise BAQM9AuditError(f"{prefix}_must_be_false:{key}")


def validate_contract(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise BAQM9AuditError("ba_qm9_contract_must_be_object")
    result = dict(value)

    if result.get("schema_version") != SCHEMA_VERSION:
        raise BAQM9AuditError("ba_qm9_contract_schema_invalid")
    if result.get("business_area") != "BA-QM9":
        raise BAQM9AuditError("ba_qm9_business_area_invalid")
    if result.get("status") != "IN_PROGRESS_PARALLEL_PREPARATION":
        raise BAQM9AuditError("ba_qm9_status_invalid")
    if result.get("runtime_audit_status") != "IMPLEMENTED_CANONICAL_7H":
        raise BAQM9AuditError("ba_qm9_runtime_audit_status_invalid")
    if result.get("orchestrated_watch_integrity_status") != "FINAL_WATCH_RESEALED_AFTER_READ_ONLY_ENRICHMENT":
        raise BAQM9AuditError("ba_qm9_orchestrated_watch_integrity_status_invalid")
    if result.get("input_path") != EXPECTED_PATH:
        raise BAQM9AuditError("ba_qm9_input_path_invalid")
    if result.get("required_checks") != EXPECTED_CHECKS:
        raise BAQM9AuditError("ba_qm9_required_checks_invalid")
    if set(result.get("forbidden_shortcuts") or []) != EXPECTED_FORBIDDEN:
        raise BAQM9AuditError("ba_qm9_forbidden_shortcuts_invalid")

    _must_true(result, "research_only", "ba_qm9")
    _must_false(result, "productive_integration_enabled", "ba_qm9")
    _must_false(result, "execution_allowed", "ba_qm9")
    _must_false(result, "empirical_promotion_performed", "ba_qm9")
    _must_false(result, "closure_claimed", "ba_qm9")

    checks = _mapping(result.get("check_contract"), "check_contract")
    if list(checks) != EXPECTED_CHECKS:
        raise BAQM9AuditError("ba_qm9_check_contract_order_or_coverage_invalid")

    identity = _mapping(checks["IDENTITY"], "IDENTITY")
    for key in (
        "research_snapshot_id_required",
        "decision_bundle_same_snapshot_required",
        "exact_symbol_identity_required",
        "fuzzy_symbol_matching_forbidden",
        "position_context_identity_must_match_7f",
    ):
        _must_true(identity, key, "ba_qm9_identity")

    time = _mapping(checks["TIME"], "TIME")
    for key in (
        "position_may_not_be_future_to_decision",
        "decision_bundle_as_of_must_match_authoritative_watch_as_of",
        "later_evidence_backdating_forbidden",
    ):
        _must_true(time, key, "ba_qm9_time")

    completeness = _mapping(checks["COMPLETENESS"], "COMPLETENESS")
    for key in (
        "one_watch_row_per_position_required",
        "duplicate_position_symbols_forbidden",
        "watch_status_must_match_available_row_counts",
        "missing_bundle_must_remain_visible",
    ):
        _must_true(completeness, key, "ba_qm9_completeness")

    position = _mapping(checks["POSITION_STATE"], "POSITION_STATE")
    for key in (
        "position_state_must_come_from_supplied_depot_snapshot",
        "model_portfolio_substitution_forbidden",
        "legacy_holdings_substitution_forbidden",
        "position_state_may_not_rewrite_universal_stance",
    ):
        _must_true(position, key, "ba_qm9_position")

    capacity = _mapping(checks["ADD_CAPACITY"], "ADD_CAPACITY")
    for key in (
        "capacity_must_derive_only_from_can_add_or_remaining_adds",
        "contradictory_capacity_fails_closed",
        "unknown_capacity_must_remain_unknown",
        "changed_capacity_invalidates_old_7f_context",
    ):
        _must_true(capacity, key, "ba_qm9_capacity")

    action = _mapping(checks["ACTION_SEMANTICS"], "ACTION_SEMANTICS")
    for key in (
        "watch_may_present_but_not_recompute_action",
        "review_states_are_not_orders",
        "position_sizing_forbidden",
        "target_weight_forbidden",
    ):
        _must_true(action, key, "ba_qm9_action")
    _must_false(action, "execution_allowed", "ba_qm9_action")

    missing = _mapping(checks["MISSING_EVIDENCE"], "MISSING_EVIDENCE")
    for key in (
        "missing_decision_bundle_produces_unavailable_row",
        "unavailable_row_decision_must_be_null",
        "scanner_scalar_fallback_forbidden",
        "missing_evidence_may_not_be_neutralized",
    ):
        _must_true(missing, key, "ba_qm9_missing")

    stale = _mapping(checks["STALE_DATA"], "STALE_DATA")
    for key in (
        "decision_snapshot_mismatch_is_stale_and_blocked",
        "decision_as_of_mismatch_is_stale_and_blocked",
        "future_position_snapshot_is_blocked",
        "undefined_depot_age_may_not_be_claimed_fresh",
    ):
        _must_true(stale, key, "ba_qm9_stale")
    _must_false(
        stale,
        "depot_wall_clock_freshness_threshold_defined",
        "ba_qm9_stale",
    )

    fail_closed = _mapping(checks["FAIL_CLOSED"], "FAIL_CLOSED")
    for key in (
        "invalid_bundle_isolated_per_symbol",
        "ambiguous_duplicate_positions_fail_hard",
        "position_context_mismatch_produces_unavailable",
        "forbidden_execution_fields_fail_hard",
    ):
        _must_true(fail_closed, key, "ba_qm9_fail_closed")

    explanation = _mapping(checks["EXPLANATION"], "EXPLANATION")
    for key in (
        "decision_explanation_id_required_for_available_rows",
        "reliability_is_not_success_probability",
        "missing_or_limited_evidence_remains_visible",
        "change_triggers_must_require_upstream_recompute",
    ):
        _must_true(explanation, key, "ba_qm9_explanation")

    reproducibility = _mapping(checks["REPRODUCIBILITY"], "REPRODUCIBILITY")
    for key in (
        "watch_id_canonical_hash_required",
        "post_build_tampering_detected",
        "sealed_w10_identity_required_in_private_orchestration",
        "compact_runtime_must_preserve_decision_semantics",
    ):
        _must_true(reproducibility, key, "ba_qm9_reproducibility")

    dependency = _mapping(result.get("ba_qm8_dependency"), "ba_qm8_dependency")
    _must_true(
        dependency,
        "parallel_engineering_preparation_allowed",
        "ba_qm9_dependency",
    )
    _must_true(
        dependency,
        "ba_qm9_engineering_closure_requires_ba_qm8_engineering_closure",
        "ba_qm9_dependency",
    )

    governance = _mapping(result.get("governance_guard"), "governance_guard")
    if governance.get("lag1_finding_id") != "QM-H-QMJ-PHASE1A-LAG1-001":
        raise BAQM9AuditError("ba_qm9_lag1_finding_invalid")
    if governance.get("lag1_capa_id") != "QM-H-CAPA-QMJ-PHASE1A-LAG1-001":
        raise BAQM9AuditError("ba_qm9_lag1_capa_invalid")
    if governance.get("evidence_impact") != "PROMOTION_BLOCKED":
        raise BAQM9AuditError("ba_qm9_lag1_promotion_block_missing")
    _must_false(
        governance,
        "ba_qm9_may_release_promotion_block",
        "ba_qm9_governance",
    )
    _must_false(
        governance,
        "automatic_release_allowed",
        "ba_qm9_governance",
    )

    return result


def validate_foundation_file(
    path: str | Path = DEFAULT_CONTRACT_PATH,
) -> dict[str, Any]:
    contract = validate_contract(load_contract(path))
    ba_qm8 = evaluate_ba_qm8_closure()

    ba_qm8_closed = (
        ba_qm8.get("engineering_closure_performed") is True
        or (
            ba_qm8.get("engineering_closure_eligible") is True
            and ba_qm8.get("status") == "ELIGIBLE_FOR_BA_QM8_ENGINEERING_CLOSURE"
        )
    )

    return {
        "schema_version": "ba_qm9_depot_watch_audit_foundation_receipt_v1",
        "status": "PASSED_CONTRACT_FOUNDATION",
        "engineering_status": "IN_PROGRESS",
        "runtime_audit_status": "IMPLEMENTED_CANONICAL_7H",
        "orchestrated_watch_integrity_status": "FINAL_WATCH_RESEALED_AFTER_READ_ONLY_ENRICHMENT",
        "parallel_preparation_allowed": True,
        "required_check_count": len(EXPECTED_CHECKS),
        "required_checks": list(EXPECTED_CHECKS),
        "forbidden_shortcut_count": len(EXPECTED_FORBIDDEN),
        "old_scanner_heuristic_fallback_forbidden": True,
        "depot_wall_clock_freshness_threshold_defined": False,
        "undefined_depot_age_may_not_be_claimed_fresh": True,
        "ba_qm8_status": ba_qm8.get("status"),
        "ba_qm8_engineering_closure_ready": ba_qm8_closed,
        "ba_qm9_closure_blocked_by_ba_qm8": not ba_qm8_closed,
        "lag1_finding_id": contract["governance_guard"]["lag1_finding_id"],
        "lag1_capa_id": contract["governance_guard"]["lag1_capa_id"],
        "evidence_impact": "PROMOTION_BLOCKED",
        "automatic_release_allowed": False,
        "empirical_promotion_performed": False,
        "closure_claimed": False,
    }
