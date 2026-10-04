from __future__ import annotations

import copy

import pytest

from scanner.research.governance.ba_qm9_depot_watch_audit import (
    BAQM9AuditError,
    EXPECTED_CHECKS,
    load_contract,
    validate_contract,
    validate_foundation_file,
)


def test_ba_qm9_foundation_covers_all_masterplan_checks_and_blocks_closure_on_ba_qm8() -> None:
    receipt = validate_foundation_file()
    assert receipt["status"] == "PASSED_CONTRACT_FOUNDATION"
    assert receipt["engineering_status"] == "IN_PROGRESS"
    assert receipt["runtime_audit_status"] == "IMPLEMENTED_CANONICAL_7H"
    assert receipt["orchestrated_watch_integrity_status"] == "FINAL_WATCH_RESEALED_AFTER_READ_ONLY_ENRICHMENT"
    assert receipt["parallel_preparation_allowed"] is True
    assert receipt["required_check_count"] == 11
    assert receipt["required_checks"] == EXPECTED_CHECKS
    assert receipt["old_scanner_heuristic_fallback_forbidden"] is True
    assert receipt["depot_wall_clock_freshness_threshold_defined"] is False
    assert receipt["undefined_depot_age_may_not_be_claimed_fresh"] is True
    assert receipt["ba_qm9_closure_blocked_by_ba_qm8"] is True
    assert receipt["evidence_impact"] == "PROMOTION_BLOCKED"
    assert receipt["automatic_release_allowed"] is False
    assert receipt["empirical_promotion_performed"] is False
    assert receipt["closure_claimed"] is False


def test_contract_rejects_missing_masterplan_check() -> None:
    value = load_contract()
    changed = copy.deepcopy(value)
    changed["required_checks"].remove("STALE_DATA")
    with pytest.raises(BAQM9AuditError, match="ba_qm9_required_checks_invalid"):
        validate_contract(changed)


def test_contract_rejects_scanner_heuristic_fallback() -> None:
    value = load_contract()
    changed = copy.deepcopy(value)
    changed["forbidden_shortcuts"].remove(
        "replace_missing_decision_state_with_old_scanner_heuristic"
    )
    with pytest.raises(BAQM9AuditError, match="ba_qm9_forbidden_shortcuts_invalid"):
        validate_contract(changed)


def test_contract_rejects_unknown_add_capacity_becoming_available() -> None:
    value = load_contract()
    changed = copy.deepcopy(value)
    changed["check_contract"]["ADD_CAPACITY"][
        "unknown_capacity_must_remain_unknown"
    ] = False
    with pytest.raises(
        BAQM9AuditError,
        match="ba_qm9_capacity_must_be_true:unknown_capacity_must_remain_unknown",
    ):
        validate_contract(changed)


def test_contract_rejects_invented_depot_freshness_threshold_claim() -> None:
    value = load_contract()
    changed = copy.deepcopy(value)
    changed["check_contract"]["STALE_DATA"][
        "depot_wall_clock_freshness_threshold_defined"
    ] = True
    with pytest.raises(
        BAQM9AuditError,
        match="ba_qm9_stale_must_be_false:depot_wall_clock_freshness_threshold_defined",
    ):
        validate_contract(changed)


def test_contract_rejects_watch_action_recomputation() -> None:
    value = load_contract()
    changed = copy.deepcopy(value)
    changed["check_contract"]["ACTION_SEMANTICS"][
        "watch_may_present_but_not_recompute_action"
    ] = False
    with pytest.raises(
        BAQM9AuditError,
        match="ba_qm9_action_must_be_true:watch_may_present_but_not_recompute_action",
    ):
        validate_contract(changed)


def test_contract_rejects_missing_decision_being_neutralized() -> None:
    value = load_contract()
    changed = copy.deepcopy(value)
    changed["check_contract"]["MISSING_EVIDENCE"][
        "missing_evidence_may_not_be_neutralized"
    ] = False
    with pytest.raises(
        BAQM9AuditError,
        match="ba_qm9_missing_must_be_true:missing_evidence_may_not_be_neutralized",
    ):
        validate_contract(changed)


def test_contract_rejects_ba_qm9_closure_without_ba_qm8_dependency() -> None:
    value = load_contract()
    changed = copy.deepcopy(value)
    changed["ba_qm8_dependency"][
        "ba_qm9_engineering_closure_requires_ba_qm8_engineering_closure"
    ] = False
    with pytest.raises(
        BAQM9AuditError,
        match="ba_qm9_dependency_must_be_true:ba_qm9_engineering_closure_requires_ba_qm8_engineering_closure",
    ):
        validate_contract(changed)


def test_contract_rejects_lag1_release_authority() -> None:
    value = load_contract()
    changed = copy.deepcopy(value)
    changed["governance_guard"]["ba_qm9_may_release_promotion_block"] = True
    with pytest.raises(
        BAQM9AuditError,
        match="ba_qm9_governance_must_be_false:ba_qm9_may_release_promotion_block",
    ):
        validate_contract(changed)
