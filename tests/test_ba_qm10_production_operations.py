from __future__ import annotations

import copy

import pytest

from scanner.research.governance.ba_qm10_production_operations import (
    BAQM10AuditError,
    EXPECTED_CHECKS,
    EXPECTED_FINDINGS,
    EXPECTED_FINDING_STATES,
    audit_current_operations,
    audit_current_runtime_capacity,
    load_contract,
    validate_contract,
)


def test_ba_qm10_contract_is_pure_qm_and_covers_all_masterplan_dimensions() -> None:
    contract = load_contract()
    assert contract["status"] == "IN_PROGRESS"
    assert contract["scope"] == "QUALITY_MANAGEMENT_ONLY"
    assert tuple(contract["required_checks"]) == EXPECTED_CHECKS
    assert len(EXPECTED_CHECKS) == 13
    assert contract["research_logic_changed"] is False
    assert contract["decision_logic_changed"] is False
    assert contract["investment_logic_changed"] is False
    assert contract["execution_enabled"] is False
    assert contract["empirical_promotion_performed"] is False
    assert contract["closure_claimed"] is False
    assert tuple(row["finding_id"] for row in contract["findings"]) == EXPECTED_FINDINGS
    assert {row["finding_id"]: row["state"] for row in contract["findings"]} == EXPECTED_FINDING_STATES


def test_ba_qm10_current_operations_audit_exposes_real_findings_fail_closed() -> None:
    result = audit_current_operations()
    assert result["status"] == "OPEN_FINDINGS_CAPA_REQUIRED"
    assert result["scope"] == "QUALITY_MANAGEMENT_ONLY"
    assert result["required_check_count"] == 13
    assert tuple(result["check_status"].keys()) == EXPECTED_CHECKS
    assert result["open_finding_count"] == 4
    assert result["implemented_capa_pending_verification_count"] == 3
    assert set(result["open_findings"]) == set(EXPECTED_FINDINGS)
    assert result["false_decision_observed"] is False
    assert result["all_observed_failures_fail_closed"] is True
    assert result["research_logic_changed"] is False
    assert result["decision_logic_changed"] is False
    assert result["investment_logic_changed"] is False
    assert result["execution_enabled"] is False
    assert result["lag1_evidence_impact"] == "PROMOTION_BLOCKED"
    assert result["ba_qm10_may_release_lag1_block"] is False
    assert result["closure_eligible"] is False


def test_scanner_autorun_has_retry_serialization_provenance_and_partial_run_guards() -> None:
    result = audit_current_operations()
    guards = result["static_guards"]
    assert guards["scanner_three_retry_slots"] is True
    assert guards["scanner_serialized"] is True
    assert guards["scanner_provenance_required"] is True
    assert guards["scanner_records_success_marker_only_after_validation"] is True
    assert guards["scanner_incomplete_run_exits_failure"] is True
    assert guards["phase2_fallback_schedule_present"] is True
    assert guards["phase2_freshness_gate_present"] is True


def test_current_decision_and_runtime_publication_are_fail_closed() -> None:
    result = audit_current_operations()
    guards = result["static_guards"]
    assert guards["decision_same_snapshot_guard_present"] is True
    assert guards["decision_stale_publication_refusal_present"] is True
    assert guards["runtime_validates_sealed_w10"] is True
    assert guards["runtime_stale_publication_refusal_present"] is True
    assert guards["symbol_views_validate_runtime_identity"] is True


def test_current_open_operational_risks_are_not_hidden() -> None:
    result = audit_current_operations()
    guards = result["static_guards"]
    assert guards["decision_any_single_upstream_can_trigger"] is True
    assert guards["decision_readiness_gate_present"] is True
    assert guards["runtime_hard_two_mb_shard_limit_present"] is True
    assert guards["symbol_views_can_trigger_on_elliott_before_runtime"] is True
    assert guards["symbol_view_runtime_readiness_gate_present"] is True
    assert guards["qm_j_canonical_archive_default_present"] is True
    assert guards["scanner_explicit_main_advance_refusal_guard"] is True
    assert result["scanner_publication_race_risk_open"] is False
    assert result["static_risk_states"]["BA-QM10-R01"] == "CAPA_IMPLEMENTED_PENDING_LIVE_VERIFICATION"


def test_f01_is_exactly_runtime_transport_capacity_and_never_false_watch() -> None:
    contract = load_contract()
    f01 = next(row for row in contract["findings"] if row["finding_id"] == "BA-QM10-F01")
    assert f01["inherited_from"] == "BA-QM9-F01"
    assert f01["category"] == "PUBLICATION_RUNTIME_TRANSPORT_CAPACITY"
    assert f01["state"] == "CAPA_IMPLEMENTED_PENDING_LIVE_VERIFICATION"
    assert f01["capa"]["implemented"] is True
    assert f01["capa"]["decision_logic_changed"] is False
    assert f01["capa"]["packet_semantics_changed"] is False
    assert len(f01["evidence"]) == 2
    assert {row["max_shard_bytes"] for row in f01["evidence"]} == {2178135}
    assert {row["hard_limit_bytes"] for row in f01["evidence"]} == {2000000}
    assert f01["effect"]["current_runtime_publication_blocked"] is True
    assert f01["effect"]["stale_runtime_remained_public"] is True
    assert f01["effect"]["false_watch_accepted"] is False
    assert f01["effect"]["decision_safety"] == "FAIL_CLOSED"


def test_f02_records_premature_fanin_but_same_snapshot_guard_prevented_publication() -> None:
    contract = load_contract()
    f02 = next(row for row in contract["findings"] if row["finding_id"] == "BA-QM10-F02")
    evidence = f02["evidence"][0]
    assert evidence["run_id"] == 37224257622
    assert evidence["current_snapshot_id"] != evidence["persisted_phase2_snapshot_id"]
    assert "snapshot_mismatch:phase2_probability" in evidence["error"]
    assert f02["effect"]["premature_workflow_started"] is True
    assert f02["effect"]["false_decision_published"] is False
    assert f02["effect"]["later_recovery_observed"] is True
    assert f02["effect"]["decision_safety"] == "FAIL_CLOSED"


def test_f03_records_symbol_view_premature_triggers_as_fail_closed() -> None:
    contract = load_contract()
    f03 = next(row for row in contract["findings"] if row["finding_id"] == "BA-QM10-F03")
    assert {row["run_id"] for row in f03["evidence"]} == {37226779236, 37227171783}
    assert all(row["error"] == "daily/runtime snapshot mismatch" for row in f03["evidence"])
    assert f03["effect"]["premature_symbol_view_build_started"] is True
    assert f03["effect"]["stale_symbol_views_published"] is False
    assert f03["effect"]["decision_safety"] == "FAIL_CLOSED"


def test_contract_cannot_silently_close_findings() -> None:
    changed = copy.deepcopy(load_contract())
    changed["findings"][0]["state"] = "CLOSED"
    with pytest.raises(
        BAQM10AuditError, match="ba_qm10_finding_state_invalid:BA-QM10-F01"
    ):
        validate_contract(changed)


def test_contract_cannot_claim_ba_qm10_releases_lag1() -> None:
    changed = copy.deepcopy(load_contract())
    changed["dependencies"]["ba_qm10_may_release_lag1_block"] = True
    with pytest.raises(BAQM10AuditError, match="ba_qm10_lag1_release_forbidden"):
        validate_contract(changed)


def test_f01_current_runtime_capacity_capa_stays_under_existing_limit() -> None:
    result = audit_current_runtime_capacity()
    assert result["status"] == "PASS"
    assert result["configured_shard_count"] == 32
    assert result["shard_count"] == 32
    assert result["max_shard_bytes"] < result["hard_limit_bytes"] == 2_000_000
    assert result["symbol_count"] > 0
    assert result["packet_count"] >= result["symbol_count"]
    assert result["private_position_data_included"] is False
    assert result["decision_logic_changed"] is False
    assert result["writes_performed"] is False


def test_f04_records_and_repairs_canonical_decision_archive_selection() -> None:
    contract = load_contract()
    f04 = next(row for row in contract["findings"] if row["finding_id"] == "BA-QM10-F04")
    assert f04["category"] == "CANONICAL_FILE_SELECTION"
    assert f04["state"] == "CAPA_IMPLEMENTED_PENDING_CI_VERIFICATION"
    evidence = f04["evidence"][0]
    assert evidence["error"] == "evidence_archive_missing"
    assert evidence["obsolete_default"].endswith("decision_evidence_7a.jsonl")
    assert evidence["canonical_archive"].endswith("decision_evidence_7a.jsonl.gz")
    assert f04["effect"]["false_decision_published"] is False
    assert f04["effect"]["decision_safety"] == "FAIL_CLOSED"
    assert f04["capa"]["falsification_logic_changed"] is False



def test_r01_scanner_publication_race_capa_is_explicit_and_non_semantic() -> None:
    contract = load_contract()
    r01 = next(
        row for row in contract["static_risks_to_verify"]
        if row["risk_id"] == "BA-QM10-R01"
    )
    assert r01["state"] == "CAPA_IMPLEMENTED_PENDING_LIVE_VERIFICATION"
    assert r01["capa"]["implemented"] is True
    assert r01["capa"]["research_logic_changed"] is False
    assert r01["capa"]["decision_logic_changed"] is False
    assert r01["capa"]["investment_logic_changed"] is False
