from __future__ import annotations

import copy

import pytest

import scanner.research.governance.ba_qm10_production_operations as qm10
from scanner.research.decision_layer.current_evidence import (
    CURRENT_PACKET_SET_SCHEMA_VERSION,
    CurrentDecisionEvidenceError,
    merge_packet_set_into_archive,
)
from scanner.research.decision_layer.input_contract import build_input_packet
from scanner.research.decision_layer.watch_runtime import SHARD_IDS
from scanner.research.governance.ba_qm10_production_operations import (
    BAQM10AuditError,
    EXPECTED_CHECKS,
    EXPECTED_FINDINGS,
    EXPECTED_FINDING_STATES,
    audit_current_operations,
    audit_current_runtime_capacity,
    evaluate_ba_qm10_closure,
    load_contract,
    validate_contract,
)


def test_ba_qm10_contract_is_pure_qm_and_covers_all_masterplan_dimensions() -> None:
    contract = load_contract()
    assert contract["status"] == "COMPLETE"
    assert contract["engineering_status"] == "COMPLETE"
    assert contract["scope"] == "QUALITY_MANAGEMENT_ONLY"
    assert tuple(contract["required_checks"]) == EXPECTED_CHECKS
    assert len(EXPECTED_CHECKS) == 13
    assert contract["research_logic_changed"] is False
    assert contract["decision_logic_changed"] is False
    assert contract["investment_logic_changed"] is False
    assert contract["execution_enabled"] is False
    assert contract["empirical_promotion_performed"] is False
    assert contract["closure_claimed"] is True
    assert tuple(row["finding_id"] for row in contract["findings"]) == EXPECTED_FINDINGS
    assert {row["finding_id"]: row["state"] for row in contract["findings"]} == EXPECTED_FINDING_STATES


def test_ba_qm10_current_operations_audit_is_complete_and_same_snapshot() -> None:
    result = audit_current_operations()
    assert result["status"] == "BA_QM10_ENGINEERING_COMPLETE"
    assert result["scope"] == "QUALITY_MANAGEMENT_ONLY"
    assert result["required_check_count"] == 13
    assert tuple(result["check_status"].keys()) == EXPECTED_CHECKS
    assert result["all_required_checks_passed"] is True
    assert result["all_required_guards_passed"] is True
    assert result["open_finding_count"] == 0
    assert result["implemented_capa_pending_verification_count"] == 0
    assert set(result["finding_states"]) == set(EXPECTED_FINDINGS)
    assert set(result["finding_states"].values()) == {"CLOSED_EFFECTIVE"}
    assert result["open_static_risk_count"] == 0
    assert result["runtime_matches_current_snapshot"] is True
    assert result["symbol_views_match_current_snapshot"] is True
    assert result["symbol_views_runtime_projection_matches"] is True
    assert result["false_decision_observed"] is False
    assert result["all_observed_failures_fail_closed"] is True
    assert result["research_logic_changed"] is False
    assert result["decision_logic_changed"] is False
    assert result["investment_logic_changed"] is False
    assert result["execution_enabled"] is False
    assert result["lag1_evidence_impact"] == "PROMOTION_BLOCKED"
    assert result["ba_qm10_may_release_lag1_block"] is False
    assert result["closure_eligible"] is True


def test_scanner_autorun_has_retry_serialization_provenance_and_partial_run_guards() -> None:
    result = audit_current_operations()
    guards = result["static_guards"]
    assert guards["scanner_current_retry_schedule"] is True
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
    assert result["static_risk_states"]["BA-QM10-R01"] == "CLOSED_EFFECTIVE"


def test_f01_is_exactly_runtime_transport_capacity_and_never_false_watch() -> None:
    contract = load_contract()
    f01 = next(row for row in contract["findings"] if row["finding_id"] == "BA-QM10-F01")
    assert f01["inherited_from"] == "BA-QM9-F01"
    assert f01["category"] == "PUBLICATION_RUNTIME_TRANSPORT_CAPACITY"
    assert f01["state"] == "CLOSED_EFFECTIVE"
    assert f01["effectiveness"]["verification_status"] == "EFFECTIVE"
    assert f01["effectiveness"]["evidence"]["live_runtime_publish_run_id"] == 37236982364
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
    assert f02["state"] == "CLOSED_EFFECTIVE"
    assert f02["effectiveness"]["verification_status"] == "EFFECTIVE"
    assert f02["effectiveness"]["evidence"]["stale_phase2_case_tested"] is True
    assert f02["effectiveness"]["evidence"]["current_phase2_case_tested"] is True


def test_f03_records_symbol_view_premature_triggers_as_fail_closed() -> None:
    contract = load_contract()
    f03 = next(row for row in contract["findings"] if row["finding_id"] == "BA-QM10-F03")
    assert {row["run_id"] for row in f03["evidence"]} == {37226779236, 37227171783}
    assert all(row["error"] == "daily/runtime snapshot mismatch" for row in f03["evidence"])
    assert f03["effect"]["premature_symbol_view_build_started"] is True
    assert f03["effect"]["stale_symbol_views_published"] is False
    assert f03["effect"]["decision_safety"] == "FAIL_CLOSED"
    assert f03["state"] == "CLOSED_EFFECTIVE"
    assert f03["effectiveness"]["verification_status"] == "EFFECTIVE"
    assert f03["effectiveness"]["evidence"]["deferred_publish_skipped"] is True


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


def test_qm3_preserves_historical_32_shard_closure_while_live_runtime_uses_64() -> None:
    contract = load_contract()
    assert contract["closure_evidence"]["public_runtime_shard_count"] == 32
    current = audit_current_runtime_capacity()
    assert current["configured_shard_count"] == len(SHARD_IDS)
    assert current["shard_count"] == current["configured_shard_count"]
    assert current["max_shard_bytes"] < current["hard_limit_bytes"]


def test_f01_current_runtime_capacity_capa_stays_under_existing_limit() -> None:
    result = audit_current_runtime_capacity()
    assert result["status"] == "PASS"
    assert result["configured_shard_count"] == len(SHARD_IDS)
    assert result["shard_count"] == result["configured_shard_count"]
    assert result["max_shard_bytes"] < result["hard_limit_bytes"] == 2_000_000
    assert result["headroom_bytes"] == result["hard_limit_bytes"] - result["max_shard_bytes"]
    assert 0.0 < result["headroom_ratio"] < 1.0
    assert result["capacity_review_recommended"] is (result["headroom_ratio"] < 0.20)
    assert result["health_status"] == "PASS"
    assert result["headroom_ratio"] >= 0.20
    assert result["partition_strategy"] == "size_balanced_v1"
    assert result["shard_sizes_bytes"][result["max_shard_id"]] == result["max_shard_bytes"]
    assert result["largest_symbol_packet_count"] > 0
    assert result["symbol_count"] > 0
    assert result["packet_count"] >= result["symbol_count"]
    assert result["private_position_data_included"] is False
    assert result["decision_logic_changed"] is False
    assert result["writes_performed"] is False


@pytest.mark.parametrize(
    ("size", "transport", "health", "review"),
    [
        (1_599_999, "PASS", "PASS", False),
        (1_600_000, "PASS", "PASS", False),
        (1_600_001, "PASS", "REVIEW_REQUIRED", True),
        (1_999_999, "PASS", "REVIEW_REQUIRED", True),
        (2_000_000, "FAIL", "FAIL", True),
        (2_000_001, "FAIL", "FAIL", True),
    ],
)
def test_runtime_capacity_states(size, transport, health, review):
    result = qm10._runtime_capacity_state(size)
    assert result["transport_status"] == transport
    assert result["health_status"] == health
    assert result["capacity_review_recommended"] is review
    assert result["headroom_bytes"] == 2_000_000 - size


@pytest.mark.parametrize("size", [-1, 1.2, "100"])
def test_capacity_monitor_rejects_invalid_measurements(size):
    with pytest.raises(BAQM10AuditError, match="runtime_capacity_invalid_measurement"):
        qm10._runtime_capacity_state(size)


def test_f04_records_and_repairs_canonical_decision_archive_selection() -> None:
    contract = load_contract()
    f04 = next(row for row in contract["findings"] if row["finding_id"] == "BA-QM10-F04")
    assert f04["category"] == "CANONICAL_FILE_SELECTION"
    assert f04["state"] == "CLOSED_EFFECTIVE"
    assert f04["effectiveness"]["verification_status"] == "EFFECTIVE"
    assert f04["effectiveness"]["evidence"]["frozen_falsification_success_run_id"] == 37230918562
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
    assert r01["state"] == "CLOSED_EFFECTIVE"
    assert r01["effectiveness"]["verification_status"] == "EFFECTIVE"
    assert r01["effectiveness"]["evidence"]["remote_advance_case_rejected"] is True
    assert r01["effectiveness"]["evidence"]["retry_remained_eligible"] is True
    assert r01["capa"]["implemented"] is True
    assert r01["capa"]["research_logic_changed"] is False
    assert r01["capa"]["decision_logic_changed"] is False
    assert r01["capa"]["investment_logic_changed"] is False


def _qm10_idempotency_packet(score: float = 42.0) -> dict:
    return build_input_packet(
        symbol="QM10TEST",
        as_of="2026-10-04T18:00:00+00:00",
        source_snapshot_id="qm10-snapshot",
        evidence=[
            {
                "family": "selection",
                "claim_id": "selection:QM10TEST:qm10-snapshot",
                "as_of": "2026-10-04T18:00:00+00:00",
                "available_from": "2026-10-04T18:00:00+00:00",
                "source_version": "qm10-test",
                "coverage_state": "available",
                "maturity_state": "not_applicable",
                "pit_state": "verified",
                "integration_mode": "production_existing",
                "payload": {"score": score},
            }
        ],
    )


def test_decision_archive_retry_is_idempotent_and_changed_identity_fails_closed(tmp_path) -> None:
    archive = tmp_path / "decision_evidence_7a.jsonl.gz"
    packet = _qm10_idempotency_packet()
    packet_set = {
        "schema_version": CURRENT_PACKET_SET_SCHEMA_VERSION,
        "snapshot_id": "qm10-snapshot",
        "packets": [packet],
    }

    first = merge_packet_set_into_archive(archive, packet_set)
    assert first["current_packets_added"] == 1
    assert first["current_packets_already_present"] == 0

    second = merge_packet_set_into_archive(archive, packet_set)
    assert second["current_packets_added"] == 0
    assert second["current_packets_already_present"] == 1
    assert second["packet_count"] == first["packet_count"] == 1

    changed = {
        **packet_set,
        "packets": [_qm10_idempotency_packet(score=99.0)],
    }
    with pytest.raises(
        CurrentDecisionEvidenceError,
        match="archive_identity_collision_with_changed_packet",
    ):
        merge_packet_set_into_archive(archive, changed)


def test_ba_qm10_retry_recovery_assessment_is_no_longer_unproven() -> None:
    contract = load_contract()
    assessment = contract["current_assessment"]
    assert assessment["RETRY"] == "PASS"
    assert assessment["IDEMPOTENCY"] == "PASS"
    assert assessment["RECOVERY"] == "PASS"



def test_ba_qm10_formal_closure_receipt_is_complete_and_preserves_lag1_block() -> None:
    result = evaluate_ba_qm10_closure()
    assert result["status"] == "BA_QM10_ENGINEERING_COMPLETE"
    assert result["scope"] == "QUALITY_MANAGEMENT_ONLY"
    assert result["required_check_count"] == 13
    assert result["all_required_checks_passed"] is True
    assert result["all_required_guards_passed"] is True
    assert result["open_finding_count"] == 0
    assert result["open_static_risk_count"] == 0
    assert result["runtime_shard_count"] == len(SHARD_IDS)
    assert result["runtime_max_shard_bytes"] < result["runtime_hard_limit_bytes"] == 2_000_000
    assert result["runtime_symbol_count"] > 0
    assert result["runtime_packet_count"] >= result["runtime_symbol_count"]
    assert result["symbol_views_runtime_projection_matches"] is True
    assert result["current_runtime_evaluated_separately"] is True
    historical = result["historical_closure_evidence"]
    assert historical["historical_record_not_live_runtime_identity"] is True
    assert historical["runtime_projection_sha256"]
    assert result["engineering_closure_performed"] is True
    assert result["research_logic_changed"] is False
    assert result["decision_logic_changed"] is False
    assert result["investment_logic_changed"] is False
    assert result["execution_enabled"] is False
    assert result["empirical_promotion_performed"] is False
    assert result["lag1_evidence_impact"] == "PROMOTION_BLOCKED"
    assert result["ba_qm10_may_release_lag1_block"] is False
    assert result["next_mandatory_work_package"] == "BA-QM11 – Gesamtsystem-Audit"



def test_historical_ba_qm10_closure_survives_new_valid_live_snapshot(monkeypatch) -> None:
    monkeypatch.setattr(
        qm10,
        "audit_current_operations",
        lambda root: {
            "status": "BA_QM10_ENGINEERING_COMPLETE",
            "closure_eligible": True,
            "all_required_checks_passed": True,
            "all_required_guards_passed": True,
            "open_finding_count": 0,
            "open_static_risk_count": 0,
            "runtime_matches_current_snapshot": True,
            "symbol_views_match_current_snapshot": True,
            "symbol_views_runtime_projection_matches": True,
            "snapshot_id": "future-live-snapshot",
            "snapshot_as_of": "2026-10-05",
            "required_check_count": 13,
            "runtime_projection_sha256": "f" * 64,
        },
    )
    monkeypatch.setattr(
        qm10,
        "audit_current_runtime_capacity",
        lambda root: {
            "status": "PASS",
            "shard_count": 32,
            "max_shard_bytes": 1_500_000,
            "hard_limit_bytes": 2_000_000,
            "symbol_count": 214,
            "packet_count": 1500,
        },
    )

    result = qm10.evaluate_ba_qm10_closure()
    assert result["snapshot_id"] == "future-live-snapshot"
    assert result["runtime_projection_sha256"] == "f" * 64
    assert result["current_runtime_evaluated_separately"] is True
    historical = result["historical_closure_evidence"]
    assert historical["snapshot_id"] == "36cf527e-ca26-489f-aa55-9c7de3b4355b"
    assert historical["runtime_projection_sha256"] != result["runtime_projection_sha256"]
    assert result["status"] == "BA_QM10_ENGINEERING_COMPLETE"


def test_qm3_capacity_recurrence_is_separate_from_historical_f01() -> None:
    contract = load_contract()
    f01 = next(row for row in contract["findings"] if row["finding_id"] == "BA-QM10-F01")
    f05 = next(row for row in contract["findings"] if row["finding_id"] == "BA-QM10-F05")
    assert f01["state"] == "CLOSED_EFFECTIVE"
    assert f01["capa"]["implementation"].startswith("Expand deterministic Watch runtime transport from 16 to 32")
    assert f01["effectiveness"]["evidence"]["published_shard_count"] == 32
    assert f05["related_prior_finding_id"] == "BA-QM10-F01"
    assert f05["state"] == "CLOSED_EFFECTIVE"
    assert f05["effectiveness"]["evidence"]["published_shard_count"] == len(SHARD_IDS)
    assert f05["effectiveness"]["historical_f01_evidence_rewritten"] is False
    assert f05["capa"]["early_warning_headroom_ratio"] == 0.20
