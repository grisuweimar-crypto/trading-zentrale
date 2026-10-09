from copy import deepcopy
import json
from pathlib import Path

import pytest

from scanner.research.governance import ba_qm12_continuous_qm as qm12
from scanner.research.governance.ba_qm12_continuous_qm import (
    BAQM12Error,
    LIFECYCLE,
    MASTERPLAN_RESIDUAL_IDS,
    REQUIRED_CONTROLS,
    evaluate_continuous_qm,
    validate_contract,
    validate_masterplan_residual_consistency,
    _snapshot_monitor,
)

ROOT = Path(__file__).resolve().parents[1]


def _contract():
    return json.loads((ROOT / "configs" / "ba_qm12_continuous_qm_v1.json").read_text(encoding="utf-8"))


def test_ba_qm12_current_system_is_continuous_qm_active():
    receipt = evaluate_continuous_qm(ROOT)
    assert receipt["engineering_status"] == "BA_QM12_ENGINEERING_COMPLETE"
    assert receipt["operating_status"] == "ACTIVE_CONTINUOUS_QM"
    assert receipt["required_control_count"] == 9
    assert receipt["finding_lifecycle_status"] == "ACTIVE_FAIL_CLOSED"
    assert "RECURRENCE" in receipt["finding_reopen_triggers"]
    assert "FALSE_DECISION_OBSERVED" in receipt["finding_escalation_required_for"]
    assert tuple(receipt["required_controls_active"]) == REQUIRED_CONTROLS
    assert tuple(receipt["future_module_lifecycle"]) == LIFECYCLE
    assert receipt["lag1_evidence_impact"] == "PROMOTION_BLOCKED"
    assert receipt["ba_qm12_may_release_lag1_block"] is False
    assert receipt["w8_promotion_eligible"] is False
    monitor = validate_masterplan_residual_consistency(receipt["masterplan_residual_monitor"])
    assert receipt["masterplan_end_state_complete"] is monitor["masterplan_end_state_complete"]
    assert receipt["full_masterplan_completion_claim_allowed"] is monitor["full_completion_claim_allowed"]
    assert tuple(monitor["residuals"]) == MASTERPLAN_RESIDUAL_IDS
    assert monitor["residuals"]["W6_ELLIOTT_LINEAGE"]["blocking_masterplan_completion"] is False
    assert monitor["residuals"]["W6_ELLIOTT_LINEAGE"]["status"] == "PROSPECTIVE_RAW_FEATURE_VALIDATION_UNCERTAINTY_LINEAGE_COMPLETE"
    assert monitor["residuals"]["PHASE1A_LAG1_CAPA"]["prospective_plan_validated"] is True
    assert monitor["residuals"]["PHASE1A_LAG1_CAPA"]["eligible_observation_from"] == "2026-10-06"
    assert monitor["residuals"]["PHASE1A_LAG1_CAPA"]["automatic_release_allowed"] is False
    actual_blockers = [
        residual_id
        for residual_id, row in monitor["residuals"].items()
        if row["blocking_masterplan_completion"] is True
    ]
    assert monitor["blocking_residual_ids"] == actual_blockers
    assert monitor["blocking_residual_count"] == len(actual_blockers)
    assert monitor["masterplan_end_state_complete"] is (not actual_blockers)
    assert monitor["full_completion_claim_allowed"] is (not actual_blockers)
    assert monitor["residuals"]["PHASE8_EXTERNAL_EVIDENCE"]["blocking_masterplan_completion"] is False
    assert receipt["snapshot_monitor"]["drift_monitoring"]["threshold_breach_claimed"] is False


def test_future_module_stage_skipping_cannot_be_enabled():
    value = deepcopy(_contract())
    value["lifecycle_guards"]["stage_skipping_allowed"] = True
    with pytest.raises(BAQM12Error, match="lifecycle_guard_invalid"):
        validate_contract(value)


def test_continuous_qm_cannot_auto_promote():
    value = deepcopy(_contract())
    value["monitoring_policy"]["automatic_promotion_allowed"] = True
    with pytest.raises(BAQM12Error, match="automatic_promotion_forbidden"):
        validate_contract(value)


def test_drift_threshold_may_not_be_invented_after_observation():
    value = deepcopy(_contract())
    value["monitoring_policy"]["drift_thresholds_may_not_be_invented"] = False
    with pytest.raises(BAQM12Error, match="drift_threshold_guard_missing"):
        validate_contract(value)


def test_masterplan_completion_gate_cannot_be_disabled():
    value = deepcopy(_contract())
    value["masterplan_completion_gate"]["full_completion_claim_requires_zero_blocking_residuals"] = False
    with pytest.raises(BAQM12Error, match="masterplan_zero_residual_gate_missing"):
        validate_contract(value)


def test_engineering_completion_cannot_be_relabelled_full_masterplan_completion():
    value = deepcopy(_contract())
    value["masterplan_completion_gate"]["engineering_completion_is_full_masterplan_completion"] = True
    with pytest.raises(BAQM12Error, match="engineering_may_not_equal_masterplan_completion"):
        validate_contract(value)


def test_closed_finding_cannot_hide_current_failure():
    value = deepcopy(_contract())
    value["finding_lifecycle_policy"]["closed_finding_may_not_hide_current_failure"] = False
    with pytest.raises(BAQM12Error, match="finding_lifecycle_guard_missing"):
        validate_contract(value)


def test_recurrence_must_reopen_or_create_linked_finding():
    value = deepcopy(_contract())
    value["finding_lifecycle_policy"]["recurrence_requires_reopen_or_linked_new_finding"] = False
    with pytest.raises(BAQM12Error, match="finding_lifecycle_guard_missing"):
        validate_contract(value)


def test_finding_escalation_cannot_auto_promote():
    value = deepcopy(_contract())
    value["finding_lifecycle_policy"]["escalation"]["automatic_promotion_allowed"] = True
    with pytest.raises(BAQM12Error, match="finding_escalation_auto_promotion_forbidden"):
        validate_contract(value)


# BA-QM-REPAIR-01 / F02: independently mutate published snapshot metadata.
# Keep upstream validation.status == 'ok' so the BA-QM12 guard is actually exercised.
def _f02_snapshot_fixture(tmp_path, *, numeric=9, min_score_ratio=0.9):
    root = tmp_path
    research = root / "artifacts" / "research"
    (research / "watch_runtime").mkdir(parents=True, exist_ok=True)
    metadata = {
        "snapshot_id": "f02-immutable-snapshot",
        "as_of": "2026-10-08",
        "latest_run_complete": True,
        "validation": {
            "status": "ok",
            "required_symbol_count": 10,
            "symbol_count": 10,
            "numeric_score_count": numeric,
            "policy": {"min_score_ratio": min_score_ratio},
        },
        "price_coverage": {
            "required_symbol_count": 10,
            "covered_symbol_count": 9,
            "unavailable_symbol_count": 1,
        },
    }
    calibration = {"source": {"snapshot_id": "f02-immutable-snapshot", "as_of": "2026-10-08"}}
    runtime = {
        "row_count": 10,
        "diagnostics": {
            "snapshot_id": "f02-immutable-snapshot",
            "bundle_count": 10,
            "missing_current_packet_symbols": [],
            "private_position_data_persisted": False,
            "scanner_scalar_fallback_used": False,
        },
    }
    for relative, value in (
        ("history_metadata.json", metadata),
        ("probability_calibration_2.json", calibration),
        ("watch_runtime/public_long_reference.json", runtime),
    ):
        path = research / relative
        path.write_text(json.dumps(value, allow_nan=True), encoding="utf-8")
    return root, metadata


def test_f02_score_coverage_at_policy_boundary_passes(tmp_path):
    root, _ = _f02_snapshot_fixture(tmp_path, numeric=9, min_score_ratio=0.9)
    coverage = _snapshot_monitor(root)["coverage"]
    assert coverage["numeric_score_count"] == 9
    assert coverage["required_numeric_score_count"] == 9
    assert coverage["numeric_score_ratio"] == pytest.approx(0.9)
    assert coverage["min_score_ratio"] == 0.9
    assert coverage["numeric_score_coverage_status"] == "PASS"


@pytest.mark.parametrize("numeric,min_score_ratio", [
    (8, 0.9),  # Sub-threshold on a previously 'ok' scanner snapshot
    (0, 0.9),  # Complete score dropout
    (9, 0.91),  # Honor actual published policy, not a fixed 90% assumption
])
def test_f02_mutated_ok_snapshot_below_policy_fails_closed(tmp_path, numeric, min_score_ratio):
    root, metadata = _f02_snapshot_fixture(tmp_path, numeric=numeric, min_score_ratio=min_score_ratio)
    assert metadata["validation"]["status"] == "ok"
    with pytest.raises(BAQM12Error, match="scanner_numeric_score_coverage_below_threshold"):
        _snapshot_monitor(root)


@pytest.mark.parametrize("numeric", [
    None, True, "9", 9.0, -1, 11,
])
def test_f02_malformed_numeric_score_count_fails_closed(tmp_path, numeric):
    root, _ = _f02_snapshot_fixture(tmp_path, numeric=numeric)
    with pytest.raises(BAQM12Error, match="scanner_numeric_score_count_invalid"):
        _snapshot_monitor(root)


@pytest.mark.parametrize("threshold", [
    None, False, "0.9", 0, -0.1, 1.01, float("nan"), float("inf"),
])
def test_f02_missing_or_invalid_snapshot_policy_fails_closed(tmp_path, threshold):
    root, _ = _f02_snapshot_fixture(tmp_path, min_score_ratio=threshold)
    with pytest.raises(BAQM12Error, match="scanner_score_ratio_policy_invalid"):
        _snapshot_monitor(root)


def test_f02_missing_policy_is_not_assumed_to_be_ninety_percent(tmp_path):
    root, metadata = _f02_snapshot_fixture(tmp_path)
    metadata["validation"].pop("policy")
    p = root / "artifacts" / "research" / "history_metadata.json"
    p.write_text(json.dumps(metadata), encoding="utf-8")
    with pytest.raises(BAQM12Error, match="mapping_required:history_metadata.validation.policy"):
        _snapshot_monitor(root)


def test_f02_current_authoritative_snapshot_coverage_remains_pass():
    metadata = json.loads((ROOT / "artifacts" / "research" / "history_metadata.json").read_text(encoding="utf-8"))
    monitor = _snapshot_monitor(ROOT)
    coverage = monitor["coverage"]
    assert coverage["numeric_score_count"] == metadata["validation"]["numeric_score_count"]
    assert coverage["min_score_ratio"] == metadata["validation"]["policy"]["min_score_ratio"]
    assert coverage["numeric_score_coverage_status"] == "PASS"
    assert coverage["numeric_score_count"] >= coverage["required_numeric_score_count"]


# BA-QM-REPAIR-01 / F04: observable state consistency instead of a fixed
# blocker count. The tests below never rewrite genuine empirical evidence.
def _f04_current_residual_monitor():
    return deepcopy(evaluate_continuous_qm(ROOT)["masterplan_residual_monitor"])


def test_f04_actual_residual_identities_and_current_blocks_are_source_derived():
    monitor = validate_masterplan_residual_consistency(_f04_current_residual_monitor())
    assert tuple(monitor["residuals"]) == MASTERPLAN_RESIDUAL_IDS
    # Current source-derived blocks are still in force. A future evidence-based
    # reduction must not require this test to be rewritten.
    assert monitor["blocking_residual_count"] == len(monitor["blocking_residual_ids"])
    assert all(monitor["residuals"][key]["blocking_masterplan_completion"] is True
               for key in monitor["blocking_residual_ids"])
    assert monitor["residuals"]["PHASE1A_LAG1_CAPA"]["automatic_release_allowed"] is False


def test_f04_synthetic_upstream_verified_release_recomputes_fewer_blockers(monkeypatch):
    # Only a nonpersistent test fixture: a hypothetical future BA-QM6 authority
    # signals ESTABLISHED. This is NOT a real promotion and changes no evidence.
    read_original = qm12._read

    def read_synthetic(path):
        value = read_original(path)
        if path.name == "ba_qm6_qm_g_closure_v1.json":
            value = deepcopy(value)
            value["empirical_validation_status"] = "ESTABLISHED"
        return value

    qmi = read_original(ROOT / "configs" / "qm_i_evidence_lineage_v1.json")
    qmj = read_original(ROOT / "configs" / "ba_qm7_qm_j_closure_v1.json")
    w8 = read_original(ROOT / "configs" / "decision_depot_action_policy_v1.json")
    current = qm12._masterplan_residual_monitor(ROOT, qmi=qmi, qmj=qmj, w8=w8)
    monkeypatch.setattr(qm12, "_read", read_synthetic)
    hypothetical = qm12._masterplan_residual_monitor(ROOT, qmi=qmi, qmj=qmj, w8=w8)
    tested = validate_masterplan_residual_consistency(hypothetical)
    assert current["residuals"]["BA_QM6_EMPIRICAL_VALIDATION"]["blocking_masterplan_completion"] is True
    assert tested["residuals"]["BA_QM6_EMPIRICAL_VALIDATION"]["status"] == "ESTABLISHED"
    assert tested["residuals"]["BA_QM6_EMPIRICAL_VALIDATION"]["blocking_masterplan_completion"] is False
    assert tested["blocking_residual_count"] == current["blocking_residual_count"] - 1
    assert tested["masterplan_end_state_complete"] is False
    assert tested["full_completion_claim_allowed"] is False
    assert tested["residuals"]["PHASE1A_LAG1_CAPA"]["blocking_masterplan_completion"] is True


@pytest.mark.parametrize("mutation,expected_error", [
    ("count_low", "masterplan_blocking_residual_count_mismatch"),
    ("count_high", "masterplan_blocking_residual_count_mismatch"),
    ("count_bool", "masterplan_blocking_residual_count_mismatch"),
    ("missing_id", "masterplan_blocking_residual_ids_mismatch"),
    ("duplicate_id", "masterplan_blocking_residual_ids_mismatch"),
    ("unknown_id", "masterplan_blocking_residual_ids_mismatch"),
    ("id_as_tuple", "masterplan_blocking_residual_ids_mismatch"),
    ("completion_too_early", "masterplan_end_state_claim_mismatch"),
    ("full_claim_too_early", "masterplan_full_completion_claim_mismatch"),
    ("missing_residual", "masterplan_residual_identity_mismatch"),
    ("unknown_residual", "masterplan_residual_identity_mismatch"),
    ("nonboolean_block_flag", "masterplan_residual_block_flag_invalid"),
])
def test_f04_inconsistent_blocker_receipt_fails_closed(mutation, expected_error):
    monitor = _f04_current_residual_monitor()
    if mutation == "count_low":
        monitor["blocking_residual_count"] -= 1
    elif mutation == "count_high":
        monitor["blocking_residual_count"] += 1
    elif mutation == "count_bool":
        monitor["blocking_residual_count"] = True
    elif mutation == "missing_id":
        monitor["blocking_residual_ids"].pop()
    elif mutation == "duplicate_id":
        monitor["blocking_residual_ids"].append(monitor["blocking_residual_ids"][0])
    elif mutation == "unknown_id":
        monitor["blocking_residual_ids"][0] = "UNREGISTERED_RESIDUAL"
    elif mutation == "id_as_tuple":
        monitor["blocking_residual_ids"] = tuple(monitor["blocking_residual_ids"])
    elif mutation == "completion_too_early":
        monitor["masterplan_end_state_complete"] = True
    elif mutation == "full_claim_too_early":
        monitor["full_completion_claim_allowed"] = True
    elif mutation == "missing_residual":
        monitor["residuals"].pop("BA_QM6_EMPIRICAL_VALIDATION")
    elif mutation == "unknown_residual":
        monitor["residuals"]["FAKE"] = {"blocking_masterplan_completion": False}
    elif mutation == "nonboolean_block_flag":
        monitor["residuals"]["BA_QM6_EMPIRICAL_VALIDATION"]["blocking_masterplan_completion"] = 1
    else:
        raise AssertionError(mutation)
    with pytest.raises(BAQM12Error, match=expected_error):
        validate_masterplan_residual_consistency(monitor)


def test_f04_synthetic_zero_blockers_only_allows_claim_with_consistent_receipt():
    monitor = _f04_current_residual_monitor()
    # Synthetic summary-only consistency check; the real promotion authorities
    # and six production blocks remain unchanged on disk and in main.
    for row in monitor["residuals"].values():
        row["blocking_masterplan_completion"] = False
    monitor["blocking_residual_ids"] = []
    monitor["blocking_residual_count"] = 0
    monitor["masterplan_end_state_complete"] = True
    monitor["full_completion_claim_allowed"] = True
    assert validate_masterplan_residual_consistency(monitor)["masterplan_end_state_complete"] is True
    monitor["residuals"]["PHASE1A_LAG1_CAPA"]["blocking_masterplan_completion"] = True
    with pytest.raises(BAQM12Error, match="masterplan_blocking_residual_ids_mismatch"):
        validate_masterplan_residual_consistency(monitor)
