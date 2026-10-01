from datetime import datetime, timedelta, timezone

import pytest

from scanner.research.governance.qm_d_dependence import (
    DependenceAuditError,
    audit_dependence,
    load_qm_de_contract,
)
from scanner.research.governance.qm_e_calibration import (
    CalibrationAuditError,
    audit_calibration,
    audit_phase2_bridge,
)
from scanner.research.governance.qm_i_lineage import LineageRegistry, content_hash


class FakePlanRegistry:
    def __init__(self, plan):
        self.plan = dict(plan)

    def get_plan(self, plan_id, version):
        assert plan_id == self.plan["analysis_plan_id"]
        assert version == self.plan["analysis_plan_version"]
        return dict(self.plan)


class FakeResultRegistry:
    def __init__(self, result):
        self.result = dict(result)

    def get_result(self, result_id, version):
        assert result_id == self.result["result_id"]
        assert version == self.result["result_version"]
        return dict(self.result)


def _lineage(tmp_path):
    registry = LineageRegistry(tmp_path / "lineage.jsonl")
    registry.register_node(
        record={
            "node_id": "CAL-PRED-001",
            "version_id": "v1",
            "node_type": "CALIBRATION",
            "content_hash": content_hash({"calibration": "prediction-source"}),
            "lineage_complete": True,
            "as_of": "2026-09-01T17:00:00Z",
            "metadata": {"purpose": "qm-de-test"},
        },
        actor_id="tester",
        actor_role="researcher",
    )
    return registry


def _registries(tmp_path):
    lineage = _lineage(tmp_path)
    plan = {
        "analysis_plan_id": "AP-1",
        "analysis_plan_version": "v1",
        "analysis_plan_hash": "plan-hash-1",
        "freeze_context": {"dataset_snapshot_hash": "dataset-hash-1"},
    }
    result = {
        "result_id": "R-1",
        "result_version": "v1",
        "result_hash": "result-hash-1",
        "analysis_plan_id": "AP-1",
        "analysis_plan_version": "v1",
        "analysis_plan_hash": "plan-hash-1",
    }
    return FakePlanRegistry(plan), FakeResultRegistry(result), lineage


def _identity(lineage, **overrides):
    payload = {
        "audit_id": "AUDIT-DE-001",
        "audit_version": "v1",
        "audit_as_of": "2026-10-01T00:00:00Z",
        "analysis_plan_id": "AP-1",
        "analysis_plan_version": "v1",
        "analysis_plan_hash": "plan-hash-1",
        "result_id": "R-1",
        "result_version": "v1",
        "result_hash": "result-hash-1",
        "dataset_snapshot_hash": "dataset-hash-1",
        "lineage_registry_head_hash": lineage.verify_integrity()["head_hash"],
        "prediction_definition_hash": "prediction-definition-hash",
        "label_definition_hash": "label-definition-hash",
    }
    payload.update(overrides)
    return payload


def _observations():
    start = datetime(2026, 9, 1, 17, tzinfo=timezone.utc)
    rows = []
    for i in range(12):
        symbol = "A" if i < 6 else "B"
        observed = start + timedelta(days=i)
        rows.append({
            "observation_id": f"O-{i}",
            "symbol": symbol,
            "observed_at": observed.isoformat().replace("+00:00", "Z"),
            "value": float(i if symbol == "A" else 12 - i),
            "interval_start": observed.isoformat().replace("+00:00", "Z"),
            "interval_end": (observed + timedelta(days=5)).isoformat().replace("+00:00", "Z"),
            "sector": "TECH" if symbol == "A" else "INDUSTRIAL",
            "time_block": "EARLY" if i < 6 else "LATE",
            "lineage_node_id": "CAL-PRED-001",
            "lineage_version_id": "v1",
        })
    return rows


def _prediction_pairs():
    start = datetime(2026, 8, 1, 17, tzinfo=timezone.utc)
    probabilities = [0.1, 0.3, 0.5, 0.7, 0.9]
    positive_counts = {0.1: 1, 0.3: 2, 0.5: 4, 0.7: 6, 0.9: 7}
    rows = []
    index = 0
    for probability in probabilities:
        for repetition in range(8):
            prediction_time = start + timedelta(days=index)
            rows.append({
                "prediction_id": f"P-{index}",
                "predicted_probability": probability,
                "outcome": 1 if repetition < positive_counts[probability] else 0,
                "prediction_as_of": prediction_time.isoformat().replace("+00:00", "Z"),
                "outcome_available_at": (prediction_time + timedelta(days=10)).isoformat().replace("+00:00", "Z"),
                "subgroup": "G1" if index < 20 else "G2",
                "time_block": "AUG" if index < 24 else "SEP",
                "lineage_node_id": "CAL-PRED-001",
                "lineage_version_id": "v1",
            })
            index += 1
    return rows


def test_contract_keeps_dependence_and_calibration_separate_and_research_only():
    contract = load_qm_de_contract()
    assert contract["business_area"] == "BA-QM4"
    assert contract["qm_axes"] == ["QM-D", "QM-E"]
    assert contract["research_only"] is True
    assert contract["productive_integration_enabled"] is False
    assert contract["principles"]["raw_n_is_not_effective_n"] is True
    assert contract["principles"]["calibration_requires_point_in_time_prediction_outcome_pairs"] is True
    assert contract["boundaries"]["probability_or_confidence_retrained"] is False


def test_qm_d_reports_multiple_effective_n_diagnostics_without_selecting_one(tmp_path):
    plans, results, lineage = _registries(tmp_path)
    report = audit_dependence(
        _observations(),
        identity=_identity(lineage),
        analysis_plans=plans,
        results=results,
        lineage=lineage,
        bootstrap_reps=50,
        random_seed=7,
    )
    assert report["status"] == "COMPLETE"
    assert report["N_raw"] == 12
    assert report["single_universal_N_eff_selected"] is False
    diagnostics = report["effective_n_diagnostics"]
    assert diagnostics["RAW_N"]["N_eff"] == 12.0
    assert diagnostics["OVERLAP_CONCURRENCY_PROXY"]["N_eff"] < 12.0
    assert diagnostics["SYMBOL_CLUSTER_CONCENTRATION"]["N_eff"] == pytest.approx(2.0)
    assert diagnostics["POOLED_WITHIN_SYMBOL_AR1"]["status"] == "AVAILABLE"
    assert report["robustness"]["LEAVE_ONE_SYMBOL_OUT"]["status"] == "AVAILABLE"
    assert report["robustness"]["SYMBOL_CLUSTER_BOOTSTRAP"]["interval_95"] is not None
    assert report["productive_change_performed"] is False


def test_qm_d_missing_cluster_metadata_is_unknown_not_neutral(tmp_path):
    plans, results, lineage = _registries(tmp_path)
    rows = _observations()
    rows[0]["sector"] = None
    report = audit_dependence(
        rows,
        identity=_identity(lineage),
        analysis_plans=plans,
        results=results,
        lineage=lineage,
        bootstrap_reps=20,
    )
    assert report["status"] == "COMPLETE_WITH_UNKNOWN_COMPONENTS"
    assert report["effective_n_diagnostics"]["SECTOR_CLUSTER_CONCENTRATION"]["status"] == "UNKNOWN_MISSING_CLUSTER_METADATA"
    assert report["robustness"]["LEAVE_ONE_SECTOR_OUT"]["status"] == "UNKNOWN_MISSING_CLUSTER_METADATA"


def test_qm_d_fails_closed_on_unregistered_lineage_node(tmp_path):
    plans, results, lineage = _registries(tmp_path)
    rows = _observations()
    rows[0]["lineage_node_id"] = "MISSING"
    with pytest.raises(Exception, match="lineage_node_not_registered"):
        audit_dependence(
            rows,
            identity=_identity(lineage),
            analysis_plans=plans,
            results=results,
            lineage=lineage,
            bootstrap_reps=10,
        )


def test_qm_e_computes_brier_logloss_slope_intercept_and_reliability_bins(tmp_path):
    plans, results, lineage = _registries(tmp_path)
    report = audit_calibration(
        _prediction_pairs(),
        identity=_identity(lineage),
        analysis_plans=plans,
        results=results,
        lineage=lineage,
    )
    assert report["status"] == "COMPLETE"
    assert report["pair_count"] == 40
    overall = report["overall"]
    assert 0.0 <= overall["BRIER_SCORE"] <= 1.0
    assert overall["LOG_LOSS"] > 0.0
    assert overall["CALIBRATION_REGRESSION"]["status"] == "AVAILABLE"
    assert overall["CALIBRATION_REGRESSION"]["intercept"] is not None
    assert overall["CALIBRATION_REGRESSION"]["slope"] is not None
    assert sum(row["N"] for row in overall["RELIABILITY_BINS"]) == 40
    assert report["automatic_recalibration_performed"] is False
    assert report["productive_probability_update_performed"] is False


def test_qm_e_requires_outcome_maturity_by_explicit_audit_as_of(tmp_path):
    plans, results, lineage = _registries(tmp_path)
    rows = _prediction_pairs()
    rows[-1]["outcome_available_at"] = "2026-10-02T00:00:00Z"
    with pytest.raises(CalibrationAuditError, match="outcome_not_available_by_audit_as_of"):
        audit_calibration(
            rows,
            identity=_identity(lineage),
            analysis_plans=plans,
            results=results,
            lineage=lineage,
        )


def test_qm_e_rejects_result_plan_identity_mismatch(tmp_path):
    plans, results, lineage = _registries(tmp_path)
    results.result["analysis_plan_hash"] = "other-plan-hash"
    with pytest.raises(CalibrationAuditError, match="audit_identity_result_plan_binding_mismatch:analysis_plan_hash"):
        audit_calibration(
            _prediction_pairs(),
            identity=_identity(lineage),
            analysis_plans=plans,
            results=results,
            lineage=lineage,
        )


def test_qm_e_keeps_small_subgroups_insufficient_instead_of_inventing_calibration(tmp_path):
    plans, results, lineage = _registries(tmp_path)
    rows = _prediction_pairs()
    for index, row in enumerate(rows):
        row["subgroup"] = "SMALL" if index < 3 else "LARGE"
    report = audit_calibration(
        rows,
        identity=_identity(lineage),
        analysis_plans=plans,
        results=results,
        lineage=lineage,
    )
    groups = {row["group"]: row for row in report["subgroups"]["groups"]}
    assert groups["SMALL"]["status"] == "INSUFFICIENT_ROWS"
    assert groups["SMALL"]["metrics"] is None
    assert groups["LARGE"]["status"] == "AVAILABLE"


def test_existing_phase2_aggregate_report_is_not_relabelled_as_row_level_qm_e_evidence():
    report = {
        "horizons": {
            "20": {
                "bootstrap": {
                    "method": "circular_moving_observation_date_blocks",
                    "block_length_sessions": 40,
                }
            }
        },
        "semantics": {"iid_intervals_and_pvalues_are_diagnostics_only": True},
    }
    bridge = audit_phase2_bridge(report)
    assert bridge["recognized_block_method_present"] is True
    assert bridge["row_level_metrics_computed"] is False
    assert bridge["aggregate_report_promoted_to_row_level_pairs"] is False
    assert bridge["status"] == "INSUFFICIENT_ROW_LEVEL_PREDICTION_OUTCOME_PAIRS"
