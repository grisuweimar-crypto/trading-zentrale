from datetime import datetime, timedelta, timezone

import pytest

from scanner.research.governance.qm_d_dependence import DependenceAuditError, audit_dependence
from scanner.research.governance.qm_i_lineage import LineageRegistry, content_hash


class PlanRegistry:
    def __init__(self):
        self.plan = {
            "analysis_plan_id": "AP-1",
            "analysis_plan_version": "v1",
            "analysis_plan_hash": "plan-hash-1",
            "freeze_context": {"dataset_snapshot_hash": "dataset-hash-1"},
        }

    def get_plan(self, plan_id, version):
        assert (plan_id, version) == ("AP-1", "v1")
        return dict(self.plan)


class ResultRegistry:
    def __init__(self):
        self.result = {
            "result_id": "R-1",
            "result_version": "v1",
            "result_hash": "result-hash-1",
            "analysis_plan_id": "AP-1",
            "analysis_plan_version": "v1",
            "analysis_plan_hash": "plan-hash-1",
        }

    def get_result(self, result_id, version):
        assert (result_id, version) == ("R-1", "v1")
        return dict(self.result)


def setup(tmp_path):
    lineage = LineageRegistry(tmp_path / "lineage.jsonl")
    lineage.register_node(
        record={
            "node_id": "CAL-1",
            "version_id": "v1",
            "node_type": "CALIBRATION",
            "content_hash": content_hash({"calibration": "qm-d-identity-test"}),
            "lineage_complete": True,
            "as_of": "2026-09-01T17:00:00Z",
            "metadata": {},
        },
        actor_id="tester",
        actor_role="researcher",
    )
    plans = PlanRegistry()
    results = ResultRegistry()
    identity = {
        "audit_id": "AUDIT-D-ID",
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
    }
    start = datetime(2026, 9, 1, 17, tzinfo=timezone.utc)
    observations = []
    for i in range(8):
        timestamp = start + timedelta(days=i)
        observations.append({
            "observation_id": f"O-{i}",
            "symbol": "A" if i < 4 else "B",
            "observed_at": timestamp.isoformat().replace("+00:00", "Z"),
            "value": float(i + 1),
            "interval_start": timestamp.isoformat().replace("+00:00", "Z"),
            "interval_end": (timestamp + timedelta(days=5)).isoformat().replace("+00:00", "Z"),
            "sector": "S1" if i < 4 else "S2",
            "time_block": "T1" if i < 4 else "T2",
            "lineage_node_id": "CAL-1",
            "lineage_version_id": "v1",
        })
    return plans, results, lineage, identity, observations


def test_qm_d_rejects_result_bound_to_other_analysis_plan(tmp_path):
    plans, results, lineage, identity, observations = setup(tmp_path)
    results.result["analysis_plan_hash"] = "different-plan-hash"
    with pytest.raises(DependenceAuditError, match="audit_identity_result_plan_binding_mismatch:analysis_plan_hash"):
        audit_dependence(
            observations,
            identity=identity,
            analysis_plans=plans,
            results=results,
            lineage=lineage,
            bootstrap_reps=10,
        )


def test_qm_d_rejects_dataset_snapshot_different_from_frozen_plan(tmp_path):
    plans, results, lineage, identity, observations = setup(tmp_path)
    identity["dataset_snapshot_hash"] = "other-dataset"
    with pytest.raises(DependenceAuditError, match="audit_identity_dataset_snapshot_hash_mismatch"):
        audit_dependence(
            observations,
            identity=identity,
            analysis_plans=plans,
            results=results,
            lineage=lineage,
            bootstrap_reps=10,
        )
