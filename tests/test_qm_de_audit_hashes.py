from copy import deepcopy
from datetime import datetime, timedelta, timezone

from scanner.research.governance.qm_d_dependence import audit_dependence
from scanner.research.governance.qm_e_calibration import audit_calibration
from scanner.research.governance.qm_i_lineage import LineageRegistry, content_hash


class Plans:
    def get_plan(self, plan_id, version):
        assert (plan_id, version) == ("AP-1", "v1")
        return {"analysis_plan_id": "AP-1", "analysis_plan_version": "v1", "analysis_plan_hash": "plan-hash", "freeze_context": {"dataset_snapshot_hash": "dataset-hash"}}


class Results:
    def get_result(self, result_id, version):
        assert (result_id, version) == ("R-1", "v1")
        return {"result_id": "R-1", "result_version": "v1", "result_hash": "result-hash", "analysis_plan_id": "AP-1", "analysis_plan_version": "v1", "analysis_plan_hash": "plan-hash"}


def setup(tmp_path):
    lineage = LineageRegistry(tmp_path / "lineage.jsonl")
    lineage.register_node(
        record={"node_id": "CAL-1", "version_id": "v1", "node_type": "CALIBRATION", "content_hash": content_hash({"source": "hash-test"}), "lineage_complete": True, "as_of": "2026-09-01T00:00:00Z", "metadata": {}},
        actor_id="tester", actor_role="researcher",
    )
    identity = {
        "audit_id": "AUDIT-1", "audit_version": "v1", "audit_as_of": "2026-10-01T00:00:00Z",
        "analysis_plan_id": "AP-1", "analysis_plan_version": "v1", "analysis_plan_hash": "plan-hash",
        "result_id": "R-1", "result_version": "v1", "result_hash": "result-hash",
        "dataset_snapshot_hash": "dataset-hash", "lineage_registry_head_hash": lineage.verify_integrity()["head_hash"],
    }
    return Plans(), Results(), lineage, identity


def observations():
    start = datetime(2026, 9, 1, tzinfo=timezone.utc)
    rows = []
    for i in range(8):
        observed = start + timedelta(days=i)
        rows.append({
            "observation_id": f"O-{i}", "symbol": "A" if i < 4 else "B", "observed_at": observed.isoformat(),
            "value": float(i + 1), "interval_start": observed.isoformat(), "interval_end": (observed + timedelta(days=5)).isoformat(),
            "sector": "S1" if i < 4 else "S2", "time_block": "T1" if i < 4 else "T2",
            "lineage_node_id": "CAL-1", "lineage_version_id": "v1",
        })
    return rows


def pairs():
    start = datetime(2026, 8, 1, tzinfo=timezone.utc)
    rows = []
    for i in range(24):
        prediction = start + timedelta(days=i)
        rows.append({
            "prediction_id": f"P-{i}", "predicted_probability": 0.25 + 0.5 * (i % 4) / 3.0, "outcome": i % 2,
            "prediction_as_of": prediction.isoformat(), "outcome_available_at": (prediction + timedelta(days=7)).isoformat(),
            "subgroup": "G1" if i < 12 else "G2", "time_block": "AUG",
            "lineage_node_id": "CAL-1", "lineage_version_id": "v1",
        })
    return rows


def test_qm_d_audit_hash_commits_values_and_bootstrap_parameters(tmp_path):
    plans, results, lineage, identity = setup(tmp_path)
    rows = observations()
    first = audit_dependence(rows, identity=identity, analysis_plans=plans, results=results, lineage=lineage, bootstrap_reps=20, random_seed=7)
    changed_rows = deepcopy(rows)
    changed_rows[0]["value"] = 999.0
    second = audit_dependence(changed_rows, identity=identity, analysis_plans=plans, results=results, lineage=lineage, bootstrap_reps=20, random_seed=7)
    third = audit_dependence(rows, identity=identity, analysis_plans=plans, results=results, lineage=lineage, bootstrap_reps=20, random_seed=8)
    fourth = audit_dependence(rows, identity=identity, analysis_plans=plans, results=results, lineage=lineage, bootstrap_reps=21, random_seed=7)
    assert len({first["audit_hash"], second["audit_hash"], third["audit_hash"], fourth["audit_hash"]}) == 4


def test_qm_e_audit_hash_commits_pairs_and_reliability_bins(tmp_path):
    plans, results, lineage, identity = setup(tmp_path)
    identity = {**identity, "prediction_definition_hash": "pred-def", "label_definition_hash": "label-def"}
    rows = pairs()
    first = audit_calibration(rows, identity=identity, analysis_plans=plans, results=results, lineage=lineage)
    changed_probability = deepcopy(rows)
    changed_probability[0]["predicted_probability"] = 0.99
    second = audit_calibration(changed_probability, identity=identity, analysis_plans=plans, results=results, lineage=lineage)
    changed_outcome = deepcopy(rows)
    changed_outcome[0]["outcome"] = 1 - changed_outcome[0]["outcome"]
    third = audit_calibration(changed_outcome, identity=identity, analysis_plans=plans, results=results, lineage=lineage)
    fourth = audit_calibration(rows, identity=identity, analysis_plans=plans, results=results, lineage=lineage, reliability_bins=[0.0, 0.5, 1.0])
    assert len({first["audit_hash"], second["audit_hash"], third["audit_hash"], fourth["audit_hash"]}) == 4
