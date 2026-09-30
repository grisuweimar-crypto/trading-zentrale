import json
from pathlib import Path

import pytest

from scanner.research.governance.qm_a import GovernanceLedger
from scanner.research.governance.qm_c import HypothesisRegistry
from scanner.research.governance.qm_c_analysis_plan import AnalysisPlanRegistry
from scanner.research.governance.qm_c_multiplicity import MultiplicityMonitoringRegistry
from scanner.research.governance.qm_c_results import (
    ResultAuditError,
    ResultRegistry,
    audit_qm_c_system,
    detect_duplicate_hypotheses,
    load_qm_c_results_contract,
)


QM_B_CLOSURE = Path("configs/qm_b_closure_v1.json")


def closure():
    return json.loads(QM_B_CLOSURE.read_text(encoding="utf-8"))


def registries(tmp_path):
    return (
        HypothesisRegistry(tmp_path / "hypotheses.jsonl"),
        AnalysisPlanRegistry(tmp_path / "plans.jsonl"),
        GovernanceLedger(tmp_path / "qm_a.jsonl"),
        MultiplicityMonitoringRegistry(tmp_path / "controls.jsonl"),
        ResultRegistry(tmp_path / "results.jsonl"),
    )


def hypothesis_record(idx: int, *, statement_suffix: str = ""):
    return {
        "hypothesis_id": f"H-C4-{idx:03d}",
        "hypothesis_version": "v1",
        "research_question": f"Does predeclared C4 pattern {idx} improve forward alpha?",
        "hypothesis_statement": f"C4 pattern {idx} has positive forward alpha versus benchmark.{statement_suffix}",
        "hypothesis_family_id": f"HF-C4-{idx:03d}",
        "research_mode": "CONFIRMATION",
        "qm_a_analysis_id": f"A-C4-{idx:03d}",
        "universe_requirement": "OBSERVED_SCANNER_UNIVERSE",
    }


def plan_record(hypothesis, idx: int):
    return {
        "analysis_plan_id": f"AP-C4-{idx:03d}",
        "analysis_plan_version": "v1",
        "hypothesis_id": hypothesis["hypothesis_id"],
        "hypothesis_version": hypothesis["hypothesis_version"],
        "hypothesis_version_hash": hypothesis["hypothesis_version_hash"],
        "research_mode": "CONFIRMATION",
        "primary_estimand": "Mean 20-session forward alpha versus benchmark.",
        "primary_metrics": ["mean_forward_alpha_20s"],
        "population_definition": "Predeclared eligible scanner observations.",
        "universe_requirement": hypothesis["universe_requirement"],
        "evaluation_windows": ["20_sessions"],
        "exclusion_rules": ["missing_start_price", "missing_forward_outcome"],
        "outcome_definitions": ["forward_total_return_minus_benchmark_total_return"],
        "planned_sensitivity_analyses": ["10_sessions", "40_sessions"],
    }


def freeze_context(idx: int):
    return {
        "code_or_commit_hash": "commit-c4-test",
        "config_hash": f"config-c4-{idx}",
        "dataset_snapshot_hash": f"dataset-c4-{idx}",
        "universe_ledger_version": "qm-b-ledger-v1",
        "instrument_master_version": "qm-b-instrument-master-v1",
        "label_definition_hash": "label-c4-v1",
        "benchmark_definition_hash": "benchmark-c4-v1",
        "environment_or_dependency_fingerprint": "python311-pytest",
        "evaluation_cohort_id": f"cohort-c4-{idx}",
        "temporal_boundary": "2026-09-30",
    }


def build_confirmatory_chain(tmp_path, idx: int = 1):
    hypotheses, plans, qm_a, controls, results = registries(tmp_path)
    hypotheses.register_hypothesis(
        record=hypothesis_record(idx), actor_id="tester", actor_role="researcher"
    )
    hypothesis = hypotheses.get_hypothesis(f"H-C4-{idx:03d}", "v1")
    plan_id = f"AP-C4-{idx:03d}"
    plans.register_plan(
        record=plan_record(hypothesis, idx), hypothesis_registry=hypotheses,
        actor_id="tester", actor_role="researcher"
    )
    plans.freeze_plan(
        analysis_plan_id=plan_id, analysis_plan_version="v1",
        freeze_context=freeze_context(idx), hypothesis_registry=hypotheses,
        qm_b_closure=closure(), actor_id="tester", actor_role="researcher",
        reason="Freeze C2 plan before confirmation."
    )
    qm_a_version = f"analysis-v{idx}"
    qm_a.register_analysis(
        analysis_id=hypothesis["qm_a_analysis_id"], version_id=qm_a_version,
        actor_id="tester", actor_role="researcher"
    )
    qm_a.transition(
        analysis_id=hypothesis["qm_a_analysis_id"], version_id=qm_a_version,
        to_state="FROZEN_FOR_CONFIRMATION", actor_id="tester", actor_role="researcher",
        reason="Freeze exact C2 identity.", analysis_identity=plans.build_qm_a_identity(plan_id, "v1")
    )
    hypotheses.transition(
        hypothesis_id=hypothesis["hypothesis_id"], hypothesis_version="v1",
        to_state="FROZEN_FOR_CONFIRMATION", actor_id="tester", actor_role="researcher",
        reason="Freeze hypothesis after C2 and QM-A."
    )
    plan = plans.get_plan(plan_id, "v1")
    member = {
        "hypothesis_id": hypothesis["hypothesis_id"],
        "hypothesis_version": hypothesis["hypothesis_version"],
        "hypothesis_version_hash": hypothesis["hypothesis_version_hash"],
        "analysis_plan_id": plan["analysis_plan_id"],
        "analysis_plan_version": plan["analysis_plan_version"],
        "analysis_plan_hash": plan["analysis_plan_hash"],
        "qm_a_analysis_id": hypothesis["qm_a_analysis_id"],
        "qm_a_version_id": qm_a_version,
    }
    control_id = f"CP-C4-{idx:03d}"
    controls.register_control_plan(
        record={
            "control_plan_id": control_id,
            "control_plan_version": "v1",
            "hypothesis_family_id": hypothesis["hypothesis_family_id"],
            "research_mode": "CONFIRMATION",
            "family_members": [member],
            "multiplicity_strategy": "PREDECLARED_SINGLE_PRIMARY",
            "multiplicity_parameters": {},
            "sequential_monitoring": {
                "mode": "FIXED_HORIZON_NO_INTERIM",
                "planned_looks": [{"look_id": "FINAL", "information_fraction": 1.0}],
                "early_stop_allowed": False,
                "stopping_rule": "FINAL_ONLY",
            },
        },
        hypothesis_registry=hypotheses, analysis_plan_registry=plans,
        actor_id="tester", actor_role="researcher"
    )
    controls.freeze_control_plan(
        control_plan_id=control_id, control_plan_version="v1",
        hypothesis_registry=hypotheses, analysis_plan_registry=plans,
        qm_a_ledger=qm_a, qm_b_closure=closure(), actor_id="tester", actor_role="researcher",
        reason="Freeze C3 family/control before outcome inspection."
    )
    controls.record_monitoring_look(
        control_plan_id=control_id, control_plan_version="v1", look_id="FINAL",
        artifact_hash=f"monitoring-artifact-{idx}", decision="FINAL_COMPLETE",
        qm_a_ledger=qm_a, actor_id="tester", actor_role="researcher"
    )
    control = controls.get_control_plan(control_id, "v1")
    return {
        "hypotheses": hypotheses,
        "plans": plans,
        "qm_a": qm_a,
        "controls": controls,
        "results": results,
        "hypothesis": hypothesis,
        "plan": plan,
        "control": control,
        "member": member,
    }


def confirmatory_result(chain, *, classification="NEGATIVE", result_id="R-C4-001", result_version="v1"):
    h = chain["hypothesis"]
    p = chain["plan"]
    c = chain["control"]
    m = chain["member"]
    return {
        "result_id": result_id,
        "result_version": result_version,
        "hypothesis_id": h["hypothesis_id"],
        "hypothesis_version": h["hypothesis_version"],
        "hypothesis_version_hash": h["hypothesis_version_hash"],
        "evidence_scope": "CONFIRMATORY",
        "outcome_classification": classification,
        "conclusion": f"Predeclared confirmatory result classified as {classification.lower()}.",
        "evidence_artifact_hash": "result-artifact-c4",
        "analysis_plan_id": p["analysis_plan_id"],
        "analysis_plan_version": p["analysis_plan_version"],
        "analysis_plan_hash": p["analysis_plan_hash"],
        "control_plan_id": c["control_plan_id"],
        "control_plan_version": c["control_plan_version"],
        "control_plan_hash": c["control_plan_hash"],
        "qm_a_analysis_id": m["qm_a_analysis_id"],
        "qm_a_version_id": m["qm_a_version_id"],
    }


def register_confirmatory_result(chain, record, **kwargs):
    return chain["results"].register_result(
        record=record,
        hypothesis_registry=chain["hypotheses"],
        analysis_plan_registry=chain["plans"],
        control_registry=chain["controls"],
        qm_a_ledger=chain["qm_a"],
        actor_id="tester",
        actor_role="researcher",
        **kwargs,
    )


def mark_evidence_consumed(chain):
    member = chain["member"]
    chain["qm_a"].transition(
        analysis_id=member["qm_a_analysis_id"], version_id=member["qm_a_version_id"],
        to_state="CONFIRMATORY_EVALUATED", actor_id="tester", actor_role="researcher",
        reason="Predeclared confirmatory outcome was evaluated."
    )


def test_contract_retains_results_symmetrically_and_stays_research_only():
    contract = load_qm_c_results_contract()
    assert contract["research_only"] is True
    assert contract["productive_integration_enabled"] is False
    assert contract["principles"]["negative_results_are_retained"] is True
    assert contract["principles"]["positive_results_do_not_receive_special_retention"] is True
    assert contract["boundaries"]["fuzzy_duplicate_merging_performed"] is False


@pytest.mark.parametrize("classification", ["POSITIVE", "NEGATIVE", "INCONCLUSIVE"])
def test_confirmatory_outcomes_use_same_exact_binding_rules(tmp_path, classification):
    chain = build_confirmatory_chain(tmp_path / classification.lower())
    with pytest.raises(ResultAuditError, match="confirmatory_result_requires_consumed_qm_a_evidence"):
        register_confirmatory_result(chain, confirmatory_result(chain, classification=classification))
    mark_evidence_consumed(chain)
    register_confirmatory_result(chain, confirmatory_result(chain, classification=classification))
    saved = chain["results"].get_result("R-C4-001", "v1")
    assert saved["outcome_classification"] == classification
    assert saved["result_hash"]


def test_negative_result_is_retained_in_integrity_counts(tmp_path):
    chain = build_confirmatory_chain(tmp_path)
    mark_evidence_consumed(chain)
    register_confirmatory_result(chain, confirmatory_result(chain, classification="NEGATIVE"))
    report = chain["results"].verify_integrity()
    assert report["valid"] is True
    assert report["classification_counts"] == {"NEGATIVE": 1}


def test_confirmatory_result_requires_exact_control_member_hashes(tmp_path):
    chain = build_confirmatory_chain(tmp_path)
    mark_evidence_consumed(chain)
    record = confirmatory_result(chain)
    record["analysis_plan_hash"] = "wrong-plan-hash"
    with pytest.raises(ResultAuditError, match="confirmatory_result_plan_binding_mismatch:analysis_plan_hash"):
        register_confirmatory_result(chain, record)


def test_rejected_pre_evaluation_hypothesis_is_retained_without_invented_evidence(tmp_path):
    hypotheses, plans, qm_a, controls, results = registries(tmp_path)
    hypotheses.register_hypothesis(
        record=hypothesis_record(1), actor_id="tester", actor_role="researcher"
    )
    hypothesis = hypotheses.get_hypothesis("H-C4-001", "v1")
    hypotheses.transition(
        hypothesis_id="H-C4-001", hypothesis_version="v1", to_state="REJECTED",
        actor_id="tester", actor_role="researcher", reason="Rejected before outcome inspection."
    )
    record = {
        "result_id": "R-REJECT-001",
        "result_version": "v1",
        "hypothesis_id": hypothesis["hypothesis_id"],
        "hypothesis_version": hypothesis["hypothesis_version"],
        "hypothesis_version_hash": hypothesis["hypothesis_version_hash"],
        "evidence_scope": "NO_OUTCOME_EVIDENCE",
        "outcome_classification": "REJECTED_PRE_EVALUATION",
        "conclusion": "Hypothesis rejected before outcome evidence was inspected.",
        "evidence_artifact_hash": None,
        "analysis_plan_id": None,
        "analysis_plan_version": None,
        "analysis_plan_hash": None,
        "control_plan_id": None,
        "control_plan_version": None,
        "control_plan_hash": None,
        "qm_a_analysis_id": None,
        "qm_a_version_id": None,
    }
    results.register_result(
        record=record, hypothesis_registry=hypotheses, analysis_plan_registry=plans,
        control_registry=controls, qm_a_ledger=qm_a, actor_id="tester", actor_role="researcher"
    )
    saved = results.get_result("R-REJECT-001", "v1")
    assert saved["evidence_scope"] == "NO_OUTCOME_EVIDENCE"
    assert saved["evidence_artifact_hash"] is None

    bad = dict(record)
    bad["result_id"] = "R-REJECT-BAD"
    bad["evidence_artifact_hash"] = "invented-evidence"
    with pytest.raises(ResultAuditError, match="no_outcome_scope_forbids_bindings"):
        results.register_result(
            record=bad, hypothesis_registry=hypotheses, analysis_plan_registry=plans,
            control_registry=controls, qm_a_ledger=qm_a, actor_id="tester", actor_role="researcher"
        )


def test_result_version_is_immutable_and_successor_explicit(tmp_path):
    chain = build_confirmatory_chain(tmp_path)
    mark_evidence_consumed(chain)
    record = confirmatory_result(chain)
    register_confirmatory_result(chain, record)
    with pytest.raises(ResultAuditError, match="result_version_already_registered"):
        register_confirmatory_result(chain, {**record, "conclusion": "Changed in place."})
    successor = {**record, "result_version": "v2", "conclusion": "Corrected successor result."}
    with pytest.raises(ResultAuditError, match="result_successor_requires_supersedes_reference"):
        register_confirmatory_result(chain, successor)
    register_confirmatory_result(chain, successor, supersedes_result_version="v1")
    assert chain["results"].get_result("R-C4-001", "v2")["supersedes_result_version"] == "v1"


def test_duplicate_audit_detects_cross_id_exact_semantic_duplicate_without_merging(tmp_path):
    hypotheses = HypothesisRegistry(tmp_path / "hypotheses.jsonl")
    first = hypothesis_record(1)
    hypotheses.register_hypothesis(record=first, actor_id="tester", actor_role="researcher")
    second = {
        **first,
        "hypothesis_id": "H-DUPLICATE-999",
        "qm_a_analysis_id": "A-DUPLICATE-999",
        "hypothesis_family_id": "HF-DIFFERENT",
    }
    hypotheses.register_hypothesis(record=second, actor_id="tester", actor_role="researcher")
    findings = detect_duplicate_hypotheses(hypotheses)
    assert len(findings) == 1
    assert findings[0]["classification"] == "POTENTIAL_EXACT_SEMANTIC_DUPLICATE"
    assert findings[0]["automatic_merge_performed"] is False
    assert findings[0]["hypothesis_ids"] == ["H-C4-001", "H-DUPLICATE-999"]


def test_duplicate_audit_does_not_flag_versions_of_same_hypothesis_id(tmp_path):
    hypotheses = HypothesisRegistry(tmp_path / "hypotheses.jsonl")
    first = hypothesis_record(1)
    hypotheses.register_hypothesis(record=first, actor_id="tester", actor_role="researcher")
    second = {**first, "hypothesis_version": "v2"}
    hypotheses.register_hypothesis(
        record=second, actor_id="tester", actor_role="researcher", supersedes_hypothesis_version="v1"
    )
    assert detect_duplicate_hypotheses(hypotheses) == []


def test_full_c1_to_c4_system_audit_passes_on_real_integrated_negative_path(tmp_path):
    chain = build_confirmatory_chain(tmp_path)
    mark_evidence_consumed(chain)
    register_confirmatory_result(chain, confirmatory_result(chain, classification="NEGATIVE"))
    audit = audit_qm_c_system(
        hypothesis_registry=chain["hypotheses"], analysis_plan_registry=chain["plans"],
        control_registry=chain["controls"], result_registry=chain["results"]
    )
    assert audit["status"] == "PASS"
    assert audit["cross_reference_errors"] == []
    assert audit["counts"] == {
        "hypothesis_versions": 1,
        "analysis_plan_versions": 1,
        "control_plan_versions": 1,
        "result_versions": 1,
        "duplicate_findings": 0,
    }
    assert all(audit["integrity"][layer]["valid"] for layer in ("QM-C1", "QM-C2", "QM-C3", "QM-C4"))


def test_tampered_result_registry_fails_closed(tmp_path):
    chain = build_confirmatory_chain(tmp_path)
    mark_evidence_consumed(chain)
    register_confirmatory_result(chain, confirmatory_result(chain))
    path = tmp_path / "results.jsonl"
    line = json.loads(path.read_text(encoding="utf-8"))
    line["payload"]["record"]["outcome_classification"] = "POSITIVE"
    path.write_text(json.dumps(line) + "\n", encoding="utf-8")
    with pytest.raises(ResultAuditError, match="result_registry_entry_hash_invalid"):
        chain["results"].verify_integrity()
