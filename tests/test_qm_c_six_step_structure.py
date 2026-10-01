import json
from pathlib import Path

import pytest

from scanner.research.governance.qm_a import GovernanceLedger
from scanner.research.governance.qm_c import HypothesisRegistry
from scanner.research.governance.qm_c_analysis_plan import AnalysisPlanRegistry
from scanner.research.governance.qm_c_families_multiplicity import (
    FamilyMultiplicityError,
    FamilyMultiplicityRegistry,
)
from scanner.research.governance.qm_c_sequential_monitoring import (
    SequentialMonitoringError,
    SequentialMonitoringRegistry,
)
from scanner.research.governance.qm_c_negative_results import NegativeResultRegistry
from scanner.research.governance.qm_c6_closure import (
    audit_qm_c_system,
    validate_ba_qm2_handoff,
    validate_six_step_manifests,
)

ROOT = Path(__file__).resolve().parents[1]
QM_B_CLOSURE = ROOT / "configs/qm_b_closure_v1.json"


def qm_b_closure():
    return json.loads(QM_B_CLOSURE.read_text(encoding="utf-8"))


def hypothesis_record(idx=1, *, family="HF-TIMING", mode="CONFIRMATION"):
    return {
        "hypothesis_id": f"H-TIMING-{idx:03d}",
        "hypothesis_version": "v1",
        "research_question": f"Does predeclared timing pattern {idx} improve forward alpha?",
        "hypothesis_statement": f"Timing pattern {idx} has positive forward alpha versus its benchmark.",
        "hypothesis_family_id": family,
        "research_mode": mode,
        "qm_a_analysis_id": f"A-H-TIMING-{idx:03d}",
        "universe_requirement": "OBSERVED_SCANNER_UNIVERSE",
    }


def analysis_plan_record(hypothesis, idx=1):
    return {
        "analysis_plan_id": f"AP-TIMING-{idx:03d}",
        "analysis_plan_version": "v1",
        "hypothesis_id": hypothesis["hypothesis_id"],
        "hypothesis_version": hypothesis["hypothesis_version"],
        "hypothesis_version_hash": hypothesis["hypothesis_version_hash"],
        "research_mode": hypothesis["research_mode"],
        "primary_estimand": "Mean 20-session forward alpha versus the predeclared benchmark.",
        "primary_metrics": ["mean_forward_alpha_20s"],
        "population_definition": "Eligible scanner observations under the declared universe requirement.",
        "universe_requirement": hypothesis["universe_requirement"],
        "evaluation_windows": ["20_sessions"],
        "exclusion_rules": ["missing_start_price", "missing_forward_outcome"],
        "outcome_definitions": ["forward_total_return_minus_benchmark_total_return"],
        "planned_sensitivity_analyses": ["10_sessions", "40_sessions"],
    }


def freeze_context(idx=1):
    return {
        "code_or_commit_hash": "commit-six-step-test",
        "config_hash": f"config-{idx}",
        "dataset_snapshot_hash": "dataset-confirmatory-v1",
        "universe_ledger_version": "qm-b-ledger-v1",
        "instrument_master_version": "qm-b-instrument-master-v1",
        "label_definition_hash": "label-v1",
        "benchmark_definition_hash": "benchmark-v1",
        "environment_or_dependency_fingerprint": "python311-pytest",
        "evaluation_cohort_id": f"cohort-{idx}",
        "temporal_boundary": "2026-09-30",
    }


def registries(tmp_path):
    return (
        HypothesisRegistry(tmp_path / "hypotheses.jsonl"),
        AnalysisPlanRegistry(tmp_path / "plans.jsonl"),
        GovernanceLedger(tmp_path / "qm_a.jsonl"),
        FamilyMultiplicityRegistry(tmp_path / "c3.jsonl"),
        SequentialMonitoringRegistry(tmp_path / "c4.jsonl"),
        NegativeResultRegistry(tmp_path / "c5.jsonl"),
    )


def build_ready_member(tmp_path, idx=1):
    hypotheses, plans, qm_a, c3, c4, c5 = registries(tmp_path)
    hypotheses.register_hypothesis(
        record=hypothesis_record(idx), actor_id="tester", actor_role="researcher"
    )
    hypothesis = hypotheses.get_hypothesis(f"H-TIMING-{idx:03d}", "v1")
    plans.register_plan(
        record=analysis_plan_record(hypothesis, idx),
        hypothesis_registry=hypotheses,
        actor_id="tester",
        actor_role="researcher",
    )
    plan_id = f"AP-TIMING-{idx:03d}"
    plans.freeze_plan(
        analysis_plan_id=plan_id,
        analysis_plan_version="v1",
        freeze_context=freeze_context(idx),
        hypothesis_registry=hypotheses,
        qm_b_closure=qm_b_closure(),
        actor_id="tester",
        actor_role="researcher",
        reason="Freeze analysis plan before confirmation.",
    )
    qm_a_version_id = f"analysis-v{idx}"
    qm_a.register_analysis(
        analysis_id=hypothesis["qm_a_analysis_id"],
        version_id=qm_a_version_id,
        actor_id="tester",
        actor_role="researcher",
    )
    qm_a.transition(
        analysis_id=hypothesis["qm_a_analysis_id"],
        version_id=qm_a_version_id,
        to_state="FROZEN_FOR_CONFIRMATION",
        actor_id="tester",
        actor_role="researcher",
        reason="Freeze exact analysis identity before confirmation.",
        analysis_identity=plans.build_qm_a_identity(plan_id, "v1"),
    )
    hypotheses.transition(
        hypothesis_id=hypothesis["hypothesis_id"],
        hypothesis_version="v1",
        to_state="FROZEN_FOR_CONFIRMATION",
        actor_id="tester",
        actor_role="researcher",
        reason="Freeze hypothesis after plan and QM-A identity.",
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
        "qm_a_version_id": qm_a_version_id,
    }
    return hypotheses, plans, qm_a, c3, c4, c5, member


def c3_record(member):
    return {
        "control_plan_id": "CP-HF-TIMING",
        "control_plan_version": "v1",
        "hypothesis_family_id": "HF-TIMING",
        "research_mode": "CONFIRMATION",
        "family_members": [member],
        "multiplicity_strategy": "PREDECLARED_SINGLE_PRIMARY",
        "multiplicity_parameters": {},
    }


def c4_record(control):
    return {
        "monitoring_plan_id": "MP-HF-TIMING",
        "monitoring_plan_version": "v1",
        "control_plan_id": control["control_plan_id"],
        "control_plan_version": control["control_plan_version"],
        "control_plan_hash": control["control_plan_hash"],
        "mode": "FIXED_HORIZON_NO_INTERIM",
        "planned_looks": [{"look_id": "FINAL", "information_fraction": 1.0}],
        "early_stop_allowed": False,
        "stopping_rule": "FINAL_ONLY",
    }


def freeze_c3_and_c4(hypotheses, plans, qm_a, c3, c4, member):
    c3.register_control_plan(
        record=c3_record(member),
        hypothesis_registry=hypotheses,
        analysis_plan_registry=plans,
        actor_id="tester",
        actor_role="researcher",
    )
    c3.freeze_control_plan(
        control_plan_id="CP-HF-TIMING",
        control_plan_version="v1",
        hypothesis_registry=hypotheses,
        analysis_plan_registry=plans,
        qm_a_ledger=qm_a,
        qm_b_closure=qm_b_closure(),
        actor_id="tester",
        actor_role="researcher",
        reason="Freeze family and multiplicity before any outcome look.",
    )
    control = c3.get_control_plan("CP-HF-TIMING", "v1")
    assert control["state"] == "FROZEN_FOR_CONFIRMATION"
    assert "sequential_monitoring" not in control

    c4.register_monitoring_plan(
        record=c4_record(control),
        control_registry=c3,
        actor_id="tester",
        actor_role="researcher",
    )
    c4.freeze_monitoring_plan(
        monitoring_plan_id="MP-HF-TIMING",
        monitoring_plan_version="v1",
        control_registry=c3,
        actor_id="tester",
        actor_role="researcher",
        reason="Freeze final-only inspection schedule.",
    )
    monitor = c4.get_monitoring_plan("MP-HF-TIMING", "v1")
    assert monitor["state"] == "FROZEN_FOR_CONFIRMATION"
    return control, monitor


def test_six_distinct_work_packages_are_declared_in_exact_order():
    result = validate_six_step_manifests()
    assert result["valid"] is True
    assert result["work_package_order"] == [
        "QM-C1", "QM-C2", "QM-C3", "QM-C4", "QM-C5", "QM-C6"
    ]
    assert result["display_status"] == "QM-C COMPLETE — SIX-STEP RESEARCH DISCIPLINE CHAIN ACTIVE"


def test_c3_is_family_and_multiplicity_only_and_c4_owns_look_schedule(tmp_path):
    hypotheses, plans, qm_a, c3, c4, c5, member = build_ready_member(tmp_path)
    control, monitor = freeze_c3_and_c4(hypotheses, plans, qm_a, c3, c4, member)
    assert control["multiplicity_strategy"] == "PREDECLARED_SINGLE_PRIMARY"
    assert "planned_looks" not in control
    assert monitor["planned_looks"] == [{"look_id": "FINAL", "information_fraction": 1.0}]
    assert monitor["control_plan_hash"] == control["control_plan_hash"]


def test_c4_rejects_wrong_c3_hash_and_out_of_order_look(tmp_path):
    hypotheses, plans, qm_a, c3, c4, c5, member = build_ready_member(tmp_path)
    c3.register_control_plan(
        record=c3_record(member), hypothesis_registry=hypotheses,
        analysis_plan_registry=plans, actor_id="tester", actor_role="researcher"
    )
    c3.freeze_control_plan(
        control_plan_id="CP-HF-TIMING", control_plan_version="v1",
        hypothesis_registry=hypotheses, analysis_plan_registry=plans,
        qm_a_ledger=qm_a, qm_b_closure=qm_b_closure(), actor_id="tester",
        actor_role="researcher", reason="Freeze C3."
    )
    control = c3.get_control_plan("CP-HF-TIMING", "v1")
    wrong = c4_record(control)
    wrong["control_plan_hash"] = "wrong-hash"
    with pytest.raises(SequentialMonitoringError, match="monitoring_plan_control_hash_mismatch"):
        c4.register_monitoring_plan(
            record=wrong, control_registry=c3, actor_id="tester", actor_role="researcher"
        )

    two_look = c4_record(control)
    two_look.update(
        {
            "mode": "PREDECLARED_LOOKS",
            "planned_looks": [
                {"look_id": "LOOK_1", "information_fraction": 0.5},
                {"look_id": "FINAL", "information_fraction": 1.0},
            ],
            "stopping_rule": "CONTINUE_UNLESS_PREDECLARED_BOUNDARY",
        }
    )
    c4.register_monitoring_plan(
        record=two_look, control_registry=c3, actor_id="tester", actor_role="researcher"
    )
    c4.freeze_monitoring_plan(
        monitoring_plan_id="MP-HF-TIMING", monitoring_plan_version="v1",
        control_registry=c3, actor_id="tester", actor_role="researcher", reason="Freeze C4."
    )
    with pytest.raises(SequentialMonitoringError, match="monitoring_look_out_of_order"):
        c4.record_monitoring_look(
            monitoring_plan_id="MP-HF-TIMING", monitoring_plan_version="v1",
            look_id="FINAL", artifact_hash="artifact-final", decision="FINAL_COMPLETE",
            control_registry=c3, qm_a_ledger=qm_a, actor_id="tester", actor_role="researcher"
        )


def test_negative_confirmatory_result_passes_complete_c1_to_c5_chain(tmp_path):
    hypotheses, plans, qm_a, c3, c4, c5, member = build_ready_member(tmp_path)
    control, _ = freeze_c3_and_c4(hypotheses, plans, qm_a, c3, c4, member)

    c4.record_monitoring_look(
        monitoring_plan_id="MP-HF-TIMING",
        monitoring_plan_version="v1",
        look_id="FINAL",
        artifact_hash="evidence-negative-final",
        decision="FINAL_COMPLETE",
        control_registry=c3,
        qm_a_ledger=qm_a,
        actor_id="tester",
        actor_role="researcher",
    )
    monitor = c4.get_monitoring_plan("MP-HF-TIMING", "v1")
    assert monitor["state"] == "COMPLETE"

    qm_a.transition(
        analysis_id=member["qm_a_analysis_id"],
        version_id=member["qm_a_version_id"],
        to_state="CONFIRMATORY_EVALUATED",
        actor_id="tester",
        actor_role="researcher",
        reason="The predeclared final look consumed confirmatory evidence.",
    )

    result_record = {
        "result_id": "R-H-TIMING-001",
        "result_version": "v1",
        "hypothesis_id": member["hypothesis_id"],
        "hypothesis_version": member["hypothesis_version"],
        "hypothesis_version_hash": member["hypothesis_version_hash"],
        "evidence_scope": "CONFIRMATORY",
        "outcome_classification": "NEGATIVE",
        "conclusion": "The predeclared confirmatory evaluation did not support the directional hypothesis.",
        "evidence_artifact_hash": "evidence-negative-final",
        "analysis_plan_id": member["analysis_plan_id"],
        "analysis_plan_version": member["analysis_plan_version"],
        "analysis_plan_hash": member["analysis_plan_hash"],
        "control_plan_id": control["control_plan_id"],
        "control_plan_version": control["control_plan_version"],
        "control_plan_hash": control["control_plan_hash"],
        "monitoring_plan_id": monitor["monitoring_plan_id"],
        "monitoring_plan_version": monitor["monitoring_plan_version"],
        "monitoring_plan_hash": monitor["monitoring_plan_hash"],
        "qm_a_analysis_id": member["qm_a_analysis_id"],
        "qm_a_version_id": member["qm_a_version_id"],
    }
    c5.register_result(
        record=result_record,
        hypothesis_registry=hypotheses,
        analysis_plan_registry=plans,
        control_registry=c3,
        monitoring_registry=c4,
        qm_a_ledger=qm_a,
        actor_id="tester",
        actor_role="researcher",
    )
    saved = c5.get_result("R-H-TIMING-001", "v1")
    assert saved["outcome_classification"] == "NEGATIVE"
    assert saved["monitoring_plan_hash"] == monitor["monitoring_plan_hash"]

    audit = audit_qm_c_system(
        hypothesis_registry=hypotheses,
        analysis_plan_registry=plans,
        control_registry=c3,
        monitoring_registry=c4,
        result_registry=c5,
        qm_a_ledger=qm_a,
    )
    assert audit["status"] == "PASS"
    assert audit["cross_reference_errors"] == []
    assert audit["counts"]["result_versions"] == 1


def test_rejected_pre_evaluation_result_retains_rejection_without_invented_evidence(tmp_path):
    hypotheses, plans, qm_a, c3, c4, c5 = registries(tmp_path)
    hypotheses.register_hypothesis(
        record=hypothesis_record(9), actor_id="tester", actor_role="researcher"
    )
    hypothesis = hypotheses.get_hypothesis("H-TIMING-009", "v1")
    hypotheses.transition(
        hypothesis_id="H-TIMING-009", hypothesis_version="v1", to_state="REJECTED",
        actor_id="tester", actor_role="researcher", reason="Rejected before any outcome evaluation."
    )
    c5.register_result(
        record={
            "result_id": "R-H-TIMING-009",
            "result_version": "v1",
            "hypothesis_id": "H-TIMING-009",
            "hypothesis_version": "v1",
            "hypothesis_version_hash": hypothesis["hypothesis_version_hash"],
            "evidence_scope": "NO_OUTCOME_EVIDENCE",
            "outcome_classification": "REJECTED_PRE_EVALUATION",
            "conclusion": "Rejected before evaluation; no performance outcome exists.",
            "evidence_artifact_hash": None,
            "analysis_plan_id": None,
            "analysis_plan_version": None,
            "analysis_plan_hash": None,
            "control_plan_id": None,
            "control_plan_version": None,
            "control_plan_hash": None,
            "monitoring_plan_id": None,
            "monitoring_plan_version": None,
            "monitoring_plan_hash": None,
            "qm_a_analysis_id": None,
            "qm_a_version_id": None,
        },
        hypothesis_registry=hypotheses,
        analysis_plan_registry=plans,
        control_registry=c3,
        monitoring_registry=c4,
        qm_a_ledger=qm_a,
        actor_id="tester",
        actor_role="researcher",
    )
    saved = c5.get_result("R-H-TIMING-009", "v1")
    assert saved["outcome_classification"] == "REJECTED_PRE_EVALUATION"
    assert saved["evidence_artifact_hash"] is None
    assert saved["monitoring_plan_id"] is None


def test_ba_qm2_handoff_preserves_real_qm_b_external_blockers_and_stops_at_qm_c():
    result = validate_ba_qm2_handoff()
    assert result["valid"] is True
    handoff = result["handoff"]
    assert handoff["qm_b"]["external_blockers"] == [
        "LISTING_EVIDENCE", "MARKET_TRADABILITY_EVIDENCE", "EXECUTION_CHANNEL_EVIDENCE"
    ]
    assert handoff["scope_boundary"]["ba_qm3_implemented_by_this_handoff"] is False
    assert handoff["scope_boundary"]["next_business_area_may_start_only_after_separate_user_authorization"] is True


def test_old_combined_qm_c_contracts_are_not_authoritative_anymore():
    obsolete = [
        "configs/qm_c_multiplicity_monitoring_v1.json",
        "configs/qm_c_results_audit_v1.json",
        "configs/qm_c3_closure_v1.json",
        "configs/qm_c4_closure_v1.json",
        "configs/ba_qm2_handoff_v1.json",
        "src/scanner/research/governance/qm_c_multiplicity.py",
        "src/scanner/research/governance/qm_c_results.py",
        "src/scanner/research/governance/qm_c3_closure.py",
        "src/scanner/research/governance/qm_c4_closure.py",
    ]
    assert all(not (ROOT / path).exists() for path in obsolete)
