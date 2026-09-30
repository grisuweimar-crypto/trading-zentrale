import json
from pathlib import Path

import pytest

from scanner.research.governance.qm_a import GovernanceLedger
from scanner.research.governance.qm_c import HypothesisRegistry
from scanner.research.governance.qm_c_analysis_plan import AnalysisPlanRegistry
from scanner.research.governance.qm_c_multiplicity import (
    MultiplicityMonitoringError,
    MultiplicityMonitoringRegistry,
    load_qm_c_multiplicity_contract,
)


QM_B_CLOSURE = Path("configs/qm_b_closure_v1.json")


def closure():
    return json.loads(QM_B_CLOSURE.read_text(encoding="utf-8"))


def hypothesis_record(idx: int, *, family: str = "HF-TIMING"):
    return {
        "hypothesis_id": f"H-TIMING-{idx:03d}",
        "hypothesis_version": "v1",
        "research_question": f"Does predeclared timing pattern {idx} improve forward alpha?",
        "hypothesis_statement": f"Timing pattern {idx} has positive forward alpha versus its benchmark.",
        "hypothesis_family_id": family,
        "research_mode": "CONFIRMATION",
        "qm_a_analysis_id": f"A-H-TIMING-{idx:03d}",
        "universe_requirement": "OBSERVED_SCANNER_UNIVERSE",
    }


def plan_record(hypothesis, idx: int):
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


def freeze_context(idx: int):
    return {
        "code_or_commit_hash": "commit-c3-test",
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


def build_ready_member(
    *,
    idx: int,
    hypotheses: HypothesisRegistry,
    plans: AnalysisPlanRegistry,
    qm_a: GovernanceLedger,
    family: str = "HF-TIMING",
):
    hypotheses.register_hypothesis(
        record=hypothesis_record(idx, family=family), actor_id="tester", actor_role="researcher"
    )
    hypothesis = hypotheses.get_hypothesis(f"H-TIMING-{idx:03d}", "v1")
    plans.register_plan(
        record=plan_record(hypothesis, idx),
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
        qm_b_closure=closure(),
        actor_id="tester",
        actor_role="researcher",
        reason="Predeclare C2 plan before confirmation.",
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
        reason="Freeze exact C2 identity before evaluation.",
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
    return {
        "hypothesis_id": hypothesis["hypothesis_id"],
        "hypothesis_version": hypothesis["hypothesis_version"],
        "hypothesis_version_hash": hypothesis["hypothesis_version_hash"],
        "analysis_plan_id": plan["analysis_plan_id"],
        "analysis_plan_version": plan["analysis_plan_version"],
        "analysis_plan_hash": plan["analysis_plan_hash"],
        "qm_a_analysis_id": hypothesis["qm_a_analysis_id"],
        "qm_a_version_id": qm_a_version_id,
    }


def registries(tmp_path):
    return (
        HypothesisRegistry(tmp_path / "hypotheses.jsonl"),
        AnalysisPlanRegistry(tmp_path / "plans.jsonl"),
        GovernanceLedger(tmp_path / "qm_a.jsonl"),
        MultiplicityMonitoringRegistry(tmp_path / "controls.jsonl"),
    )


def fixed_monitoring():
    return {
        "mode": "FIXED_HORIZON_NO_INTERIM",
        "planned_looks": [{"look_id": "FINAL", "information_fraction": 1.0}],
        "early_stop_allowed": False,
        "stopping_rule": "FINAL_ONLY",
    }


def interim_monitoring(*, early_stop: bool = False):
    return {
        "mode": "PREDECLARED_LOOKS",
        "planned_looks": [
            {"look_id": "LOOK_1", "information_fraction": 0.5},
            {"look_id": "FINAL", "information_fraction": 1.0},
        ],
        "early_stop_allowed": early_stop,
        "stopping_rule": "Continue unless a predeclared early-stop boundary is met.",
    }


def control_record(members, *, strategy="PREDECLARED_SINGLE_PRIMARY", parameters=None, monitoring=None):
    return {
        "control_plan_id": "CP-HF-TIMING",
        "control_plan_version": "v1",
        "hypothesis_family_id": "HF-TIMING",
        "research_mode": "CONFIRMATION",
        "family_members": members,
        "multiplicity_strategy": strategy,
        "multiplicity_parameters": {} if parameters is None else parameters,
        "sequential_monitoring": fixed_monitoring() if monitoring is None else monitoring,
    }


def test_contract_is_research_only_and_does_not_compute_statistics():
    contract = load_qm_c_multiplicity_contract()
    assert contract["research_only"] is True
    assert contract["productive_integration_enabled"] is False
    assert contract["execution_allowed"] is False
    assert contract["boundaries"]["adjusted_p_values_computed_here"] is False
    assert contract["boundaries"]["effect_estimates_computed_here"] is False
    assert contract["boundaries"]["scanner_semantics_changed"] is False


def test_single_primary_requires_exactly_one_member(tmp_path):
    hypotheses, plans, qm_a, controls = registries(tmp_path)
    m1 = build_ready_member(idx=1, hypotheses=hypotheses, plans=plans, qm_a=qm_a)
    m2 = build_ready_member(idx=2, hypotheses=hypotheses, plans=plans, qm_a=qm_a)
    with pytest.raises(MultiplicityMonitoringError, match="single_primary_requires_exactly_one_family_member"):
        controls.register_control_plan(
            record=control_record([m1, m2]),
            hypothesis_registry=hypotheses,
            analysis_plan_registry=plans,
            actor_id="tester",
            actor_role="researcher",
        )


def test_holm_family_requires_predeclared_family_alpha_and_exact_members(tmp_path):
    hypotheses, plans, qm_a, controls = registries(tmp_path)
    m1 = build_ready_member(idx=1, hypotheses=hypotheses, plans=plans, qm_a=qm_a)
    m2 = build_ready_member(idx=2, hypotheses=hypotheses, plans=plans, qm_a=qm_a)
    controls.register_control_plan(
        record=control_record(
            [m1, m2], strategy="HOLM_FWER", parameters={"family_alpha": 0.05}
        ),
        hypothesis_registry=hypotheses,
        analysis_plan_registry=plans,
        actor_id="tester",
        actor_role="researcher",
    )
    saved = controls.get_control_plan("CP-HF-TIMING", "v1")
    assert saved["multiplicity_strategy"] == "HOLM_FWER"
    assert saved["multiplicity_parameters"] == {"family_alpha": 0.05}
    assert len(saved["family_members"]) == 2


def test_family_member_must_share_declared_qm_c1_family(tmp_path):
    hypotheses, plans, qm_a, controls = registries(tmp_path)
    m1 = build_ready_member(idx=1, hypotheses=hypotheses, plans=plans, qm_a=qm_a, family="HF-OTHER")
    with pytest.raises(MultiplicityMonitoringError, match="family_member_hypothesis_family_mismatch"):
        controls.register_control_plan(
            record=control_record([m1]),
            hypothesis_registry=hypotheses,
            analysis_plan_registry=plans,
            actor_id="tester",
            actor_role="researcher",
        )


def test_monitoring_schedule_requires_increasing_fractions_and_final_one(tmp_path):
    hypotheses, plans, qm_a, controls = registries(tmp_path)
    member = build_ready_member(idx=1, hypotheses=hypotheses, plans=plans, qm_a=qm_a)
    invalid = {
        "mode": "PREDECLARED_LOOKS",
        "planned_looks": [
            {"look_id": "A", "information_fraction": 0.7},
            {"look_id": "B", "information_fraction": 0.6},
        ],
        "early_stop_allowed": False,
        "stopping_rule": "No early stop.",
    }
    with pytest.raises(MultiplicityMonitoringError, match="information_fractions_must_be_strictly_increasing"):
        controls.register_control_plan(
            record=control_record([member], monitoring=invalid),
            hypothesis_registry=hypotheses,
            analysis_plan_registry=plans,
            actor_id="tester",
            actor_role="researcher",
        )


def test_freeze_requires_every_member_to_be_c2_confirmation_ready(tmp_path):
    hypotheses, plans, qm_a, controls = registries(tmp_path)
    member = build_ready_member(idx=1, hypotheses=hypotheses, plans=plans, qm_a=qm_a)
    controls.register_control_plan(
        record=control_record([member]),
        hypothesis_registry=hypotheses,
        analysis_plan_registry=plans,
        actor_id="tester",
        actor_role="researcher",
    )
    controls.freeze_control_plan(
        control_plan_id="CP-HF-TIMING",
        control_plan_version="v1",
        hypothesis_registry=hypotheses,
        analysis_plan_registry=plans,
        qm_a_ledger=qm_a,
        qm_b_closure=closure(),
        actor_id="tester",
        actor_role="researcher",
        reason="Freeze multiplicity and monitoring rules before outcome inspection.",
    )
    saved = controls.get_control_plan("CP-HF-TIMING", "v1")
    assert saved["state"] == "FROZEN_FOR_CONFIRMATION"
    assert saved["freeze_binding_hash"]
    ready = controls.validate_evaluation_ready(
        control_plan_id="CP-HF-TIMING",
        control_plan_version="v1",
        hypothesis_registry=hypotheses,
        analysis_plan_registry=plans,
        qm_a_ledger=qm_a,
        qm_b_closure=closure(),
    )
    assert ready["valid"] is True
    assert ready["next_look"] == {"look_id": "FINAL", "information_fraction": 1.0}


def test_out_of_order_or_unplanned_look_is_rejected(tmp_path):
    hypotheses, plans, qm_a, controls = registries(tmp_path)
    member = build_ready_member(idx=1, hypotheses=hypotheses, plans=plans, qm_a=qm_a)
    controls.register_control_plan(
        record=control_record([member], monitoring=interim_monitoring()),
        hypothesis_registry=hypotheses,
        analysis_plan_registry=plans,
        actor_id="tester",
        actor_role="researcher",
    )
    controls.freeze_control_plan(
        control_plan_id="CP-HF-TIMING", control_plan_version="v1",
        hypothesis_registry=hypotheses, analysis_plan_registry=plans,
        qm_a_ledger=qm_a, qm_b_closure=closure(), actor_id="tester", actor_role="researcher",
        reason="Predeclare two-look schedule."
    )
    with pytest.raises(MultiplicityMonitoringError, match="monitoring_look_out_of_order:expected=LOOK_1"):
        controls.record_monitoring_look(
            control_plan_id="CP-HF-TIMING", control_plan_version="v1", look_id="FINAL",
            artifact_hash="artifact-wrong-order", decision="FINAL_COMPLETE", qm_a_ledger=qm_a,
            actor_id="tester", actor_role="researcher"
        )


def test_second_look_requires_qm_a_to_mark_first_evidence_consumed(tmp_path):
    hypotheses, plans, qm_a, controls = registries(tmp_path)
    member = build_ready_member(idx=1, hypotheses=hypotheses, plans=plans, qm_a=qm_a)
    controls.register_control_plan(
        record=control_record([member], monitoring=interim_monitoring()),
        hypothesis_registry=hypotheses, analysis_plan_registry=plans,
        actor_id="tester", actor_role="researcher"
    )
    controls.freeze_control_plan(
        control_plan_id="CP-HF-TIMING", control_plan_version="v1",
        hypothesis_registry=hypotheses, analysis_plan_registry=plans,
        qm_a_ledger=qm_a, qm_b_closure=closure(), actor_id="tester", actor_role="researcher",
        reason="Freeze before first look."
    )
    controls.record_monitoring_look(
        control_plan_id="CP-HF-TIMING", control_plan_version="v1", look_id="LOOK_1",
        artifact_hash="artifact-look-1", decision="CONTINUE", qm_a_ledger=qm_a,
        actor_id="tester", actor_role="researcher"
    )
    assert controls.get_control_plan("CP-HF-TIMING", "v1")["state"] == "MONITORING"

    with pytest.raises(MultiplicityMonitoringError, match="subsequent_monitoring_look_requires_consumed_qm_a_evidence"):
        controls.record_monitoring_look(
            control_plan_id="CP-HF-TIMING", control_plan_version="v1", look_id="FINAL",
            artifact_hash="artifact-final-too-early", decision="FINAL_COMPLETE", qm_a_ledger=qm_a,
            actor_id="tester", actor_role="researcher"
        )

    qm_a.transition(
        analysis_id=member["qm_a_analysis_id"], version_id=member["qm_a_version_id"],
        to_state="CONFIRMATORY_EVALUATED", actor_id="tester", actor_role="researcher",
        reason="First predeclared confirmatory look consumed the confirmation evidence."
    )
    controls.record_monitoring_look(
        control_plan_id="CP-HF-TIMING", control_plan_version="v1", look_id="FINAL",
        artifact_hash="artifact-final", decision="FINAL_COMPLETE", qm_a_ledger=qm_a,
        actor_id="tester", actor_role="researcher"
    )
    saved = controls.get_control_plan("CP-HF-TIMING", "v1")
    assert saved["state"] == "COMPLETE"
    assert [look["look_id"] for look in saved["recorded_looks"]] == ["LOOK_1", "FINAL"]


def test_unplanned_early_stop_is_rejected_and_predeclared_stop_closes_family(tmp_path):
    hypotheses, plans, qm_a, controls = registries(tmp_path)
    member = build_ready_member(idx=1, hypotheses=hypotheses, plans=plans, qm_a=qm_a)
    controls.register_control_plan(
        record=control_record([member], monitoring=interim_monitoring(early_stop=False)),
        hypothesis_registry=hypotheses, analysis_plan_registry=plans,
        actor_id="tester", actor_role="researcher"
    )
    controls.freeze_control_plan(
        control_plan_id="CP-HF-TIMING", control_plan_version="v1",
        hypothesis_registry=hypotheses, analysis_plan_registry=plans,
        qm_a_ledger=qm_a, qm_b_closure=closure(), actor_id="tester", actor_role="researcher",
        reason="Freeze no-early-stop schedule."
    )
    with pytest.raises(MultiplicityMonitoringError, match="early_stop_decision_not_predeclared"):
        controls.record_monitoring_look(
            control_plan_id="CP-HF-TIMING", control_plan_version="v1", look_id="LOOK_1",
            artifact_hash="artifact-look-1", decision="STOP_FUTILITY", qm_a_ledger=qm_a,
            actor_id="tester", actor_role="researcher"
        )

    h2, p2, a2, c2 = registries(tmp_path / "allowed")
    m2 = build_ready_member(idx=1, hypotheses=h2, plans=p2, qm_a=a2)
    c2.register_control_plan(
        record=control_record([m2], monitoring=interim_monitoring(early_stop=True)),
        hypothesis_registry=h2, analysis_plan_registry=p2, actor_id="tester", actor_role="researcher"
    )
    c2.freeze_control_plan(
        control_plan_id="CP-HF-TIMING", control_plan_version="v1",
        hypothesis_registry=h2, analysis_plan_registry=p2, qm_a_ledger=a2,
        qm_b_closure=closure(), actor_id="tester", actor_role="researcher", reason="Allow early stop."
    )
    c2.record_monitoring_look(
        control_plan_id="CP-HF-TIMING", control_plan_version="v1", look_id="LOOK_1",
        artifact_hash="artifact-stop", decision="STOP_FUTILITY", qm_a_ledger=a2,
        actor_id="tester", actor_role="researcher"
    )
    assert c2.get_control_plan("CP-HF-TIMING", "v1")["state"] == "STOPPED"
    with pytest.raises(MultiplicityMonitoringError, match="monitoring_look_not_allowed_in_state:STOPPED"):
        c2.record_monitoring_look(
            control_plan_id="CP-HF-TIMING", control_plan_version="v1", look_id="FINAL",
            artifact_hash="artifact-after-stop", decision="FINAL_COMPLETE", qm_a_ledger=a2,
            actor_id="tester", actor_role="researcher"
        )


def test_control_plan_is_immutable_and_successor_explicit(tmp_path):
    hypotheses, plans, qm_a, controls = registries(tmp_path)
    member = build_ready_member(idx=1, hypotheses=hypotheses, plans=plans, qm_a=qm_a)
    record = control_record([member])
    controls.register_control_plan(
        record=record, hypothesis_registry=hypotheses, analysis_plan_registry=plans,
        actor_id="tester", actor_role="researcher"
    )
    with pytest.raises(MultiplicityMonitoringError, match="control_plan_version_already_registered"):
        controls.register_control_plan(
            record={**record, "sequential_monitoring": interim_monitoring()},
            hypothesis_registry=hypotheses, analysis_plan_registry=plans,
            actor_id="tester", actor_role="researcher"
        )
    v2 = {**record, "control_plan_version": "v2", "sequential_monitoring": interim_monitoring()}
    with pytest.raises(MultiplicityMonitoringError, match="control_plan_successor_requires_supersedes_reference"):
        controls.register_control_plan(
            record=v2, hypothesis_registry=hypotheses, analysis_plan_registry=plans,
            actor_id="tester", actor_role="researcher"
        )
    controls.register_control_plan(
        record=v2, hypothesis_registry=hypotheses, analysis_plan_registry=plans,
        actor_id="tester", actor_role="researcher", supersedes_control_plan_version="v1"
    )
    assert controls.get_control_plan("CP-HF-TIMING", "v2")["supersedes_control_plan_version"] == "v1"


def test_tampered_control_registry_fails_closed(tmp_path):
    hypotheses, plans, qm_a, controls = registries(tmp_path)
    member = build_ready_member(idx=1, hypotheses=hypotheses, plans=plans, qm_a=qm_a)
    controls.register_control_plan(
        record=control_record([member]), hypothesis_registry=hypotheses, analysis_plan_registry=plans,
        actor_id="tester", actor_role="researcher"
    )
    path = tmp_path / "controls.jsonl"
    line = json.loads(path.read_text(encoding="utf-8"))
    line["payload"]["record"]["hypothesis_family_id"] = "TAMPERED"
    path.write_text(json.dumps(line) + "\n", encoding="utf-8")
    with pytest.raises(MultiplicityMonitoringError, match="control_registry_entry_hash_invalid"):
        controls.verify_integrity()
