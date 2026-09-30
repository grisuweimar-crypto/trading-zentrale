import json
from pathlib import Path

import pytest

from scanner.research.governance.qm_a import GovernanceLedger
from scanner.research.governance.qm_c import HypothesisRegistry, HypothesisRegistryError
from scanner.research.governance.qm_c_analysis_plan import (
    AnalysisPlanError,
    AnalysisPlanRegistry,
    load_qm_c_analysis_plan_contract,
)


QM_B_CLOSURE = Path("configs/qm_b_closure_v1.json")


def hypothesis_record(**overrides):
    payload = {
        "hypothesis_id": "H-TIMING-001",
        "hypothesis_version": "v1",
        "research_question": "Does a predeclared timing pattern improve forward alpha?",
        "hypothesis_statement": "The predeclared timing pattern has positive forward alpha versus its benchmark.",
        "hypothesis_family_id": "HF-TIMING",
        "research_mode": "CONFIRMATION",
        "qm_a_analysis_id": "A-H-TIMING-001",
        "universe_requirement": "OBSERVED_SCANNER_UNIVERSE",
    }
    payload.update(overrides)
    return payload


def registered_hypothesis(tmp_path, **overrides):
    registry = HypothesisRegistry(tmp_path / "hypotheses.jsonl")
    registry.register_hypothesis(
        record=hypothesis_record(**overrides), actor_id="tester", actor_role="researcher"
    )
    return registry, registry.get_hypothesis("H-TIMING-001", "v1")


def plan_record(hypothesis, **overrides):
    payload = {
        "analysis_plan_id": "AP-TIMING-001",
        "analysis_plan_version": "v1",
        "hypothesis_id": hypothesis["hypothesis_id"],
        "hypothesis_version": hypothesis["hypothesis_version"],
        "hypothesis_version_hash": hypothesis["hypothesis_version_hash"],
        "research_mode": hypothesis["research_mode"],
        "primary_estimand": "Mean 20-session forward alpha versus the predeclared benchmark.",
        "primary_metrics": ["mean_forward_alpha_20s", "positive_alpha_rate_20s"],
        "population_definition": "Eligible scanner observations under the declared universe requirement.",
        "universe_requirement": hypothesis["universe_requirement"],
        "evaluation_windows": ["20_sessions"],
        "exclusion_rules": ["missing_start_price", "missing_forward_outcome"],
        "outcome_definitions": ["forward_total_return_minus_benchmark_total_return"],
        "planned_sensitivity_analyses": ["10_sessions", "40_sessions"],
    }
    payload.update(overrides)
    return payload


def freeze_context(**overrides):
    payload = {
        "code_or_commit_hash": "commit-abc123",
        "config_hash": "config-def456",
        "dataset_snapshot_hash": "dataset-789",
        "universe_ledger_version": "qm-b-ledger-v1",
        "instrument_master_version": "qm-b-instrument-master-v1",
        "label_definition_hash": "label-123",
        "benchmark_definition_hash": "benchmark-456",
        "environment_or_dependency_fingerprint": "python311-pytest",
        "evaluation_cohort_id": "cohort-confirm-001",
        "temporal_boundary": "2026-09-30",
    }
    payload.update(overrides)
    return payload


def closure():
    return json.loads(QM_B_CLOSURE.read_text(encoding="utf-8"))


def test_contract_is_research_only_and_keeps_later_blocks_out_of_scope():
    contract = load_qm_c_analysis_plan_contract()
    assert contract["research_only"] is True
    assert contract["productive_integration_enabled"] is False
    assert contract["execution_allowed"] is False
    assert contract["boundaries"]["multiplicity_implemented_here"] is False
    assert contract["boundaries"]["sequential_monitoring_implemented_here"] is False
    assert contract["boundaries"]["scanner_semantics_changed"] is False


def test_plan_registration_is_bound_to_exact_qm_c1_hypothesis(tmp_path):
    hypotheses, hypothesis = registered_hypothesis(tmp_path)
    plans = AnalysisPlanRegistry(tmp_path / "plans.jsonl")
    plans.register_plan(
        record=plan_record(hypothesis),
        hypothesis_registry=hypotheses,
        actor_id="tester",
        actor_role="researcher",
    )
    saved = plans.get_plan("AP-TIMING-001", "v1")
    assert saved["hypothesis_version_hash"] == hypothesis["hypothesis_version_hash"]
    assert saved["state"] == "DRAFT"
    assert saved["analysis_plan_hash"]
    assert plans.verify_integrity()["valid"] is True

    bad = plan_record(hypothesis, analysis_plan_id="AP-BAD", hypothesis_version_hash="wrong")
    with pytest.raises(AnalysisPlanError, match="analysis_plan_hypothesis_binding_mismatch:hypothesis_version_hash"):
        plans.register_plan(
            record=bad,
            hypothesis_registry=hypotheses,
            actor_id="tester",
            actor_role="researcher",
        )


def test_plan_version_is_immutable_and_successor_is_explicit(tmp_path):
    hypotheses, hypothesis = registered_hypothesis(tmp_path)
    plans = AnalysisPlanRegistry(tmp_path / "plans.jsonl")
    plans.register_plan(
        record=plan_record(hypothesis), hypothesis_registry=hypotheses,
        actor_id="tester", actor_role="researcher"
    )
    with pytest.raises(AnalysisPlanError, match="analysis_plan_version_already_registered"):
        plans.register_plan(
            record=plan_record(hypothesis, primary_estimand="Post-hoc changed estimand."),
            hypothesis_registry=hypotheses, actor_id="tester", actor_role="researcher"
        )

    v2 = plan_record(hypothesis, analysis_plan_version="v2", primary_estimand="Predeclared successor estimand.")
    with pytest.raises(AnalysisPlanError, match="analysis_plan_successor_requires_supersedes_reference"):
        plans.register_plan(
            record=v2, hypothesis_registry=hypotheses, actor_id="tester", actor_role="researcher"
        )
    plans.register_plan(
        record=v2, hypothesis_registry=hypotheses, actor_id="tester", actor_role="researcher",
        supersedes_analysis_plan_version="v1"
    )
    assert plans.get_plan("AP-TIMING-001", "v2")["supersedes_analysis_plan_version"] == "v1"


def test_confirmation_plan_freezes_before_hypothesis_and_builds_qm_a_identity(tmp_path):
    hypotheses, hypothesis = registered_hypothesis(tmp_path)
    plans = AnalysisPlanRegistry(tmp_path / "plans.jsonl")
    plans.register_plan(
        record=plan_record(hypothesis), hypothesis_registry=hypotheses,
        actor_id="tester", actor_role="researcher"
    )
    plans.freeze_plan(
        analysis_plan_id="AP-TIMING-001", analysis_plan_version="v1",
        freeze_context=freeze_context(), hypothesis_registry=hypotheses,
        qm_b_closure=closure(), actor_id="tester", actor_role="researcher",
        reason="Predeclared analysis plan frozen before confirmation."
    )
    saved = plans.get_plan("AP-TIMING-001", "v1")
    assert saved["state"] == "FROZEN_FOR_CONFIRMATION"
    assert saved["freeze_context"] == freeze_context()
    assert saved["freeze_context_hash"]

    identity = plans.build_qm_a_identity("AP-TIMING-001", "v1")
    assert identity["hypothesis_version_hash"] == hypothesis["hypothesis_version_hash"]
    assert identity["analysis_plan_hash"] == saved["analysis_plan_hash"]
    for field, value in freeze_context().items():
        assert identity[field] == value


def test_direct_hypothesis_freeze_does_not_allow_late_plan_freeze(tmp_path):
    hypotheses, hypothesis = registered_hypothesis(tmp_path)
    hypotheses.transition(
        hypothesis_id=hypothesis["hypothesis_id"], hypothesis_version=hypothesis["hypothesis_version"],
        to_state="FROZEN_FOR_CONFIRMATION", actor_id="tester", actor_role="researcher",
        reason="Negative test: direct C1 freeze before plan."
    )
    plans = AnalysisPlanRegistry(tmp_path / "plans.jsonl")
    plans.register_plan(
        record=plan_record(hypothesis), hypothesis_registry=hypotheses,
        actor_id="tester", actor_role="researcher"
    )
    with pytest.raises(AnalysisPlanError, match="analysis_plan_must_freeze_before_hypothesis_confirmation_freeze"):
        plans.freeze_plan(
            analysis_plan_id="AP-TIMING-001", analysis_plan_version="v1",
            freeze_context=freeze_context(), hypothesis_registry=hypotheses,
            qm_b_closure=closure(), actor_id="tester", actor_role="researcher",
            reason="Must fail because plan came too late."
        )


def test_discovery_plan_can_be_exploratory_but_cannot_freeze_for_confirmation(tmp_path):
    hypotheses, hypothesis = registered_hypothesis(tmp_path, research_mode="DISCOVERY")
    plans = AnalysisPlanRegistry(tmp_path / "plans.jsonl")
    plans.register_plan(
        record=plan_record(hypothesis), hypothesis_registry=hypotheses,
        actor_id="tester", actor_role="researcher"
    )
    plans.transition(
        analysis_plan_id="AP-TIMING-001", analysis_plan_version="v1", to_state="EXPLORATORY",
        actor_id="tester", actor_role="researcher", reason="Discovery analysis started."
    )
    assert plans.get_plan("AP-TIMING-001", "v1")["state"] == "EXPLORATORY"

    hypotheses2, hypothesis2 = registered_hypothesis(tmp_path / "other", research_mode="DISCOVERY")
    plans2 = AnalysisPlanRegistry(tmp_path / "other" / "plans.jsonl")
    plans2.register_plan(
        record=plan_record(hypothesis2), hypothesis_registry=hypotheses2,
        actor_id="tester", actor_role="researcher"
    )
    with pytest.raises(AnalysisPlanError, match="only_confirmation_plan_can_freeze"):
        plans2.freeze_plan(
            analysis_plan_id="AP-TIMING-001", analysis_plan_version="v1",
            freeze_context=freeze_context(), hypothesis_registry=hypotheses2,
            qm_b_closure=closure(), actor_id="tester", actor_role="researcher",
            reason="Discovery must not become confirmation in-place."
        )


def test_current_qm_b_external_blocker_prevents_strict_confirmation_plan_freeze(tmp_path):
    hypotheses, hypothesis = registered_hypothesis(tmp_path, universe_requirement="STRICT_ASOF_INVESTABLE")
    plans = AnalysisPlanRegistry(tmp_path / "plans.jsonl")
    plans.register_plan(
        record=plan_record(hypothesis), hypothesis_registry=hypotheses,
        actor_id="tester", actor_role="researcher"
    )
    with pytest.raises(HypothesisRegistryError, match="qm_b_strict_universe_not_promoted:BLOCKED_EXTERNAL_EVIDENCE"):
        plans.freeze_plan(
            analysis_plan_id="AP-TIMING-001", analysis_plan_version="v1",
            freeze_context=freeze_context(), hypothesis_registry=hypotheses,
            qm_b_closure=closure(), actor_id="tester", actor_role="researcher",
            reason="Strict universe must remain blocked."
        )


def test_end_to_end_confirmation_readiness_binds_plan_hypothesis_qm_a_and_qm_b(tmp_path):
    hypotheses, hypothesis = registered_hypothesis(tmp_path)
    plans = AnalysisPlanRegistry(tmp_path / "plans.jsonl")
    plans.register_plan(
        record=plan_record(hypothesis), hypothesis_registry=hypotheses,
        actor_id="tester", actor_role="researcher"
    )
    plans.freeze_plan(
        analysis_plan_id="AP-TIMING-001", analysis_plan_version="v1",
        freeze_context=freeze_context(), hypothesis_registry=hypotheses,
        qm_b_closure=closure(), actor_id="tester", actor_role="researcher",
        reason="Plan frozen before confirmation."
    )

    qm_a = GovernanceLedger(tmp_path / "qm_a.jsonl")
    qm_a.register_analysis(
        analysis_id=hypothesis["qm_a_analysis_id"], version_id="analysis-v1",
        actor_id="tester", actor_role="researcher"
    )
    qm_a.transition(
        analysis_id=hypothesis["qm_a_analysis_id"], version_id="analysis-v1",
        to_state="FROZEN_FOR_CONFIRMATION", actor_id="tester", actor_role="researcher",
        reason="Freeze exact QM-C2 identity.",
        analysis_identity=plans.build_qm_a_identity("AP-TIMING-001", "v1")
    )
    hypotheses.transition(
        hypothesis_id=hypothesis["hypothesis_id"], hypothesis_version=hypothesis["hypothesis_version"],
        to_state="FROZEN_FOR_CONFIRMATION", actor_id="tester", actor_role="researcher",
        reason="Freeze hypothesis only after plan and QM-A identity."
    )

    result = plans.validate_confirmation_ready(
        analysis_plan_id="AP-TIMING-001", analysis_plan_version="v1",
        hypothesis_registry=hypotheses, qm_a_ledger=qm_a, qm_a_version_id="analysis-v1",
        qm_b_closure=closure()
    )
    assert result["valid"] is True
    assert result["states"] == {
        "analysis_plan": "FROZEN_FOR_CONFIRMATION",
        "hypothesis": "FROZEN_FOR_CONFIRMATION",
        "qm_a": "FROZEN_FOR_CONFIRMATION",
    }


def test_readiness_rejects_wrong_qm_a_analysis_plan_hash(tmp_path):
    hypotheses, hypothesis = registered_hypothesis(tmp_path)
    plans = AnalysisPlanRegistry(tmp_path / "plans.jsonl")
    plans.register_plan(
        record=plan_record(hypothesis), hypothesis_registry=hypotheses,
        actor_id="tester", actor_role="researcher"
    )
    plans.freeze_plan(
        analysis_plan_id="AP-TIMING-001", analysis_plan_version="v1",
        freeze_context=freeze_context(), hypothesis_registry=hypotheses,
        qm_b_closure=closure(), actor_id="tester", actor_role="researcher", reason="Freeze plan."
    )
    wrong_identity = plans.build_qm_a_identity("AP-TIMING-001", "v1")
    wrong_identity["analysis_plan_hash"] = "wrong-plan-hash"

    qm_a = GovernanceLedger(tmp_path / "qm_a.jsonl")
    qm_a.register_analysis(
        analysis_id=hypothesis["qm_a_analysis_id"], version_id="analysis-v1",
        actor_id="tester", actor_role="researcher"
    )
    qm_a.transition(
        analysis_id=hypothesis["qm_a_analysis_id"], version_id="analysis-v1",
        to_state="FROZEN_FOR_CONFIRMATION", actor_id="tester", actor_role="researcher",
        reason="Negative test identity.", analysis_identity=wrong_identity
    )
    hypotheses.transition(
        hypothesis_id=hypothesis["hypothesis_id"], hypothesis_version=hypothesis["hypothesis_version"],
        to_state="FROZEN_FOR_CONFIRMATION", actor_id="tester", actor_role="researcher",
        reason="Freeze hypothesis for readiness test."
    )
    with pytest.raises(AnalysisPlanError, match="confirmation_qm_a_identity_mismatch:analysis_plan_hash"):
        plans.validate_confirmation_ready(
            analysis_plan_id="AP-TIMING-001", analysis_plan_version="v1",
            hypothesis_registry=hypotheses, qm_a_ledger=qm_a, qm_a_version_id="analysis-v1",
            qm_b_closure=closure()
        )


def test_tampered_analysis_plan_registry_fails_closed(tmp_path):
    hypotheses, hypothesis = registered_hypothesis(tmp_path)
    path = tmp_path / "plans.jsonl"
    plans = AnalysisPlanRegistry(path)
    plans.register_plan(
        record=plan_record(hypothesis), hypothesis_registry=hypotheses,
        actor_id="tester", actor_role="researcher"
    )
    line = json.loads(path.read_text(encoding="utf-8"))
    line["payload"]["record"]["primary_estimand"] = "tampered"
    path.write_text(json.dumps(line) + "\n", encoding="utf-8")
    with pytest.raises(AnalysisPlanError, match="analysis_plan_registry_entry_hash_invalid"):
        plans.verify_integrity()
