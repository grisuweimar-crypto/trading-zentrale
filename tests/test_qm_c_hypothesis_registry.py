import json
from pathlib import Path

import pytest

from scanner.research.governance.qm_a import GovernanceLedger
from scanner.research.governance.qm_c import (
    HypothesisRegistry,
    HypothesisRegistryError,
    hypothesis_version_hash,
    load_qm_c_contract,
)


QM_B_CLOSURE = Path("configs/qm_b_closure_v1.json")


def record(**overrides):
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


def qm_a_identity(hypothesis_hash, **overrides):
    payload = {
        "hypothesis_version_hash": hypothesis_hash,
        "analysis_plan_hash": "plan-hash",
        "code_or_commit_hash": "code-hash",
        "config_hash": "config-hash",
        "dataset_snapshot_hash": "dataset-hash",
        "universe_ledger_version": "qm-b-ledger-v1",
        "instrument_master_version": "qm-b-instrument-master-v1",
        "label_definition_hash": "label-hash",
        "benchmark_definition_hash": "benchmark-hash",
        "environment_or_dependency_fingerprint": "env-hash",
        "evaluation_cohort_id": "cohort-1",
        "temporal_boundary": "2026-09-30",
    }
    payload.update(overrides)
    return payload


def test_contract_is_research_only_and_keeps_later_qm_c_blocks_out_of_scope():
    contract = load_qm_c_contract()
    assert contract["research_only"] is True
    assert contract["productive_integration_enabled"] is False
    assert contract["execution_allowed"] is False
    assert contract["boundaries"]["analysis_plan_implemented_here"] is False
    assert contract["boundaries"]["multiplicity_implemented_here"] is False
    assert contract["boundaries"]["sequential_monitoring_implemented_here"] is False
    assert contract["boundaries"]["scanner_semantics_changed"] is False


def test_registration_creates_stable_hash_and_hash_chained_registry(tmp_path):
    registry = HypothesisRegistry(tmp_path / "hypotheses.jsonl")
    registry.register_hypothesis(record=record(), actor_id="tester", actor_role="researcher")

    saved = registry.get_hypothesis("H-TIMING-001", "v1")
    assert saved["state"] == "DRAFT"
    assert saved["hypothesis_version_hash"] == hypothesis_version_hash(record())

    integrity = registry.verify_integrity()
    assert integrity["valid"] is True
    assert integrity["event_count"] == 1
    assert integrity["hypothesis_version_count"] == 1
    assert integrity["head_hash"]


def test_registered_version_cannot_be_overwritten(tmp_path):
    registry = HypothesisRegistry(tmp_path / "hypotheses.jsonl")
    registry.register_hypothesis(record=record(), actor_id="tester", actor_role="researcher")
    changed = record(hypothesis_statement="A different post-hoc statement.")
    with pytest.raises(HypothesisRegistryError, match="hypothesis_version_already_registered"):
        registry.register_hypothesis(record=changed, actor_id="tester", actor_role="researcher")


def test_successor_version_must_explicitly_supersede_latest(tmp_path):
    registry = HypothesisRegistry(tmp_path / "hypotheses.jsonl")
    registry.register_hypothesis(record=record(), actor_id="tester", actor_role="researcher")

    v2 = record(hypothesis_version="v2", hypothesis_statement="Predeclared successor statement.")
    with pytest.raises(HypothesisRegistryError, match="successor_version_requires_supersedes_reference"):
        registry.register_hypothesis(record=v2, actor_id="tester", actor_role="researcher")

    registry.register_hypothesis(
        record=v2,
        actor_id="tester",
        actor_role="researcher",
        supersedes_hypothesis_version="v1",
    )
    assert registry.get_hypothesis("H-TIMING-001", "v2")["supersedes_hypothesis_version"] == "v1"


def test_discovery_and_confirmation_cannot_silently_change_roles(tmp_path):
    registry = HypothesisRegistry(tmp_path / "hypotheses.jsonl")
    registry.register_hypothesis(
        record=record(research_mode="DISCOVERY"),
        actor_id="tester",
        actor_role="researcher",
    )
    registry.transition(
        hypothesis_id="H-TIMING-001",
        hypothesis_version="v1",
        to_state="EXPLORATORY",
        actor_id="tester",
        actor_role="researcher",
        reason="Discovery work started.",
    )
    with pytest.raises(HypothesisRegistryError, match="hypothesis_transition_forbidden|incompatible_with_mode"):
        registry.transition(
            hypothesis_id="H-TIMING-001",
            hypothesis_version="v1",
            to_state="FROZEN_FOR_CONFIRMATION",
            actor_id="tester",
            actor_role="researcher",
            reason="Must require a successor confirmation version instead.",
        )

    confirmation = record(
        hypothesis_version="v2",
        research_mode="CONFIRMATION",
        hypothesis_statement="Frozen confirmation statement.",
    )
    registry.register_hypothesis(
        record=confirmation,
        actor_id="tester",
        actor_role="researcher",
        supersedes_hypothesis_version="v1",
    )
    with pytest.raises(HypothesisRegistryError, match="incompatible_with_mode"):
        registry.transition(
            hypothesis_id="H-TIMING-001",
            hypothesis_version="v2",
            to_state="EXPLORATORY",
            actor_id="tester",
            actor_role="researcher",
            reason="Confirmation may not become exploratory in-place.",
        )


def test_rejected_hypothesis_is_retained(tmp_path):
    registry = HypothesisRegistry(tmp_path / "hypotheses.jsonl")
    registry.register_hypothesis(record=record(), actor_id="tester", actor_role="researcher")
    registry.transition(
        hypothesis_id="H-TIMING-001",
        hypothesis_version="v1",
        to_state="REJECTED",
        actor_id="tester",
        actor_role="researcher",
        reason="Predefined rejection condition met.",
    )
    assert registry.get_hypothesis("H-TIMING-001", "v1")["state"] == "REJECTED"
    assert registry.verify_integrity()["hypothesis_version_count"] == 1


def test_tampered_registry_fails_closed(tmp_path):
    path = tmp_path / "hypotheses.jsonl"
    registry = HypothesisRegistry(path)
    registry.register_hypothesis(record=record(), actor_id="tester", actor_role="researcher")
    line = json.loads(path.read_text(encoding="utf-8"))
    line["payload"]["record"]["hypothesis_statement"] = "tampered"
    path.write_text(json.dumps(line) + "\n", encoding="utf-8")
    with pytest.raises(HypothesisRegistryError, match="registry_entry_hash_invalid"):
        registry.verify_integrity()


def test_qm_a_binding_accepts_same_frozen_hypothesis_hash(tmp_path):
    registry = HypothesisRegistry(tmp_path / "hypotheses.jsonl")
    registry.register_hypothesis(record=record(), actor_id="tester", actor_role="researcher")
    saved = registry.get_hypothesis("H-TIMING-001", "v1")

    qm_a = GovernanceLedger(tmp_path / "qm_a.jsonl")
    qm_a.register_analysis(
        analysis_id=saved["qm_a_analysis_id"],
        version_id="analysis-v1",
        actor_id="tester",
        actor_role="researcher",
    )
    qm_a.transition(
        analysis_id=saved["qm_a_analysis_id"],
        version_id="analysis-v1",
        to_state="FROZEN_FOR_CONFIRMATION",
        actor_id="tester",
        actor_role="researcher",
        reason="Analysis identity frozen.",
        analysis_identity=qm_a_identity(saved["hypothesis_version_hash"]),
    )
    registry.transition(
        hypothesis_id="H-TIMING-001",
        hypothesis_version="v1",
        to_state="FROZEN_FOR_CONFIRMATION",
        actor_id="tester",
        actor_role="researcher",
        reason="Hypothesis frozen after QM-A identity freeze.",
    )

    result = registry.validate_qm_a_binding(
        hypothesis_id="H-TIMING-001",
        hypothesis_version="v1",
        qm_a_ledger=qm_a,
        qm_a_version_id="analysis-v1",
    )
    assert result["valid"] is True
    assert result["qm_a_state"] == "FROZEN_FOR_CONFIRMATION"
    assert result["hypothesis_version_hash"] == saved["hypothesis_version_hash"]


def test_qm_a_binding_rejects_wrong_frozen_hypothesis_hash(tmp_path):
    registry = HypothesisRegistry(tmp_path / "hypotheses.jsonl")
    registry.register_hypothesis(record=record(), actor_id="tester", actor_role="researcher")
    saved = registry.get_hypothesis("H-TIMING-001", "v1")

    qm_a = GovernanceLedger(tmp_path / "qm_a.jsonl")
    qm_a.register_analysis(
        analysis_id=saved["qm_a_analysis_id"],
        version_id="analysis-v1",
        actor_id="tester",
        actor_role="researcher",
    )
    qm_a.transition(
        analysis_id=saved["qm_a_analysis_id"],
        version_id="analysis-v1",
        to_state="FROZEN_FOR_CONFIRMATION",
        actor_id="tester",
        actor_role="researcher",
        reason="Analysis identity frozen with wrong hash for negative test.",
        analysis_identity=qm_a_identity("wrong-hypothesis-hash"),
    )
    with pytest.raises(HypothesisRegistryError, match="qm_a_hypothesis_version_hash_mismatch"):
        registry.validate_qm_a_binding(
            hypothesis_id="H-TIMING-001",
            hypothesis_version="v1",
            qm_a_ledger=qm_a,
            qm_a_version_id="analysis-v1",
        )


def test_qm_b_current_external_blockers_fail_closed_for_strict_confirmation(tmp_path):
    closure = json.loads(QM_B_CLOSURE.read_text(encoding="utf-8"))
    assert closure["strict_historical_promotion_status"] == "BLOCKED_EXTERNAL_EVIDENCE"
    assert closure["productive_strict_asof_investable_universe_promoted"] is False

    registry = HypothesisRegistry(tmp_path / "hypotheses.jsonl")
    registry.register_hypothesis(
        record=record(universe_requirement="STRICT_ASOF_INVESTABLE"),
        actor_id="tester",
        actor_role="researcher",
    )
    with pytest.raises(HypothesisRegistryError, match="qm_b_strict_universe_not_promoted:BLOCKED_EXTERNAL_EVIDENCE"):
        registry.validate_qm_b_binding(
            hypothesis_id="H-TIMING-001",
            hypothesis_version="v1",
            qm_b_closure=closure,
        )


def test_qm_b_observed_scanner_universe_does_not_claim_strict_promotion(tmp_path):
    closure = json.loads(QM_B_CLOSURE.read_text(encoding="utf-8"))
    registry = HypothesisRegistry(tmp_path / "hypotheses.jsonl")
    registry.register_hypothesis(record=record(), actor_id="tester", actor_role="researcher")
    result = registry.validate_qm_b_binding(
        hypothesis_id="H-TIMING-001",
        hypothesis_version="v1",
        qm_b_closure=closure,
    )
    assert result["valid"] is True
    assert result["strict_universe_required"] is False
    assert result["strict_universe_promoted"] is False
