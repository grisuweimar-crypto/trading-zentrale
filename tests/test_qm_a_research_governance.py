from __future__ import annotations

import json
from pathlib import Path

import pytest

from scanner.research.governance.qm_a import (
    GovernanceLedger,
    GovernanceLedgerError,
    classify_change,
    load_qm_a_contract,
)


def identity() -> dict[str, str]:
    return {
        "hypothesis_version_hash": "h-hyp-v1",
        "analysis_plan_hash": "h-plan-v1",
        "code_or_commit_hash": "commit-abc123",
        "config_hash": "h-config-v1",
        "dataset_snapshot_hash": "h-data-v1",
        "universe_ledger_version": "universe-v1",
        "instrument_master_version": "instrument-v1",
        "label_definition_hash": "h-label-v1",
        "benchmark_definition_hash": "h-benchmark-v1",
        "environment_or_dependency_fingerprint": "python311-lock-v1",
        "evaluation_cohort_id": "cohort-2026q4",
        "temporal_boundary": "prospective_from_2026-09-26",
    }


def inspection_record(**overrides) -> dict:
    record = {
        "dataset_snapshot_id": "snapshot-1",
        "label_version_id": "label-v1",
        "universe_ledger_version_id": "universe-v1",
        "analysis_plan_version_id": "plan-v1",
        "artifact_hash": "artifact-sha256",
        "inspection_scope": "primary confirmatory summary",
        "outcome_visibility_level": "NONE",
        "access_mode": "GENERATED_REPORT",
        "question": "Does the frozen analysis satisfy its preregistered rule?",
        "reason_for_access": "scheduled confirmatory review",
        "decision_taken": "no design change",
        "change_class": "MECHANICALLY_EQUIVALENT_REPAIR",
        "spent_for_design": False,
        "affected_hypothesis_ids": ["H-1"],
        "code_or_config_version": "commit-abc123",
        "evidence_effect": "NO_OUTCOME_EVIDENCE_SEEN",
        "review_or_approval_reference": "review-QMA-1",
        "successor_version_id": None,
        "reviewer_actor_id": "reviewer-2",
        "independent_review": True,
    }
    record.update(overrides)
    return record


def make_ledger(tmp_path: Path) -> GovernanceLedger:
    return GovernanceLedger(tmp_path / "qm_a.jsonl")


def register(ledger: GovernanceLedger, version: str = "v1", *, supersedes: str | None = None) -> None:
    ledger.register_analysis(
        analysis_id="analysis-1",
        version_id=version,
        actor_id="researcher-1",
        actor_role="researcher",
        supersedes_version_id=supersedes,
        description="QM-A regression fixture",
    )


def freeze(ledger: GovernanceLedger, version: str = "v1") -> None:
    ledger.transition(
        analysis_id="analysis-1",
        version_id=version,
        to_state="FROZEN_FOR_CONFIRMATION",
        actor_id="researcher-1",
        actor_role="researcher",
        reason="freeze complete analysis identity before outcomes",
        analysis_identity=identity(),
        review_or_approval_reference="freeze-review-1",
    )


def test_contract_is_research_only_and_not_productive():
    contract = load_qm_a_contract()
    assert contract["module"] == "QM-A"
    assert contract["research_only"] is True
    assert contract["productive_integration_enabled"] is False
    assert contract["execution_allowed"] is False
    assert contract["principles"]["spent_evidence_may_not_be_relabelled_unspent"] is True
    assert contract["principles"]["uncertain_equivalence_defaults_to_new_version"] is True


def test_uncertain_equivalence_defaults_to_preventive_new_version():
    assert classify_change(equivalence_demonstrated=False, outcome_driven=False) == "PREVENTIVE_QA_NEW_VERSION"
    assert classify_change(equivalence_demonstrated=True, outcome_driven=False) == "MECHANICALLY_EQUIVALENT_REPAIR"
    assert classify_change(equivalence_demonstrated=True, outcome_driven=True) == "OUTCOME_DRIVEN_RESEARCH_CHANGE"


def test_freeze_requires_complete_immutable_identity(tmp_path: Path):
    ledger = make_ledger(tmp_path)
    register(ledger)
    broken = identity()
    broken.pop("dataset_snapshot_hash")
    with pytest.raises(GovernanceLedgerError, match="analysis_identity_missing"):
        ledger.transition(
            analysis_id="analysis-1",
            version_id="v1",
            to_state="FROZEN_FOR_CONFIRMATION",
            actor_id="researcher-1",
            actor_role="researcher",
            reason="attempt incomplete freeze",
            analysis_identity=broken,
        )


def test_identity_cannot_change_after_freeze(tmp_path: Path):
    ledger = make_ledger(tmp_path)
    register(ledger)
    freeze(ledger)
    changed = identity()
    changed["config_hash"] = "changed-after-outcomes"
    with pytest.raises(GovernanceLedgerError, match="analysis_identity_changed_after_freeze"):
        ledger.transition(
            analysis_id="analysis-1",
            version_id="v1",
            to_state="CONFIRMATORY_EVALUATED",
            actor_id="researcher-1",
            actor_role="researcher",
            reason="forbidden identity drift",
            analysis_identity=changed,
        )


def test_evaluated_confirmatory_evidence_cannot_return_to_unspent_state(tmp_path: Path):
    ledger = make_ledger(tmp_path)
    register(ledger)
    freeze(ledger)
    ledger.transition(
        analysis_id="analysis-1",
        version_id="v1",
        to_state="CONFIRMATORY_EVALUATED",
        actor_id="reviewer-2",
        actor_role="reviewer",
        reason="scheduled confirmatory evaluation",
    )
    with pytest.raises(GovernanceLedgerError, match="transition_forbidden"):
        ledger.transition(
            analysis_id="analysis-1",
            version_id="v1",
            to_state="EXPLORATORY",
            actor_id="researcher-1",
            actor_role="researcher",
            reason="attempt to reuse spent evidence",
        )


def test_outcome_driven_change_requires_successor_and_spends_design_evidence(tmp_path: Path):
    ledger = make_ledger(tmp_path)
    register(ledger)
    freeze(ledger)

    bad = inspection_record(
        access_mode="DASHBOARD",
        outcome_visibility_level="AGGREGATE",
        change_class="OUTCOME_DRIVEN_RESEARCH_CHANGE",
        spent_for_design=False,
        evidence_effect="OUTCOME_EVIDENCE_INSPECTED",
        successor_version_id=None,
    )
    with pytest.raises(GovernanceLedgerError):
        ledger.log_inspection(
            analysis_id="analysis-1",
            version_id="v1",
            actor_id="researcher-1",
            actor_role="researcher",
            record=bad,
        )

    good = inspection_record(
        access_mode="DASHBOARD",
        outcome_visibility_level="AGGREGATE",
        decision_taken="change threshold after inspecting outcome summary",
        change_class="OUTCOME_DRIVEN_RESEARCH_CHANGE",
        spent_for_design=True,
        evidence_effect="SPENT_FOR_DESIGN",
        successor_version_id="v2",
    )
    ledger.log_inspection(
        analysis_id="analysis-1",
        version_id="v1",
        actor_id="researcher-1",
        actor_role="researcher",
        record=good,
    )
    state = ledger.get_analysis("analysis-1", "v1")
    assert state["spent_for_design"] is True
    assert state["outcome_evidence_inspected"] is True


def test_aggregate_views_are_recorded_as_outcome_evidence_even_without_design_spend(tmp_path: Path):
    ledger = make_ledger(tmp_path)
    register(ledger)
    ledger.log_inspection(
        analysis_id="analysis-1",
        version_id="v1",
        actor_id="researcher-1",
        actor_role="researcher",
        record=inspection_record(
            access_mode="AGGREGATE_METRIC",
            outcome_visibility_level="AGGREGATE",
            decision_taken="open a preventive successor for unrelated provenance hardening",
            change_class="PREVENTIVE_QA_NEW_VERSION",
            spent_for_design=False,
            evidence_effect="OUTCOME_EVIDENCE_INSPECTED",
            successor_version_id="v2",
        ),
    )
    state = ledger.get_analysis("analysis-1", "v1")
    assert state["outcome_evidence_inspected"] is True
    assert state["spent_for_design"] is False


def test_mechanical_repair_requires_equivalence_reference(tmp_path: Path):
    ledger = make_ledger(tmp_path)
    register(ledger)
    with pytest.raises(GovernanceLedgerError, match="mechanical_repair_requires_equivalence_reference"):
        ledger.log_inspection(
            analysis_id="analysis-1",
            version_id="v1",
            actor_id="researcher-1",
            actor_role="researcher",
            record=inspection_record(review_or_approval_reference=""),
        )


def test_same_actor_cannot_claim_independent_review(tmp_path: Path):
    ledger = make_ledger(tmp_path)
    register(ledger)
    with pytest.raises(GovernanceLedgerError, match="same_actor_review_cannot_be_marked_independent"):
        ledger.log_inspection(
            analysis_id="analysis-1",
            version_id="v1",
            actor_id="researcher-1",
            actor_role="researcher",
            record=inspection_record(reviewer_actor_id="researcher-1", independent_review=True),
        )


def test_superseding_version_is_explicit_not_in_place(tmp_path: Path):
    ledger = make_ledger(tmp_path)
    register(ledger, "v1")
    register(ledger, "v2", supersedes="v1")
    v2 = ledger.get_analysis("analysis-1", "v2")
    assert v2["supersedes_version_id"] == "v1"
    assert v2["state"] == "DRAFT"
    with pytest.raises(GovernanceLedgerError, match="analysis_version_already_registered"):
        register(ledger, "v1")


def test_hash_chain_detects_in_place_tampering(tmp_path: Path):
    ledger = make_ledger(tmp_path)
    register(ledger)
    original = ledger.path.read_text(encoding="utf-8")
    ledger.path.write_text(original.replace("QM-A regression fixture", "tampered fixture"), encoding="utf-8")
    with pytest.raises(GovernanceLedgerError, match="ledger_entry_hash_invalid"):
        ledger.verify_integrity()


def test_ledger_verification_reports_head_state(tmp_path: Path):
    ledger = make_ledger(tmp_path)
    register(ledger)
    ledger.transition(
        analysis_id="analysis-1",
        version_id="v1",
        to_state="EXPLORATORY",
        actor_id="researcher-1",
        actor_role="researcher",
        reason="exploratory work before confirmation freeze",
    )
    result = ledger.verify_integrity()
    assert result["valid"] is True
    assert result["event_count"] == 2
    assert result["analysis_version_count"] == 1
    assert result["states"]["analysis-1::v1"] == "EXPLORATORY"
    assert len(result["head_hash"]) == 64
