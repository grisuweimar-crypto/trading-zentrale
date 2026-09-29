from pathlib import Path

import pytest

from scanner.research.governance.qm_a import GovernanceLedger, GovernanceLedgerError


def _identity() -> dict[str, str]:
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


def test_stale_candidate_fails_before_bytes_are_appended(tmp_path: Path):
    ledger = GovernanceLedger(tmp_path / "qm_a.jsonl")
    ledger.register_analysis(
        analysis_id="analysis-1",
        version_id="v1",
        actor_id="researcher-1",
        actor_role="researcher",
    )
    ledger.transition(
        analysis_id="analysis-1",
        version_id="v1",
        to_state="EXPLORATORY",
        actor_id="researcher-1",
        actor_role="researcher",
        reason="valid first writer transition",
    )
    before = ledger.path.read_text(encoding="utf-8")

    # Simulate a second writer that derived its payload from the old DRAFT state.
    # _append_event must re-read and replay the candidate while holding the lock,
    # and reject this stale transition before touching the append-only file.
    with pytest.raises(GovernanceLedgerError, match="transition_from_state_mismatch"):
        ledger._append_event(
            event_type="STATE_TRANSITION",
            analysis_id="analysis-1",
            version_id="v1",
            actor_id="researcher-2",
            actor_role="researcher",
            payload={
                "from_state": "DRAFT",
                "to_state": "FROZEN_FOR_CONFIRMATION",
                "reason": "stale concurrent transition",
                "analysis_identity": _identity(),
                "analysis_identity_hash": None,
                "review_or_approval_reference": "stale-test",
            },
        )

    assert ledger.path.read_text(encoding="utf-8") == before
    assert ledger.verify_integrity()["states"]["analysis-1::v1"] == "EXPLORATORY"
