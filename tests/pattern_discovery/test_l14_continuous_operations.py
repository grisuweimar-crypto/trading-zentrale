from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import json

import pytest

import scanner.research.pattern_discovery.continuous_operations as l14
from scanner.research.pattern_discovery.continuous_operations import (
    ContinuousOperationsError,
    OperationsRegistry,
    build_operations_cycle,
    evaluate_discovery_trigger,
    verify_operations_cycle,
)


def contract():
    return l14.load_operations_contract()


def manifest(*, cutoff="2026-09-01", budget=20):
    return {
        "run_id": "DISC-TEST",
        "manifest_hash": "a" * 64,
        "preregistration": {
            "data_cutoff": f"{cutoff}T20:00:00+00:00",
            "candidate_budget": {
                "max_frozen_candidates_total": budget,
                "max_frozen_candidates_per_horizon": budget,
            },
        },
    }


def test_regular_discovery_requires_all_versioned_trigger_conditions():
    history = {
        "available": True,
        "baseline_observation_count": 1000,
        "new_observation_count": 200,
        "new_trading_dates": [
            f"2026-09-{day:02d}" for day in range(2, 22)
        ],
    }
    result = evaluate_discovery_trigger(
        history_information=history,
        last_regular_manifest=manifest(),
        run_processed=True,
        unprocessed_candidate_count=0,
        candidate_budget_total=20,
        contract=contract(),
    )
    assert result["eligible"] is True
    assert result["status"] == "ELIGIBLE_REGULAR"
    assert all(result["checks"].values())
    assert result["automatic_search_start_allowed"] is False

    blocked = deepcopy(history)
    blocked["new_observation_count"] = 199
    result = evaluate_discovery_trigger(
        history_information=blocked,
        last_regular_manifest=manifest(),
        run_processed=True,
        unprocessed_candidate_count=0,
        candidate_budget_total=20,
        contract=contract(),
    )
    assert result["eligible"] is False
    assert result["status"] == "BLOCKED_REGULAR_TRIGGER"
    assert (
        "MINIMUM_USABLE_OBSERVATION_GROWTH_FRACTION"
        in result["reason_codes"]
    )


def test_candidate_budget_blocks_unprocessed_backlog():
    history = {
        "available": True,
        "baseline_observation_count": 1000,
        "new_observation_count": 300,
        "new_trading_dates": [
            f"2026-09-{day:02d}" for day in range(2, 27)
        ],
    }
    result = evaluate_discovery_trigger(
        history_information=history,
        last_regular_manifest=manifest(budget=20),
        run_processed=False,
        unprocessed_candidate_count=6,
        candidate_budget_total=20,
        contract=contract(),
    )
    assert result["eligible"] is False
    assert result["metrics"]["open_unprocessed_candidate_fraction"] == 0.3
    assert (
        result["checks"][
            "previous_run_processed_or_open_budget_below_limit"
        ]
        is False
    )


def test_extraordinary_discovery_requires_explicit_predeclared_reason():
    result = evaluate_discovery_trigger(
        history_information={"available": False},
        last_regular_manifest=manifest(),
        run_processed=False,
        unprocessed_candidate_count=99,
        candidate_budget_total=20,
        extraordinary_request={
            "reason_code": "NEW_PIT_SAFE_FEATURE",
            "evidence_hash": "b" * 64,
            "requested_at": "2026-10-07T20:00:00Z",
        },
        contract=contract(),
    )
    assert result["eligible"] is True
    assert result["mode"] == "EXTRAORDINARY"
    assert result["new_l1_preregistration_required"] is True
    assert result["automatic_search_start_allowed"] is False

    with pytest.raises(
        ContinuousOperationsError,
        match="extraordinary_reason_not_allowed",
    ):
        evaluate_discovery_trigger(
            history_information={"available": True},
            last_regular_manifest=manifest(),
            run_processed=True,
            unprocessed_candidate_count=0,
            candidate_budget_total=20,
            extraordinary_request={
                "reason_code": "I_SAW_A_GOOD_CHART",
                "evidence_hash": "b" * 64,
                "requested_at": "2026-10-07T20:00:00Z",
            },
            contract=contract(),
        )


def _patch_cycle_environment(monkeypatch, *, qm_status="PASS"):
    monkeypatch.setattr(
        l14,
        "_current_scanner_state",
        lambda root: {
            "snapshot_id": "SNAP-NEW",
            "as_of": "2026-10-07",
            "generated_at": "2026-10-07T19:00:00Z",
            "required_symbol_count": 200,
            "observed_symbol_count": 200,
        },
    )
    monkeypatch.setattr(l14, "_manifest_rows", lambda root: [])
    monkeypatch.setattr(l14, "_freeze_rows", lambda root: [])
    monkeypatch.setattr(
        l14,
        "_rating_state",
        lambda root: (
            {},
            [],
            {"valid": True, "registry_event_count": 0, "head_hash": None},
        ),
    )
    monkeypatch.setattr(
        l14,
        "_confirmation_state",
        lambda root: (
            [],
            {"valid": True, "registry_event_count": 0, "look_count": 0, "head_hash": None},
        ),
    )
    monkeypatch.setattr(
        l14,
        "_claim_state",
        lambda root: {
            "registry_event_count": 0,
            "registry_claim_count": 0,
            "registry_head_hash": None,
        },
    )
    monkeypatch.setattr(
        l14,
        "_maturation_state",
        lambda root: {
            "registry_event_count": 0,
            "registry_outcome_count": 0,
            "registry_head_hash": None,
        },
    )
    monkeypatch.setattr(
        l14,
        "_dependency_state",
        lambda root, frozen: {
            "current_pattern_set_hash": l14._hash([]),
            "graph_count": 0,
            "current_graph_available": True,
            "current_graph_hash": None,
            "graphs": [],
        },
    )
    monkeypatch.setattr(l14, "_captured_snapshot_ids", lambda root: set())
    monkeypatch.setattr(
        l14,
        "_history_information",
        lambda root, cutoff=None: {
            "available": True,
            "reason": None,
            "observation_count": 1000,
            "trading_dates": ["2026-10-07"],
            "baseline_observation_count": None,
            "new_observation_count": None,
            "new_trading_dates": [],
        },
    )
    monkeypatch.setattr(
        l14,
        "_qm_state",
        lambda root, evaluator, contract: {
            "status": qm_status,
            "error": None if qm_status == "PASS" else "test-block",
            "continuous_qm_receipt_hash": (
                "c" * 64 if qm_status == "PASS" else None
            ),
            "qm_c_bindings": {
                "negative_results": "QM-C5"
            },
            "negative_result_registry": {
                "valid": True,
                "event_count": 0,
                "head_hash": None,
            },
            "negative_result_authority": "QM-C5",
            "parallel_negative_registry_created": False,
        },
    )


def test_operations_cycle_is_deterministic_research_only_scheduler(
    monkeypatch, tmp_path
):
    _patch_cycle_environment(monkeypatch)
    cycle = build_operations_cycle(
        tmp_path,
        evaluated_at="2026-10-07T22:40:00Z",
        contract=contract(),
    )
    assert cycle["cycle_status"] == "READY_WITH_WORK"
    assert cycle["discovery"]["trigger"]["mode"] == "INITIAL"
    assert cycle["work_items"][0]["status"] == "DUE_PREREGISTRATION"
    assert cycle["work_items"][6]["authority"] == "QM-C5"
    assert cycle["work_items"][6]["parallel_registry_created"] is False
    assert cycle["boundaries"]["l12_promotion_performed"] is False
    assert cycle["boundaries"]["productive_decision_change_performed"] is False
    assert verify_operations_cycle(cycle)["valid"] is True

    tampered = deepcopy(cycle)
    tampered["work_items"][0]["automatic_statistical_override_allowed"] = True
    with pytest.raises(
        ContinuousOperationsError,
        match="operations_cycle_hash_mismatch",
    ):
        verify_operations_cycle(tampered)


def test_qm_failure_blocks_every_scientific_due_stage(monkeypatch, tmp_path):
    _patch_cycle_environment(monkeypatch, qm_status="BLOCKED")
    cycle = build_operations_cycle(
        tmp_path,
        evaluated_at="2026-10-07T22:40:00Z",
        contract=contract(),
    )
    assert cycle["cycle_status"] == "BLOCKED_FAIL_CLOSED"
    assert "CONTINUOUS_QM_BLOCKED" in cycle["blocking_reasons"]
    for item in cycle["work_items"][:6]:
        assert item["status"] == "BLOCKED"


def test_decay_alerts_are_warnings_not_rating_mutations():
    alerts = l14._decay_alerts(
        rating_events=[
            {
                "transition": {
                    "pattern_id": "PAT-A",
                    "pattern_version": "v1",
                    "pattern_spec_hash": "d" * 64,
                    "previous_rating": "A",
                    "new_rating": "B",
                    "observed_at": "2026-10-07T20:00:00Z",
                    "transition_hash": "e" * 64,
                }
            }
        ],
        confirmation_events=[],
        current_ratings={"PAT-A::v1::" + "d" * 64: "B"},
        since_at="2026-10-06T20:00:00Z",
        contract=contract(),
    )
    assert len(alerts) == 1
    assert alerts[0]["level"] == "WARNING"
    assert alerts[0]["rating_change_performed_by_alert"] is False


def _minimal_cycle():
    spec = contract()
    work = []
    for stage in spec["scheduler"]["stage_order"]:
        row = {"stage": stage, "status": "PASS"}
        if stage == "NEGATIVE_RESULT_AUDIT":
            row.update({
                "authority": "QM-C5",
                "parallel_registry_created": False,
            })
        work.append(row)
    cycle = {
        "schema_version": l14.CYCLE_SCHEMA_VERSION,
        "module": "pattern_discovery_lab",
        "phase": "L14",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "evaluated_at": "2026-10-07T22:40:00Z",
        "cycle_status": "QUIET",
        "blocking_reasons": [],
        "current_scanner": {},
        "history_information": {},
        "discovery": {},
        "laboratory_state": {},
        "work_items": work,
        "decay_alerts": [],
        "qm_audit": {},
        "source_heads": {},
        "l14_contract_hash": l14.operations_contract_hash(spec),
        "boundaries": dict(spec["boundaries"]),
    }
    cycle["cycle_hash"] = l14._hash(cycle)
    return cycle


def test_operations_registry_is_hash_chained_and_idempotent(tmp_path):
    registry = OperationsRegistry(tmp_path / "operations.jsonl")
    cycle = _minimal_cycle()
    first = registry.record_cycle(
        cycle, actor_id="ci", actor_role="automation"
    )
    second = registry.record_cycle(
        cycle, actor_id="ci", actor_role="automation"
    )
    assert first["idempotent"] is False
    assert second["idempotent"] is True

    classification = registry.classify_discovery_run(
        run_id="DISC-TEST",
        manifest_hash="f" * 64,
        mode="INITIAL",
        trigger_cycle_hash=cycle["cycle_hash"],
        recorded_at="2026-10-07T22:41:00Z",
        actor_id="ci",
        actor_role="automation",
    )
    assert classification["idempotent"] is False
    assert registry.verify_integrity()["event_count"] == 2

    rows = [
        json.loads(line)
        for line in (tmp_path / "operations.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    rows[0]["actor_id"] = "tampered"
    (tmp_path / "operations.jsonl").write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(
        ContinuousOperationsError,
        match="operations_registry_hash_invalid",
    ):
        registry.verify_integrity()


def test_l14_workflow_stages_first_run_receipts_before_diff():
    """New L14 cycles are untracked initially and must still be committed."""
    workflow = Path(".github/workflows/pattern_discovery_l14.yml").read_text(
        encoding="utf-8"
    )
    receipt_step = workflow.split(
        "- name: Persist operations receipt on main", 1
    )[1].split("- name: Enforce fail-closed cycle result", 1)[0]
    stage = "git add -A -- artifacts/research/pattern_discovery/operations/"
    check = (
        "git diff --cached --quiet -- "
        "artifacts/research/pattern_discovery/operations/"
    )
    assert stage in receipt_step
    assert check in receipt_step
    assert receipt_step.index(stage) < receipt_step.index(check)
    assert (
        "git diff --quiet -- artifacts/research/pattern_discovery/operations/"
        not in receipt_step
    )


def test_l14_main_push_produces_verifiable_first_real_receipt():
    """After merge the on-main job runs, not only test steps on pull requests."""
    workflow = Path(".github/workflows/pattern_discovery_l14.yml").read_text(
        encoding="utf-8"
    )
    scheduled = workflow.split("  scheduled-cycle:", 1)[1]
    assert "github.event_name == 'push'" in scheduled
    assert "github.ref == 'refs/heads/main'" in scheduled
    assert "needs: l14-regression" in scheduled
    assert "run_l14_operations.py cycle" in scheduled
    assert "run_l14_operations.py verify-registry" in scheduled
    assert "verify_operations_cycle(cycle)" in scheduled
    assert scheduled.index("run_l14_operations.py verify-registry") < scheduled.index(
        "- name: Persist operations receipt on main"
    )
    assert "permissions:\n  contents: read\n" in workflow
    assert "scheduled-cycle:\n    permissions:\n      contents: write" in workflow
    assert 'schedule:' in workflow
    assert 'cron: "40 22 * * *"' in workflow
