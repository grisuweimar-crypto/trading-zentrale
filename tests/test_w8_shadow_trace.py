from __future__ import annotations

import json
from datetime import date

import pytest

from scanner.research.decision_layer.depot_action_policy import apply_depot_action_policy
from scanner.research.decision_layer.phase7_state_history import attach_state_history_to_7f
from scanner.research.decision_layer.portfolio_action import compute_portfolio_action
from scanner.research.decision_layer.promotion_validation import validate_shadow_trace_summary
from scanner.research.decision_layer.w8_shadow_trace import (
    W8ShadowTraceError,
    append_private_trace,
    build_w8_trace_row,
    load_private_trace,
    summarize_w8_trace,
)


SYMBOL = "W8TEST"
SNAPSHOT = "w8-test-snapshot"


def _transition(as_of: str) -> dict:
    return {
        "schema_version": "decision_state_transition_v1",
        "phase": "7E",
        "symbol": SYMBOL,
        "as_of": as_of,
        "source_snapshot_id": SNAPSHOT,
        "raw_stance": {
            "state": "positive",
            "direction": "positive",
            "portfolio_independent": True,
            "research_only": True,
        },
        "transition_state": {
            "status": "stable_confirmed",
            "stable_directional_anchor": "positive",
            "pending_direction": None,
            "pending_confirmation_count": 0,
            "required_confirmation_count": 2,
            "stable_anchor_is_current_stance": True,
        },
        "candidate_rule": {
            "rule_id": "minimal_repeat_candidate_v1",
            "min_consecutive_directional_observations": 2,
            "require_distinct_calendar_dates": True,
            "require_distinct_source_snapshot_ids": True,
            "reset_pending_on_nondirectional_raw_state": True,
            "empirically_validated": False,
            "production_eligible": False,
        },
        "events": [],
        "semantics": {
            "raw_7d_stance_preserved": True,
            "conflict_treated_as_neutral": False,
            "insufficient_treated_as_neutral": False,
            "stable_anchor_replaces_raw_stance": False,
            "weighted_super_score_used": False,
            "portfolio_action_computed": False,
            "order_instruction_computed": False,
            "threshold_optimized_on_spent_data": False,
        },
        "validation": {
            "status": "prospective_unconfirmed",
            "latest_research_partition": "prospective_unspent",
            "hysteresis_rule_empirically_validated": False,
            "future_mature_outcomes_required": True,
            "productive_integration_enabled": False,
            "promotion_eligible": False,
        },
    }


def _position() -> dict:
    return {
        "schema_version": "decision_position_snapshot_v1",
        "symbol": SYMBOL,
        "as_of": "2026-10-05T17:59:00+00:00",
        "source_snapshot_id": "private-broker-snapshot",
        "position_state": "long",
        "quantity": 17,
        "currency": "USD",
        "average_entry_price": 123.45,
        "current_price": 130.00,
    }


def _state(as_of: str) -> dict:
    return {
        "schema_version": "decision_state_history_context_v1",
        "symbol": SYMBOL,
        "as_of": as_of,
        "source_snapshot_id": SNAPSHOT,
        "source_claim_id": "risk:W8TEST:path",
        "source_claim_available_from": as_of,
        "source_context_type": "scanner_path_state_v1",
        "state": "overextension_with_momentum_loss",
        "state_sequence": ["overextended", "overextension_with_momentum_loss"],
        "path_memory": {},
        "current": {},
        "dynamics": {},
        "persistence": {},
        "classification": {},
        "historical_matches": {},
        "semantics": {
            "existing_history_transported_not_reconstructed": True,
            "new_overextension_threshold_created": False,
            "new_trading_rule_created": False,
            "directional_vote_created": False,
            "universal_stance_changed": False,
            "portfolio_action_changed": False,
            "w8_action_policy_evaluated": False,
            "missing_values_remain_missing": True,
        },
        "research_only": True,
    }


def _action(as_of: str = "2026-10-05T18:00:00+00:00") -> dict:
    transition = _transition(as_of)
    context = _state(as_of)
    base = compute_portfolio_action(transition, _position())
    attached = attach_state_history_to_7f(base, context)
    return apply_depot_action_policy(attached, context)


def test_w8_trace_whitelists_state_and_excludes_raw_position_values() -> None:
    row = build_w8_trace_row(_action())
    raw = json.dumps(row, sort_keys=True)
    assert row["position_state"] == "long"
    assert row["resolved_action_state"] == "REDUCE_REVIEW"
    assert row["metrics_ready"] is False
    assert row["execution_allowed"] is False
    for forbidden in (
        "quantity",
        "average_entry_price",
        "current_price",
        "market_value",
        "unrealized_pnl",
        "realized_pnl",
    ):
        assert forbidden not in raw


def test_w8_trace_rejects_pre_prospective_observation() -> None:
    with pytest.raises(W8ShadowTraceError, match="pre_w8_prospective_trace_forbidden"):
        build_w8_trace_row(_action("2026-10-01T18:00:00+00:00"))


def test_w8_private_trace_append_is_idempotent_and_summary_stays_unready(tmp_path) -> None:
    path = tmp_path / "w8-private.jsonl"
    row = build_w8_trace_row(_action())
    first = append_private_trace(path, [row])
    second = append_private_trace(path, [row])
    assert first["rows_added"] == 1
    assert second["rows_added"] == 0
    assert second["rows_already_present"] == 1

    rows = load_private_trace(path)
    summary = summarize_w8_trace(rows)
    assert summary["w8_trace_rows"] == 1
    assert summary["captured_layers"] == ["W8"]
    assert summary["layer_metrics_ready"] == {"W8": False}
    assert summary["contains_raw_position_values"] is False
    assert summary["public_repository_persistence"] is False

    validated = validate_shadow_trace_summary(
        summary,
        prospective_start="2026-09-26",
        reviewed_as_of=date(2026, 10, 5),
        w8_prospective_start="2026-10-02",
    )
    assert validated["w8_trace_rows"] == 1
    assert validated["layer_metrics_ready"]["W8"] is False


def test_w8_private_trace_refuses_repository_persistence(tmp_path) -> None:
    from scanner.research.decision_layer import w8_shadow_trace as module

    inside_repo = module.ROOT / "artifacts" / "research" / "w8-private.jsonl"
    with pytest.raises(
        W8ShadowTraceError,
        match="w8_private_trace_must_not_be_persisted_in_repository",
    ):
        append_private_trace(inside_repo, [build_w8_trace_row(_action())])
