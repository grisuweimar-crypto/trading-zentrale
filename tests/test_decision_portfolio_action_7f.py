from __future__ import annotations

import copy

import pytest

from scanner.research.decision_layer.portfolio_action import (
    PortfolioActionError,
    compute_portfolio_action,
    validate_portfolio_action,
    validate_position_snapshot,
)


def _transition(raw_state="positive", status="stable_confirmed", stable="positive", pending=None):
    direction = raw_state if raw_state in {"positive", "negative"} else None
    return {
        "schema_version": "decision_state_transition_v1",
        "phase": "7E",
        "symbol": "TEST",
        "as_of": "2026-09-28T18:00:00+02:00",
        "source_snapshot_id": "stance-snap",
        "raw_stance": {
            "state": raw_state,
            "direction": direction,
            "portfolio_independent": True,
            "research_only": True,
        },
        "transition_state": {
            "status": status,
            "stable_directional_anchor": stable,
            "pending_direction": pending,
            "pending_confirmation_count": 1 if pending else 0,
            "required_confirmation_count": 2,
            "stable_anchor_is_current_stance": (
                raw_state in {"positive", "negative"}
                and raw_state == stable
                and status not in {"transition_pending", "bootstrap_pending"}
            ),
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


def _position(state="flat", **extra):
    base = {
        "schema_version": "decision_position_snapshot_v1",
        "symbol": "TEST",
        "as_of": "2026-09-28T17:59:00+02:00",
        "source_snapshot_id": "broker-snap",
        "position_state": state,
    }
    if state == "long":
        base["quantity"] = 10
    base.update(extra)
    return base


def _swing(*contexts):
    return {
        "source": "elliott_vnext_6h",
        "source_output_id": "elliott-output-1",
        "as_of": "2026-09-28T17:50:00+02:00",
        "review_contexts": list(contexts),
        "routing_is_trade_decision": False,
        "research_only": True,
    }


def test_flat_positive_confirmed_maps_to_entry_review():
    result = compute_portfolio_action(_transition(), _position())
    assert result["portfolio_action"]["state"] == "ENTER_REVIEW"
    assert result["portfolio_action"]["execution_allowed"] is False
    assert result["portfolio_action"]["order_instruction"] is None


def test_flat_positive_pending_waits_for_confirmation():
    transition = _transition("positive", "bootstrap_pending", stable=None, pending="positive")
    result = compute_portfolio_action(transition, _position())
    assert result["portfolio_action"]["state"] == "WAIT_CONFIRMATION"


def test_flat_negative_confirmed_is_no_action():
    result = compute_portfolio_action(
        _transition("negative", "stable_confirmed", stable="negative"), _position()
    )
    assert result["portfolio_action"]["state"] == "NO_ACTION"


def test_long_positive_confirmed_holds_by_default():
    result = compute_portfolio_action(_transition(), _position("long"))
    assert result["portfolio_action"]["state"] == "HOLD"


def test_long_negative_confirmed_maps_to_exit_review():
    result = compute_portfolio_action(
        _transition("negative", "transition_confirmed", stable="negative"), _position("long")
    )
    assert result["portfolio_action"]["state"] == "EXIT_REVIEW"


def test_negative_raw_pending_from_positive_anchor_waits_and_preserves_raw_negative():
    transition = _transition("negative", "transition_pending", stable="positive", pending="negative")
    result = compute_portfolio_action(transition, _position("long"))
    assert result["portfolio_action"]["state"] == "WAIT_CONFIRMATION"
    assert result["universal_stance_context"]["raw_state"] == "negative"
    assert result["transition_context"]["stable_directional_anchor"] == "positive"


@pytest.mark.parametrize("raw,status", [
    ("conflicted", "blocked_conflict"),
    ("insufficient_evidence", "blocked_insufficient"),
])
def test_blocking_states_do_not_open_or_force_close(raw, status):
    flat = compute_portfolio_action(_transition(raw, status, stable=None), _position())
    held = compute_portfolio_action(_transition(raw, status, stable="positive"), _position("long"))
    assert flat["portfolio_action"]["state"] == "NO_ACTION"
    assert held["portfolio_action"]["state"] == "HOLD"
    assert held["universal_stance_context"]["raw_state"] == raw


def test_positive_long_add_context_requires_explicit_capacity():
    unknown = compute_portfolio_action(
        _transition(), _position("long"), _swing("entry_or_add_review")
    )
    allowed = compute_portfolio_action(
        _transition(), _position("long", can_add=True), _swing("entry_or_add_review")
    )
    assert unknown["portfolio_action"]["state"] == "HOLD"
    assert unknown["position_context"]["add_capacity_state"] == "unknown"
    assert allowed["portfolio_action"]["state"] == "ADD_REVIEW"


def test_positive_long_reduce_context_maps_to_reduce_not_exit():
    result = compute_portfolio_action(
        _transition(), _position("long"), _swing("larger_reduce_or_exit_review")
    )
    assert result["portfolio_action"]["state"] == "REDUCE_REVIEW"
    assert result["universal_stance_context"]["raw_state"] == "positive"


def test_conflicting_add_and_reduce_swing_contexts_are_not_arbitrarily_resolved():
    result = compute_portfolio_action(
        _transition(),
        _position("long", can_add=True),
        _swing("entry_or_add_review", "profit_protection_review"),
    )
    assert result["portfolio_action"]["state"] == "HOLD"
    assert result["swing_management"]["context_conflict"] is True


def test_profit_and_loss_are_context_only_and_do_not_change_action():
    gain = compute_portfolio_action(
        _transition(),
        _position("long", currency="EUR", average_entry_price=100, current_price=120),
    )
    loss = compute_portfolio_action(
        _transition(),
        _position("long", currency="EUR", average_entry_price=100, current_price=80),
    )
    assert gain["portfolio_action"]["state"] == loss["portfolio_action"]["state"] == "HOLD"
    assert gain["pnl_context"]["unrealized_return_pct"] == pytest.approx(20.0)
    assert loss["pnl_context"]["unrealized_return_pct"] == pytest.approx(-20.0)
    assert gain["pnl_context"]["used_for_action_direction"] is False


def test_missing_prices_remain_missing():
    result = compute_portfolio_action(_transition(), _position("long"))
    assert result["position_context"]["current_price"] is None
    assert result["pnl_context"]["unrealized_return_pct"] is None
    assert result["semantics"]["score_used_as_price_proxy"] is False


def test_future_position_snapshot_fails_closed():
    with pytest.raises(PortfolioActionError, match="future_position_snapshot"):
        compute_portfolio_action(
            _transition(), _position(as_of="2026-09-28T19:00:00+02:00")
        )


def test_future_swing_context_fails_closed():
    future = _swing("hold_review")
    future["as_of"] = "2026-09-28T19:00:00+02:00"
    with pytest.raises(PortfolioActionError, match="future_swing_context"):
        compute_portfolio_action(_transition(), _position("long"), future)


def test_short_position_is_not_silently_supported():
    bad = _position()
    bad["position_state"] = "short"
    with pytest.raises(PortfolioActionError, match="unsupported_position_state_long_only_v1"):
        validate_position_snapshot(bad)


def test_flat_positive_quantity_is_inconsistent():
    with pytest.raises(PortfolioActionError, match="flat_position_cannot_have_positive_size"):
        validate_position_snapshot(_position("flat", quantity=1))


def test_contradictory_add_capacity_fails_closed():
    with pytest.raises(PortfolioActionError, match="contradictory_add_capacity"):
        validate_position_snapshot(_position("long", can_add=False, remaining_adds=1))


def test_validator_rejects_execution_or_sizing_fields():
    result = compute_portfolio_action(_transition(), _position())
    bad = copy.deepcopy(result)
    bad["portfolio_action"]["execution_allowed"] = True
    with pytest.raises(PortfolioActionError, match="portfolio_action_must_remain_review_only"):
        validate_portfolio_action(bad)

    bad2 = copy.deepcopy(result)
    bad2["target_weight"] = 0.1
    with pytest.raises(PortfolioActionError, match="forbidden_sizing_or_order_fields"):
        validate_portfolio_action(bad2)
