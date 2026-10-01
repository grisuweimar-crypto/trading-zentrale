from __future__ import annotations

from scanner.research.decision_layer.depot_action_policy import apply_depot_action_policy
from scanner.research.decision_layer.phase7_state_history import attach_state_history_to_7f
from scanner.research.decision_layer.portfolio_action import compute_portfolio_action


SYMBOL = "RACE"
AS_OF = "2026-09-29T18:00:00+00:00"
SNAPSHOT = "race-snapshot"


def _transition(raw_state="positive", status="stable_confirmed", stable="positive", pending=None):
    direction = raw_state if raw_state in {"positive", "negative"} else None
    return {
        "schema_version": "decision_state_transition_v1",
        "phase": "7E",
        "symbol": SYMBOL,
        "as_of": AS_OF,
        "source_snapshot_id": SNAPSHOT,
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


def _position(**extra):
    base = {
        "schema_version": "decision_position_snapshot_v1",
        "symbol": SYMBOL,
        "as_of": "2026-09-29T17:59:00+00:00",
        "source_snapshot_id": "race-broker-snapshot",
        "position_state": "long",
        "quantity": 1,
        "currency": "USD",
        "average_entry_price": 350.0,
        "current_price": 410.0,
    }
    base.update(extra)
    return base


def _swing(*contexts):
    return {
        "source": "elliott_vnext_6h",
        "source_output_id": "race-elliott-6h",
        "as_of": "2026-09-29T17:50:00+00:00",
        "review_contexts": list(contexts),
        "routing_is_trade_decision": False,
        "research_only": True,
    }


def _state(state):
    return {
        "schema_version": "decision_state_history_context_v1",
        "symbol": SYMBOL,
        "as_of": AS_OF,
        "source_snapshot_id": SNAPSHOT,
        "source_claim_id": "risk:RACE:scanner-path:2026-09-29",
        "source_claim_available_from": AS_OF,
        "source_context_type": "scanner_path_state_v1",
        "state": state,
        "state_sequence": {
            "never_overextended": ["never_overextended"],
            "overextended": ["overextended"],
            "overextension_with_momentum_loss": [
                "overextended", "overextension_with_momentum_loss"
            ],
            "post_overextension_correction": [
                "overextended", "post_overextension_correction"
            ],
        }[state],
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


def _resolve(transition=None, position=None, swing=None, state=None):
    transition = transition or _transition()
    position = position or _position()
    base = compute_portfolio_action(transition, position, swing_context=swing)
    context = _state(state) if state is not None else None
    attached = attach_state_history_to_7f(base, context)
    return base, apply_depot_action_policy(attached, context)


def test_f1_overextended_but_strong_holds():
    base, result = _resolve(state="overextended")
    assert base["portfolio_action"]["state"] == "HOLD"
    assert result["portfolio_action"]["state"] == "HOLD"
    assert result["depot_action_policy"]["policy_case"] == "f1_overextended_but_strong_hold"
    assert result["universal_stance_context"] == base["universal_stance_context"]


def test_f2_overextension_with_momentum_loss_triggers_timely_reduce_review():
    base, result = _resolve(state="overextension_with_momentum_loss")
    assert base["portfolio_action"]["state"] == "HOLD"
    assert result["portfolio_action"]["state"] == "REDUCE_REVIEW"
    assert result["depot_action_policy"]["policy_case"] == "f2_profit_protection_before_correction"
    assert result["depot_action_policy"]["action_changed"] is True
    assert result["cost_context"]["cost_sensitive_action"] is True
    assert result["portfolio_action"]["execution_allowed"] is False
    assert result["portfolio_action"]["order_instruction"] is None
    assert result["universal_stance_context"] == base["universal_stance_context"]


def test_f3_post_correction_suppresses_late_profit_protection_sell():
    base, result = _resolve(
        state="post_overextension_correction",
        swing=_swing("profit_protection_review"),
    )
    assert base["portfolio_action"]["state"] == "REDUCE_REVIEW"
    assert result["portfolio_action"]["state"] == "HOLD"
    assert result["depot_action_policy"]["policy_case"] == "f3_delayed_profit_protection_suppressed"
    assert result["depot_action_policy"]["reassessment_required"] is True
    assert result["depot_action_policy"]["warning_code"] == "profit_protection_window_already_passed"
    assert result["swing_management"]["pre_w8_mode"] == "position_reduce_review"
    assert result["swing_management"]["mode"] == "position_hold"
    assert result["cost_context"]["cost_sensitive_action"] is False


def test_f4_confirmed_negative_higher_level_evidence_exits_even_after_correction():
    transition = _transition("negative", "transition_confirmed", stable="negative")
    base, result = _resolve(
        transition=transition,
        state="post_overextension_correction",
        swing=_swing("entry_or_add_review"),
    )
    assert base["portfolio_action"]["state"] == "EXIT_REVIEW"
    assert result["portfolio_action"]["state"] == "EXIT_REVIEW"
    assert result["depot_action_policy"]["policy_case"] == "f4_confirmed_negative_exit_review"
    assert result["universal_stance_context"]["raw_state"] == "negative"


def test_positive_add_context_with_explicit_capacity_remains_add_review():
    _, result = _resolve(
        state="never_overextended",
        position=_position(can_add=True),
        swing=_swing("entry_or_add_review"),
    )
    assert result["portfolio_action"]["state"] == "ADD_REVIEW"
    assert result["depot_action_policy"]["policy_case"] == "positive_normal_state_existing_swing_policy"


def test_negative_unconfirmed_remains_wait_confirmation():
    transition = _transition(
        "negative", "transition_pending", stable="positive", pending="negative"
    )
    _, result = _resolve(transition=transition, state="overextension_with_momentum_loss")
    assert result["portfolio_action"]["state"] == "WAIT_CONFIRMATION"
    assert result["depot_action_policy"]["policy_case"] == "negative_or_directional_transition_unconfirmed_wait"


def test_w7_reduce_vs_w6_add_conflict_holds_and_warns():
    base, result = _resolve(
        state="overextension_with_momentum_loss",
        position=_position(can_add=True),
        swing=_swing("entry_or_add_review"),
    )
    assert base["portfolio_action"]["state"] == "ADD_REVIEW"
    assert result["portfolio_action"]["state"] == "HOLD"
    assert result["depot_action_policy"]["conflict"] is True
    assert result["depot_action_policy"]["warning_code"] == "add_reduce_context_conflict"


def test_directional_conflict_holds_with_warning():
    transition = _transition("conflicted", "blocked_conflict", stable=None)
    _, result = _resolve(transition=transition, state="never_overextended")
    assert result["portfolio_action"]["state"] == "HOLD"
    assert result["depot_action_policy"]["conflict"] is True
    assert result["depot_action_policy"]["warning_code"] == "directional_conflict_requires_review"


def test_insufficient_evidence_holds_without_inventing_action():
    transition = _transition("insufficient_evidence", "blocked_insufficient", stable=None)
    _, result = _resolve(transition=transition, state=None)
    assert result["portfolio_action"]["state"] == "HOLD"
    assert result["depot_action_policy"]["policy_case"] == "insufficient_evidence_hold_without_inference"
    assert result["depot_action_policy"]["state_history_state"] is None
    assert result["depot_action_policy"]["new_market_evidence_created"] is False
    assert result["depot_action_policy"]["new_indicator_threshold_created"] is False


def test_missing_state_history_preserves_positive_hold_and_marks_gap():
    _, result = _resolve(state=None)
    assert result["portfolio_action"]["state"] == "HOLD"
    assert result["depot_action_policy"]["policy_case"] == "positive_state_history_missing_preserve_existing_action"
    assert result["depot_action_policy"]["warning_code"] == "state_history_missing"


def test_post_correction_does_not_erase_independent_structural_reduce_context():
    _, result = _resolve(
        state="post_overextension_correction",
        swing=_swing("larger_reduce_or_exit_review"),
    )
    assert result["portfolio_action"]["state"] == "REDUCE_REVIEW"
    assert result["depot_action_policy"]["policy_case"] == "post_correction_independent_structural_reduce_preserved"


def test_existing_add_reduce_swing_conflict_stays_hold_and_is_visible_to_w8():
    _, result = _resolve(
        state="never_overextended",
        position=_position(can_add=True),
        swing=_swing("entry_or_add_review", "profit_protection_review"),
    )
    assert result["portfolio_action"]["state"] == "HOLD"
    assert result["swing_management"]["context_conflict"] is True
    assert result["depot_action_policy"]["resolved_action_state"] == "HOLD"
    assert result["portfolio_action"]["execution_allowed"] is False
