from __future__ import annotations

from scanner.research.decision_layer.depot_action_policy import apply_depot_action_policy
from scanner.research.decision_layer.phase7_state_history import attach_state_history_to_7f
from scanner.research.decision_layer.portfolio_action import compute_portfolio_action


SYMBOL = "RACE"
AS_OF = "2026-09-29T18:00:00+00:00"
SNAPSHOT = "ferrari-regression-snapshot"


def _transition(
    raw_state: str = "positive",
    status: str = "stable_confirmed",
    stable: str | None = "positive",
    pending: str | None = None,
) -> dict[str, object]:
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


def _position() -> dict[str, object]:
    return {
        "schema_version": "decision_position_snapshot_v1",
        "symbol": SYMBOL,
        "as_of": "2026-09-29T17:59:00+00:00",
        "source_snapshot_id": "ferrari-private-position",
        "position_state": "long",
        "quantity": 1,
        "currency": "USD",
        "average_entry_price": 350.0,
        "current_price": 410.0,
    }


def _swing(*contexts: str) -> dict[str, object]:
    return {
        "source": "elliott_vnext_6h",
        "source_output_id": "ferrari-elliott-6h",
        "as_of": "2026-09-29T17:50:00+00:00",
        "review_contexts": list(contexts),
        "routing_is_trade_decision": False,
        "research_only": True,
    }


def _state(state: str) -> dict[str, object]:
    sequences = {
        "overextended": ["overextended"],
        "overextension_with_momentum_loss": [
            "overextended",
            "overextension_with_momentum_loss",
        ],
        "post_overextension_correction": [
            "overextended",
            "post_overextension_correction",
        ],
    }
    return {
        "schema_version": "decision_state_history_context_v1",
        "symbol": SYMBOL,
        "as_of": AS_OF,
        "source_snapshot_id": SNAPSHOT,
        "source_claim_id": "risk:RACE:scanner-path:2026-09-29",
        "source_claim_available_from": AS_OF,
        "source_context_type": "scanner_path_state_v1",
        "state": state,
        "state_sequence": sequences[state],
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


def _resolve(
    *,
    state: str,
    transition: dict[str, object] | None = None,
    swing: dict[str, object] | None = None,
) -> tuple[dict[str, object], dict[str, object]]:
    base = compute_portfolio_action(
        transition or _transition(),
        _position(),
        swing_context=swing,
    )
    context = _state(state)
    attached = attach_state_history_to_7f(base, context)
    resolved = apply_depot_action_policy(attached, context)
    return base, resolved


def _assert_regression_guards(
    base: dict[str, object], resolved: dict[str, object]
) -> None:
    policy = resolved["depot_action_policy"]
    semantics = resolved["semantics"]
    validation = resolved["validation"]
    action = resolved["portfolio_action"]

    assert resolved["universal_stance_context"] == base["universal_stance_context"]
    assert validation["w8_regression_contract"] == "ferrari_f1_f4_v1"
    assert policy["new_indicator_threshold_created"] is False
    assert policy["new_market_evidence_created"] is False
    assert policy["review_only"] is True
    assert policy["execution_allowed"] is False
    assert semantics["state_history_is_directional_vote"] is False
    assert semantics["state_history_changed_universal_stance"] is False
    assert semantics["w8_created_indicator_threshold"] is False
    assert semantics["w8_created_market_evidence"] is False
    assert semantics["w8_generated_broker_order"] is False
    assert action["execution_allowed"] is False
    assert action["order_instruction"] is None


def test_w12_f1_overextended_positive_does_not_create_unnecessary_sale() -> None:
    base, resolved = _resolve(state="overextended")

    assert base["portfolio_action"]["state"] == "HOLD"
    assert resolved["portfolio_action"]["state"] == "HOLD"
    assert resolved["depot_action_policy"]["policy_case"] == "f1_overextended_but_strong_hold"
    assert resolved["portfolio_action"]["state"] not in {"REDUCE_REVIEW", "EXIT_REVIEW"}
    _assert_regression_guards(base, resolved)


def test_w12_f2_overextended_momentum_loss_allows_early_reduce_review() -> None:
    base, resolved = _resolve(state="overextension_with_momentum_loss")

    assert base["portfolio_action"]["state"] == "HOLD"
    assert resolved["portfolio_action"]["state"] == "REDUCE_REVIEW"
    assert resolved["depot_action_policy"]["policy_case"] == "f2_profit_protection_before_correction"
    assert resolved["depot_action_policy"]["action_changed"] is True
    assert resolved["cost_context"]["cost_sensitive_action"] is True
    _assert_regression_guards(base, resolved)


def test_w12_f3_post_correction_positive_suppresses_delayed_sale() -> None:
    base, resolved = _resolve(
        state="post_overextension_correction",
        swing=_swing("profit_protection_review"),
    )

    assert base["portfolio_action"]["state"] == "REDUCE_REVIEW"
    assert resolved["portfolio_action"]["state"] == "HOLD"
    assert resolved["depot_action_policy"]["policy_case"] == "f3_delayed_profit_protection_suppressed"
    assert resolved["depot_action_policy"]["warning_code"] == "profit_protection_window_already_passed"
    assert resolved["depot_action_policy"]["reassessment_required"] is True
    assert resolved["portfolio_action"]["state"] not in {"REDUCE_REVIEW", "EXIT_REVIEW"}
    _assert_regression_guards(base, resolved)


def test_w12_f4_confirmed_negative_higher_level_evidence_exits() -> None:
    negative = _transition(
        raw_state="negative",
        status="transition_confirmed",
        stable="negative",
    )
    base, resolved = _resolve(
        state="post_overextension_correction",
        transition=negative,
        swing=_swing("entry_or_add_review"),
    )

    assert base["portfolio_action"]["state"] == "EXIT_REVIEW"
    assert resolved["portfolio_action"]["state"] == "EXIT_REVIEW"
    assert resolved["depot_action_policy"]["policy_case"] == "f4_confirmed_negative_exit_review"
    assert resolved["universal_stance_context"]["raw_state"] == "negative"
    _assert_regression_guards(base, resolved)


def test_w12_ferrari_sequence_is_hold_reduce_hold_exit() -> None:
    f1_base, f1 = _resolve(state="overextended")
    f2_base, f2 = _resolve(state="overextension_with_momentum_loss")
    f3_base, f3 = _resolve(
        state="post_overextension_correction",
        swing=_swing("profit_protection_review"),
    )
    f4_base, f4 = _resolve(
        state="post_overextension_correction",
        transition=_transition(
            raw_state="negative",
            status="transition_confirmed",
            stable="negative",
        ),
        swing=_swing("entry_or_add_review"),
    )

    assert [
        f1["portfolio_action"]["state"],
        f2["portfolio_action"]["state"],
        f3["portfolio_action"]["state"],
        f4["portfolio_action"]["state"],
    ] == ["HOLD", "REDUCE_REVIEW", "HOLD", "EXIT_REVIEW"]

    for base, resolved in (
        (f1_base, f1),
        (f2_base, f2),
        (f3_base, f3),
        (f4_base, f4),
    ):
        _assert_regression_guards(base, resolved)
