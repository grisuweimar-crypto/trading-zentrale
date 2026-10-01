from __future__ import annotations

from copy import deepcopy

from scanner.research.decision_layer.depot_watch_orchestrator import (
    build_orchestrated_depot_watch,
)
from scanner.research.decision_layer.input_contract import build_input_packet
from scanner.research.decision_layer.phase7_state_history import (
    StateHistoryContextError,
    attach_state_history_to_7f,
    build_state_history_context,
)
from scanner.research.decision_layer.portfolio_action import compute_portfolio_action


CURRENT_SNAPSHOT = "snapshot-current"
CURRENT_TIME = "2026-09-29T18:00:00+00:00"


def _path_row(
    *,
    active: bool,
    last_date: str | None,
    sessions_since: int | None,
    rs3m_falling: bool = False,
    score_falling: bool = False,
    rank_worsening: bool = False,
    trend_falling: bool = False,
    r_downgrade: bool = False,
    sequence_state: str = "legacy-state",
    review_state: str = "monitor",
):
    return {
        "family": "risk",
        "claim_id": "risk:TEST:scanner-path:2026-09-29",
        "as_of": CURRENT_TIME,
        "available_from": CURRENT_TIME,
        "source_version": "scanner_path_state_v1",
        "coverage_state": "available",
        "maturity_state": "not_yet_mature",
        "pit_state": "verified",
        "integration_mode": "research_only",
        "payload": {
            "context_type": "scanner_path_state_v1",
            "overextension_threshold_rs3m": 0.15,
            "memory_horizon_sessions": 20,
            "overextension_active": active,
            "last_overextension_date": last_date,
            "last_overextension_rs3m": 0.23 if last_date else None,
            "sessions_since_last_overextension": sessions_since,
            "recent_overextension": (
                sessions_since is not None and sessions_since <= 20
            ),
            "sequence_state": sequence_state,
            "review_state": review_state,
            "deterioration": {
                "score_falling_5t": score_falling,
                "rank_worsening_5t": rank_worsening,
                "rs3m_falling_5t": rs3m_falling,
                "trend200_falling_5t": trend_falling,
                "r_code_downgrade": r_downgrade,
            },
            "review_is_trade_decision": False,
            "execution_allowed": False,
        },
    }


def _timing_row(snapshot: str, as_of: str):
    return {
        "family": "timing",
        "claim_id": f"timing:TEST:frozen-positive:{snapshot}",
        "as_of": as_of,
        "available_from": as_of,
        "source_version": "phase1b_frozen_patterns_v1:test",
        "coverage_state": "available",
        "maturity_state": "directional_but_immature",
        "pit_state": "verified",
        "integration_mode": "research_only",
        "payload": {
            "pattern_id": "frozen-positive",
            "horizon_sessions": 5,
            "pattern_frozen": True,
            "match_from_pit_features": True,
            "direction": "positive",
        },
    }


def _packet(path_row=None, *, snapshot=CURRENT_SNAPSHOT, as_of=CURRENT_TIME):
    evidence = [_timing_row(snapshot, as_of)]
    if path_row is not None:
        evidence.append(path_row)
    return build_input_packet(
        symbol="TEST",
        as_of=as_of,
        source_snapshot_id=snapshot,
        evidence=evidence,
    )


def _daily_symbol(*, active: bool):
    return {
        "current": {
            "name": "Test Corp",
            "score": 30.0,
            "rank": 20.0,
            "rank_percentile": 0.10,
            "r_code": "R4",
            "rs3m": 0.18 if active else 0.12,
            "trend200": 0.22,
            "cycle": 82.0,
            "confidence": 70.0,
            "confidence_label": "MED",
            "close": 100.0,
            "currency": "USD",
        },
        "dynamics": {
            "score_delta_1d": -0.2,
            "score_delta_5d": -1.0,
            "rank_delta_5d": 3.0,
            "rs3m_delta_5d": -0.04,
            "trend200_delta_5d": -0.01,
            "r_code_previous": "R5",
            "r_code_days_current_state": 2,
        },
        "persistence": {
            "days_r4": 4,
            "days_r5": 12,
            "consecutive_days_current_r_code": 2,
            "consecutive_days_rs3m_negative": 0,
            "consecutive_days_trend200_negative": 0,
        },
        "classification": {
            "top_10_percent": True,
            "top_20_percent": True,
            "bottom_20_percent": False,
            "bottom_10_percent": False,
            "pullback_candidate": False,
            "dead_cat_warning": False,
            "overextension_warning": active,
            "mining_override_warning": False,
        },
        "historical_matches": {
            "filter_id": "level_2",
            "N": 42,
            "forward_5t": {"N": 39, "median_return": 0.01},
            "forward_20t": {"N": 24, "median_return": 0.03},
        },
    }


def _transition(raw_state="positive", status="stable_confirmed", stable="positive"):
    return {
        "schema_version": "decision_state_transition_v1",
        "phase": "7E",
        "symbol": "TEST",
        "as_of": CURRENT_TIME,
        "source_snapshot_id": CURRENT_SNAPSHOT,
        "raw_stance": {
            "state": raw_state,
            "direction": raw_state if raw_state in {"positive", "negative"} else None,
            "portfolio_independent": True,
            "research_only": True,
        },
        "transition_state": {
            "status": status,
            "stable_directional_anchor": stable,
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


def _position():
    return {
        "schema_version": "decision_position_snapshot_v1",
        "symbol": "TEST",
        "as_of": "2026-09-29T17:59:00+00:00",
        "source_snapshot_id": "broker-snap",
        "position_state": "long",
        "quantity": 10,
        "currency": "USD",
        "average_entry_price": 90.0,
        "current_price": 100.0,
    }


def test_w7_distinguishes_never_overextended():
    context = build_state_history_context(
        _packet(_path_row(active=False, last_date=None, sessions_since=None)),
        _daily_symbol(active=False),
    )
    assert context["state"] == "never_overextended"
    assert context["state_sequence"] == ["never_overextended"]


def test_w7_distinguishes_active_overextension_without_new_threshold():
    context = build_state_history_context(
        _packet(_path_row(active=True, last_date="2026-09-29", sessions_since=0)),
        _daily_symbol(active=True),
    )
    assert context["state"] == "overextended"
    assert context["semantics"]["new_overextension_threshold_created"] is False


def test_w7_distinguishes_overextension_with_momentum_loss():
    context = build_state_history_context(
        _packet(_path_row(
            active=True,
            last_date="2026-09-29",
            sessions_since=0,
            rs3m_falling=True,
            score_falling=True,
            sequence_state="overextension_with_fading_dynamics",
            review_state="profit_protection_review",
        )),
        _daily_symbol(active=True),
    )
    assert context["state"] == "overextension_with_momentum_loss"
    assert context["state_sequence"] == [
        "overextended",
        "overextension_with_momentum_loss",
    ]


def test_w7_post_correction_persists_beyond_legacy_20_session_memory():
    context = build_state_history_context(
        _packet(_path_row(
            active=False,
            last_date="2026-08-01",
            sessions_since=35,
            rs3m_falling=False,
            sequence_state="no_recent_overextension_context",
            review_state="none",
        )),
        _daily_symbol(active=False),
    )
    assert context["state"] == "post_overextension_correction"
    assert context["path_memory"]["recent_overextension"] is False
    assert context["path_memory"]["last_overextension_date"] == "2026-08-01"


def test_w7_transports_daily_research_blocks_without_reconstruction():
    daily = _daily_symbol(active=False)
    context = build_state_history_context(
        _packet(_path_row(active=False, last_date="2026-09-20", sessions_since=6)),
        daily,
    )
    for field in ("current", "dynamics", "persistence", "classification", "historical_matches"):
        assert context[field] == daily[field]
    assert context["semantics"]["existing_history_transported_not_reconstructed"] is True
    assert context["semantics"]["missing_values_remain_missing"] is True


def test_w7_attachment_cannot_change_positive_hold_action_or_stance():
    base = compute_portfolio_action(_transition(), _position())
    context = build_state_history_context(
        _packet(_path_row(
            active=True,
            last_date="2026-09-29",
            sessions_since=0,
            rs3m_falling=True,
            score_falling=True,
        )),
        _daily_symbol(active=True),
    )
    attached = attach_state_history_to_7f(base, context)
    assert base["portfolio_action"]["state"] == "HOLD"
    assert attached["portfolio_action"] == base["portfolio_action"]
    assert attached["universal_stance_context"] == base["universal_stance_context"]
    assert attached["state_history_context"]["state"] == "overextension_with_momentum_loss"
    assert attached["validation"]["state_history_action_policy_evaluated"] is False


def test_w7_missing_path_context_remains_missing_and_does_not_change_action():
    assert build_state_history_context(_packet(None), _daily_symbol(active=False)) is None
    base = compute_portfolio_action(_transition(), _position())
    assert attach_state_history_to_7f(base, None) == base


def test_w7_fails_closed_on_daily_path_state_mismatch():
    try:
        build_state_history_context(
            _packet(_path_row(active=True, last_date="2026-09-29", sessions_since=0)),
            _daily_symbol(active=False),
        )
    except StateHistoryContextError as exc:
        assert "overextension_state_source_mismatch" in str(exc)
    else:
        raise AssertionError("expected state source mismatch")


def _daily_for_watch():
    symbol = _daily_symbol(active=False)
    return {
        "schema_version": "daily_research_v1",
        "snapshot_id": CURRENT_SNAPSHOT,
        "source_snapshot_id": CURRENT_SNAPSHOT,
        "as_of": "2026-09-29",
        "generated_at": "2026-09-29T17:59:30+00:00",
        "universe_size": 1,
        "symbols": {"TEST": symbol},
    }


def _position_book():
    return {
        "schema_version": "decision_depot_position_book_v1",
        "source_snapshot_id": "private-portfolio-1",
        "as_of": "2026-09-29T17:59:00+00:00",
        "positions": [_position()],
    }


def test_w7_end_to_end_watch_exposes_post_correction_but_leaves_7f_hold():
    old = _packet(
        None,
        snapshot="snapshot-old",
        as_of="2026-09-28T18:00:00+00:00",
    )
    current = _packet(_path_row(
        active=False,
        last_date="2026-09-20",
        sessions_since=6,
        sequence_state="post_overextension_memory",
        review_state="monitor",
    ))
    watch, diagnostics = build_orchestrated_depot_watch(
        _daily_for_watch(), _position_book(), [old, current]
    )
    row = watch["rows"][0]
    assert row["decision"]["universal_stance_state"] == "positive"
    assert row["decision"]["portfolio_action_state"] == "HOLD"
    assert row["state_history_context"]["state"] == "post_overextension_correction"
    assert row["decision"]["state_history_state"] == "post_overextension_correction"
    assert row["decision"]["state_history_changed_portfolio_action"] is False
    assert row["decision"]["state_history_w8_action_policy_evaluated"] is False
    assert diagnostics["state_history_symbols"] == ["TEST"]
    assert diagnostics["state_history_counts"] == {"post_overextension_correction": 1}
    assert diagnostics["state_history_changes_portfolio_action"] is False
