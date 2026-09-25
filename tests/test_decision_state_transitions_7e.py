from __future__ import annotations

import copy

import pytest

from scanner.research.decision_layer.state_transition import (
    StateTransitionError,
    build_state_transition_history,
    compare_confirmation_depths,
    validate_state_transition,
)
from scanner.research.decision_layer.universal_stance import compute_universal_stance


def _common(family: str, claim_id: str, payload: dict, as_of: str) -> dict:
    return {
        "family": family,
        "claim_id": claim_id,
        "as_of": as_of,
        "available_from": as_of,
        "source_version": "test-v1",
        "coverage_state": "available",
        "maturity_state": "robust",
        "pit_state": "verified",
        "integration_mode": "research_only",
        "payload": payload,
    }


def _stance(state: str, as_of: str, snapshot: str) -> dict:
    if state == "positive":
        evidence = [_common("selection", f"sel-{snapshot}", {"direction": "positive"}, as_of)]
    elif state == "negative":
        evidence = [_common("selection", f"sel-{snapshot}", {"direction": "negative"}, as_of)]
    elif state == "conflicted":
        evidence = [
            _common("selection", f"sel-{snapshot}", {"direction": "positive"}, as_of),
            _common(
                "timing",
                f"tim-{snapshot}",
                {
                    "direction": "negative",
                    "pattern_id": f"p-{snapshot}",
                    "horizon_sessions": 20,
                    "pattern_frozen": True,
                    "match_from_pit_features": True,
                },
                as_of,
            ),
        ]
    elif state == "insufficient_evidence":
        evidence = [
            _common(
                "timing",
                f"tim-{snapshot}",
                {
                    "pattern_id": f"p-{snapshot}",
                    "horizon_sessions": 20,
                    "pattern_frozen": True,
                    "match_from_pit_features": True,
                },
                as_of,
            )
        ]
    else:
        raise AssertionError(state)
    return compute_universal_stance({
        "schema_version": "decision_layer_input_contract_v1",
        "symbol": "TEST",
        "as_of": as_of,
        "source_snapshot_id": snapshot,
        "evidence": evidence,
    })


def test_two_distinct_days_bootstrap_directional_anchor() -> None:
    result = build_state_transition_history([
        _stance("positive", "2026-09-26T18:00:00+02:00", "s1"),
        _stance("positive", "2026-09-27T18:00:00+02:00", "s2"),
    ])
    assert result["transition_state"]["status"] == "bootstrap_confirmed"
    assert result["transition_state"]["stable_directional_anchor"] == "positive"
    assert result["raw_stance"]["state"] == "positive"
    assert result["validation"]["hysteresis_rule_empirically_validated"] is False


def test_two_snapshots_same_calendar_day_do_not_confirm() -> None:
    result = build_state_transition_history([
        _stance("positive", "2026-09-26T10:00:00+02:00", "s1"),
        _stance("positive", "2026-09-26T18:00:00+02:00", "s2"),
    ])
    assert result["transition_state"]["status"] == "bootstrap_pending"
    assert result["transition_state"]["stable_directional_anchor"] is None
    assert result["transition_state"]["pending_confirmation_count"] == 1


def test_single_opposite_observation_is_pending_and_does_not_rewrite_raw() -> None:
    result = build_state_transition_history([
        _stance("positive", "2026-09-26T18:00:00+02:00", "s1"),
        _stance("positive", "2026-09-27T18:00:00+02:00", "s2"),
        _stance("negative", "2026-09-28T18:00:00+02:00", "s3"),
    ])
    assert result["raw_stance"]["state"] == "negative"
    assert result["transition_state"]["status"] == "transition_pending"
    assert result["transition_state"]["stable_directional_anchor"] == "positive"
    assert result["transition_state"]["stable_anchor_is_current_stance"] is False
    assert result["semantics"]["stable_anchor_replaces_raw_stance"] is False


def test_second_opposite_day_confirms_transition() -> None:
    result = build_state_transition_history([
        _stance("positive", "2026-09-26T18:00:00+02:00", "s1"),
        _stance("positive", "2026-09-27T18:00:00+02:00", "s2"),
        _stance("negative", "2026-09-28T18:00:00+02:00", "s3"),
        _stance("negative", "2026-09-29T18:00:00+02:00", "s4"),
    ])
    assert result["transition_state"]["status"] == "transition_confirmed"
    assert result["transition_state"]["stable_directional_anchor"] == "negative"
    assert result["transition_state"]["stable_anchor_is_current_stance"] is True


def test_conflict_blocks_and_resets_pending_transition() -> None:
    result = build_state_transition_history([
        _stance("positive", "2026-09-26T18:00:00+02:00", "s1"),
        _stance("positive", "2026-09-27T18:00:00+02:00", "s2"),
        _stance("negative", "2026-09-28T18:00:00+02:00", "s3"),
        _stance("conflicted", "2026-09-29T18:00:00+02:00", "s4"),
        _stance("negative", "2026-09-30T18:00:00+02:00", "s5"),
    ])
    assert result["events"][3]["transition_status"] == "blocked_conflict"
    assert result["events"][4]["transition_status"] == "transition_pending"
    assert result["events"][4]["pending_confirmation_count"] == 1
    assert result["raw_stance"]["state"] == "negative"


def test_insufficient_is_blocking_not_neutral() -> None:
    result = build_state_transition_history([
        _stance("positive", "2026-09-26T18:00:00+02:00", "s1"),
        _stance("positive", "2026-09-27T18:00:00+02:00", "s2"),
        _stance("insufficient_evidence", "2026-09-28T18:00:00+02:00", "s3"),
    ])
    assert result["raw_stance"]["state"] == "insufficient_evidence"
    assert result["transition_state"]["status"] == "blocked_insufficient"
    assert result["transition_state"]["stable_directional_anchor"] == "positive"
    assert result["transition_state"]["stable_anchor_is_current_stance"] is False
    assert result["semantics"]["insufficient_treated_as_neutral"] is False


def test_candidate_comparison_does_not_select_winner() -> None:
    comparison = compare_confirmation_depths([
        _stance("positive", "2026-09-26T18:00:00+02:00", "s1"),
        _stance("negative", "2026-09-27T18:00:00+02:00", "s2"),
        _stance("positive", "2026-09-28T18:00:00+02:00", "s3"),
        _stance("positive", "2026-09-29T18:00:00+02:00", "s4"),
    ])
    assert [row["confirmation_depth"] for row in comparison["candidates"]] == [1, 2, 3, 5]
    assert comparison["winner_selected"] is False
    assert comparison["selection_recommendation"] is None
    assert comparison["historical_churn_reduction_is_outcome_validation"] is False


def test_mixed_symbols_fail_closed() -> None:
    first = _stance("positive", "2026-09-26T18:00:00+02:00", "s1")
    second = _stance("positive", "2026-09-27T18:00:00+02:00", "s2")
    second["symbol"] = "OTHER"
    with pytest.raises(StateTransitionError, match="mixed_symbols_not_allowed"):
        build_state_transition_history([first, second])


def test_validator_rejects_portfolio_action() -> None:
    result = build_state_transition_history([
        _stance("positive", "2026-09-26T18:00:00+02:00", "s1"),
        _stance("positive", "2026-09-27T18:00:00+02:00", "s2"),
    ])
    bad = copy.deepcopy(result)
    bad["portfolio_action"] = "HOLD"
    with pytest.raises(StateTransitionError, match="forbidden_portfolio_or_action_fields"):
        validate_state_transition(bad)
