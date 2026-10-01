from __future__ import annotations

from copy import deepcopy

import pytest

from scanner.research.decision_layer.depot_watch_orchestrator import (
    DepotWatchOrchestrationError,
    build_orchestrated_depot_watch,
)
from scanner.research.decision_layer.input_contract import build_input_packet
from scanner.research.decision_layer.phase6_elliott import (
    Elliott6HAdapterError,
    build_elliott_7f_swing_context,
    validate_elliott_6h_output,
)


CURRENT_SNAPSHOT = "w6-current"
CURRENT_TIME = "2026-09-29T18:00:00+00:00"
ELLIOTT_AVAILABLE = "2026-09-29T17:59:30+00:00"
ELLIOTT_COMMIT = "df0d8d497449baae683980368d581c14af4694ee"


def _daily():
    return {
        "schema_version": "daily_research_v1",
        "snapshot_id": CURRENT_SNAPSHOT,
        "source_snapshot_id": CURRENT_SNAPSHOT,
        "as_of": "2026-09-29",
        "generated_at": "2026-09-29T17:55:00+00:00",
        "universe_size": 1,
        "symbols": {
            "TEST": {
                "current": {
                    "name": "Test Corp",
                    "score": 30.0,
                    "r_code": "R4",
                    "close": 100.0,
                    "currency": "USD",
                }
            }
        },
    }


def _timing_packet(snapshot: str, as_of: str):
    row = {
        "family": "timing",
        "claim_id": f"timing:TEST:w6-positive:{snapshot}",
        "as_of": as_of,
        "available_from": as_of,
        "source_version": "phase1b_frozen_patterns_v1:w6",
        "coverage_state": "available",
        "maturity_state": "directional_but_immature",
        "pit_state": "verified",
        "integration_mode": "research_only",
        "payload": {
            "pattern_id": "w6-positive",
            "horizon_sessions": 5,
            "pattern_frozen": True,
            "match_from_pit_features": True,
            "direction": "positive",
        },
    }
    return build_input_packet(
        symbol="TEST",
        as_of=as_of,
        source_snapshot_id=snapshot,
        evidence=[row],
    )


def _packets():
    return [
        _timing_packet("w6-old", "2026-09-28T18:00:00+00:00"),
        _timing_packet(CURRENT_SNAPSHOT, CURRENT_TIME),
    ]


def _position_book(*, can_add=None):
    position = {
        "schema_version": "decision_position_snapshot_v1",
        "symbol": "TEST",
        "source_snapshot_id": "w6-position",
        "as_of": "2026-09-29T17:58:00+00:00",
        "position_state": "long",
        "quantity": 10,
        "currency": "USD",
        "average_entry_price": 90.0,
        "current_price": 100.0,
    }
    if can_add is not None:
        position["can_add"] = can_add
    return {
        "schema_version": "decision_depot_position_book_v1",
        "source_snapshot_id": "w6-book",
        "as_of": "2026-09-29T17:58:30+00:00",
        "positions": [position],
    }


def _route(context: str, *, role: str = "primary"):
    return {
        "trigger": "W6_TEST",
        "review_context": context,
        "reason": "W6 regression fixture",
        "scenario_id": f"scenario-{role}",
        "scenario_role": role,
        "available_from": "2026-09-29",
        "actionability": "review_only_not_trade_instruction",
        "requires_external_confirmation": True,
        "final_decision_owned_by_global_layer": True,
        "research_only": True,
    }


def _output(*contexts: str):
    routes = [_route(context, role="primary" if index == 0 else f"alternative_{index}") for index, context in enumerate(contexts)]
    return {
        "schema_version": "elliott_vnext_output_v2",
        "module": "6H_module_output",
        "symbol": "TEST",
        "as_of": "2026-09-29",
        "timeframe": "daily",
        "degree": "intermediate",
        "selection_policy": "deterministic_structural_recency_not_probability_or_truth",
        "single_true_count_claimed": False,
        # Scenario direction is legitimate Elliott structure but W6 must ignore it.
        "primary_scenario": {"scenario_id": "scenario-primary", "direction": "down"},
        "alternative_scenarios": [],
        "pivots": [],
        "fibonacci": {"anchor_start": {}, "anchor_end": {}, "zones": []},
        "fibonacci_selects_wave_count": False,
        "current_wave_stage": "uncertain",
        "projection_zones": [],
        "wave_cycle_map": {"current_stage": "uncertain", "next_expected_structures": [], "scenario_maps": []},
        "hard_invalidations": [],
        "structural_fit": None,
        "confirmation_strength": None,
        "historical_expectancy": None,
        "routing_triggers": ["W6_TEST"] if routes else [],
        "swing_routing": routes,
        "routing_summary": {
            "primary_review_contexts": [contexts[0]] if contexts else [],
            "conflicts_preserved_not_resolved": len(set(contexts)) > 1,
            "final_decision_required": True,
        },
        "routing_is_trade_decision": False,
        "integration": {
            "contract_version": "elliott_vnext_integration_contract_v1",
            "decision_layer_required": True,
            "decision_authority": "future_global_decision_layer_or_orchestrating_depot_watch",
            "productive_integration_enabled": False,
            "direct_ordering_allowed": False,
            "review_contexts_are_not_actions": True,
            "technical_module_6_complete": True,
            "empirical_promotion_status": "awaiting_unspent_prospective_evidence",
            "primary_scenario_id": "scenario-primary",
        },
        "warnings": [],
        "research_only": True,
        "output_id": "w6-output-test",
    }


def _source(*contexts: str, available_from: str = ELLIOTT_AVAILABLE):
    return {
        "schema_version": "decision_elliott_6h_source_v1",
        "source_commit": ELLIOTT_COMMIT,
        "available_from": available_from,
        "outputs": [_output(*contexts)],
    }


def test_w6_positive_stance_plus_reduce_context_becomes_reduce_review_without_direction_change():
    watch, diagnostics = build_orchestrated_depot_watch(
        _daily(),
        _position_book(),
        _packets(),
        elliott_6h_source=_source("profit_protection_review"),
    )
    row = watch["rows"][0]
    assert row["decision"]["universal_stance_state"] == "positive"
    assert row["decision"]["portfolio_action_state"] == "REDUCE_REVIEW"
    assert row["decision"]["elliott_review_contexts"] == ["profit_protection_review"]
    assert row["decision"]["elliott_changed_universal_stance"] is False
    assert row["elliott_swing_context"]["w6"]["direction_from_elliott_used"] is False
    assert row["elliott_swing_context"]["w6"]["stance_from_elliott_used"] is False
    assert diagnostics["elliott_changed_universal_stance"] is False
    assert diagnostics["elliott_direction_used_as_vote"] is False


def test_w6_elliott_scenario_direction_is_ignored_even_when_opposite_to_stance():
    output = _output("partial_reduce_review")
    assert output["primary_scenario"]["direction"] == "down"
    swing = build_elliott_7f_swing_context(
        output,
        source_commit=ELLIOTT_COMMIT,
        source_available_from=ELLIOTT_AVAILABLE,
    )
    assert "direction" not in swing
    assert swing["review_contexts"] == ["partial_reduce_review"]
    assert swing["w6"]["direction_from_elliott_used"] is False


def test_w6_add_reduce_conflict_is_preserved_and_7f_does_not_arbitrarily_choose():
    watch, _ = build_orchestrated_depot_watch(
        _daily(),
        _position_book(can_add=True),
        _packets(),
        elliott_6h_source=_source("entry_or_add_review", "larger_reduce_or_exit_review"),
    )
    row = watch["rows"][0]
    assert row["decision"]["universal_stance_state"] == "positive"
    assert row["decision"]["portfolio_action_state"] == "HOLD"
    assert row["elliott_swing_context"]["w6"]["add_reduce_conflict_preserved"] is True
    assert row["decision"]["elliott_review_contexts"] == [
        "entry_or_add_review",
        "larger_reduce_or_exit_review",
    ]


def test_w6_hold_review_is_valid_6h_research_but_not_transported_as_actionable_context():
    swing = build_elliott_7f_swing_context(
        _output("hold_review"),
        source_commit=ELLIOTT_COMMIT,
        source_available_from=ELLIOTT_AVAILABLE,
    )
    assert swing["review_contexts"] == []
    assert swing["w6"]["hold_review_routes_omitted"] == 1


def test_w6_missing_elliott_source_preserves_existing_hold_behavior():
    watch, diagnostics = build_orchestrated_depot_watch(
        _daily(), _position_book(), _packets()
    )
    row = watch["rows"][0]
    assert row["decision"]["universal_stance_state"] == "positive"
    assert row["decision"]["portfolio_action_state"] == "HOLD"
    assert "elliott_swing_context" not in row
    assert diagnostics["elliott_6h_source_status"] == "not_supplied"


def test_w6_rejects_unknown_context_or_trading_instruction():
    bad_context = _output("profit_protection_review")
    bad_context["swing_routing"][0]["review_context"] = "sell_now"
    with pytest.raises(Elliott6HAdapterError, match="unknown_review_context"):
        validate_elliott_6h_output(bad_context)

    bad_trade = _output("profit_protection_review")
    bad_trade["primary_scenario"]["trade_decision"] = "SELL"
    with pytest.raises(Elliott6HAdapterError, match="forbidden_trading_fields"):
        validate_elliott_6h_output(bad_trade)


def test_w6_future_source_fails_closed_in_orchestrator():
    with pytest.raises(DepotWatchOrchestrationError, match="future_elliott_6h_source"):
        build_orchestrated_depot_watch(
            _daily(),
            _position_book(),
            _packets(),
            elliott_6h_source=_source(
                "profit_protection_review",
                available_from="2026-09-29T18:00:01+00:00",
            ),
        )


def test_w6_source_output_after_its_claimed_availability_fails_closed():
    source = _source("profit_protection_review", available_from="2026-09-28T17:00:00+00:00")
    with pytest.raises(DepotWatchOrchestrationError, match="output_after_source_availability"):
        build_orchestrated_depot_watch(_daily(), _position_book(), _packets(), elliott_6h_source=source)


def test_w6_does_not_accept_6h_as_productively_promoted_or_direct_ordering():
    bad = _output("profit_protection_review")
    bad["integration"]["productive_integration_enabled"] = True
    with pytest.raises(Elliott6HAdapterError, match="productive_integration_must_remain_disabled"):
        validate_elliott_6h_output(bad)

    bad2 = deepcopy(_output("profit_protection_review"))
    bad2["integration"]["direct_ordering_allowed"] = True
    with pytest.raises(Elliott6HAdapterError, match="direct_ordering_must_remain_disabled"):
        validate_elliott_6h_output(bad2)
