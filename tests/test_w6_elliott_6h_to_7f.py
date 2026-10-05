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
    build_elliott_6h_source_from_prospective_capture,
    build_elliott_7f_multidegree_swing_context,
    build_elliott_7f_swing_context,
    index_elliott_6h_source,
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


def _output(
    *contexts: str,
    timeframe: str = "daily",
    degree: str = "intermediate",
    output_id: str = "w6-output-test",
    direction: str = "down",
):
    routes = [_route(context, role="primary" if index == 0 else f"alternative_{index}") for index, context in enumerate(contexts)]
    return {
        "schema_version": "elliott_vnext_output_v2",
        "module": "6H_module_output",
        "symbol": "TEST",
        "as_of": "2026-09-29",
        "timeframe": timeframe,
        "degree": degree,
        "selection_policy": "deterministic_structural_recency_not_probability_or_truth",
        "single_true_count_claimed": False,
        # Scenario direction is legitimate Elliott structure but W6 must ignore it.
        "primary_scenario": {"scenario_id": "scenario-primary", "direction": direction},
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
        "output_id": output_id,
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


def test_stage3_source_adapter_preserves_all_degrees_without_reducer():
    first = _output(
        "entry_or_add_review",
        timeframe="daily",
        degree="fine",
        output_id="fine-output",
        direction="up",
    )
    second = _output(
        "profit_protection_review",
        timeframe="weekly",
        degree="coarse",
        output_id="coarse-output",
        direction="down",
    )
    capture = {
        "schema_version": "elliott_vnext_prospective_capture_v1",
        "capture_id": "capture-stage3",
        "snapshot_id": CURRENT_SNAPSHOT,
        "as_of": "2026-09-29",
        "run_id": "github-123-1",
        "source_publication_commit": "a" * 40,
        "captured_at": "2026-09-29T17:58:45+00:00",
        "validation_partition": "prospective_unspent",
        "source_hashes": {
            "market_ohlcv_sha256": "a" * 64,
            "daily_research_sha256": "b" * 64,
        },
        "validation_source": {
            "adapter": "stage4_compact_aggregate_to_frozen_6g_v1",
            "stage4_result_hash": "c" * 64,
        },
        "outputs": [first, second],
        "guards": {
            "research_only": True,
            "productive_integration_enabled": False,
            "w10_source_emitted": False,
            "changes_universal_stance": False,
            "changes_portfolio_action": False,
            "direct_ordering_allowed": False,
            "future_rows_used": False,
            "missing_evidence_not_imputed": True,
            "frozen_elliott_core_modified": False,
            "multi_degree_outputs_retained_without_reducer": True,
        },
    }

    source = build_elliott_6h_source_from_prospective_capture(
        capture,
        source_commit=ELLIOTT_COMMIT,
        available_from=ELLIOTT_AVAILABLE,
        evidence_available_from="2026-09-29T17:58:45+00:00",
        expected_snapshot_id=CURRENT_SNAPSHOT,
        expected_as_of="2026-09-29",
    )
    indexed, meta = index_elliott_6h_source(source, decision_as_of=CURRENT_TIME)

    assert source["integration"]["multi_degree_reducer_used"] is False
    assert source["integration"]["all_available_degrees_retained"] is True
    assert len(indexed["TEST"]) == 2
    assert meta["output_count"] == 2
    assert meta["symbol_count"] == 1
    assert meta["source_capture_id"] == "capture-stage3"
    assert meta["source_hashes"] == capture["source_hashes"]
    assert meta["validation_source"] == capture["validation_source"]

    swing = build_elliott_7f_multidegree_swing_context(
        indexed["TEST"],
        source_commit=str(meta["source_commit"]),
        source_available_from=str(meta["available_from"]),
        source_provenance=meta,
    )
    assert swing["w6"]["source_provenance"]["source_hashes"] == capture["source_hashes"]
    assert swing["w6"]["source_provenance"]["validation_source"] == capture["validation_source"]
    for row in swing["w6"]["timeframe_degrees"]:
        assert set(row["lineage_features"]) == {
            "elliott_structure",
            "fibonacci_geometry",
            "swing_routing",
        }
        assert all(len(value) == 64 for value in row["lineage_features"].values())


def test_w6_multidegree_add_reduce_conflict_is_preserved_without_direction_vote():
    fine = _output(
        "entry_or_add_review",
        timeframe="daily",
        degree="fine",
        output_id="fine-output",
        direction="up",
    )
    coarse = _output(
        "larger_reduce_or_exit_review",
        timeframe="weekly",
        degree="coarse",
        output_id="coarse-output",
        direction="down",
    )
    source = {
        "schema_version": "decision_elliott_6h_source_v1",
        "source_commit": ELLIOTT_COMMIT,
        "available_from": ELLIOTT_AVAILABLE,
        "snapshot_id": CURRENT_SNAPSHOT,
        "as_of": "2026-09-29",
        "source_capture_id": "capture-stage3",
        "outputs": [fine, coarse],
    }

    watch, diagnostics = build_orchestrated_depot_watch(
        _daily(),
        _position_book(can_add=True),
        _packets(),
        elliott_6h_source=source,
    )
    row = watch["rows"][0]
    context = row["elliott_swing_context"]

    assert row["decision"]["universal_stance_state"] == "positive"
    assert row["decision"]["portfolio_action_state"] == "HOLD"
    assert row["decision"]["elliott_review_contexts"] == [
        "entry_or_add_review",
        "larger_reduce_or_exit_review",
    ]
    assert context["w6"]["output_count"] == 2
    assert context["w6"]["multi_degree_reducer_used"] is False
    assert context["w6"]["all_available_degrees_aggregated"] is True
    assert context["w6"]["add_reduce_conflict_preserved"] is True
    assert context["w6"]["direction_from_elliott_used"] is False
    assert sorted(context["w6"]["source_output_ids"]) == ["coarse-output", "fine-output"]
    assert diagnostics["elliott_6h_source_output_count"] == 2
    assert diagnostics["elliott_6h_source_symbol_count"] == 1
    assert diagnostics["elliott_direction_used_as_vote"] is False
    assert diagnostics["elliott_changed_universal_stance"] is False


def test_w6_multidegree_context_builder_refuses_cross_symbol_mix():
    first = _output("entry_or_add_review", output_id="first")
    second = _output("profit_protection_review", output_id="second")
    second["symbol"] = "OTHER"

    with pytest.raises(Elliott6HAdapterError, match="multidegree_symbol_mismatch"):
        build_elliott_7f_multidegree_swing_context(
            [first, second],
            source_commit=ELLIOTT_COMMIT,
            source_available_from=ELLIOTT_AVAILABLE,
        )


def test_stage3_source_adapter_refuses_capture_for_different_snapshot():
    capture = {
        "schema_version": "elliott_vnext_prospective_capture_v1",
        "capture_id": "capture-stage3",
        "snapshot_id": "wrong-snapshot",
        "as_of": "2026-09-29",
        "outputs": [_output("profit_protection_review")],
        "guards": {
            "research_only": True,
            "productive_integration_enabled": False,
            "w10_source_emitted": False,
            "changes_universal_stance": False,
            "changes_portfolio_action": False,
            "direct_ordering_allowed": False,
            "future_rows_used": False,
            "missing_evidence_not_imputed": True,
            "frozen_elliott_core_modified": False,
            "multi_degree_outputs_retained_without_reducer": True,
        },
    }
    with pytest.raises(Elliott6HAdapterError, match="prospective_snapshot_mismatch"):
        build_elliott_6h_source_from_prospective_capture(
            capture,
            source_commit=ELLIOTT_COMMIT,
            available_from=ELLIOTT_AVAILABLE,
            expected_snapshot_id=CURRENT_SNAPSHOT,
            expected_as_of="2026-09-29",
        )
