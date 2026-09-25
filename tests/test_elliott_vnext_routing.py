import copy

import pytest

from scanner.research.elliott_vnext.routing import (
    ALLOWED_REVIEW_CONTEXTS,
    TRIGGER_POLICY,
    ExecutionCostSpec,
    RoutingInputError,
    attach_swing_routing,
    route_wave2_monitor,
)


def _pivot(role, price, kind, confirmed):
    return {
        "role": role,
        "pivot_time": confirmed,
        "confirmed_time": confirmed,
        "price": float(price),
        "kind": kind,
    }


def _scenario(stage, scenario_id, pivots, *, truncated=False):
    return {
        "scenario_id": scenario_id,
        "pattern_class": "impulse",
        "family": "motive",
        "direction": "up",
        "timeframe": "daily",
        "degree": "intermediate",
        "stage": stage,
        "status": "valid_structural_candidate",
        "support_level": "full",
        "available_from": pivots[-1]["confirmed_time"],
        "pivots": pivots,
        "truncated_fifth": truncated,
        "research_only": True,
    }


def _zone(wave_role, status, zone_id, *, available="2026-01-19", projection_type="candidate"):
    return {
        "zone_id": zone_id,
        "scenario_id": "unused-here",
        "wave_role": wave_role,
        "projection_type": projection_type,
        "price_low": 100.0,
        "price_high": 101.0,
        "basis": {"level": 1.0},
        "available_from": available,
        "status": status,
        "research_only": True,
    }


def _geometry_set(primary, *, alternatives=None, primary_zones=None, alternative_zones=None, as_of="2026-03-01"):
    alternatives = list(alternatives or [])
    geometry = [
        {
            "scenario_id": primary["scenario_id"],
            "supported": True,
            "stage": primary["stage"],
            "projection_zones": list(primary_zones or []),
            "warnings": [],
            "research_only": True,
        }
    ]
    alternative_zones = alternative_zones or {}
    for scenario in alternatives:
        geometry.append(
            {
                "scenario_id": scenario["scenario_id"],
                "supported": True,
                "stage": scenario["stage"],
                "projection_zones": list(alternative_zones.get(scenario["scenario_id"], [])),
                "warnings": [],
                "research_only": True,
            }
        )
    return {
        "symbol": "TEST",
        "timeframe": "daily",
        "degree": "intermediate",
        "as_of": as_of,
        "primary_scenario": primary,
        "alternative_scenarios": alternatives,
        "fibonacci_geometry": geometry,
        "projection_zones": [zone for item in geometry for zone in item["projection_zones"]],
        "fibonacci_used": True,
        "fibonacci_selects_wave_count": False,
        "routing_triggers": [],
        "warnings": [],
        "research_only": True,
    }


def _pivots_to_w5():
    return [
        _pivot("origin", 100, "low", "2026-01-04"),
        _pivot("wave_1", 120, "high", "2026-01-12"),
        _pivot("wave_2", 110, "low", "2026-01-19"),
        _pivot("wave_3", 150, "high", "2026-01-30"),
        _pivot("wave_4", 135, "low", "2026-02-07"),
        _pivot("wave_5", 145, "high", "2026-02-17"),
    ]


def test_trigger_policy_contains_only_allowed_review_contexts():
    assert TRIGGER_POLICY
    assert set(ALLOWED_REVIEW_CONTEXTS) == {
        "entry_or_add_review",
        "hold_review",
        "partial_reduce_review",
        "reentry_or_add_review",
        "profit_protection_review",
        "larger_reduce_or_exit_review",
    }
    assert all(policy["review_context"] in ALLOWED_REVIEW_CONTEXTS for policy in TRIGGER_POLICY.values())


def test_wave2_monitor_887_is_hold_review_not_hard_invalidation_and_event_is_not_backdated():
    wave2_map = {
        "scenario_id": "prewatch",
        "available_from": "2026-01-12",
        "trigger_state": {
            "retracement_ratio": 0.90,
            "trigger": "EW_W2_DANGER",
            "hard_invalidation": False,
            "danger_zone_887_is_hard_invalidation": False,
        },
    }
    route = route_wave2_monitor(wave2_map, observed_at="2026-01-16")[0]
    assert route["trigger"] == "EW_W2_DANGER"
    assert route["review_context"] == "hold_review"
    assert route["available_from"] == "2026-01-16"
    assert route["evidence"]["geometry_available_from"] == "2026-01-12"
    assert route["requires_external_confirmation"] is True


def test_wave2_monitor_hard_invalidation_routes_only_to_reduce_exit_review():
    wave2_map = {
        "scenario_id": "prewatch",
        "available_from": "2026-01-12",
        "trigger_state": {
            "retracement_ratio": 1.01,
            "trigger": "EW_INVALIDATED",
            "hard_invalidation": True,
            "danger_zone_887_is_hard_invalidation": False,
        },
    }
    route = route_wave2_monitor(wave2_map, observed_at="2026-01-18")[0]
    assert route["review_context"] == "larger_reduce_or_exit_review"
    assert route["actionability"] == "review_only_not_trade_instruction"


def test_confirmed_wave2_depth_uses_wave2_endpoint_not_current_market_price():
    pivots = _pivots_to_w5()
    primary = _scenario("wave_2_complete", "s-w2", pivots[:3])
    result = attach_swing_routing(_geometry_set(primary, as_of="2026-02-01"))
    route = next(route for route in result["swing_routing"] if route["trigger"] == "EW_DEEP_SCAN_500")
    assert route["review_context"] == "entry_or_add_review"
    assert route["available_from"] == "2026-01-19"
    assert route["evidence"]["wave2_endpoint_price"] == 110.0
    assert route["evidence"]["uses_current_market_price"] is False


def test_w3_target_approach_is_partial_reduce_review_and_observed_at_as_of():
    pivots = _pivots_to_w5()
    primary = _scenario("wave_2_complete", "s-w2", pivots[:3])
    zones = [
        _zone("wave_3", "reached", "z1"),
        _zone("wave_3", "approaching", "z2", projection_type="1.618"),
        _zone("wave_3", "projected", "z3", projection_type="2.0"),
    ]
    result = attach_swing_routing(_geometry_set(primary, primary_zones=zones, as_of="2026-02-01"))
    route = next(route for route in result["swing_routing"] if route["trigger"] == "EW_W3_TARGET_APPROACH")
    assert route["review_context"] == "partial_reduce_review"
    assert route["available_from"] == "2026-02-01"
    assert route["evidence"]["zone_ids"] == ["z2"]
    assert route["requires_external_confirmation"] is True


def test_single_early_reached_projection_does_not_create_permanent_target_route():
    pivots = _pivots_to_w5()
    primary = _scenario("wave_2_complete", "s-w2", pivots[:3])
    zones = [
        _zone("wave_3", "reached", "z1"),
        _zone("wave_3", "projected", "z2"),
    ]
    result = attach_swing_routing(_geometry_set(primary, primary_zones=zones))
    assert "EW_W3_TARGET_APPROACH" not in result["routing_triggers"]


def test_all_mapped_candidates_reached_keeps_frontier_review_active():
    pivots = _pivots_to_w5()
    primary = _scenario("wave_2_complete", "s-w2", pivots[:3])
    zones = [_zone("wave_3", "reached", "z1"), _zone("wave_3", "reached", "z2")]
    result = attach_swing_routing(_geometry_set(primary, primary_zones=zones))
    route = next(route for route in result["swing_routing"] if route["trigger"] == "EW_W3_TARGET_APPROACH")
    assert route["evidence"]["mode"] == "all_mapped_candidates_reached"


def test_confirmed_wave3_wave4_and_wave5_endpoints_route_to_review_contexts_only():
    pivots = _pivots_to_w5()

    w3 = attach_swing_routing(_geometry_set(_scenario("wave_3_complete", "s3", pivots[:4])))
    assert ("EW_W3_EXHAUSTION", "partial_reduce_review") in {
        (route["trigger"], route["review_context"]) for route in w3["swing_routing"]
    }

    w4 = attach_swing_routing(_geometry_set(_scenario("wave_4_complete", "s4", pivots[:5])))
    assert ("EW_W4_COMPLETION", "reentry_or_add_review") in {
        (route["trigger"], route["review_context"]) for route in w4["swing_routing"]
    }

    w5 = attach_swing_routing(_geometry_set(_scenario("wave_5_complete", "s5", pivots[:6], truncated=True)))
    route = next(route for route in w5["swing_routing"] if route["trigger"] == "EW_W5_COMPLETION_RISK")
    assert route["review_context"] == "larger_reduce_or_exit_review"
    assert route["evidence"]["truncated_fifth"] is True


def test_w4_and_w5_projection_frontiers_use_reentry_and_profit_protection_reviews():
    pivots = _pivots_to_w5()
    w3_scenario = _scenario("wave_3_complete", "s3", pivots[:4])
    w4_zone = [_zone("wave_4", "inside", "w4-zone")]
    w3_result = attach_swing_routing(_geometry_set(w3_scenario, primary_zones=w4_zone))
    route = next(route for route in w3_result["swing_routing"] if route["trigger"] == "EW_W4_TARGET_ZONE")
    assert route["review_context"] == "reentry_or_add_review"

    w4_scenario = _scenario("wave_4_complete", "s4", pivots[:5])
    w5_zone = [_zone("wave_5", "approaching", "w5-zone")]
    w4_result = attach_swing_routing(_geometry_set(w4_scenario, primary_zones=w5_zone))
    route = next(route for route in w4_result["swing_routing"] if route["trigger"] == "EW_W5_TARGET_APPROACH")
    assert route["review_context"] == "profit_protection_review"


def test_round_trip_cost_model_is_explicit_and_does_not_claim_net_edge():
    cost = ExecutionCostSpec(spread_bps=8.0, commission_bps_per_side=2.0, slippage_bps_per_side=3.0)
    assert cost.estimated_round_trip_cost_bps == 18.0

    pivots = _pivots_to_w5()
    primary = _scenario("wave_3_complete", "s3", pivots[:4])
    result = attach_swing_routing(_geometry_set(primary), cost_spec=cost)
    route = result["swing_routing"][0]
    assert route["estimated_round_trip_cost_bps"] == 18.0
    assert route["cost_model_status"] == "estimated_round_trip_cost_attached"
    assert route["historical_net_benefit_evaluated"] is False
    assert route["historical_outperformance_claimed"] is False
    assert result["execution_cost_model"]["parameter_validated"] is False


def test_missing_cost_model_is_warned_but_does_not_hide_research_route():
    pivots = _pivots_to_w5()
    primary = _scenario("wave_3_complete", "s3", pivots[:4])
    result = attach_swing_routing(_geometry_set(primary))
    assert result["swing_routing"]
    assert result["swing_routing"][0]["cost_model_status"] == "required_not_supplied"
    assert "transaction_cost_model_missing_for_cost_sensitive_reviews" in result["warnings"]


def test_primary_alternative_routing_conflict_is_preserved_not_resolved():
    pivots = _pivots_to_w5()
    primary = _scenario("wave_3_complete", "primary", pivots[:4])
    alternative = _scenario("wave_4_complete", "alt", pivots[:5])
    original = _geometry_set(primary, alternatives=[alternative])
    original_ids = [original["primary_scenario"]["scenario_id"]] + [
        scenario["scenario_id"] for scenario in original["alternative_scenarios"]
    ]

    result = attach_swing_routing(original)
    result_ids = [result["primary_scenario"]["scenario_id"]] + [
        scenario["scenario_id"] for scenario in result["alternative_scenarios"]
    ]
    assert result_ids == original_ids
    assert result["routing_summary"]["cross_scenario_conflict"] is True
    assert result["routing_summary"]["conflicts_preserved_not_resolved"] is True
    assert "scenario_routing_conflict_preserved" in result["warnings"]
    assert {route["review_context"] for route in result["swing_routing"]} == {
        "partial_reduce_review",
        "reentry_or_add_review",
    }


def test_multiple_contexts_inside_one_scenario_are_preserved():
    pivots = _pivots_to_w5()
    primary = _scenario("wave_4_complete", "s4", pivots[:5])
    zones = [_zone("wave_5", "inside", "w5-zone")]
    result = attach_swing_routing(_geometry_set(primary, primary_zones=zones))
    assert {route["review_context"] for route in result["swing_routing"]} == {
        "reentry_or_add_review",
        "profit_protection_review",
    }
    assert result["routing_summary"]["within_scenario_conflict"] is True
    assert "within_scenario_routing_context_conflict_preserved" in result["warnings"]


def test_6d_requires_6c_guard_and_never_emits_trade_or_order_instruction():
    pivots = _pivots_to_w5()
    primary = _scenario("wave_3_complete", "s3", pivots[:4])
    bad = _geometry_set(primary)
    bad["fibonacci_used"] = False
    with pytest.raises(RoutingInputError, match="6d_requires_6c_fibonacci_geometry"):
        attach_swing_routing(bad)

    source = _geometry_set(primary)
    source["trade_decision"] = "SELL"
    source["order_instruction"] = "market"
    result = attach_swing_routing(source)
    assert "trade_decision" not in result
    assert "order_instruction" not in result
    assert result["routing_is_trade_decision"] is False
    assert result["routing_changes_scenario_order"] is False
    assert result["routing_summary"]["final_decision_required"] is True


def test_routing_output_is_deterministic():
    pivots = _pivots_to_w5()
    primary = _scenario("wave_2_complete", "s-w2", pivots[:3])
    zones = [_zone("wave_3", "approaching", "z1")]
    source = _geometry_set(primary, primary_zones=zones)
    assert attach_swing_routing(copy.deepcopy(source)) == attach_swing_routing(copy.deepcopy(source))
