import math

import pytest

from scanner.research.elliott_vnext.fibonacci import (
    DEFAULT_ZONE_WIDTH_SPECS,
    FibonacciInputError,
    attach_fibonacci_geometry,
    build_wave2_retracement_map,
    wave2_depth_trigger,
)


def _sp(role, day, confirmed, price, kind):
    return {
        "role": role,
        "pivot_time": day,
        "confirmed_time": confirmed,
        "price": float(price),
        "kind": kind,
    }


def _raw(day, confirmed, price, kind, *, atr=2.0, degree="intermediate"):
    return {
        "symbol": "TEST",
        "timeframe": "daily",
        "degree": degree,
        "pivot_time": day,
        "confirmed_time": confirmed,
        "available_from": confirmed,
        "price": float(price),
        "kind": kind,
        "atr_at_pivot": atr,
        "price_basis": "raw_unadjusted",
        "research_only": True,
    }


def _scenario(stage, pivots, *, scenario_id="scenario-primary", pattern="impulse", family="motive", status="valid_structural_candidate"):
    return {
        "scenario_id": scenario_id,
        "pattern_class": pattern,
        "family": family,
        "direction": "up",
        "timeframe": "daily",
        "degree": "intermediate",
        "stage": stage,
        "status": status,
        "support_level": "full",
        "available_from": pivots[-1]["confirmed_time"] if pivots else "2026-01-01",
        "pivots": pivots,
        "research_only": True,
    }


def _set(primary, alternatives=None):
    return {
        "symbol": "TEST",
        "timeframe": "daily",
        "degree": "intermediate",
        "as_of": primary["available_from"],
        "selection_policy": "deterministic_structural_recency_not_probability_or_truth",
        "single_true_count_claimed": False,
        "primary_scenario": primary,
        "alternative_scenarios": list(alternatives or []),
        "invalidated_scenarios": [],
        "uncertainty_alternatives": [],
        "warnings": [],
        "fibonacci_used": False,
        "trade_decision": None,
        "research_only": True,
    }


def _base_pivots():
    scenario_pivots = [
        _sp("origin", "2026-01-02", "2026-01-04", 100, "low"),
        _sp("wave_1", "2026-01-10", "2026-01-12", 120, "high"),
        _sp("wave_2", "2026-01-17", "2026-01-19", 110, "low"),
        _sp("wave_3", "2026-01-28", "2026-01-30", 150, "high"),
        _sp("wave_4", "2026-02-05", "2026-02-07", 135, "low"),
        _sp("wave_5", "2026-02-15", "2026-02-17", 155, "high"),
    ]
    raw = [
        _raw("2026-01-02", "2026-01-04", 100, "low", atr=1.5),
        _raw("2026-01-10", "2026-01-12", 120, "high", atr=1.8),
        _raw("2026-01-17", "2026-01-19", 110, "low", atr=2.0),
        _raw("2026-01-28", "2026-01-30", 150, "high", atr=2.5),
        _raw("2026-02-05", "2026-02-07", 135, "low", atr=2.2),
        _raw("2026-02-15", "2026-02-17", 155, "high", atr=2.0),
    ]
    return scenario_pivots, raw


def test_w2_887_is_danger_but_not_hard_invalidation():
    state = wave2_depth_trigger(100, 120, 102)
    assert state["trigger"] == "EW_W2_DANGER"
    assert state["hard_invalidation"] is False
    assert state["danger_zone_887_is_hard_invalidation"] is False

    invalid = wave2_depth_trigger(100, 120, 100)
    assert invalid["trigger"] == "EW_INVALIDATED"
    assert invalid["hard_invalidation"] is True


def test_wave2_map_uses_structurally_supplied_anchors_and_origin_as_hard_invalidation():
    origin = {
        "pivot_time": "2026-01-02",
        "confirmed_time": "2026-01-04",
        "price": 100.0,
        "kind": "low",
        "atr_at_pivot": 1.5,
    }
    wave1 = {
        "pivot_time": "2026-01-10",
        "confirmed_time": "2026-01-12",
        "price": 120.0,
        "kind": "high",
        "atr_at_pivot": 2.0,
    }
    result = build_wave2_retracement_map(origin, wave1, degree="intermediate", current_price=102)
    assert result["anchor_selection_by_fibonacci"] is False
    assert result["available_from"] == "2026-01-12"
    assert result["hard_invalidation_price"] == 100.0
    assert result["danger_level_887_is_hard_invalidation"] is False
    assert [zone["trigger"] for zone in result["zones"]] == [
        "EW_PREWATCH_382",
        "EW_DEEP_SCAN_500",
        "EW_W2_CORE",
        "EW_W2_DEEP",
        "EW_W2_DANGER",
    ]
    assert result["trigger_state"]["trigger"] == "EW_W2_DANGER"


def test_w3_projection_is_scenario_specific_and_causal():
    pivots, raw = _base_pivots()
    primary = _scenario("wave_2_complete", pivots[:3])
    result = attach_fibonacci_geometry(_set(primary), source_pivots=raw)

    zones = result["projection_zones"]
    assert len(zones) == 5
    assert all(zone["scenario_id"] == "scenario-primary" for zone in zones)
    assert all(zone["wave_role"] == "wave_3" for zone in zones)
    assert all(zone["available_from"] == "2026-01-19" for zone in zones)
    assert [round(zone["center_price"], 3) for zone in zones] == [130.0, 142.36, 150.0, 162.36, 174.72]
    assert all(zone["price_low"] < zone["center_price"] < zone["price_high"] for zone in zones)
    assert all(zone["basis"]["level_validated"] is False for zone in zones)


def test_w4_projection_uses_confirmed_wave3_and_no_future_wave4_anchor():
    pivots, raw = _base_pivots()
    primary = _scenario("wave_3_complete", pivots[:4])
    result = attach_fibonacci_geometry(_set(primary), source_pivots=raw)
    zones = result["projection_zones"]

    assert len(zones) == 4
    assert all(zone["wave_role"] == "wave_4" for zone in zones)
    assert all(zone["available_from"] == "2026-01-30" for zone in zones)
    assert [round(zone["center_price"], 2) for zone in zones] == [144.16, 140.56, 134.72, 130.0]
    assert all(zone["basis"]["anchor_roles"] == ["wave_2", "wave_3"] for zone in zones)


def test_w5_candidates_use_multiple_bases_and_remain_unfrozen_unvalidated():
    pivots, raw = _base_pivots()
    primary = _scenario("wave_4_complete", pivots[:5])
    result = attach_fibonacci_geometry(_set(primary), source_pivots=raw)
    zones = result["projection_zones"]

    assert len(zones) == 2
    assert all(zone["wave_role"] == "wave_5" for zone in zones)
    assert all(zone["available_from"] == "2026-02-07" for zone in zones)
    assert {zone["projection_type"] for zone in zones} == {
        "wave_1_length_relative_to_wave_4",
        "wave_1_to_wave_3_structure_relative_to_wave_4",
    }
    assert sorted(round(zone["center_price"], 2) for zone in zones) == [155.0, 165.9]
    assert all(zone["basis"]["numeric_level_frozen"] is False for zone in zones)
    assert all(zone["basis"]["level_validated"] is False for zone in zones)
    assert result["numeric_w5_levels_frozen"] is False


def test_completed_wave5_does_not_backfill_old_projections_with_future_knowledge():
    pivots, raw = _base_pivots()
    primary = _scenario("wave_5_complete", pivots[:6])
    result = attach_fibonacci_geometry(_set(primary), source_pivots=raw)
    assert result["projection_zones"] == []
    assert "cycle_complete_no_historical_projection_backfill" in result["warnings"]


def test_missing_anchor_atr_suppresses_projection_instead_of_inventing_width():
    pivots, raw = _base_pivots()
    raw = [dict(row) for row in raw]
    raw[2]["atr_at_pivot"] = None
    primary = _scenario("wave_2_complete", pivots[:3])
    result = attach_fibonacci_geometry(_set(primary), source_pivots=raw)
    assert result["projection_zones"] == []
    assert "wave3_projection_suppressed_missing_wave2_atr" in result["warnings"]


def test_zone_width_is_volatility_price_and_degree_dependent():
    origin = {"pivot_time": "2026-01-01", "confirmed_time": "2026-01-02", "price": 100.0, "kind": "low"}
    wave1_minor = {"pivot_time": "2026-01-10", "confirmed_time": "2026-01-11", "price": 120.0, "kind": "high", "atr_at_pivot": 2.0}
    wave1_primary = dict(wave1_minor)

    minor = build_wave2_retracement_map(origin, wave1_minor, degree="minor")
    primary = build_wave2_retracement_map(origin, wave1_primary, degree="primary")
    minor_width = minor["zones"][0]["zone_width_basis"]["half_width"]
    primary_width = primary["zones"][0]["zone_width_basis"]["half_width"]
    assert primary_width > minor_width
    assert minor["zones"][0]["zone_width_basis"]["atr_component"] > 0
    assert minor["zones"][0]["zone_width_basis"]["price_component"] > 0
    assert all(spec.validated is False for spec in DEFAULT_ZONE_WIDTH_SPECS)


def test_unknown_degree_fails_closed_instead_of_using_hidden_default():
    pivots, raw = _base_pivots()
    primary = _scenario("wave_2_complete", pivots[:3])
    scenario_set = _set(primary)
    scenario_set["degree"] = "mystery"
    with pytest.raises(FibonacciInputError, match="unsupported_degree_zone_width"):
        attach_fibonacci_geometry(scenario_set, source_pivots=raw)


def test_geometry_does_not_select_or_reorder_counts():
    pivots, raw = _base_pivots()
    primary = _scenario("wave_2_complete", pivots[:3], scenario_id="p")
    alt = _scenario("wave_2_complete", pivots[:3], scenario_id="a", pattern="leading_diagonal", status="conservative_structural_candidate")
    scenario_set = _set(primary, [alt])
    result = attach_fibonacci_geometry(scenario_set, source_pivots=raw)

    assert result["primary_scenario"]["scenario_id"] == "p"
    assert [item["scenario_id"] for item in result["alternative_scenarios"]] == ["a"]
    assert result["fibonacci_selects_wave_count"] is False
    assert result["single_true_count_claimed"] is False


def test_geometry_output_contains_no_top_level_trade_or_order_instruction():
    pivots, raw = _base_pivots()
    primary = _scenario("wave_2_complete", pivots[:3])
    result = attach_fibonacci_geometry(_set(primary), source_pivots=raw)
    assert "trade_decision" not in result
    assert "order_instruction" not in result
    assert result["routing_triggers"] == []
    assert result["research_only"] is True


def test_projection_status_is_zone_based_not_exact_target_claim():
    pivots, raw = _base_pivots()
    primary = _scenario("wave_2_complete", pivots[:3])
    result = attach_fibonacci_geometry(_set(primary), source_pivots=raw, current_price=142.36)
    zone = next(zone for zone in result["projection_zones"] if math.isclose(zone["center_price"], 142.36, rel_tol=1e-9))
    assert zone["status"] == "inside"
    assert zone["price_low"] < zone["price_high"]
    assert zone["distance_to_zone_pct"] == 0.0


def test_downtrend_wave2_depth_and_retracement_map_are_symmetric():
    state = wave2_depth_trigger(120, 100, 118)
    assert state["trigger"] == "EW_W2_DANGER"
    assert state["hard_invalidation"] is False

    origin = {"pivot_time": "2026-01-01", "confirmed_time": "2026-01-02", "price": 120.0, "kind": "high"}
    wave1 = {"pivot_time": "2026-01-10", "confirmed_time": "2026-01-11", "price": 100.0, "kind": "low", "atr_at_pivot": 2.0}
    result = build_wave2_retracement_map(origin, wave1, degree="intermediate")
    assert result["direction"] == "down"
    centers = [round(zone["center_price"], 2) for zone in result["zones"]]
    assert centers == [107.64, 110.0, 112.36, 115.72, 117.74]
