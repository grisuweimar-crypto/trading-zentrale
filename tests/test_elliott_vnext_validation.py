import math

import pandas as pd

from scanner.research.elliott_vnext.fibonacci import ZoneWidthSpec
from scanner.research.elliott_vnext.pivots import PivotSpec
from scanner.research.elliott_vnext.validation import (
    ValidationConfig,
    attach_forward_outcomes,
    block_bootstrap_mean,
    build_validation_report,
    effective_block_length,
    evaluate_structure_progression,
    extract_projection_claims,
    extract_route_claims,
    extract_structure_claims,
    validation_partition,
)
from scanner.research.elliott_vnext.validation_replay import replay_symbol_states


def _price_rows(symbol="AAA", closes=None, start="2026-09-01"):
    closes = closes or [100 + i for i in range(15)]
    days = pd.bdate_range(start, periods=len(closes))
    rows = []
    for day, close in zip(days, closes):
        rows.append({
            "date": day.date().isoformat(),
            "symbol": symbol,
            "currency": "USD",
            "open": float(close),
            "high": float(close) + 1.0,
            "low": float(close) - 1.0,
            "close": float(close),
            "adj_close": float(close),
            "volume": 1000.0,
            "source": "test",
            "retrieved_at": "2026-09-25T00:00:00+00:00",
            "observation_type": "price_backfill",
        })
    return rows


def _pivot(role, day, price, kind):
    return {
        "role": role,
        "pivot_time": day,
        "confirmed_time": day,
        "available_from": day,
        "price": float(price),
        "kind": kind,
    }


def _scenario(stage="wave_2_complete", scenario_id="s2", direction="up"):
    pivots = [
        _pivot("origin", "2026-09-01", 100, "low"),
        _pivot("wave_1", "2026-09-03", 120, "high"),
        _pivot("wave_2", "2026-09-05", 110, "low"),
    ]
    if stage in {"wave_3_complete", "wave_4_complete", "wave_5_complete"}:
        pivots.append(_pivot("wave_3", "2026-09-08", 140, "high"))
    if stage in {"wave_4_complete", "wave_5_complete"}:
        pivots.append(_pivot("wave_4", "2026-09-10", 130, "low"))
    if stage == "wave_5_complete":
        pivots.append(_pivot("wave_5", "2026-09-12", 150, "high"))
    return {
        "scenario_id": scenario_id,
        "pattern_class": "impulse",
        "family": "motive",
        "direction": direction,
        "timeframe": "daily",
        "degree": "minor",
        "stage": stage,
        "status": "valid_structural_candidate",
        "support_level": "full",
        "available_from": pivots[-1]["confirmed_time"],
        "pivots": pivots,
        "rule_violations": [],
        "research_only": True,
    }


def _snapshot(day="2026-09-05", scenario=None, zones=None, routes=None):
    scenario = scenario or _scenario()
    return {
        "symbol": "AAA",
        "timeframe": "daily",
        "degree": "minor",
        "as_of": day,
        "primary_scenario": scenario,
        "alternative_scenarios": [],
        "invalidated_scenarios": [],
        "fibonacci_used": True,
        "fibonacci_selects_wave_count": False,
        "projection_zones": zones or [],
        "swing_routing": routes or [],
        "routing_is_trade_decision": False,
        "research_only": True,
    }


def test_freeze_partition_never_calls_legacy_data_unspent():
    config = ValidationConfig(rules_frozen_through="2026-09-25")
    assert validation_partition("2026-09-25", config) == "legacy_development_descriptive_only"
    assert validation_partition("2026-09-26", config) == "prospective_unspent"


def test_projection_hit_starts_next_session_not_same_day_high():
    prices = _price_rows(closes=[100, 100, 100, 100, 100, 100])
    prices[0]["high"] = 110.0
    claim = {
        "claim_id": "z1",
        "claim_type": "projection",
        "symbol": "AAA",
        "available_from": prices[0]["date"],
        "partition": "legacy_development_descriptive_only",
        "elliott_direction": "up",
        "price_low": 109.0,
        "price_high": 111.0,
        "clean_future_hit_claim": True,
    }
    outcome = attach_forward_outcomes([claim], prices, ValidationConfig(horizons=(5,)))[0]
    assert outcome["matured"] is True
    assert outcome["projection_hit"] is False
    assert outcome["first_hit_session"] is None


def test_adjusted_close_missing_has_no_raw_fallback():
    prices = _price_rows(closes=[100, 101, 102, 103, 104, 105])
    prices[5]["adj_close"] = ""
    claim = {
        "claim_id": "r1",
        "claim_type": "route_review",
        "symbol": "AAA",
        "available_from": prices[0]["date"],
        "partition": "legacy_development_descriptive_only",
        "elliott_direction": "up",
        "review_orientation": "supportive_review",
    }
    outcome = attach_forward_outcomes([claim], prices, ValidationConfig(horizons=(5,)))[0]
    assert outcome["matured"] is True
    assert outcome["outcome_available"] is False
    assert outcome["forward_return"] is None
    assert outcome["outcome_missing_reason"] == "adjusted_close_missing_no_raw_fallback"


def test_directional_return_is_symmetric_for_up_and_down():
    up = _price_rows("UP", [100, 102, 104, 106, 108, 110])
    down = _price_rows("DOWN", [100, 98, 96, 94, 92, 90])
    claims = [
        {
            "claim_id": "up",
            "claim_type": "route_review",
            "symbol": "UP",
            "available_from": up[0]["date"],
            "partition": "legacy_development_descriptive_only",
            "elliott_direction": "up",
            "review_orientation": "supportive_review",
        },
        {
            "claim_id": "down",
            "claim_type": "route_review",
            "symbol": "DOWN",
            "available_from": down[0]["date"],
            "partition": "legacy_development_descriptive_only",
            "elliott_direction": "down",
            "review_orientation": "supportive_review",
        },
    ]
    outcomes = attach_forward_outcomes(claims, up + down, ValidationConfig(horizons=(5,)))
    assert math.isclose(outcomes[0]["signed_forward_return"], 0.10, rel_tol=1e-12)
    assert math.isclose(outcomes[1]["signed_forward_return"], 0.10, rel_tol=1e-12)
    assert outcomes[0]["review_correct"] is True
    assert outcomes[1]["review_correct"] is True


def test_block_length_is_two_times_horizon_and_fails_closed_with_one_region():
    assert effective_block_length(5) == 10
    frame = pd.DataFrame({
        "event_date": pd.bdate_range("2026-01-01", periods=8),
        "metric": [1.0] * 8,
    })
    stats = block_bootstrap_mean(
        frame,
        metric="metric",
        date_col="event_date",
        horizon=5,
        config=ValidationConfig(horizons=(5,), bootstrap_reps=20),
    )
    assert stats["block_length"] == 10
    assert stats["support_regions"] == 1
    assert stats["mean_95"] is None
    assert stats["robust_interval_available"] is False


def test_structure_progression_is_separate_from_returns():
    s2 = _scenario("wave_2_complete", "s2")
    s3 = _scenario("wave_3_complete", "s3")
    snapshots = [
        _snapshot("2026-09-05", s2),
        _snapshot("2026-09-08", s3),
    ]
    claims = extract_structure_claims(snapshots)
    evaluated = evaluate_structure_progression(claims, snapshots)
    w2 = next(row for row in evaluated if row["wave_stage"] == "wave_2_complete")
    assert w2["structure_resolution"] == "progressed"
    assert w2["next_stage"] == "wave_3_complete"
    assert w2["structural_fit"] == 1.0
    assert w2["performance_used_for_structure_resolution"] is False


def test_projection_claim_retains_unfrozen_wave5_candidate_status():
    scenario = _scenario("wave_4_complete", "s4")
    zone = {
        "zone_id": "w5z",
        "scenario_id": "s4",
        "wave_role": "wave_5",
        "projection_type": "wave_1_length_relative_to_wave_4",
        "price_low": 149.0,
        "price_high": 151.0,
        "center_price": 150.0,
        "available_from": "2026-09-10",
        "status": "projected",
        "basis": {"level": 1.0, "level_validated": False, "numeric_level_frozen": False},
        "research_only": True,
    }
    claims = extract_projection_claims([_snapshot("2026-09-10", scenario, [zone])])
    assert len(claims) == 1
    assert claims[0]["numeric_level_frozen_before_6g"] is False
    assert claims[0]["level_validated_before_6g"] is False
    assert claims[0]["fibonacci_selected_count"] is False


def test_route_claim_has_no_execution_policy_or_round_trip_pnl():
    scenario = _scenario("wave_2_complete", "s2")
    route = {
        "trigger": "EW_W2_CORE",
        "review_context": "entry_or_add_review",
        "reason": "test",
        "scenario_id": "s2",
        "scenario_role": "primary",
        "available_from": "2026-09-05",
        "source": "test",
        "evidence": {},
        "requires_external_confirmation": True,
        "final_decision_owned_by_global_layer": True,
        "actionability": "review_only_not_trade_instruction",
        "research_only": True,
    }
    claims = extract_route_claims([_snapshot("2026-09-05", scenario, routes=[route])])
    assert len(claims) == 1
    assert claims[0]["execution_policy_frozen"] is False
    assert claims[0]["round_trip_pnl_testable"] is False


def test_report_awaits_unspent_evidence_and_never_auto_promotes():
    report = build_validation_report(
        [],
        pd.DataFrame(_price_rows()),
        config=ValidationConfig(horizons=(5,), bootstrap_reps=0),
    )
    assert report["promotion_status"] == "awaiting_unspent_prospective_evidence"
    assert report["automatic_promotion_allowed"] is False
    assert report["technical_completion_is_empirical_validation"] is False
    assert report["round_trip_pnl_evaluated"] is False
    assert report["numeric_w5_levels_promoted"] is False
    assert report["trade_decision"] is None
    assert report["order_instruction"] is None


def test_report_marks_prospective_mature_evidence_as_accumulating_only():
    scenario = _scenario("wave_2_complete", "postfreeze")
    # Move scenario pivots and route to after the freeze while keeping structure valid.
    scenario["available_from"] = "2026-09-28"
    for pivot in scenario["pivots"]:
        pivot["confirmed_time"] = "2026-09-28"
        pivot["available_from"] = "2026-09-28"
    route = {
        "trigger": "EW_W2_CORE",
        "review_context": "entry_or_add_review",
        "reason": "test",
        "scenario_id": "postfreeze",
        "scenario_role": "primary",
        "available_from": "2026-09-28",
        "source": "test",
        "evidence": {},
        "requires_external_confirmation": True,
        "final_decision_owned_by_global_layer": True,
        "actionability": "review_only_not_trade_instruction",
        "research_only": True,
    }
    prices = _price_rows(start="2026-09-28", closes=[100, 101, 102, 103, 104, 105, 106])
    report = build_validation_report(
        [_snapshot("2026-09-28", scenario, routes=[route])],
        pd.DataFrame(prices),
        config=ValidationConfig(horizons=(5,), bootstrap_reps=0),
    )
    assert report["evidence_policy"]["prospective_unspent_mature_outcomes"] >= 1
    assert report["promotion_status"] == "prospective_evidence_accumulating_no_automatic_promotion"
    assert report["automatic_promotion_allowed"] is False


def test_prefix_replay_is_invariant_to_future_rows():
    closes = [100, 105, 99, 110, 102, 120, 108, 125, 112, 130, 115, 135]
    rows = _price_rows(closes=closes, start="2026-04-15")
    early_dates = [row["date"] for row in rows[:10]]
    config = ValidationConfig(
        stable_start="2026-04-15",
        horizons=(5,),
        bootstrap_reps=0,
        replay_price_basis="adjusted",
    )
    pivot_specs = (PivotSpec("minor", "daily", 1, 1, 1, 0.0),)
    widths = (ZoneWidthSpec("minor", 0.2, 0.001),)
    first, coverage_first = replay_symbol_states(
        rows[:10],
        "AAA",
        config=config,
        pivot_specs=pivot_specs,
        width_specs=widths,
        as_of_dates=early_dates,
        keep_unchanged=True,
    )
    second, coverage_second = replay_symbol_states(
        rows,
        "AAA",
        config=config,
        pivot_specs=pivot_specs,
        width_specs=widths,
        as_of_dates=early_dates,
        keep_unchanged=True,
    )
    assert coverage_first["errors"] == []
    assert coverage_second["errors"] == []
    assert first == second
    assert all(item["validation_replay"]["future_rows_used"] is False for item in first)
    assert all("trade_decision" not in item and "order_instruction" not in item for item in first)


def test_missing_context_stays_missing_not_scanner_peer_fallback():
    report = build_validation_report(
        [],
        pd.DataFrame(_price_rows()),
        config=ValidationConfig(horizons=(5,), bootstrap_reps=0),
    )
    context = report["market_context_validation"]
    assert context["status"] == "not_supplied_missing_context_stays_missing"
    assert context["scanner_peer_fallback_used"] is False
    assert context["alpha_rows"] == []
