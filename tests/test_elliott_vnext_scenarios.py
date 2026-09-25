import pytest

from scanner.research.elliott_vnext.scenarios import (
    ScenarioInputError,
    evaluate_correction_window,
    evaluate_motive_window,
    generate_scenario_sets,
)


def p(day, kind, price, *, confirmed=None, symbol="TEST", timeframe="daily", degree="minor", ambiguous=False):
    confirmed = confirmed or day
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "degree": degree,
        "pivot_time": day,
        "confirmed_time": confirmed,
        "available_from": confirmed,
        "price": price,
        "kind": kind,
        "sequence_ambiguous": ambiguous,
        "price_basis": "adjusted_via_adj_close_ratio",
        "research_only": True,
    }


def bullish_impulse(*, overlap=False, truncated=True):
    wave_4 = 118 if overlap else 130
    wave_5 = 145 if truncated else 160
    return [
        p("2026-01-01", "low", 100),
        p("2026-01-02", "high", 120),
        p("2026-01-03", "low", 110),
        p("2026-01-04", "high", 150),
        p("2026-01-05", "low", wave_4),
        p("2026-01-06", "high", wave_5),
    ]


def bearish_impulse():
    return [
        p("2026-01-01", "high", 150),
        p("2026-01-02", "low", 130),
        p("2026-01-03", "high", 140),
        p("2026-01-04", "low", 100),
        p("2026-01-05", "high", 120),
        p("2026-01-06", "low", 105),
    ]


def test_valid_bullish_impulse_allows_truncated_fifth():
    result = evaluate_motive_window(bullish_impulse(truncated=True))
    assert result["valid"] is True
    assert result["direction"] == "up"
    assert result["stage"] == "wave_5_complete"
    assert result["hard_rule_violations"] == []
    assert result["truncated_fifth"] is True


def test_valid_bearish_impulse_is_direction_symmetric():
    result = evaluate_motive_window(bearish_impulse())
    assert result["valid"] is True
    assert result["direction"] == "down"
    assert result["truncated_fifth"] is True


def test_wave_2_crossing_origin_is_hard_invalidation():
    window = bullish_impulse()
    window[2] = p("2026-01-03", "low", 95)
    result = evaluate_motive_window(window[:3])
    assert result["valid"] is False
    assert "wave_2_must_not_cross_wave_1_origin" in result["hard_rule_violations"]


def test_wave_3_shortest_is_hard_invalidation_after_wave_5_exists():
    window = [
        p("2026-01-01", "low", 100),
        p("2026-01-02", "high", 120),  # W1 = 20
        p("2026-01-03", "low", 110),
        p("2026-01-04", "high", 125),  # W3 = 15
        p("2026-01-05", "low", 122),
        p("2026-01-06", "high", 150),  # W5 = 28
    ]
    result = evaluate_motive_window(window)
    assert result["valid"] is False
    assert "wave_3_must_not_be_shortest_of_1_3_5" in result["hard_rule_violations"]


def test_wave_3_shortest_rule_is_not_applied_before_wave_5_is_known():
    window = [
        p("2026-01-01", "low", 100),
        p("2026-01-02", "high", 120),
        p("2026-01-03", "low", 110),
        p("2026-01-04", "high", 125),
    ]
    result = evaluate_motive_window(window)
    assert "wave_3_must_not_be_shortest_of_1_3_5" not in result["hard_rule_violations"]


def test_wave_4_overlap_invalidates_classic_impulse_but_keeps_diagonal_alternatives():
    scenario_set = generate_scenario_sets(bullish_impulse(overlap=True))[0]
    all_live = [scenario_set["primary_scenario"], *scenario_set["alternative_scenarios"]]
    full_window = [s for s in all_live if len(s.get("pivots", [])) == 6]
    classes = {s["pattern_class"] for s in full_window}
    assert "impulse" not in classes
    assert {"leading_diagonal", "ending_diagonal"} <= classes
    diagonals = [s for s in full_window if "diagonal" in s["pattern_class"]]
    assert all(s["diagonal_overlap_exception_applied"] is True for s in diagonals)
    assert all("wave_4_must_not_overlap_wave_1_price_territory" not in s["hard_rule_violations"] for s in diagonals)

    invalid_impulses = [
        s for s in scenario_set["invalidated_scenarios"]
        if s["pattern_class"] == "impulse" and len(s["pivots"]) == 6
    ]
    assert invalid_impulses
    assert "wave_4_must_not_overlap_wave_1_price_territory" in invalid_impulses[0]["hard_rule_violations"]


def test_primary_is_only_deterministic_presentation_not_true_count_claim():
    scenario_set = generate_scenario_sets(bullish_impulse())[0]
    assert scenario_set["single_true_count_claimed"] is False
    assert scenario_set["selection_policy"] == "deterministic_structural_recency_not_probability_or_truth"
    assert scenario_set["primary_scenario"]
    assert scenario_set["alternative_scenarios"]


def test_complete_impulse_keeps_fibonacci_and_performance_out_of_6b():
    scenario_set = generate_scenario_sets(bullish_impulse())[0]
    assert scenario_set["fibonacci_used"] is False
    assert scenario_set["historical_performance_used"] is False
    assert scenario_set["scanner_state_used"] is False
    for scenario in [scenario_set["primary_scenario"], *scenario_set["alternative_scenarios"]]:
        assert scenario["fibonacci"] is None
        assert scenario["projection_zones"] == []
        assert scenario["historical_expectancy"] is None
        assert scenario["trade_decision"] is None


def test_zigzag_and_flat_can_coexist_when_geometry_is_not_yet_available():
    correction = [
        p("2026-02-01", "high", 150),
        p("2026-02-02", "low", 120),
        p("2026-02-03", "high", 140),
        p("2026-02-04", "low", 100),
    ]
    evaluation = evaluate_correction_window(correction)
    assert evaluation["valid"] is True
    assert evaluation["b_crosses_origin"] is False
    assert evaluation["c_extends_past_a"] is True

    scenario_set = generate_scenario_sets(correction)[0]
    live = [scenario_set["primary_scenario"], *scenario_set["alternative_scenarios"]]
    classes = {s["pattern_class"] for s in live}
    assert {"zigzag", "flat"} <= classes
    flats = [s for s in live if s["pattern_class"] == "flat"]
    assert flats and all(s["geometry_status"] == "unresolved_until_6c" for s in flats)


def test_b_crossing_origin_invalidates_zigzag_but_not_flat_structural_candidate():
    correction = [
        p("2026-02-01", "high", 150),
        p("2026-02-02", "low", 120),
        p("2026-02-03", "high", 155),
        p("2026-02-04", "low", 110),
    ]
    scenario_set = generate_scenario_sets(correction)[0]
    live = [scenario_set["primary_scenario"], *scenario_set["alternative_scenarios"]]
    assert any(s["pattern_class"] == "flat" for s in live)
    assert not any(s["pattern_class"] == "zigzag" and len(s["pivots"]) == 4 for s in live)
    invalid_zigzags = [s for s in scenario_set["invalidated_scenarios"] if s["pattern_class"] == "zigzag"]
    assert invalid_zigzags


def test_future_confirmed_pivot_is_invisible_at_historical_as_of():
    pivots = bullish_impulse()
    pivots[-1] = p("2026-01-06", "high", 145, confirmed="2026-01-10")

    historical = generate_scenario_sets(pivots, as_of="2026-01-06")[0]
    physical_prefix = generate_scenario_sets(pivots[:-1], as_of="2026-01-06")[0]

    ids_historical = {
        historical["primary_scenario"]["scenario_id"],
        *(s["scenario_id"] for s in historical["alternative_scenarios"]),
    }
    ids_prefix = {
        physical_prefix["primary_scenario"]["scenario_id"],
        *(s["scenario_id"] for s in physical_prefix["alternative_scenarios"]),
    }
    assert ids_historical == ids_prefix
    assert not any(
        any(pivot["pivot_time"] == "2026-01-06" for pivot in scenario.get("pivots", []))
        for scenario in [historical["primary_scenario"], *historical["alternative_scenarios"]]
    )


def test_scenario_ids_are_deterministic():
    first = generate_scenario_sets(bullish_impulse())[0]
    second = generate_scenario_sets(list(reversed(bullish_impulse())))[0]
    first_ids = [first["primary_scenario"]["scenario_id"], *(s["scenario_id"] for s in first["alternative_scenarios"])]
    second_ids = [second["primary_scenario"]["scenario_id"], *(s["scenario_id"] for s in second["alternative_scenarios"])]
    assert first_ids == second_ids


def test_ambiguous_outside_bar_is_sequence_boundary_not_invented_order():
    pivots = [
        p("2026-03-01", "low", 100),
        p("2026-03-02", "high", 120),
        p("2026-03-03", "high", 125, ambiguous=True),
        p("2026-03-03", "low", 90, ambiguous=True),
        p("2026-03-04", "high", 115),
        p("2026-03-05", "low", 95),
    ]
    scenario_set = generate_scenario_sets(pivots)[0]
    assert scenario_set["warnings"] == ["ambiguous_intrabar_sequence_excluded"]
    assert scenario_set["ambiguities"][0]["pivot_time"] == "2026-03-03"
    every = [scenario_set["primary_scenario"], *scenario_set["alternative_scenarios"], *scenario_set["invalidated_scenarios"]]
    assert not any(
        pivot["pivot_time"] == "2026-03-03"
        for scenario in every
        for pivot in scenario.get("pivots", [])
    )


def test_complex_corrections_are_uncertainty_alternatives_not_asserted_counts():
    pivots = []
    prices = [100, 120, 105, 125, 108, 128, 110, 130, 112]
    kinds = ["low", "high", "low", "high", "low", "high", "low", "high", "low"]
    for i, (kind, price) in enumerate(zip(kinds, prices), start=1):
        pivots.append(p(f"2026-04-{i:02d}", kind, price))
    scenario_set = generate_scenario_sets(pivots)[0]
    classes = {s["pattern_class"] for s in scenario_set["uncertainty_alternatives"]}
    assert {"triangle", "double_three", "triple_three"} <= classes
    assert all(s["support_level"] == "conservative" for s in scenario_set["uncertainty_alternatives"])
    assert all(s["status"] == "conservative_uncertainty_alternative" for s in scenario_set["uncertainty_alternatives"])


def test_available_from_must_match_confirmed_time():
    bad = p("2026-05-01", "low", 100, confirmed="2026-05-03")
    bad["available_from"] = "2026-05-02"
    with pytest.raises(ScenarioInputError, match="available_from_must_equal_confirmed_time"):
        generate_scenario_sets([bad])


def test_groups_are_kept_separate_by_symbol_timeframe_and_degree():
    pivots = bullish_impulse()
    pivots += [p("2026-01-01", "low", 50, symbol="OTHER"), p("2026-01-02", "high", 60, symbol="OTHER"), p("2026-01-03", "low", 55, symbol="OTHER")]
    outputs = generate_scenario_sets(pivots)
    assert {(o["symbol"], o["timeframe"], o["degree"]) for o in outputs} == {
        ("TEST", "daily", "minor"),
        ("OTHER", "daily", "minor"),
    }
