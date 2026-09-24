import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_contract():
    return json.loads((ROOT / "configs" / "elliott_vnext_contract_v2.json").read_text(encoding="utf-8"))


def load_market_schema():
    return json.loads((ROOT / "configs" / "market_context_history_schema_v1.json").read_text(encoding="utf-8"))


def load_output_schema():
    return json.loads((ROOT / "configs" / "elliott_vnext_output_schema_v2.json").read_text(encoding="utf-8"))


def test_pivot_causality_contract():
    c = load_contract()
    assert c["pivot_causality"]["required_fields"] == ["pivot_time", "confirmed_time"]
    assert c["pivot_causality"]["usable_from"] == "confirmed_time"
    assert c["pivot_causality"]["future_revision_of_historical_state"] is False


def test_fibonacci_cannot_choose_count():
    c = load_contract()
    p = c["fibonacci_policy"]
    assert p["wave_structure_selects_anchors"] is True
    assert p["fibonacci_must_not_select_wave_count"] is True
    assert p["fib_geometry_must_remain_separate_from_confirmation"] is True


def test_0887_is_warning_not_hard_invalidation():
    c = load_contract()
    p = c["fibonacci_policy"]
    assert p["wave_2_0_887_is_hard_invalidation"] is False
    assert p["wave_2_hard_invalidation"] == "wave_1_origin_crossed"


def test_classic_impulse_hard_rules_diagonal_and_truncation_are_separate():
    c = load_contract()
    rules = c["hard_rules"]
    assert "wave_2_must_not_cross_wave_1_origin" in rules["classic_impulse"]
    assert "wave_3_must_not_be_shortest_of_1_3_5" in rules["classic_impulse"]
    assert "wave_4_must_not_overlap_wave_1_price_territory" in rules["classic_impulse"]
    assert "wave_4_wave_1_overlap_is_allowed_for_leading_or_ending_diagonal" in rules["diagonal_exception"]
    assert "wave_5_new_extreme_must_not_be_hard_required" in rules["truncation_policy"]


def test_multiple_scenarios_and_scenario_specific_projections_required():
    c = load_contract()
    p = c["scenario_policy"]
    assert p["single_true_count_claim_allowed"] is False
    assert p["primary_required"] is True
    assert p["alternatives_required_when_plausible"] is True
    assert p["projection_zones_must_be_scenario_specific"] is True


def test_projection_policy_is_prospective_not_prediction():
    c = load_contract()
    p = c["projection_policy"]
    assert p["purpose"] == "prospective_price_map_not_forecast_or_trade_order"
    assert p["requires_structurally_valid_scenario"] is True
    assert p["requires_causally_available_anchors"] is True
    assert p["projection_must_store_available_from"] is True
    assert p["projection_must_store_basis"] is True
    assert p["single_exact_price_target_forbidden"] is True
    assert p["zones_not_points"] is True


def test_wave_5_projection_levels_are_not_prematurely_frozen():
    c = load_contract()
    assert c["soft_guidelines"]["wave_5_projection_levels"] == "not_frozen_before_empirical_research"
    w5 = c["projection_policy"]["wave_5"]
    assert w5["numeric_levels_frozen"] is False
    assert w5["must_be_empirically_compared_before_production"] is True
    assert w5["truncated_fifth_must_remain_possible"] is True


def test_full_cycle_routing_contains_w3_w4_w5_states():
    c = load_contract()
    trigger_ids = {t["id"] for t in c["routing_triggers"]}
    assert {
        "EW_W3_TARGET_APPROACH",
        "EW_W3_EXHAUSTION",
        "EW_W4_TARGET_ZONE",
        "EW_W4_COMPLETION",
        "EW_W5_TARGET_APPROACH",
        "EW_W5_COMPLETION_RISK",
    } <= trigger_ids


def test_swing_routing_is_review_only():
    c = load_contract()
    p = c["swing_routing_policy"]
    assert p["module_may_propose_review_context"] is True
    assert p["module_may_emit_direct_trade_order"] is False
    assert p["review_context_is_not_decision"] is True
    assert p["final_action_requires_global_decision_layer"] is True
    assert {
        "entry_or_add_review",
        "partial_reduce_review",
        "reentry_or_add_review",
        "profit_protection_review",
        "larger_reduce_or_exit_review",
    } <= set(p["allowed_review_contexts"])


def test_module_cannot_emit_trade_decision_or_order_instruction():
    c = load_contract()
    assert c["decision_boundary"]["module_may_emit_trade_decision"] is False
    assert c["decision_boundary"]["module_may_emit_review_priority"] is True
    schema = load_output_schema()
    assert schema["properties"]["trade_decision"] == {"not": {}}
    assert schema["properties"]["order_instruction"] == {"not": {}}


def test_output_requires_confirmed_pivots_and_causal_projection_timestamp():
    schema = load_output_schema()
    pivot = schema["properties"]["pivots"]["items"]
    assert "pivot_time" in pivot["required"]
    assert "confirmed_time" in pivot["required"]
    projection = schema["properties"]["projection_zones"]["items"]
    assert "scenario_id" in projection["required"]
    assert "available_from" in projection["required"]
    assert "basis" in projection["required"]
    assert "price_low" in projection["required"]
    assert "price_high" in projection["required"]


def test_output_requires_full_cycle_map_and_swing_routing():
    schema = load_output_schema()
    required = set(schema["required"])
    assert "current_wave_stage" in required
    assert "projection_zones" in required
    assert "wave_cycle_map" in required
    assert "swing_routing" in required


def test_market_context_is_separate_and_quality_gated():
    c = load_contract()
    m = c["market_context"]
    assert m["separate_from_scanner_history"] is True
    assert m["history_artifact"] == "artifacts/research/market_context_history.csv"
    assert set(m["unusable_as_real_market_evidence"]) == {"unreliable", "unavailable"}


def test_market_context_registry_starts_without_invented_proxies():
    path = ROOT / "data" / "inputs" / "market_context_registry.csv"
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(lines) == 1
    assert lines[0].startswith("context_id,context_type,name,symbol")


def test_cross_system_research_keeps_holdout_separate_and_is_stage_specific():
    c = load_contract()
    x = c["cross_system_research"]
    assert x["discovery_validation_holdout_separation_required"] is True
    assert x["stage_specific_windows_required"] is True
    assert set(x["questions"]) >= {
        "redundancy",
        "confirmation",
        "elliott_rescue",
        "scanner_rescue",
        "conflict",
        "lead_lag",
        "stage_specific_incremental_value",
        "swing_routing_value",
    }


def test_forbidden_shortcuts_block_target_only_sales_and_future_anchor_leakage():
    c = load_contract()
    f = set(c["forbidden_shortcuts"])
    assert "wave_3_target_zone_alone_triggers_sale" in f
    assert "wave_5_target_zone_alone_triggers_sale" in f
    assert "projection_uses_anchor_before_anchor_confirmed_time" in f
    assert "projection_zone_is_presented_as_certain_future_price" in f
