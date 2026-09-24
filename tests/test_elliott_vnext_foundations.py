import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_contract():
    return json.loads((ROOT / "configs" / "elliott_vnext_contract_v1.json").read_text(encoding="utf-8"))


def load_market_schema():
    return json.loads((ROOT / "configs" / "market_context_history_schema_v1.json").read_text(encoding="utf-8"))


def load_output_schema():
    return json.loads((ROOT / "configs" / "elliott_vnext_output_schema_v1.json").read_text(encoding="utf-8"))


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


def test_classic_impulse_hard_rules_and_diagonal_exception_are_separate():
    c = load_contract()
    rules = c["hard_rules"]
    assert "wave_2_must_not_cross_wave_1_origin" in rules["classic_impulse"]
    assert "wave_3_must_not_be_shortest_of_1_3_5" in rules["classic_impulse"]
    assert "wave_4_must_not_overlap_wave_1_price_territory" in rules["classic_impulse"]
    assert "wave_4_wave_1_overlap_is_allowed_for_leading_or_ending_diagonal" in rules["diagonal_exception"]


def test_multiple_scenarios_required_when_plausible():
    c = load_contract()
    p = c["scenario_policy"]
    assert p["single_true_count_claim_allowed"] is False
    assert p["primary_required"] is True
    assert p["alternatives_required_when_plausible"] is True


def test_module_cannot_emit_trade_decision():
    c = load_contract()
    assert c["decision_boundary"]["module_may_emit_trade_decision"] is False
    schema = load_output_schema()
    assert "trade_decision" in schema["properties"]
    assert schema["properties"]["trade_decision"] == {"not": {}}


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


def test_output_requires_confirmed_pivot_timestamps():
    schema = load_output_schema()
    pivot = schema["properties"]["pivots"]["items"]
    assert "pivot_time" in pivot["required"]
    assert "confirmed_time" in pivot["required"]


def test_cross_system_research_keeps_holdout_separate():
    c = load_contract()
    x = c["cross_system_research"]
    assert x["discovery_validation_holdout_separation_required"] is True
    assert set(x["questions"]) >= {
        "redundancy", "confirmation", "elliott_rescue", "scanner_rescue", "conflict", "lead_lag"
    }
