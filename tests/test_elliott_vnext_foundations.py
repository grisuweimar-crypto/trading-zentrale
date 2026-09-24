import csv
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _load_json(relative_path: str):
    return json.loads((ROOT / relative_path).read_text(encoding="utf-8"))


def test_elliott_contract_is_foundation_only_and_not_trade_decision():
    contract = _load_json("configs/elliott_vnext_contract_v1.json")
    assert contract["status"] == "foundation_only_not_productive"
    assert contract["decision_boundary"]["module_may_emit_trade_decision"] is False
    assert contract["decision_boundary"]["final_decision_owner"] == "global_decision_layer"


def test_pivots_are_point_in_time_causal():
    contract = _load_json("configs/elliott_vnext_contract_v1.json")
    pivot = contract["pivot_causality"]
    assert pivot["required_fields"] == ["pivot_time", "confirmed_time"]
    assert pivot["usable_from"] == "confirmed_time"
    assert pivot["future_revision_of_historical_state"] is False


def test_classic_impulse_hard_rules_are_explicit():
    contract = _load_json("configs/elliott_vnext_contract_v1.json")
    rules = set(contract["hard_rules"]["classic_impulse"])
    assert "wave_2_must_not_cross_wave_1_origin" in rules
    assert "wave_3_must_not_be_shortest_of_1_3_5" in rules
    assert "wave_4_must_not_overlap_wave_1_price_territory" in rules
    assert (
        "wave_4_wave_1_overlap_is_allowed_for_leading_or_ending_diagonal"
        in contract["hard_rules"]["diagonal_exception"]
    )


def test_887_is_warning_zone_not_hard_invalidation():
    contract = _load_json("configs/elliott_vnext_contract_v1.json")
    fib = contract["fibonacci_policy"]
    assert fib["wave_2_0_887_is_hard_invalidation"] is False
    assert fib["wave_2_hard_invalidation"] == "wave_1_origin_crossed"

    trigger_ids = {item["id"] for item in contract["routing_triggers"]}
    assert "EW_W2_DANGER" in trigger_ids
    assert "EW_INVALIDATED" in trigger_ids


def test_fibonacci_cannot_choose_wave_count_or_mix_confirmation():
    contract = _load_json("configs/elliott_vnext_contract_v1.json")
    fib = contract["fibonacci_policy"]
    assert fib["wave_structure_selects_anchors"] is True
    assert fib["fibonacci_must_not_select_wave_count"] is True
    assert fib["fib_geometry_must_remain_separate_from_confirmation"] is True


def test_multiple_scenarios_are_part_of_contract():
    contract = _load_json("configs/elliott_vnext_contract_v1.json")
    policy = contract["scenario_policy"]
    assert policy["single_true_count_claim_allowed"] is False
    assert policy["primary_required"] is True
    assert policy["alternatives_required_when_plausible"] is True


def test_market_context_is_separate_and_quality_gated():
    contract = _load_json("configs/elliott_vnext_contract_v1.json")
    context = contract["market_context"]
    assert context["separate_from_scanner_history"] is True
    assert context["history_artifact"] == "artifacts/research/market_context_history.csv"
    assert set(context["quality_levels"]) == {
        "sufficient",
        "limited",
        "unreliable",
        "unavailable",
    }
    assert set(context["unusable_as_real_market_evidence"]) == {
        "unreliable",
        "unavailable",
    }


def test_market_context_schema_requires_ohlc_and_retrieval_time():
    schema = _load_json("configs/market_context_history_schema_v1.json")
    required = set(schema["required_columns"])
    for field in (
        "date",
        "context_id",
        "open",
        "high",
        "low",
        "close",
        "currency",
        "data_source",
        "retrieved_at",
        "quality",
    ):
        assert field in required


def test_output_schema_decomposes_evidence_and_forbids_trade_decision():
    schema = _load_json("configs/elliott_vnext_output_schema_v1.json")
    required = set(schema["required"])
    assert "structural_fit" in required
    assert "confirmation_strength" in required
    assert "historical_expectancy" in required
    assert schema["constraints"]["trade_decision_field_forbidden"] is True
    assert schema["constraints"]["single_confidence_field_without_decomposition_forbidden"] is True


def test_market_context_registry_starts_as_schema_only_not_invented_benchmarks():
    path = ROOT / "data/inputs/market_context_registry.csv"
    rows = list(csv.reader(path.read_text(encoding="utf-8").splitlines()))
    assert rows == [[
        "context_id",
        "context_type",
        "name",
        "symbol",
        "proxy_kind",
        "currency",
        "valid_from",
        "valid_to",
        "source_url",
        "quality",
        "notes",
    ]]
