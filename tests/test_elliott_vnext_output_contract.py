import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_6h_integration_contract_preserves_decision_boundary():
    contract = json.loads((ROOT / "configs/elliott_vnext_integration_contract_v1.json").read_text(encoding="utf-8"))
    assert contract["schema_version"] == "elliott_vnext_integration_contract_v1"
    assert contract["productive_integration_enabled"] is False
    assert contract["research_only"] is True
    assert contract["decision_authority"] == "future_global_decision_layer_or_orchestrating_depot_watch"
    assert contract["required_guards"]["single_true_count_claimed"] is False
    assert contract["required_guards"]["fibonacci_selects_wave_count"] is False
    assert contract["required_guards"]["routing_is_trade_decision"] is False
    assert contract["required_guards"]["technical_completion_equals_empirical_validation"] is False
    assert set(contract["forbidden_output_keys_recursive"]) == {"trade_decision", "order_instruction"}


def test_uncalibrated_fields_stay_null_and_legacy_evidence_cannot_promote():
    contract = json.loads((ROOT / "configs/elliott_vnext_integration_contract_v1.json").read_text(encoding="utf-8"))
    assert contract["uncalibrated_fields"]["structural_fit"] is None
    assert contract["uncalibrated_fields"]["confirmation_strength"] is None
    assert contract["historical_expectancy_policy"]["legacy_development_data_can_support_promotion"] is False
    assert contract["historical_expectancy_policy"]["formal_promotion_partition"] == "prospective_unspent"
    assert contract["historical_expectancy_policy"]["rules_frozen_through"] == "2026-09-25"


def test_foundation_output_schema_still_forbids_direct_trade_fields():
    schema = json.loads((ROOT / "configs/elliott_vnext_output_schema_v2.json").read_text(encoding="utf-8"))
    assert schema["properties"]["trade_decision"] == {"not": {}}
    assert schema["properties"]["order_instruction"] == {"not": {}}
    required = set(schema["required"])
    assert {"primary_scenario", "alternative_scenarios", "pivots", "projection_zones", "swing_routing", "warnings"} <= required


def test_6h_contract_does_not_enable_scanner_peer_context_fallback():
    contract = json.loads((ROOT / "configs/elliott_vnext_integration_contract_v1.json").read_text(encoding="utf-8"))
    context = contract["market_context_policy"]
    assert context["missing_context_stays_missing"] is True
    assert context["scanner_peer_fallback_used"] is False
    assert context["unreliable_or_unavailable_as_real_evidence"] is False
