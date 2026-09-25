from copy import deepcopy
import json

import pandas as pd
import pytest

from scanner.research.elliott_vnext.output import (
    ModuleOutputError,
    build_module_output,
    validate_module_output,
)


def _pivot(role, day, confirmed, price, kind):
    return {
        "role": role,
        "pivot_time": day,
        "confirmed_time": confirmed,
        "available_from": confirmed,
        "price": price,
        "kind": kind,
    }


def _snapshot(as_of="2026-09-25"):
    origin = _pivot("origin", "2026-09-01", "2026-09-03", 100.0, "low")
    wave1 = _pivot("wave_1", "2026-09-08", "2026-09-10", 120.0, "high")
    wave2 = _pivot("wave_2", "2026-09-18", "2026-09-20", 108.0, "low")
    primary = {
        "scenario_id": "scenario-primary",
        "pattern_class": "impulse",
        "family": "motive",
        "direction": "up",
        "timeframe": "daily",
        "degree": "intermediate",
        "stage": "wave_2_complete",
        "status": "valid_structural_candidate",
        "available_from": "2026-09-20",
        "pivots": [origin, wave1, wave2],
        "hard_invalidations": [],
        "rule_violations": [],
        "trade_decision": None,
        "research_only": True,
    }
    alternative = deepcopy(primary)
    alternative["scenario_id"] = "scenario-alt"
    alternative["order_instruction"] = None
    zone = {
        "zone_id": "zone-1",
        "scenario_id": "scenario-primary",
        "wave_role": "wave_3",
        "projection_type": "wave1_extension_from_wave2",
        "price_low": 139.0,
        "price_high": 141.0,
        "basis": {"level": 1.618, "level_validated": False},
        "available_from": "2026-09-20",
        "status": "projected",
        "distance_to_zone_pct": 10.0,
        "research_only": True,
    }
    return {
        "symbol": "AAA",
        "as_of": as_of,
        "timeframe": "daily",
        "degree": "intermediate",
        "selection_policy": "deterministic_structural_recency_not_probability_or_truth",
        "single_true_count_claimed": False,
        "primary_scenario": primary,
        "alternative_scenarios": [alternative],
        "fibonacci_used": True,
        "fibonacci_selects_wave_count": False,
        "fibonacci_geometry": [
            {
                "scenario_id": "scenario-primary",
                "supported": True,
                "fibonacci": {
                    "anchor_start": origin,
                    "anchor_end": wave1,
                    "zones": [{"level": 0.618, "available_from": "2026-09-20"}],
                    "anchor_selection_by_fibonacci": False,
                },
                "projection_zones": [zone],
                "warnings": [],
                "research_only": True,
            }
        ],
        "projection_zones": [zone],
        "current_wave_stage": "possible_wave_2_completion",
        "wave_cycle_map": {
            "current_stage": "possible_wave_2_completion",
            "next_expected_structures": ["wave_3_in_progress"],
            "scenario_maps": [{"scenario_id": "scenario-primary", "projection_zone_ids": ["zone-1"]}],
        },
        "routing_triggers": ["EW_W2_CORE"],
        "swing_routing": [
            {
                "trigger": "EW_W2_CORE",
                "review_context": "entry_or_add_review",
                "reason": "confirmed_wave2_depth",
                "scenario_id": "scenario-primary",
                "scenario_role": "primary",
                "available_from": "2026-09-20",
                "actionability": "review_only_not_trade_instruction",
                "requires_external_confirmation": True,
            }
        ],
        "routing_summary": {
            "primary_review_contexts": ["entry_or_add_review"],
            "final_decision_required": True,
        },
        "routing_is_trade_decision": False,
        "warnings": [],
        "research_only": True,
    }


def _source_pivots():
    return [
        {
            "symbol": "AAA", "timeframe": "daily", "degree": "intermediate",
            "pivot_time": "2026-09-01", "confirmed_time": "2026-09-03", "available_from": "2026-09-03",
            "price": 100.0, "kind": "low", "atr_at_pivot": 2.0,
        },
        {
            "symbol": "AAA", "timeframe": "daily", "degree": "intermediate",
            "pivot_time": "2026-09-08", "confirmed_time": "2026-09-10", "available_from": "2026-09-10",
            "price": 120.0, "kind": "high", "atr_at_pivot": 2.2,
        },
        {
            "symbol": "AAA", "timeframe": "daily", "degree": "intermediate",
            "pivot_time": "2026-09-18", "confirmed_time": "2026-09-20", "available_from": "2026-09-20",
            "price": 108.0, "kind": "low", "atr_at_pivot": 2.1,
        },
        {
            "symbol": "OTHER", "timeframe": "daily", "degree": "intermediate",
            "pivot_time": "2026-09-15", "confirmed_time": "2026-09-17", "available_from": "2026-09-17",
            "price": 50.0, "kind": "high",
        },
    ]


def _validation_report():
    return {
        "schema_version": "elliott_vnext_validation_v1",
        "module": "6G_historical_validation",
        "rules_frozen_through": "2026-09-25",
        "projection_summary": [
            {
                "partition": "legacy_development_descriptive_only",
                "horizon_sessions": 20,
                "wave_role": "wave_3",
                "projection_type": "wave1_extension_from_wave2",
                "degree": "intermediate",
                "zone_hit": {"N": 7, "mean": 0.57, "mean_95": None, "robust_interval_available": False},
                "signed_return": {"N": 7, "mean": 0.01, "mean_95": None, "robust_interval_available": False},
                "formal_promotion_evidence": False,
                "numeric_level_promoted": False,
                "research_only": True,
            },
            {
                "partition": "legacy_development_descriptive_only",
                "horizon_sessions": 20,
                "wave_role": "wave_4",
                "projection_type": "not_current_stage",
                "degree": "intermediate",
                "formal_promotion_evidence": False,
                "research_only": True,
            },
        ],
        "route_summary": [
            {
                "partition": "legacy_development_descriptive_only",
                "horizon_sessions": 20,
                "review_context": "entry_or_add_review",
                "wave_stage": "wave_2_complete",
                "degree": "intermediate",
                "directional_review_correctness": {"N": 5, "mean": 0.6, "mean_95": None},
                "formal_promotion_evidence": False,
                "round_trip_pnl_evaluated": False,
                "research_only": True,
            }
        ],
        "evidence_policy": {
            "legacy_data_can_support_promotion": False,
            "formal_claims_require_available_from_after_freeze": True,
        },
        "promotion_status": "awaiting_unspent_prospective_evidence",
        "automatic_promotion_allowed": False,
        "technical_completion_is_empirical_validation": False,
        "round_trip_pnl_evaluated": False,
        "numeric_w5_levels_promoted": False,
        "trade_decision": None,
        "order_instruction": None,
        "research_only": True,
    }


def _context():
    return pd.DataFrame([
        {
            "asset_symbol": "AAA",
            "as_of": pd.Timestamp("2026-09-25"),
            "context_id": "market-1",
            "relationship": "market",
            "assignment_quality": "sufficient",
            "assignment_pit_verified": True,
            "status": "observed",
            "context_date": pd.Timestamp("2026-09-24"),
            "context_quality": "sufficient",
            "usable_as_real_market_evidence": True,
        }
    ])


def _contains_key(value, wanted):
    if isinstance(value, dict):
        return wanted in value or any(_contains_key(item, wanted) for item in value.values())
    if isinstance(value, list):
        return any(_contains_key(item, wanted) for item in value)
    return False


def test_6h_builds_stable_research_only_output_and_recursively_strips_trade_fields():
    output = build_module_output(
        _snapshot(),
        source_pivots=_source_pivots(),
        validation_report=_validation_report(),
        market_context_snapshot=_context(),
    )
    assert output["schema_version"] == "elliott_vnext_output_v2"
    assert output["module"] == "6H_module_output"
    assert output["integration"]["decision_layer_required"] is True
    assert output["integration"]["productive_integration_enabled"] is False
    assert output["structural_fit"] is None
    assert output["confirmation_strength"] is None
    assert not _contains_key(output, "trade_decision")
    assert not _contains_key(output, "order_instruction")
    assert len(output["pivots"]) == 3
    assert output["market_context"]["eligible_real_evidence_count"] == 1
    assert output["historical_expectancy"]["expected_next_wave_role"] == "wave_3"
    assert len(output["historical_expectancy"]["projection_evidence"]) == 1
    assert output["historical_expectancy"]["formal_promotion_evidence_rows"] == 0
    validate_module_output(output)


def test_output_id_is_deterministic_across_source_pivot_order():
    first = build_module_output(_snapshot(), source_pivots=_source_pivots())
    second = build_module_output(_snapshot(), source_pivots=list(reversed(_source_pivots())))
    assert first["output_id"] == second["output_id"]


def test_missing_optional_evidence_stays_missing_instead_of_being_invented():
    output = build_module_output(_snapshot())
    assert output["historical_expectancy"] is None
    assert output["market_context"] is None
    assert output["relative_strength"] is None
    assert "historical_expectancy_not_supplied" in output["warnings"]
    assert "external_market_context_not_supplied" in output["warnings"]
    assert "top_level_pivots_limited_to_scenario_embedded_subset" in output["warnings"]


def test_future_confirmed_source_pivot_fails_closed():
    pivots = _source_pivots()
    pivots.append({
        "symbol": "AAA", "timeframe": "daily", "degree": "intermediate",
        "pivot_time": "2026-09-24", "confirmed_time": "2026-09-26", "available_from": "2026-09-26",
        "price": 150.0, "kind": "high",
    })
    with pytest.raises(ModuleOutputError, match="future_confirmed_pivot"):
        build_module_output(_snapshot(), source_pivots=pivots)


def test_future_projection_or_route_fails_closed():
    bad_zone = _snapshot()
    bad_zone["projection_zones"][0]["available_from"] = "2026-09-26"
    with pytest.raises(ModuleOutputError, match="future_projection"):
        build_module_output(bad_zone)

    bad_route = _snapshot()
    bad_route["swing_routing"][0]["available_from"] = "2026-09-26"
    with pytest.raises(ModuleOutputError, match="future_route"):
        build_module_output(bad_route)


def test_future_market_context_and_unverified_usable_context_fail_closed():
    future = _context()
    future.loc[0, "as_of"] = pd.Timestamp("2026-09-26")
    with pytest.raises(ModuleOutputError, match="future_market_context"):
        build_module_output(_snapshot(), market_context_snapshot=future)

    unverified = _context()
    unverified.loc[0, "assignment_pit_verified"] = False
    with pytest.raises(ModuleOutputError, match="unverified_context"):
        build_module_output(_snapshot(), market_context_snapshot=unverified)


def test_validation_report_cannot_claim_automatic_promotion_or_future_freeze():
    promoted = _validation_report()
    promoted["automatic_promotion_allowed"] = True
    with pytest.raises(ModuleOutputError, match="automatic_validation_promotion"):
        build_module_output(_snapshot(), validation_report=promoted)

    future = _validation_report()
    future["rules_frozen_through"] = "2026-09-26"
    with pytest.raises(ModuleOutputError, match="validation_freeze_after_output_as_of"):
        build_module_output(_snapshot(), validation_report=future)


def test_manual_forbidden_field_is_rejected_even_when_nested():
    output = build_module_output(_snapshot())
    output["primary_scenario"]["nested"] = {"trade_decision": "BUY"}
    with pytest.raises(ModuleOutputError, match="forbidden_output_fields"):
        validate_module_output(output)


def test_output_is_json_serializable_and_primary_fibonacci_has_required_shape():
    output = build_module_output(_snapshot(), market_context_snapshot=_context())
    dumped = json.dumps(output, sort_keys=True)
    assert dumped
    assert set(("anchor_start", "anchor_end", "zones")) <= set(output["fibonacci"])
    assert output["fibonacci"]["anchor_selection_by_fibonacci"] is False
