from __future__ import annotations

import pytest

from scanner.research.elliott_vnext.stage6_prospective_behavior import (
    Stage6ProspectiveBehaviorError,
    build_stage6_prospective_behavior,
)
from scanner.research.governance.qm_g_scenario_stability import (
    build_scenario_stability_feature,
)


SHA = "1" * 40


def _route(context: str, as_of: str) -> dict[str, object]:
    return {
        "review_context": context,
        "available_from": as_of,
        "actionability": "review_only_not_trade_instruction",
        "final_decision_owned_by_global_layer": True,
    }


def _output(
    output_id: str,
    scenario_id: str | None,
    as_of: str,
    *,
    symbol: str = "TEST",
    timeframe: str = "daily",
    degree: str = "intermediate",
    stage: str = "wave_4_complete",
    contexts: tuple[str, ...] = (),
    warnings: tuple[str, ...] = (),
) -> dict[str, object]:
    primary = {} if scenario_id is None else {
        "scenario_id": scenario_id,
        "pattern_class": "impulse",
        "family": "motive",
        "direction": "up",
        "stage": stage,
    }
    return {
        "schema_version": "elliott_vnext_output_v2",
        "module": "6H_module_output",
        "symbol": symbol,
        "as_of": as_of,
        "timeframe": timeframe,
        "degree": degree,
        "primary_scenario": primary,
        "alternative_scenarios": [],
        "pivots": [],
        "fibonacci": {"anchor_start": {}, "anchor_end": {}, "zones": []},
        "fibonacci_selects_wave_count": False,
        "current_wave_stage": stage,
        "projection_zones": [],
        "wave_cycle_map": {
            "current_stage": stage,
            "next_expected_structures": [],
            "scenario_maps": [],
        },
        "hard_invalidations": [],
        "structural_fit": None,
        "confirmation_strength": None,
        "historical_expectancy": None,
        "routing_triggers": [],
        "swing_routing": [_route(context, as_of) for context in contexts],
        "routing_is_trade_decision": False,
        "single_true_count_claimed": False,
        "integration": {
            "contract_version": "elliott_vnext_integration_contract_v1",
            "decision_layer_required": True,
            "productive_integration_enabled": False,
            "direct_ordering_allowed": False,
            "review_contexts_are_not_actions": True,
        },
        "warnings": list(warnings),
        "research_only": True,
        "output_id": output_id,
    }


def _capture(
    number: int,
    as_of: str,
    outputs: list[dict[str, object]],
    *,
    engine: str = "prospective_capture_engine_v2_iso_date_replay",
) -> dict[str, object]:
    return {
        "schema_version": "elliott_vnext_prospective_capture_v1",
        "capture_engine_version": engine,
        "module": "6H_prospective_shadow_capture",
        "capture_id": f"capture-{number}",
        "snapshot_id": f"snapshot-{number}",
        "as_of": as_of,
        "run_id": f"github-100-{number}",
        "source_publication_commit": SHA,
        "scanner_published_at": f"{as_of}T16:00:00+00:00",
        "captured_at": f"{as_of}T16:05:00+00:00",
        "rules_frozen_through": "2026-09-25",
        "validation_partition": "prospective_unspent",
        "universe_size": 3,
        "symbols_with_outputs": len({str(row["symbol"]) for row in outputs}),
        "output_count": len(outputs),
        "outputs": outputs,
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


def test_stage6_scenario_stability_matches_existing_qm_g_semantics() -> None:
    captures = [
        _capture(1, "2026-09-29", [_output("out-1", "scenario-a", "2026-09-29")]),
        _capture(2, "2026-09-30", [_output("out-2", "scenario-a", "2026-09-30")]),
        _capture(3, "2026-10-01", [_output("out-3", "scenario-b", "2026-10-01")]),
    ]
    report = build_stage6_prospective_behavior(captures)
    qm_g = build_scenario_stability_feature([
        {
            "source_commit": SHA,
            "available_from": capture["captured_at"],
            "output": capture["outputs"][0],
        }
        for capture in captures
    ])

    assert report["scenario_stability"]["counts"] == qm_g["counts"]
    assert report["scenario_stability"]["counts"] == {
        "STABLE": 1,
        "CHANGED": 1,
        "INSUFFICIENT_EVIDENCE": 0,
    }
    assert report["scenario_stability"]["threshold_used"] is False
    assert report["scenario_stability"]["smoothing_used"] is False
    assert report["boundaries"]["automatic_promotion_allowed"] is False


def test_missing_scenario_is_insufficient_and_wave_stage_is_separate() -> None:
    captures = [
        _capture(1, "2026-09-29", [_output("out-1", None, "2026-09-29", stage="uncertain")]),
        _capture(2, "2026-09-30", [_output("out-2", "scenario-a", "2026-09-30", stage="wave_2_complete")]),
    ]
    report = build_stage6_prospective_behavior(captures)

    assert report["scenario_stability"]["counts"]["INSUFFICIENT_EVIDENCE"] == 1
    assert report["wave_stage_stability"]["counts"]["CHANGED"] == 1
    assert report["interpretation"]["no_minimum_sample_threshold_invented"] is True


def test_multidegree_add_reduce_conflict_is_preserved_and_counted() -> None:
    capture = _capture(1, "2026-09-29", [
        _output(
            "add",
            "scenario-a",
            "2026-09-29",
            degree="intermediate",
            contexts=("entry_or_add_review", "hold_review"),
        ),
        _output(
            "reduce",
            "scenario-b",
            "2026-09-29",
            degree="primary",
            contexts=("profit_protection_review",),
        ),
    ])
    report = build_stage6_prospective_behavior([capture])

    review = report["review_contexts"]
    assert review["symbol_capture_observations"] == 1
    assert review["actionable_symbol_capture_observations"] == 1
    assert review["add_reduce_conflict_observations"] == 1
    assert review["route_occurrences"]["entry_or_add_review"] == 1
    assert review["route_occurrences"]["profit_protection_review"] == 1
    assert report["capture_summaries"][0]["add_reduce_conflict_symbols"] == ["TEST"]
    assert report["boundaries"]["degree_reducer_used"] is False


def test_warning_and_missing_routes_remain_descriptive_only() -> None:
    capture = _capture(1, "2026-09-29", [
        _output(
            "out-1",
            "scenario-a",
            "2026-09-29",
            warnings=("external_market_context_not_supplied",),
        ),
    ])
    report = build_stage6_prospective_behavior([capture])

    assert report["coverage"]["outputs_without_routes"] == 1
    assert report["warnings"]["occurrences"]["external_market_context_not_supplied"] == 1
    assert report["empirical_promotion_status"] == "NOT_PROMOTED"
    assert report["boundaries"]["changes_portfolio_action"] is False
    assert report["boundaries"]["trade_decision"] is None
    assert report["boundaries"]["order_instruction"] is None


def test_legacy_empty_capture_is_excluded_not_relabelled_as_current_evidence() -> None:
    legacy = _capture(
        0,
        "2026-09-28",
        [],
        engine="prospective_capture_engine_v1_pre_iso_fix",
    )
    current = _capture(
        1,
        "2026-09-29",
        [_output("out-1", "scenario-a", "2026-09-29")],
    )
    report = build_stage6_prospective_behavior([legacy, current])

    assert report["source"]["excluded_legacy_capture_count"] == 1
    assert report["source"]["eligible_capture_count"] == 1
    assert report["coverage"]["validated_6h_output_count"] == 1


def test_stage6_fails_closed_on_productive_or_future_guard_violation() -> None:
    bad = _capture(
        1,
        "2026-09-29",
        [_output("out-1", "scenario-a", "2026-09-29")],
    )
    bad["guards"]["changes_portfolio_action"] = True
    with pytest.raises(Stage6ProspectiveBehaviorError, match="capture_guard_false_required"):
        build_stage6_prospective_behavior([bad])

    bad = _capture(
        1,
        "2026-09-29",
        [_output("out-1", "scenario-a", "2026-09-29")],
    )
    bad["captured_at"] = "2026-09-28T16:05:00+00:00"
    with pytest.raises(Stage6ProspectiveBehaviorError, match="capture_before_scanner_publication"):
        build_stage6_prospective_behavior([bad])
