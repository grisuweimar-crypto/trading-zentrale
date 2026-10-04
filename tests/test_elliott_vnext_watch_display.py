from __future__ import annotations

from copy import deepcopy

import pytest

from scanner.research.decision_layer.elliott_readonly_display import (
    AVAILABLE_STATUS,
    ElliottWatchDisplayError,
    MISSING_STATUS,
    NO_SYMBOL_STATUS,
    STALE_STATUS,
    build_elliott_watch_display,
    build_prepared_elliott_watch_display,
    prepare_elliott_watch_display_capture,
)


def _output(
    *,
    symbol: str = "AAA",
    timeframe: str = "daily",
    degree: str = "fine",
    stage: str = "wave_2_complete",
    direction: str = "up",
) -> dict[str, object]:
    return {
        "schema_version": "elliott_vnext_output_v2",
        "module": "6H_module_output",
        "symbol": symbol,
        "as_of": "2026-10-02",
        "timeframe": timeframe,
        "degree": degree,
        "selection_policy": "test",
        "single_true_count_claimed": False,
        "primary_scenario": {
            "scenario_id": f"{symbol}-{timeframe}-{degree}-primary",
            "family": "motive",
            "direction": direction,
            "stage": stage,
            "status": "valid",
            "available_from": "2026-10-02",
        },
        "alternative_scenarios": [
            {
                "scenario_id": f"{symbol}-{timeframe}-{degree}-alt",
                "family": "motive",
                "direction": "down" if direction == "up" else "up",
                "stage": "uncertain",
                "status": "valid",
                "available_from": "2026-10-02",
            }
        ],
        "pivots": [],
        "fibonacci": {
            "anchor_start": {},
            "anchor_end": {},
            "zones": [],
            "scenario_id": f"{symbol}-{timeframe}-{degree}-primary",
            "status": "unavailable_for_primary_scenario",
            "anchor_selection_by_fibonacci": False,
        },
        "fibonacci_geometry": [],
        "fibonacci_selects_wave_count": False,
        "current_wave_stage": stage,
        "projection_zones": [],
        "wave_cycle_map": {
            "current_stage": stage,
            "next_expected_structures": ["wave_3"],
            "scenario_maps": [],
        },
        "hard_invalidations": [],
        "rule_violations": [],
        "structural_fit": None,
        "confirmation_strength": None,
        "historical_expectancy": None,
        "routing_triggers": ["EW_W2_CORE"],
        "swing_routing": [
            {
                "trigger": "EW_W2_CORE",
                "review_context": "entry_or_add_review",
                "reason": "research review only",
                "scenario_id": f"{symbol}-{timeframe}-{degree}-primary",
                "scenario_role": "primary",
                "available_from": "2026-10-02",
                "source": "test",
                "requires_external_confirmation": True,
                "final_decision_owned_by_global_layer": True,
                "actionability": "review_only_not_trade_instruction",
                "research_only": True,
            }
        ],
        "routing_summary": {
            "primary_review_contexts": ["entry_or_add_review"],
            "alternative_review_contexts": [],
            "within_scenario_conflict": False,
            "cross_scenario_conflict": False,
            "conflicts_preserved_not_resolved": False,
            "final_decision_required": True,
            "historical_net_benefit_evaluated": False,
        },
        "routing_is_trade_decision": False,
        "market_context": None,
        "relative_strength": None,
        "cross_system": None,
        "validation": {
            "status": "validation_report_not_supplied",
            "technical_completion_is_empirical_validation": False,
            "automatic_promotion_allowed": False,
            "rules_frozen_through": None,
            "evidence_policy": None,
        },
        "integration": {
            "contract_version": "elliott_vnext_integration_contract_v1",
            "decision_layer_required": True,
            "decision_authority": "future_global_decision_layer_or_orchestrating_depot_watch",
            "productive_integration_enabled": False,
            "direct_ordering_allowed": False,
            "review_contexts_are_not_actions": True,
            "technical_module_6_complete": True,
            "empirical_promotion_status": "validation_report_not_supplied",
            "primary_scenario_id": f"{symbol}-{timeframe}-{degree}-primary",
        },
        "warnings": ["historical_expectancy_not_supplied"],
        "research_only": True,
        "output_id": f"{symbol}-{timeframe}-{degree}-id",
    }


def _capture(*outputs: dict[str, object]) -> dict[str, object]:
    return {
        "schema_version": "elliott_vnext_prospective_capture_v1",
        "module": "6H_prospective_shadow_capture",
        "capture_id": "capture-2026-10-02",
        "snapshot_id": "snapshot-2026-10-02",
        "as_of": "2026-10-02",
        "run_id": "github-123-1",
        "source_publication_commit": "a" * 40,
        "scanner_published_at": "2026-10-02T17:00:00+00:00",
        "captured_at": "2026-10-02T17:05:00+00:00",
        "rules_frozen_through": "2026-09-25",
        "validation_partition": "prospective_unspent",
        "replay_price_basis": "adjusted",
        "universe_size": 2,
        "symbols_with_outputs": len({str(row["symbol"]) for row in outputs}),
        "output_count": len(outputs),
        "outputs": list(outputs),
        "coverage": {
            "symbols_requested": 2,
            "symbols_with_outputs": len({str(row["symbol"]) for row in outputs}),
            "symbols_without_outputs": 2 - len({str(row["symbol"]) for row in outputs}),
            "output_count": len(outputs),
            "details": [],
            "missing_evidence_not_imputed": True,
        },
        "source_hashes": {
            "market_ohlcv_sha256": "price-hash",
            "daily_research_sha256": "daily-hash",
        },
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


def test_available_display_keeps_all_current_symbol_degrees_read_only():
    capture = _capture(
        _output(timeframe="daily", degree="fine"),
        _output(timeframe="weekly", degree="coarse", stage="wave_4_complete"),
        _output(symbol="BBB", timeframe="daily", degree="fine"),
    )

    display = build_elliott_watch_display(
        capture,
        symbol="AAA",
        expected_snapshot_id="snapshot-2026-10-02",
        expected_as_of="2026-10-02",
    )

    assert display["status"] == AVAILABLE_STATUS
    assert display["output_count"] == 2
    assert {(row["timeframe"], row["degree"]) for row in display["outputs"]} == {
        ("daily", "fine"),
        ("weekly", "coarse"),
    }
    assert display["research_only"] is True
    assert display["read_only_presentation"] is True
    assert display["w10_source_emitted"] is False
    assert display["changes_universal_stance"] is False
    assert display["changes_portfolio_action"] is False
    assert display["decision_effect"] == "none"
    assert display["outputs"][0]["swing_routing"][0]["review_context"] == "entry_or_add_review"
    assert display["outputs"][0]["swing_routing"][0]["actionability"] == "review_only_not_trade_instruction"


def test_missing_capture_is_visible_and_does_not_invent_neutral_state():
    display = build_elliott_watch_display(
        None,
        symbol="AAA",
        expected_snapshot_id="snapshot-2026-10-02",
        expected_as_of="2026-10-02",
    )

    assert display["status"] == MISSING_STATUS
    assert display["outputs"] == []
    assert display["output_count"] == 0
    assert display["decision_effect"] == "none"


def test_stale_capture_is_not_joined_to_current_watch_snapshot():
    display = build_elliott_watch_display(
        _capture(_output()),
        symbol="AAA",
        expected_snapshot_id="different-current-snapshot",
        expected_as_of="2026-10-02",
    )

    assert display["status"] == STALE_STATUS
    assert display["source_snapshot_id"] == "snapshot-2026-10-02"
    assert display["outputs"] == []
    assert display["decision_effect"] == "none"


def test_symbol_without_6h_output_stays_missing():
    display = build_elliott_watch_display(
        _capture(_output(symbol="BBB")),
        symbol="AAA",
        expected_snapshot_id="snapshot-2026-10-02",
        expected_as_of="2026-10-02",
    )

    assert display["status"] == NO_SYMBOL_STATUS
    assert display["outputs"] == []
    assert display["decision_effect"] == "none"


def test_invalid_decision_effect_guard_fails_closed():
    capture = _capture(_output())
    broken = deepcopy(capture)
    broken["guards"]["changes_portfolio_action"] = True

    with pytest.raises(ElliottWatchDisplayError, match="elliott_capture_guard_invalid:changes_portfolio_action"):
        build_elliott_watch_display(
            broken,
            symbol="AAA",
            expected_snapshot_id="snapshot-2026-10-02",
            expected_as_of="2026-10-02",
        )


def test_prepared_capture_matches_one_off_display_without_revalidation_semantics_change():
    capture = _capture(
        _output(timeframe="daily", degree="fine"),
        _output(timeframe="weekly", degree="coarse", stage="wave_4_complete"),
        _output(symbol="BBB", timeframe="daily", degree="fine"),
    )
    prepared = prepare_elliott_watch_display_capture(capture)

    prepared_display = build_prepared_elliott_watch_display(
        prepared,
        symbol="AAA",
        expected_snapshot_id="snapshot-2026-10-02",
        expected_as_of="2026-10-02",
    )
    one_off_display = build_elliott_watch_display(
        capture,
        symbol="AAA",
        expected_snapshot_id="snapshot-2026-10-02",
        expected_as_of="2026-10-02",
    )

    assert prepared_display == one_off_display
    assert prepared_display["output_count"] == 2
    assert prepared_display["decision_effect"] == "none"
    assert prepared_display["changes_universal_stance"] is False
    assert prepared_display["changes_portfolio_action"] is False
