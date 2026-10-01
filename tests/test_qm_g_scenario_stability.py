from __future__ import annotations

import pytest

from scanner.research.governance.qm_g_scenario_stability import (
    ScenarioStabilityError,
    build_scenario_stability_feature,
    register_scenario_stability_lineage,
)
from scanner.research.governance.qm_i_lineage import LineageRegistry, content_hash


def _output(output_id: str, scenario_id: str | None, as_of: str = "2026-09-29", symbol: str = "TEST"):
    primary = {} if scenario_id is None else {
        "scenario_id": scenario_id,
        "pattern_class": "impulse",
        "family": "motive",
        "direction": "up",
        "stage": "wave_4_complete",
    }
    return {
        "schema_version": "elliott_vnext_output_v2",
        "module": "6H_module_output",
        "symbol": symbol,
        "as_of": as_of,
        "timeframe": "daily",
        "degree": "intermediate",
        "primary_scenario": primary,
        "alternative_scenarios": [],
        "pivots": [],
        "fibonacci": {"anchor_start": {}, "anchor_end": {}, "zones": []},
        "fibonacci_selects_wave_count": False,
        "current_wave_stage": "wave_4_complete" if scenario_id else "uncertain",
        "projection_zones": [],
        "wave_cycle_map": {"current_stage": "uncertain", "next_expected_structures": [], "scenario_maps": []},
        "hard_invalidations": [],
        "structural_fit": None,
        "confirmation_strength": None,
        "historical_expectancy": None,
        "routing_triggers": [],
        "swing_routing": [],
        "routing_is_trade_decision": False,
        "single_true_count_claimed": False,
        "integration": {
            "contract_version": "elliott_vnext_integration_contract_v1",
            "decision_layer_required": True,
            "productive_integration_enabled": False,
            "direct_ordering_allowed": False,
            "review_contexts_are_not_actions": True,
        },
        "warnings": [],
        "research_only": True,
        "output_id": output_id,
    }


def _snap(output, available_from):
    return {"source_commit": "1" * 40, "available_from": available_from, "output": output}


def test_scenario_stability_uses_strict_consecutive_scenario_id_equality_only():
    feature = build_scenario_stability_feature([
        _snap(_output("out-1", "scenario-a"), "2026-09-29T17:00:00+00:00"),
        _snap(_output("out-2", "scenario-a", "2026-09-30"), "2026-09-30T17:00:00+00:00"),
        _snap(_output("out-3", "scenario-b", "2026-10-01"), "2026-10-01T17:00:00+00:00"),
    ])
    assert feature["counts"] == {"STABLE": 1, "CHANGED": 1, "INSUFFICIENT_EVIDENCE": 0}
    assert [row["feature_status"] for row in feature["observations"]] == ["STABLE", "CHANGED"]
    assert feature["feature_definition"]["threshold_used"] is False
    assert feature["feature_definition"]["smoothing_used"] is False
    assert feature["outcomes_used_to_build_feature"] is False
    assert feature["creates_review_context"] is False
    assert feature["changes_elliott_core"] is False
    assert feature["changes_portfolio_action"] is False


def test_missing_primary_scenario_remains_insufficient_not_neutral():
    feature = build_scenario_stability_feature([
        _snap(_output("out-1", None), "2026-09-29T17:00:00+00:00"),
        _snap(_output("out-2", "scenario-a", "2026-09-30"), "2026-09-30T17:00:00+00:00"),
    ])
    row = feature["observations"][0]
    assert row["feature_status"] == "INSUFFICIENT_EVIDENCE"
    assert row["primary_scenario_id_stable"] is None
    assert row["missing_is_not_neutral"] is True


def test_future_output_relative_to_publication_fails_closed():
    with pytest.raises(ScenarioStabilityError, match="future_output_relative_to_source_availability"):
        build_scenario_stability_feature([
            _snap(_output("out-1", "a", "2026-09-30"), "2026-09-29T17:00:00+00:00"),
            _snap(_output("out-2", "a", "2026-10-01"), "2026-10-01T17:00:00+00:00"),
        ])


def test_cross_symbol_or_duplicate_output_identity_fails_closed():
    with pytest.raises(ScenarioStabilityError, match="snapshot_identity_mismatch"):
        build_scenario_stability_feature([
            _snap(_output("out-1", "a"), "2026-09-29T17:00:00+00:00"),
            _snap(_output("out-2", "a", "2026-09-30", symbol="OTHER"), "2026-09-30T17:00:00+00:00"),
        ])
    with pytest.raises(ScenarioStabilityError, match="duplicate_output_id"):
        build_scenario_stability_feature([
            _snap(_output("same", "a"), "2026-09-29T17:00:00+00:00"),
            _snap(_output("same", "a", "2026-09-30"), "2026-09-30T17:00:00+00:00"),
        ])


def test_invalid_productive_6h_input_is_rejected():
    bad = _output("out-1", "a")
    bad["integration"]["productive_integration_enabled"] = True
    with pytest.raises(ScenarioStabilityError, match="invalid_6h_output"):
        build_scenario_stability_feature([
            _snap(bad, "2026-09-29T17:00:00+00:00"),
            _snap(_output("out-2", "a", "2026-09-30"), "2026-09-30T17:00:00+00:00"),
        ])


def _register_raw(registry, node_id, version_id, payload, complete=True):
    digest = content_hash(payload)
    registry.register_node(
        record={
            "node_id": node_id,
            "version_id": version_id,
            "node_type": "RAW_SOURCE",
            "content_hash": digest,
            "lineage_complete": complete,
            "as_of": payload["available_from"],
            "metadata": {"source": "elliott_6h_snapshot"},
        },
        actor_id="tester",
        actor_role="TEST",
    )
    return {"node_id": node_id, "version_id": version_id, "content_hash": digest}


def test_feature_lineage_is_materially_derived_from_each_declared_6h_source(tmp_path):
    feature = build_scenario_stability_feature([
        _snap(_output("out-1", "a"), "2026-09-29T17:00:00+00:00"),
        _snap(_output("out-2", "a", "2026-09-30"), "2026-09-30T17:00:00+00:00"),
    ])
    lineage = LineageRegistry(tmp_path / "lineage.jsonl")
    one = _register_raw(lineage, "elliott-6h-1", "v1", {"available_from": "2026-09-29T17:00:00+00:00"})
    two = _register_raw(lineage, "elliott-6h-2", "v1", {"available_from": "2026-09-30T17:00:00+00:00"})
    ref = register_scenario_stability_lineage(
        feature,
        source_refs=[one, two],
        lineage_registry=lineage,
        node_id="qm-g-scenario-stability",
        version_id="v1",
        actor_id="tester",
        actor_role="TEST",
    )
    node = lineage.get_node(ref["node_id"], ref["version_id"])
    assert node["node_type"] == "FEATURE"
    assert node["lineage_complete"] is True
    ancestry = lineage.analyze_ancestry(
        left_node_id=one["node_id"], left_version_id=one["version_id"],
        right_node_id=ref["node_id"], right_version_id=ref["version_id"],
    )
    assert ancestry["classification"] == "DIRECT_DEPENDENCY"


def test_incomplete_source_lineage_blocks_feature_registration(tmp_path):
    feature = build_scenario_stability_feature([
        _snap(_output("out-1", "a"), "2026-09-29T17:00:00+00:00"),
        _snap(_output("out-2", "a", "2026-09-30"), "2026-09-30T17:00:00+00:00"),
    ])
    lineage = LineageRegistry(tmp_path / "lineage.jsonl")
    one = _register_raw(lineage, "elliott-6h-1", "v1", {"available_from": "2026-09-29T17:00:00+00:00"}, complete=False)
    two = _register_raw(lineage, "elliott-6h-2", "v1", {"available_from": "2026-09-30T17:00:00+00:00"})
    with pytest.raises(ScenarioStabilityError, match="source_lineage_incomplete"):
        register_scenario_stability_lineage(
            feature, source_refs=[one, two], lineage_registry=lineage,
            node_id="qm-g-scenario-stability", version_id="v1",
            actor_id="tester", actor_role="TEST",
        )
