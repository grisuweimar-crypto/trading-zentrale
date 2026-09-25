from __future__ import annotations

import pandas as pd

from scanner.research.decision_layer.conflict_research import (
    ConflictResearchConfig,
    analyze_conflicts,
    timing_topology,
)
from scanner.research.decision_layer.relation_graph import packet_relation_graph


def _dataset() -> pd.DataFrame:
    rows = []
    states = [
        (1, 0, 0.04),
        (1, 0, 0.03),
        (0, 1, -0.04),
        (0, 1, -0.03),
        (1, 1, 0.00),
        (1, 1, -0.01),
        (0, 0, 0.01),
        (0, 0, -0.01),
    ]
    for index, (pos, neg, peer) in enumerate(states):
        obs = pd.Timestamp("2026-06-01") + pd.Timedelta(days=index)
        row = {
            "symbol": f"S{index}",
            "obs_date": obs,
            "start_market_date": obs,
            "research_partition": "legacy_replay_spent",
            "quality_band": "B3" if index % 2 else "B4",
            "aggregate_risk": float(index),
            "volatility": 0.1 + index * 0.01,
            "dataset_schema_version": "decision_research_dataset_v1",
        }
        for horizon in (5, 20, 40, 60):
            row[f"timing_positive_match_count_{horizon}t"] = pos
            row[f"timing_negative_match_count_{horizon}t"] = neg
            row[f"peer_excess_{horizon}t"] = peer
            row[f"adverse_excursion_{horizon}t"] = 0.02 + index * 0.001
            row[f"path_max_drawdown_{horizon}t"] = 0.03 + index * 0.001
            row[f"label_available_from_{horizon}t"] = (
                obs + pd.Timedelta(days=horizon)
            ).date().isoformat()
        rows.append(row)
    return pd.DataFrame(rows)


def _common(family: str, claim_id: str, payload: dict, **extra) -> dict:
    row = {
        "family": family,
        "claim_id": claim_id,
        "as_of": "2026-09-26T18:00:00Z",
        "available_from": "2026-09-26T18:00:00Z",
        "source_version": "test-v1",
        "coverage_state": "available",
        "maturity_state": "robust",
        "pit_state": "verified",
        "integration_mode": "research_only",
        "payload": payload,
    }
    row.update(extra)
    return row


def test_timing_topology_distinguishes_conflict_from_none() -> None:
    assert timing_topology(0, 0) == "none"
    assert timing_topology(2, 0) == "positive_only"
    assert timing_topology(0, 3) == "negative_only"
    assert timing_topology(1, 1) == "mixed_conflict"


def test_spent_research_never_computes_stance_or_action() -> None:
    contract = {
        "schema_version": "decision_conflict_research_v1",
        "research_only": True,
        "productive_integration_enabled": False,
        "discovery_partition": "legacy_replay_spent",
        "prospective_confirmation_partition": "prospective_unspent",
        "prospective_confirmation_start": "2026-09-26",
        "min_group_n": 1,
        "bootstrap_reps": 0,
        "random_seed": 1,
        "pre_registered_comparisons": [
            {
                "id": "p5",
                "horizon_sessions": 5,
                "left": "positive_only",
                "right": "none",
                "target": "peer_excess",
                "expected_direction": "positive",
            }
        ],
    }
    report = analyze_conflicts(_dataset(), contract, ConflictResearchConfig.from_contract(contract))
    assert report["research_only"] is True
    assert report["semantics"]["universal_stance_computed"] is False
    assert report["semantics"]["portfolio_action_computed"] is False
    assert report["horizons"]["5"]["mixed_conflict"]["expected_direction"] is None
    assert report["horizons"]["5"]["pre_registered_comparisons"]["p5"][
        "prospective_confirmation_required"
    ] is True


def test_packet_graph_marks_cross_family_conflict_without_resolving_it() -> None:
    packet = {
        "schema_version": "decision_layer_input_contract_v1",
        "symbol": "TEST",
        "as_of": "2026-09-26T18:00:00Z",
        "source_snapshot_id": "snap-1",
        "evidence": [
            _common("selection", "sel", {"direction": "positive"}),
            _common(
                "timing",
                "tim",
                {
                    "direction": "negative",
                    "pattern_id": "p1",
                    "horizon_sessions": 20,
                    "pattern_frozen": True,
                    "match_from_pit_features": True,
                },
            ),
            _common(
                "probability",
                "prob",
                {"horizon_sessions": 20, "probability": 0.61},
                claim_ref="tim",
            ),
            _common(
                "confidence",
                "conf",
                {"reliability": 0.7},
                claim_ref="tim",
            ),
            _common("risk", "risk", {"aggregate_risk": 42.0}),
        ],
    }
    graph = packet_relation_graph(packet)
    assert graph["relation_state"] == "conflict_present"
    assert graph["conflicts"][0]["relation_type"] == "cross_family_conflict"
    assert graph["conflicts"][0]["resolved"] is False
    assert graph["probability_and_confidence_count_as_votes"] is False
    assert graph["risk_and_elliott_count_as_votes"] is False
    assert graph["universal_stance_computed"] is False


def test_probability_and_confidence_do_not_turn_one_timing_claim_into_confirmation() -> None:
    packet = {
        "schema_version": "decision_layer_input_contract_v1",
        "symbol": "TEST",
        "as_of": "2026-09-26T18:00:00Z",
        "source_snapshot_id": "snap-2",
        "evidence": [
            _common(
                "timing",
                "tim",
                {
                    "direction": "positive",
                    "pattern_id": "p1",
                    "horizon_sessions": 5,
                    "pattern_frozen": True,
                    "match_from_pit_features": True,
                },
            ),
            _common(
                "probability",
                "prob",
                {"horizon_sessions": 5, "probability": 0.64},
                claim_ref="tim",
            ),
            _common(
                "confidence",
                "conf",
                {"reliability": 0.8},
                claim_ref="tim",
            ),
        ],
    }
    graph = packet_relation_graph(packet)
    assert graph["relation_state"] == "single_direction_or_unopposed"
    assert graph["support_relations"] == []
    assert len(graph["annotations"]) == 2


def test_missing_direction_is_unknown_not_neutral() -> None:
    packet = {
        "schema_version": "decision_layer_input_contract_v1",
        "symbol": "TEST",
        "as_of": "2026-09-26T18:00:00Z",
        "source_snapshot_id": "snap-3",
        "evidence": [
            _common(
                "timing",
                "tim",
                {
                    "pattern_id": "p1",
                    "horizon_sessions": 5,
                    "pattern_frozen": True,
                    "match_from_pit_features": True,
                },
            )
        ],
    }
    graph = packet_relation_graph(packet)
    assert graph["relation_state"] == "insufficient_directional_relation"
    assert graph["unknown_direction_claim_ids"] == ["tim"]
    assert graph["missing_direction_is_neutral"] is False


def test_ineligible_directional_claim_cannot_create_conflict() -> None:
    unavailable_selection = _common("selection", "sel", {"direction": "positive"})
    unavailable_selection["coverage_state"] = "unavailable"
    packet = {
        "schema_version": "decision_layer_input_contract_v1",
        "symbol": "TEST",
        "as_of": "2026-09-26T18:00:00Z",
        "source_snapshot_id": "snap-4",
        "evidence": [
            unavailable_selection,
            _common(
                "timing",
                "tim",
                {
                    "direction": "negative",
                    "pattern_id": "p1",
                    "horizon_sessions": 20,
                    "pattern_frozen": True,
                    "match_from_pit_features": True,
                },
            ),
        ],
    }
    graph = packet_relation_graph(packet)
    assert graph["relation_state"] == "single_direction_or_unopposed"
    assert graph["conflicts"] == []
    assert [row["claim_id"] for row in graph["ineligible_directional_claims"]] == ["sel"]
