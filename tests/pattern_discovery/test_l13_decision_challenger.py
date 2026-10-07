from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json

import pytest

import scanner.research.pattern_discovery.challenger_integration as l13
from scanner.research.pattern_discovery.challenger_integration import (
    ChallengerIntegrationError,
    build_challenger_trace,
    evaluate_incremental_value,
    verify_challenger_trace,
    verify_incremental_evaluation,
)


def canon(value):
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def digest(value):
    return sha256(canon(value).encode("utf-8")).hexdigest()


class FakeRegistry:
    def __init__(self, status="ADMITTED"):
        self.status = status

    def current_status(self, identity):
        return {
            "status": self.status,
            "decision_id": f"DEC-{identity['pattern_id']}",
        }


def packet(*, direction="positive", horizon=20):
    return {
        "schema_version": "decision_layer_input_contract_v1",
        "symbol": "AAA",
        "as_of": "2026-10-07T15:00:00Z",
        "source_snapshot_id": "SNAP-L13",
        "evidence": [
            {
                "family": "timing",
                "claim_id": "TIMING-1",
                "payload": {
                    "pattern_id": "OLD-TIMING",
                    "horizon_sessions": horizon,
                    "direction": direction,
                },
            }
        ],
        "research_only": True,
        "productive_integration_enabled": False,
    }


def decision(pattern_id, *, direction="positive", horizon=20):
    return {
        "pattern_identity": {
            "pattern_id": pattern_id,
            "pattern_version": "v1",
            "pattern_spec_hash": (
                "a" if pattern_id.endswith("A") else "b"
            ) * 64,
            "target_id": "PEER_EXCESS",
            "horizon_sessions": horizon,
            "baseline": "PIT_PEER_BASELINE",
        },
        "promotion_status": "ADMITTED",
        "decision_id": f"DEC-{pattern_id}",
        "decision_hash": ("c" if pattern_id.endswith("A") else "d") * 64,
        "admission_contract": {
            "integration_mode": "SHADOW_CHALLENGER",
            "directional_authority": "SHADOW_ONLY",
            "decision_layer_effect_active": False,
            "requires_l13_before_activation": True,
            "direction": direction.upper(),
            "target_id": "PEER_EXCESS",
            "horizon_sessions": horizon,
            "baseline": "PIT_PEER_BASELINE",
            "probability_attachment": {
                "values": {"direction_probability": 0.68}
            },
            "dependency_handling": "CORRELATED_NO_COUNTING",
        },
    }


def graph(*, severity="NONE", relationship="RELATED"):
    edges = []
    if severity != "NONE" or relationship != "RELATED":
        edges.append(
            {
                "source_node_id": "N1",
                "target_node_id": "N2",
                "relationship": relationship,
                "dependency_severity": severity,
            }
        )
    return {
        "graph_id": "PDG-L13",
        "graph_hash": "e" * 64,
        "nodes": [
            {
                "node_id": "N1",
                "pattern_id": "PAT-L13-A",
                "pattern_version": "v1",
                "pattern_spec_hash": "a" * 64,
            },
            {
                "node_id": "N2",
                "pattern_id": "PAT-L13-B",
                "pattern_version": "v1",
                "pattern_spec_hash": "b" * 64,
            },
        ],
        "edges": edges,
    }


def context(pattern_id, *, direction="positive", dep_graph=None):
    return {
        "decision": decision(pattern_id, direction=direction),
        "frozen_pattern": {"stub": True},
        "rating_history": {"stub": True},
        "confirmation_reports": [{"stub": True}],
        "dependency_graph": dep_graph or graph(),
    }


def claim(pattern_id, *, direction="positive", claim_id=None):
    spec_hash = ("a" if pattern_id.endswith("A") else "b") * 64
    return {
        "claim_id": claim_id or f"CLAIM-{pattern_id}",
        "claim_hash": "f" * 64,
        "pattern": {
            "pattern_id": pattern_id,
            "pattern_version": "v1",
            "pattern_spec_hash": spec_hash,
        },
        "match": {
            "symbol": "AAA",
            "snapshot_id": "SNAP-L13",
            "observation_as_of": "2026-10-07T15:00:00Z",
        },
        "forecast": {
            "expected_direction": direction.upper(),
            "target_id": "PEER_EXCESS",
            "horizon_sessions": 20,
            "baseline": "PIT_PEER_BASELINE",
        },
    }


@pytest.fixture(autouse=True)
def isolate_upstream(monkeypatch):
    monkeypatch.setattr(
        l13, "validate_input_packet", lambda value: deepcopy(dict(value))
    )
    monkeypatch.setattr(
        l13,
        "packet_relation_graph",
        lambda value: {
            "eligible_directional_claim_ids": [
                row["claim_id"]
                for row in value.get("evidence", [])
                if row.get("family") == "timing"
            ]
        },
    )
    monkeypatch.setattr(
        l13,
        "compute_universal_stance",
        lambda value: {
            "schema_version": "decision_universal_stance_v1",
            "universal_stance": {
                "state": "positive",
                "direction": "positive",
            },
        },
    )
    monkeypatch.setattr(
        l13, "verify_promotion_decision", lambda value: {"valid": True}
    )
    monkeypatch.setattr(
        l13,
        "validate_admission_current",
        lambda *args, **kwargs: {"valid": True, "status": "CURRENT"},
    )
    monkeypatch.setattr(
        l13, "verify_prospective_claim", lambda value: {"valid": True}
    )
    monkeypatch.setattr(
        l13,
        "verify_matured_outcome",
        lambda value: {
            "valid": True,
            "claim_id": value["claim"]["claim_id"],
            "outcome_hash": value["outcome_hash"],
        },
    )


def test_same_direction_pattern_corroborates_without_action_change():
    trace = build_challenger_trace(
        packet(direction="positive"),
        [claim("PAT-L13-A", direction="positive")],
        [context("PAT-L13-A", direction="positive")],
        promotion_registry=FakeRegistry(),
        horizon_sessions=20,
        generated_at="2026-10-07T15:01:00Z",
    )
    assert trace["timing_ablation"]["baseline_timing"]["state"] == "positive"
    assert trace["timing_ablation"]["pattern_challenger"]["state"] == "positive"
    assert (
        trace["timing_ablation"]["shadow_fused_timing"]["relation"]
        == "PATTERN_CORROBORATES_TIMING"
    )
    assert trace["boundaries"]["productive_decision_change_performed"] is False
    assert trace["relation_graph"]["numeric_vote_counting_used"] is False


def test_opposite_pattern_exposes_conflict_without_flipping_baseline():
    trace = build_challenger_trace(
        packet(direction="positive"),
        [claim("PAT-L13-A", direction="negative")],
        [context("PAT-L13-A", direction="negative")],
        promotion_registry=FakeRegistry(),
        horizon_sessions=20,
        generated_at="2026-10-07T15:01:00Z",
    )
    assert trace["timing_ablation"]["pattern_challenger"]["state"] == "negative"
    assert trace["timing_ablation"]["shadow_fused_timing"]["state"] == "conflicted"
    assert (
        trace["timing_ablation"]["shadow_fused_timing"]["relation"]
        == "PATTERN_CONFLICTS_WITH_TIMING"
    )


def test_highly_dependent_patterns_collapse_into_one_cluster():
    dep = graph(severity="HIGH")
    trace = build_challenger_trace(
        packet(direction="positive"),
        [
            claim("PAT-L13-A", direction="positive", claim_id="C-A"),
            claim("PAT-L13-B", direction="positive", claim_id="C-B"),
        ],
        [
            context("PAT-L13-A", direction="positive", dep_graph=dep),
            context("PAT-L13-B", direction="positive", dep_graph=dep),
        ],
        promotion_registry=FakeRegistry(),
        horizon_sessions=20,
        generated_at="2026-10-07T15:01:00Z",
    )
    clusters = trace["relation_graph"]["pattern_clusters"]
    assert len(clusters) == 1
    assert clusters[0]["member_count"] == 2
    assert clusters[0]["counts_as_numeric_vote"] is False


def test_correlated_opposite_patterns_become_conflicted_cluster():
    dep = graph(severity="CRITICAL", relationship="DUPLICATE")
    trace = build_challenger_trace(
        packet(direction="positive"),
        [
            claim("PAT-L13-A", direction="positive", claim_id="C-A"),
            claim("PAT-L13-B", direction="negative", claim_id="C-B"),
        ],
        [
            context("PAT-L13-A", direction="positive", dep_graph=dep),
            context("PAT-L13-B", direction="negative", dep_graph=dep),
        ],
        promotion_registry=FakeRegistry(),
        horizon_sessions=20,
        generated_at="2026-10-07T15:01:00Z",
    )
    assert trace["timing_ablation"]["pattern_challenger"]["state"] == "conflicted"
    assert trace["timing_ablation"]["shadow_fused_timing"]["state"] == "conflicted"


def test_rolled_back_l12_admission_is_rejected():
    with pytest.raises(
        ChallengerIntegrationError,
        match="l12_registry_state_not_currently_admitted",
    ):
        build_challenger_trace(
            packet(),
            [claim("PAT-L13-A")],
            [context("PAT-L13-A")],
            promotion_registry=FakeRegistry(status="ROLLED_BACK"),
            horizon_sessions=20,
            generated_at="2026-10-07T15:01:00Z",
        )


def test_stale_l12_admission_is_rejected(monkeypatch):
    monkeypatch.setattr(
        l13,
        "validate_admission_current",
        lambda *args, **kwargs: {
            "valid": False,
            "status": "STALE_REVIEW_REQUIRED",
        },
    )
    with pytest.raises(
        ChallengerIntegrationError,
        match="l12_admission_stale_review_required",
    ):
        build_challenger_trace(
            packet(),
            [claim("PAT-L13-A")],
            [context("PAT-L13-A")],
            promotion_registry=FakeRegistry(),
            horizon_sessions=20,
            generated_at="2026-10-07T15:01:00Z",
        )


def test_other_snapshot_claim_does_not_leak_into_trace():
    other = claim("PAT-L13-A")
    other["match"]["snapshot_id"] = "OTHER-SNAPSHOT"
    trace = build_challenger_trace(
        packet(),
        [other],
        [context("PAT-L13-A")],
        promotion_registry=FakeRegistry(),
        horizon_sessions=20,
        generated_at="2026-10-07T15:01:00Z",
    )
    assert trace["active_pattern_claims"] == []
    assert (
        trace["timing_ablation"]["pattern_challenger"]["state"]
        == "insufficient_evidence"
    )


def test_trace_hash_is_tamper_evident():
    trace = build_challenger_trace(
        packet(),
        [claim("PAT-L13-A")],
        [context("PAT-L13-A")],
        promotion_registry=FakeRegistry(),
        horizon_sessions=20,
        generated_at="2026-10-07T15:01:00Z",
    )
    assert verify_challenger_trace(trace)["valid"] is True
    tampered = deepcopy(trace)
    tampered["timing_ablation"]["pattern_challenger"]["state"] = "negative"
    with pytest.raises(
        ChallengerIntegrationError,
        match="challenger_trace_hash_mismatch",
    ):
        verify_challenger_trace(tampered)


def synthetic_trace(index, *, baseline, challenger, target=0.05, contract=None):
    spec = contract or l13.load_challenger_contract()
    trace = {
        "schema_version": l13.TRACE_SCHEMA_VERSION,
        "module": "pattern_discovery_lab",
        "phase": "L13",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "generated_at": "2026-10-07T15:01:00Z",
        "symbol": f"S{index:03d}",
        "observation_as_of": f"2026-09-{(index % 28)+1:02d}T15:00:00Z",
        "snapshot_id": f"SNAP-{index:03d}",
        "horizon_sessions": 20,
        "target_scope": {
            "target_id": "PEER_EXCESS",
            "baseline": "PIT_PEER_BASELINE",
            "horizon_sessions": 20,
        },
        "baseline_decision_context": {
            "state": baseline,
            "direction": baseline if baseline in {"positive", "negative"} else None,
            "source_schema": "decision_universal_stance_v1",
            "packet_hash": "1" * 64,
            "relation_graph_hash": "2" * 64,
            "mutated_by_l13": False,
        },
        "timing_ablation": {
            "comparison": spec["ablation"]["comparison"],
            "baseline_timing": {
                "state": baseline,
                "direction": baseline if baseline in {"positive", "negative"} else None,
            },
            "pattern_challenger": {
                "state": challenger,
                "direction": challenger if challenger in {"positive", "negative"} else None,
            },
            "shadow_fused_timing": {
                "state": (
                    baseline
                    if baseline == challenger
                    else "conflicted"
                ),
                "direction": baseline if baseline == challenger else None,
                "relation": "SYNTHETIC",
                "action_change_allowed": False,
            },
        },
        "relation_graph": {
            "pattern_clusters": [],
            "pattern_dependency_edges": [],
            "pattern_vs_baseline_decision": "SYNTHETIC",
            "numeric_vote_counting_used": False,
            "correlated_pattern_double_counting_allowed": False,
        },
        "active_pattern_claims": [
            {
                "claim_id": f"CLAIM-{index:03d}",
                "claim_hash": "3" * 64,
            }
        ],
        "excluded_pattern_claims": [],
        "admission_decision_ids": [f"DEC-{index:03d}"],
        "boundaries": {
            "decision_packet_mutated": False,
            "existing_relation_graph_mutated": False,
            "universal_stance_mutated": False,
            "portfolio_action_mutated": False,
            "scanner_score_change_performed": False,
            "productive_decision_change_performed": False,
            "execution_effect_created": False,
            "automatic_regular_integration_performed": False,
        },
        "l13_contract_hash": l13.challenger_contract_hash(spec),
    }
    trace["trace_hash"] = digest(trace)
    outcome = {
        "claim": {
            "claim_id": f"CLAIM-{index:03d}",
            "symbol": f"S{index:03d}",
            "capture_snapshot_id": f"SNAP-{index:03d}",
        },
        "target": {
            "horizon_sessions": 20,
            "target_id": "PEER_EXCESS",
            "baseline": "PIT_PEER_BASELINE",
        },
        "outcome": {"target_value": target},
        "outcome_hash": f"{(index % 10):x}" * 64,
    }
    return trace, outcome


def relaxed_contract():
    spec = deepcopy(l13.load_challenger_contract())
    spec["ablation"]["minimum_paired_n"] = 6
    spec["ablation"]["minimum_disagreement_n"] = 4
    spec["ablation"]["minimum_temporal_support_regions"] = 1
    spec["ablation"]["bootstrap_repetitions"] = 64
    return spec


def test_incremental_value_candidate_requires_positive_paired_ablation():
    spec = relaxed_contract()
    traces = []
    outcomes = []
    for index in range(12):
        trace, outcome = synthetic_trace(
            index,
            baseline="negative",
            challenger="positive",
            target=0.05,
            contract=spec,
        )
        traces.append(trace)
        outcomes.append(outcome)
    result = evaluate_incremental_value(
        traces,
        outcomes,
        horizon_sessions=20,
        evaluated_at="2026-10-07T18:00:00Z",
        contract=spec,
    )
    assert result["incremental_value_status"] == "INCREMENTAL_VALUE_CANDIDATE"
    assert result["positive_incremental_evidence"] is True
    assert result["metrics"]["directional_hit_rate_lift"] > 0
    assert result["metrics"]["mean_aligned_outcome_lift"] > 0
    assert result["regular_integration"]["approved"] is False
    assert verify_incremental_evaluation(result, contract=spec)["valid"] is True


def test_no_incremental_value_when_challenger_is_worse():
    spec = relaxed_contract()
    traces = []
    outcomes = []
    for index in range(12):
        trace, outcome = synthetic_trace(
            index,
            baseline="positive",
            challenger="negative",
            target=0.05,
            contract=spec,
        )
        traces.append(trace)
        outcomes.append(outcome)
    result = evaluate_incremental_value(
        traces,
        outcomes,
        horizon_sessions=20,
        evaluated_at="2026-10-07T18:00:00Z",
        contract=spec,
    )
    assert result["incremental_value_status"] == "NO_INCREMENTAL_VALUE"
    assert result["positive_incremental_evidence"] is False


def test_insufficient_support_never_opens_regular_integration():
    spec = relaxed_contract()
    trace, outcome = synthetic_trace(
        1,
        baseline="negative",
        challenger="positive",
        target=0.05,
        contract=spec,
    )
    result = evaluate_incremental_value(
        [trace],
        [outcome],
        horizon_sessions=20,
        evaluated_at="2026-10-07T18:00:00Z",
        contract=spec,
    )
    assert result["incremental_value_status"] == "INSUFFICIENT_SUPPORT"
    assert result["regular_integration"]["approved"] is False
