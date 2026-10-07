from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json

import pytest

from scanner.research.pattern_discovery import (
    DependencyGraphError,
    build_dependency_graph,
    build_event_context,
    dependency_graph_repo_path,
    load_dependency_graph_contract,
    verify_dependency_graph,
    verify_event_context,
    write_dependency_graph,
)


def _canonical(value):
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _hash(value):
    return sha256(_canonical(value).encode("utf-8")).hexdigest()


def _condition(
    feature_id: str,
    state: str,
    *,
    transformation_id: str = "change_direction",
    feature_version: str = "v1",
    transformation_version: str = "v1",
    parameters=None,
):
    return {
        "atom_id": f"ATOM-{feature_id}-{state}",
        "feature_id": feature_id,
        "feature_version": feature_version,
        "feature_version_hash": _hash({"feature_id": feature_id, "version": feature_version}),
        "transformation_id": transformation_id,
        "transformation_version": transformation_version,
        "parameters": dict(parameters or {"lag_observations": 1}),
        "state": state,
    }


def _record(
    pattern_id: str,
    version: str,
    conditions,
    *,
    target_id: str = "return_5t_gt_0",
    expected_direction: str = "POSITIVE",
    horizon_sessions: int = 5,
    baseline: str = "same_horizon_universe",
    run_id: str = "DISC-L6-TEST",
):
    spec = {
        "identity": {
            "pattern_id": pattern_id,
            "pattern_version": version,
            "discovery_run_id": run_id,
            "candidate_id": f"CAND-{pattern_id}-{version}",
            "candidate_spec_hash": _hash([pattern_id, version, "candidate"]),
        },
        "semantics": {
            "pattern_type": "DIRECTIONAL",
            "natural_language_description": f"Synthetic {pattern_id} {version}",
            "conditions": list(conditions),
            "feature_versions": [],
            "transformation_rules": [],
        },
        "forecast": {
            "target_id": target_id,
            "expected_direction": expected_direction,
            "horizon_sessions": horizon_sessions,
            "baseline": baseline,
            "reference_definition": baseline,
        },
        "data": {},
        "statistics": {},
        "freeze": {},
    }
    record = {
        "schema_version": "pattern_discovery_l5_frozen_pattern_v1",
        "state": "FROZEN_CANDIDATE",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "pattern_id": pattern_id,
        "pattern_version": version,
        "pattern_spec_hash": _hash(spec),
        "natural_language_description": f"Synthetic {pattern_id} {version}",
        "pattern_spec": spec,
        "freeze_timestamp": "2026-10-07T07:00:00Z",
        "data_cutoff": "2026-10-06T21:00:00Z",
        "code_version": "l6-test",
        "universe_version": "universe-test",
        "feature_library_version": "pattern_discovery_feature_library_v1",
        "candidate_id": f"CAND-{pattern_id}-{version}",
        "discovery_run_id": run_id,
        "l4_evidence_hash": "e" * 64,
        "discovery_evidence": {},
        "qm_c_handoff": {},
        "confirmation_data_used": False,
        "prospective_capture_started": False,
        "rating": None,
        "promotion_status": "NOT_EVALUATED",
    }
    record["frozen_record_hash"] = _hash(record)
    return record


def _snapshot(records, *, run_id="DISC-L6-TEST"):
    snapshot = {
        "schema_version": "pattern_discovery_l5_freeze_snapshot_v1",
        "module": "pattern_discovery_lab",
        "phase": "L5",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "run_id": run_id,
        "freeze_timestamp": "2026-10-07T07:00:00Z",
        "l1_manifest_hash": "1" * 64,
        "l3_result_hash": "3" * 64,
        "l4_evidence_hash": "4" * 64,
        "l5_contract_hash": "5" * 64,
        "frozen_pattern_count": len(records),
        "frozen_patterns": list(records),
        "pattern_registry": {},
        "qm_c_handoff": {
            "package_count": len(records),
            "all_packages_ready_not_applied_by_l5": True,
            "direct_qm_registry_write_performed": False,
        },
        "boundaries": {
            "dependency_graph_performed": False,
            "prospective_capture_started": False,
            "confirmation_evaluation_performed": False,
            "rating_assigned": False,
            "promotion_performed": False,
            "decision_layer_integration_performed": False,
        },
    }
    snapshot["snapshot_hash"] = _hash(snapshot)
    return snapshot


def _event_set(record, events, *, coverage="COMPLETE"):
    return {
        "pattern_id": record["pattern_id"],
        "pattern_version": record["pattern_version"],
        "pattern_spec_hash": record["pattern_spec_hash"],
        "coverage_status": coverage,
        "events": events,
    }


def _event(symbol, day, *, sector="Technology", regime="bull"):
    return {
        "symbol": symbol,
        "as_of": f"2026-09-{day:02d}T12:00:00Z",
        "sector": sector,
        "regime": regime,
    }


def _context(*sets):
    return build_event_context(
        list(sets),
        source_id="l6-test-event-context",
        pit_cutoff="2026-10-06T21:00:00Z",
    )


def _edge(graph):
    assert len(graph["edges"]) == 1
    return graph["edges"][0]


def test_l6_contract_is_research_only_and_non_directional():
    contract = load_dependency_graph_contract()
    assert contract["phase"] == "L6"
    assert contract["research_only"] is True
    assert contract["productive_integration_enabled"] is False
    assert contract["execution_allowed"] is False
    assert contract["principles"]["directional_authority_created"] is False
    assert contract["principles"]["single_similarity_blackbox_forbidden"] is True
    assert contract["boundaries"]["promotion_performed"] is False


def test_event_context_is_deterministic_hash_bound_and_missing_stays_null():
    a = _record("PAT-A", "v1", [_condition("score", "UP")])
    b = _record("PAT-B", "v1", [_condition("rs3m", "UP")])
    first = _context(
        _event_set(a, [_event("AAA", 1, sector=None, regime="bull")]),
        _event_set(b, [_event("BBB", 2, sector="Industrials", regime=None)]),
    )
    second = _context(
        _event_set(b, [_event("BBB", 2, sector="Industrials", regime=None)]),
        _event_set(a, [_event("AAA", 1, sector=None, regime="bull")]),
    )
    assert first == second
    assert verify_event_context(first)["valid"] is True
    assert first["pattern_event_sets"][0]["events"][0]["sector"] is None

    tampered = deepcopy(first)
    tampered["pattern_event_sets"][0]["events"][0]["symbol"] = "TAMPER"
    with pytest.raises(DependencyGraphError, match="event_context_hash_mismatch"):
        verify_event_context(tampered)


def test_identical_pattern_semantics_are_duplicate_and_critical():
    conditions = [_condition("score", "UP"), _condition("trend200", "DOWN")]
    a = _record("PAT-A", "v1", conditions)
    b = _record("PAT-B", "v1", list(reversed(conditions)))
    snapshot = _snapshot([a, b])
    context = _context(
        _event_set(a, [_event("AAA", 1)]),
        _event_set(b, [_event("BBB", 2)]),
    )
    graph = build_dependency_graph([snapshot], context)
    edge = _edge(graph)
    assert edge["relationship"] == "DUPLICATE"
    assert edge["dependency_severity"] == "CRITICAL"
    assert edge["provenance"]["structural"]["duplicate_criterion_met"] is True
    assert edge["provenance"]["single_similarity_score_used"] is False


def test_strict_condition_superset_is_nested_and_high():
    base = [_condition("score", "UP")]
    a = _record("PAT-A", "v1", base)
    b = _record("PAT-B", "v1", base + [_condition("trend200", "DOWN")])
    graph = build_dependency_graph(
        [_snapshot([a, b])],
        _context(_event_set(a, [_event("AAA", 1)]), _event_set(b, [_event("BBB", 2)])),
    )
    edge = _edge(graph)
    assert edge["relationship"] == "NESTED"
    assert edge["dependency_severity"] == "HIGH"
    assert edge["nested_direction"] in {
        "SOURCE_CONDITIONS_SUBSET_OF_TARGET",
        "TARGET_CONDITIONS_SUBSET_OF_SOURCE",
    }


def test_feature_overlap_can_mark_related_without_event_data():
    a = _record(
        "PAT-A",
        "v1",
        [_condition("score", "UP"), _condition("trend200", "UP")],
    )
    b = _record(
        "PAT-B",
        "v1",
        [_condition("score", "DOWN"), _condition("trend200", "DOWN")],
        target_id="return_20t_gt_0",
        horizon_sessions=20,
    )
    graph = build_dependency_graph(
        [_snapshot([a, b])],
        _context(
            _event_set(a, [], coverage="UNAVAILABLE"),
            _event_set(b, [], coverage="UNAVAILABLE"),
        ),
    )
    edge = _edge(graph)
    assert edge["relationship"] == "RELATED"
    assert "FEATURE_OVERLAP" in edge["provenance"]["threshold_triggers"]
    assert edge["provenance"]["empirical"]["event_overlap"]["status"] == "UNAVAILABLE"


def test_strong_event_overlap_can_create_high_dependency_for_structurally_distinct_patterns():
    a = _record("PAT-A", "v1", [_condition("score", "UP")])
    b = _record(
        "PAT-B",
        "v1",
        [_condition("rs3m", "DOWN")],
        target_id="return_20t_gt_0",
        horizon_sessions=20,
    )
    shared = [_event("AAA", 1), _event("BBB", 2), _event("CCC", 3), _event("DDD", 4)]
    graph = build_dependency_graph(
        [_snapshot([a, b])],
        _context(_event_set(a, shared), _event_set(b, shared)),
    )
    edge = _edge(graph)
    assert edge["relationship"] == "RELATED"
    assert edge["dependency_severity"] == "HIGH"
    assert edge["provenance"]["empirical"]["event_overlap"]["jaccard"] == pytest.approx(1.0)
    assert "EVENT_OVERLAP" in edge["provenance"]["threshold_triggers"]


def test_temporal_overlap_is_separate_from_symbol_and_exact_event_overlap():
    a = _record("PAT-A", "v1", [_condition("score", "UP")])
    b = _record(
        "PAT-B",
        "v1",
        [_condition("rs3m", "UP")],
        target_id="return_20t_gt_0",
        horizon_sessions=20,
    )
    graph = build_dependency_graph(
        [_snapshot([a, b])],
        _context(
            _event_set(a, [_event("AAA", 1), _event("BBB", 2)]),
            _event_set(b, [_event("CCC", 1), _event("DDD", 2)]),
        ),
    )
    empirical = _edge(graph)["provenance"]["empirical"]
    assert empirical["temporal_overlap"]["jaccard"] == pytest.approx(1.0)
    assert empirical["symbol_overlap"]["jaccard"] == pytest.approx(0.0)
    assert empirical["event_overlap"]["jaccard"] == pytest.approx(0.0)


def test_sector_and_regime_overlap_are_unavailable_not_zero_when_context_is_missing():
    a = _record("PAT-A", "v1", [_condition("score", "UP")])
    b = _record(
        "PAT-B",
        "v1",
        [_condition("rs3m", "UP")],
        target_id="return_20t_gt_0",
        horizon_sessions=20,
    )
    graph = build_dependency_graph(
        [_snapshot([a, b])],
        _context(
            _event_set(a, [_event("AAA", 1, sector=None, regime=None)]),
            _event_set(b, [_event("BBB", 2, sector=None, regime=None)]),
        ),
    )
    empirical = _edge(graph)["provenance"]["empirical"]
    assert empirical["sector_overlap"]["status"] == "UNAVAILABLE"
    assert empirical["sector_overlap"]["distribution_overlap"] is None
    assert empirical["regime_overlap"]["status"] == "UNAVAILABLE"
    assert empirical["regime_overlap"]["distribution_overlap"] is None


def test_different_versions_of_same_pat_are_distinct_graph_nodes():
    conditions = [_condition("score", "UP")]
    a1 = _record("PAT-A", "v1", conditions)
    a2 = _record("PAT-A", "v2", conditions, run_id="DISC-L6-TEST-2")
    graph = build_dependency_graph(
        [_snapshot([a1], run_id="DISC-L6-TEST"), _snapshot([a2], run_id="DISC-L6-TEST-2")],
        _context(_event_set(a1, [_event("AAA", 1)]), _event_set(a2, [_event("AAA", 1)])),
    )
    assert graph["counts"]["node_count"] == 2
    assert len({node["node_id"] for node in graph["nodes"]}) == 2
    assert {node["pattern_version"] for node in graph["nodes"]} == {"v1", "v2"}


def test_graph_is_deterministic_under_input_order_changes():
    a = _record("PAT-A", "v1", [_condition("score", "UP")], run_id="DISC-A")
    b = _record("PAT-B", "v1", [_condition("trend200", "DOWN")], run_id="DISC-B")
    s1 = _snapshot([a], run_id="DISC-A")
    s2 = _snapshot([b], run_id="DISC-B")
    c1 = _context(
        _event_set(a, [_event("AAA", 1), _event("BBB", 2)]),
        _event_set(b, [_event("BBB", 2), _event("CCC", 3)]),
    )
    c2 = _context(
        _event_set(b, [_event("CCC", 3), _event("BBB", 2)]),
        _event_set(a, [_event("BBB", 2), _event("AAA", 1)]),
    )
    first = build_dependency_graph([s1, s2], c1)
    second = build_dependency_graph([s2, s1], c2)
    assert first == second
    assert verify_dependency_graph(first)["graph_hash"] == first["graph_hash"]


def test_tampered_pattern_spec_hash_fails_closed():
    a = _record("PAT-A", "v1", [_condition("score", "UP")])
    snapshot = _snapshot([a])
    tampered = deepcopy(snapshot)
    tampered["frozen_patterns"][0]["pattern_spec"]["semantics"]["conditions"][0]["state"] = "DOWN"
    changed_record = tampered["frozen_patterns"][0]
    changed_body = dict(changed_record)
    changed_body.pop("frozen_record_hash", None)
    changed_record["frozen_record_hash"] = _hash(changed_body)
    tampered_body = dict(tampered)
    tampered_body.pop("snapshot_hash", None)
    tampered["snapshot_hash"] = _hash(tampered_body)

    context = _context(_event_set(a, [_event("AAA", 1)]))
    with pytest.raises(DependencyGraphError, match="pattern_spec_hash_mismatch"):
        build_dependency_graph([tampered], context)


def test_l6_does_not_mutate_l5_or_l4_embedded_evidence():
    a = _record("PAT-A", "v1", [_condition("score", "UP")])
    b = _record("PAT-B", "v1", [_condition("rs3m", "DOWN")])
    a["discovery_evidence"] = {"raw_n": 123, "marker": "immutable"}
    body = dict(a)
    body.pop("frozen_record_hash", None)
    a["frozen_record_hash"] = _hash(body)
    snapshot = _snapshot([a, b])
    before = deepcopy(snapshot)
    build_dependency_graph(
        [snapshot],
        _context(_event_set(a, [_event("AAA", 1)]), _event_set(b, [_event("BBB", 2)])),
    )
    assert snapshot == before
    assert snapshot["frozen_patterns"][0]["discovery_evidence"] == {
        "raw_n": 123,
        "marker": "immutable",
    }


def test_event_context_must_bind_exactly_to_graph_nodes():
    a = _record("PAT-A", "v1", [_condition("score", "UP")])
    b = _record("PAT-B", "v1", [_condition("rs3m", "UP")])
    snapshot = _snapshot([a, b])
    missing = _context(_event_set(a, [_event("AAA", 1)]))
    with pytest.raises(DependencyGraphError, match="event_context_missing_pattern_nodes"):
        build_dependency_graph([snapshot], missing)


def test_dependency_graph_write_is_inside_l0_namespace_and_idempotent(tmp_path):
    a = _record("PAT-A", "v1", [_condition("score", "UP")])
    graph = build_dependency_graph(
        [_snapshot([a])],
        _context(_event_set(a, [_event("AAA", 1)])),
    )
    path = write_dependency_graph(tmp_path, graph)
    assert path.exists()
    assert "artifacts/research/pattern_discovery/dependency_graphs" in path.as_posix()
    assert path == write_dependency_graph(tmp_path, graph)
    saved = json.loads(path.read_text(encoding="utf-8"))
    assert verify_dependency_graph(saved)["valid"] is True
    assert dependency_graph_repo_path(graph["graph_id"]).startswith(
        "artifacts/research/pattern_discovery/"
    )


def test_graph_contains_no_directional_portfolio_or_execution_authority():
    a = _record("PAT-A", "v1", [_condition("score", "UP")])
    graph = build_dependency_graph(
        [_snapshot([a])],
        _context(_event_set(a, [_event("AAA", 1)])),
    )
    serialized = _canonical(graph)
    for forbidden in (
        "universal_stance",
        "portfolio_action",
        "trade_decision",
        "order_instruction",
        "buy_signal",
        "sell_signal",
        "position_size",
        "target_weight",
    ):
        assert forbidden not in serialized
    assert graph["boundaries"]["directional_authority_created"] is False
    assert graph["boundaries"]["promotion_performed"] is False
