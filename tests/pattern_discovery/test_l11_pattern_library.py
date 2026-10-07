from __future__ import annotations

from copy import deepcopy

import pytest

import scanner.research.pattern_discovery.pattern_library as library_module
from scanner.research.pattern_discovery.pattern_library import (
    PatternLibraryError,
    build_pattern_library,
    persist_pattern_library,
    render_pattern_library_html,
    verify_pattern_library,
)


def pattern(
    pattern_id: str = "PAT-L11-A",
    *,
    horizon: int = 5,
    discovery_probability: float = 0.91,
):
    return {
        "pattern_id": pattern_id,
        "pattern_version": "v1",
        "pattern_spec_hash": "a" * 64,
        "frozen_record_hash": "b" * 64,
        "l4_evidence_hash": "c" * 64,
        "discovery_run_id": "DISC-L11",
        "freeze_timestamp": "2026-10-01T12:00:00Z",
        "data_cutoff": "2026-10-01T11:00:00Z",
        "code_version": "l11-test",
        "universe_version": "u-test",
        "feature_library_version": "f-test",
        "natural_language_description": (
            f"Synthetic {pattern_id} fixture for L11 presentation tests."
        ),
        "pattern_spec": {
            "semantics": {
                "pattern_type": "RELATIVE_ALPHA",
                "natural_language_description": (
                    f"Synthetic {pattern_id} fixture for L11 presentation tests."
                ),
            },
            "forecast": {
                "target_id": "PEER_EXCESS",
                "expected_direction": "POSITIVE",
                "horizon_sessions": horizon,
                "baseline": "PIT_PEER_BASELINE",
            },
        },
        "discovery_evidence": {
            "raw_n": 100,
            "effective_n_proxy": 22,
            "symbol_count": 18,
            "observation_date_count": 60,
            "support_region_count": 11,
            "direction_probability": discovery_probability,
            "baseline_probability": 0.50,
            "probability_advantage_lift": (
                discovery_probability - 0.50
            ),
            "mean_aligned_outcome": 0.09,
            "median_aligned_outcome": 0.08,
            "effect_size_vs_baseline": None,
            "robust_uncertainty": {
                "aligned_effect_interval_95": [0.04, 0.13],
                "probability_lift_interval_95": [0.18, 0.55],
            },
            "concentration": {"top_symbol_share": 0.12},
            "diagnostic_splits": {"regime": {"RISK_ON": {"raw_n": 45}}},
            "confirmation_period": None,
            "open_blockers": [],
        },
    }


def snapshot(*patterns):
    return {
        "snapshot_hash": "d" * 64,
        "frozen_patterns": list(patterns),
    }


def rating_history(
    p,
    *,
    rating: str = "B",
    observed_at: str = "2026-10-06T20:00:00Z",
):
    return {
        "pattern_id": p["pattern_id"],
        "pattern_version": p["pattern_version"],
        "pattern_spec_hash": p["pattern_spec_hash"],
        "history_hash": (
            ("e" if p["pattern_id"].endswith("A") else "f") * 64
        ),
        "current_rating": rating,
        "transitions": [
            {
                "observed_at": observed_at,
                "previous_rating": "C",
                "new_rating": rating,
                "reason_codes": ["L9_CONFIRMATION_GATES_PASSED"],
                "transition_hash": "1" * 64,
            }
        ],
    }


def l9_report(p, *, probability: float = 0.67):
    return {
        "look_hash": "2" * 64,
        "evaluated_at": "2026-10-06T19:30:00Z",
        "confirmation_look_id": "LOOK-L11-1",
        "qm_governance": {
            "look_id": "FINAL",
            "monitoring_plan_id": "MON-L11",
            "monitoring_plan_version": "v1",
            "is_final_look": True,
        },
        "pattern_results": [
            {
                "pattern_id": p["pattern_id"],
                "pattern_version": p["pattern_version"],
                "pattern_spec_hash": p["pattern_spec_hash"],
                "result_class": "SUPPORTED",
                "result_reasons": [],
                "prospective_evidence": {
                    "raw_n": 28,
                    "effective_n": 9,
                    "symbol_count": 7,
                    "support_region_count": 5,
                    "direction_probability": probability,
                    "baseline_probability": 0.49,
                    "probability_advantage_lift": probability - 0.49,
                    "mean_aligned_outcome": 0.045,
                    "median_aligned_outcome": 0.039,
                    "effect_size_vs_baseline": 0.031,
                    "robust_uncertainty": {
                        "aligned_effect_interval_95": [0.01, 0.08],
                        "probability_lift_interval_95": [0.03, 0.30],
                    },
                    "concentration": {"top_symbol_share": 0.22},
                    "regime_diagnostics": {
                        "qualified_sign_reversal": False,
                    },
                    "context_splits": {},
                    "confirmation_period": {
                        "start": "2026-10-02",
                        "end": "2026-10-06",
                    },
                    "open_blockers": [],
                },
            }
        ],
    }


def graph(p, other=None):
    nodes = [
        {
            "node_id": "N1",
            "pattern_id": p["pattern_id"],
            "pattern_version": p["pattern_version"],
            "pattern_spec_hash": p["pattern_spec_hash"],
        }
    ]
    edges = []
    if other is not None:
        nodes.append(
            {
                "node_id": "N2",
                "pattern_id": other["pattern_id"],
                "pattern_version": other["pattern_version"],
                "pattern_spec_hash": other["pattern_spec_hash"],
            }
        )
        edges.append(
            {
                "source_node_id": "N1",
                "target_node_id": "N2",
                "relationship": "RELATED",
                "dependency_severity": "HIGH",
                "feature_overlap": 0.8,
                "event_overlap": 0.5,
                "context_overlap": 0.4,
            }
        )
    return {
        "graph_id": "PDG-L11",
        "graph_hash": "3" * 64,
        "nodes": nodes,
        "edges": edges,
    }


@pytest.fixture(autouse=True)
def isolate_upstream_verifiers(monkeypatch):
    # L5/L6/L9/L10 are regression-tested separately in the L11 workflow.
    monkeypatch.setattr(
        library_module,
        "verify_freeze_snapshot",
        lambda value: {"valid": True},
    )
    monkeypatch.setattr(
        library_module,
        "verify_rating_history",
        lambda value: {"valid": True},
    )
    monkeypatch.setattr(
        library_module,
        "verify_confirmation_look",
        lambda value: {"valid": True},
    )
    monkeypatch.setattr(
        library_module,
        "verify_dependency_graph",
        lambda value: {"valid": True},
    )


def test_library_keeps_discovery_and_prospective_evidence_separate():
    p = pattern()
    library = build_pattern_library(
        [snapshot(p)],
        rating_histories=[rating_history(p)],
        confirmation_reports=[l9_report(p, probability=0.67)],
        dependency_graph=graph(p),
        generated_at="2026-10-07T08:00:00Z",
    )
    item = library["patterns"][0]

    assert item["rating"] == "B"
    assert item["horizon_sessions"] == 5
    assert (
        item["discovery_evidence"]["direction_probability"]
        == 0.91
    )
    assert (
        item["prospective_evidence"]["direction_probability"]
        == 0.67
    )
    assert item["latest_confirmation"]["result_class"] == "SUPPORTED"
    assert library["boundaries"]["rating_recomputed"] is False
    assert library["boundaries"]["promotion_performed"] is False


def test_missing_prospective_evidence_stays_missing():
    p = pattern()
    library = build_pattern_library(
        [snapshot(p)],
        rating_histories=[rating_history(p, rating="D")],
        generated_at="2026-10-07T08:00:00Z",
    )
    item = library["patterns"][0]

    assert item["prospective_evidence"] is None
    assert item["latest_confirmation"]["result_class"] is None
    assert {
        "code": "NO_PROSPECTIVE_CONFIRMATION_RESULT",
        "source": "L11_DERIVED",
    } in item["open_blockers"]


def test_html_requires_no_raw_json_and_shows_uncertainty():
    p = pattern()
    library = build_pattern_library(
        [snapshot(p)],
        rating_histories=[rating_history(p)],
        confirmation_reports=[l9_report(p)],
        generated_at="2026-10-07T08:00:00Z",
    )
    html = render_pattern_library_html(library)

    assert "Discovery Evidence" in html
    assert "Prospective / Confirmed Evidence" in html
    assert "95% Effekt" in html
    assert "[0.010, 0.080]" in html
    assert "Confirmation History" in html
    assert "Research-only" in html
    assert "<pre>" not in html
    assert "JSON.stringify" not in html


def test_horizons_remain_separate_pattern_objects():
    p5 = pattern("PAT-L11-A", horizon=5)
    p20 = pattern("PAT-L11-B", horizon=20)
    p20["pattern_spec_hash"] = "9" * 64
    library = build_pattern_library(
        [snapshot(p5, p20)],
        rating_histories=[
            rating_history(p5, rating="B"),
            rating_history(p20, rating="U"),
        ],
        generated_at="2026-10-07T08:00:00Z",
    )
    assert library["counts"]["pattern_count"] == 2
    assert [
        item["horizon_sessions"]
        for item in library["patterns"]
    ] == [5, 20]


def test_dependency_is_visible_but_does_not_change_rating():
    p1 = pattern("PAT-L11-A")
    p2 = pattern("PAT-L11-B")
    p2["pattern_spec_hash"] = "9" * 64
    library = build_pattern_library(
        [snapshot(p1, p2)],
        rating_histories=[
            rating_history(p1, rating="A"),
            rating_history(p2, rating="C"),
        ],
        dependency_graph=graph(p1, p2),
        generated_at="2026-10-07T08:00:00Z",
    )
    first = library["patterns"][0]
    assert first["rating"] == "A"
    assert first["dependency"]["highest_severity"] == "HIGH"
    assert first["dependency"]["relationship_count"] == 1


def test_unknown_l9_pattern_fails_closed():
    p = pattern()
    unknown = pattern("PAT-L11-X")
    unknown["pattern_spec_hash"] = "8" * 64
    with pytest.raises(
        PatternLibraryError,
        match="l9_references_unknown_l5_pattern",
    ):
        build_pattern_library(
            [snapshot(p)],
            rating_histories=[rating_history(p)],
            confirmation_reports=[l9_report(unknown)],
            generated_at="2026-10-07T08:00:00Z",
        )


def test_library_hash_is_tamper_evident():
    p = pattern()
    library = build_pattern_library(
        [snapshot(p)],
        rating_histories=[rating_history(p)],
        generated_at="2026-10-07T08:00:00Z",
    )
    assert verify_pattern_library(library)["valid"] is True

    tampered = deepcopy(library)
    tampered["patterns"][0]["rating"] = "A"
    with pytest.raises(
        PatternLibraryError,
        match="pattern_library_hash_mismatch",
    ):
        verify_pattern_library(tampered)


def test_persistence_writes_human_ui_and_model(tmp_path):
    p = pattern()
    library = build_pattern_library(
        [snapshot(p)],
        rating_histories=[rating_history(p)],
        confirmation_reports=[l9_report(p)],
        generated_at="2026-10-07T08:00:00Z",
    )
    result = persist_pattern_library(tmp_path, library)

    assert result["valid"] is True
    assert (tmp_path / result["json_path"]).exists()
    html_path = tmp_path / result["html_path"]
    assert html_path.exists()
    text = html_path.read_text(encoding="utf-8")
    assert "Pattern Library / Research UI" in text
    assert "Discovery Evidence" in text
    assert "Prospective / Confirmed Evidence" in text
