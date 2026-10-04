from __future__ import annotations

from copy import deepcopy

import scanner.research.elliott_vnext.prospective_observation as observation


def _capture(
    capture_id: str,
    *,
    snapshot_id: str,
    captured_at: str,
    output_id: str,
    scenario_id: str,
    supersedes: list[str] | None = None,
) -> dict:
    row = {
        "schema_version": "elliott_vnext_prospective_capture_v1",
        "capture_id": capture_id,
        "snapshot_id": snapshot_id,
        "source_publication_commit": "a" * 40,
        "captured_at": captured_at,
        "validation_partition": "prospective_unspent",
        "outputs": [
            {
                "output_id": output_id,
                "symbol": "DRO.AX",
                "timeframe": "daily",
                "degree": "intermediate",
                "primary_scenario": {"scenario_id": scenario_id},
            }
        ],
        "guards": {
            "research_only": True,
            "productive_integration_enabled": False,
            "changes_universal_stance": False,
            "changes_portfolio_action": False,
            "direct_ordering_allowed": False,
            "future_rows_used": False,
            "missing_evidence_not_imputed": True,
            "frozen_elliott_core_modified": False,
            "multi_degree_outputs_retained_without_reducer": True,
        },
    }
    if supersedes:
        row["repair"] = {
            "supersedes_capture_ids": supersedes,
            "legacy_records_preserved": True,
        }
    return row


def _stub_scenario_feature(snapshots):
    observations = []
    counts = {"STABLE": 0, "CHANGED": 0, "INSUFFICIENT_EVIDENCE": 0}
    for prior, current in zip(snapshots, snapshots[1:]):
        prior_id = prior["output"]["primary_scenario"]["scenario_id"]
        current_id = current["output"]["primary_scenario"]["scenario_id"]
        status = "STABLE" if prior_id == current_id else "CHANGED"
        counts[status] += 1
        observations.append(
            {
                "prior_output_id": prior["output"]["output_id"],
                "current_output_id": current["output"]["output_id"],
                "available_from": current["available_from"],
                "feature_status": status,
            }
        )
    return {
        "observation_count": len(observations),
        "counts": counts,
        "observations": observations,
    }


def _patch_validators(monkeypatch) -> None:
    monkeypatch.setattr(
        observation,
        "validate_elliott_6h_output",
        lambda value: deepcopy(dict(value)),
    )
    monkeypatch.setattr(
        observation,
        "build_scenario_stability_feature",
        _stub_scenario_feature,
    )


def test_single_capture_remains_insufficient_without_empirical_claim(monkeypatch) -> None:
    _patch_validators(monkeypatch)
    report = observation.build_prospective_observation(
        [
            _capture(
                "capture-1",
                snapshot_id="snapshot-1",
                captured_at="2026-10-04T16:00:00+00:00",
                output_id="output-1",
                scenario_id="scenario-a",
            )
        ],
        generated_at="2026-10-04T17:00:00+00:00",
    )

    assert report["status"] == "INSUFFICIENT_PROSPECTIVE_HISTORY"
    assert report["effective_capture_count"] == 1
    assert report["comparable_dimension_count"] == 0
    assert report["scenario_stability"]["cumulative_counts"] == {
        "STABLE": 0,
        "CHANGED": 0,
        "INSUFFICIENT_EVIDENCE": 0,
    }
    assert report["guards"]["market_outcomes_used"] is False
    assert report["guards"]["empirical_conclusion_allowed"] is False
    assert report["guards"]["productive_promotion_performed"] is False


def test_two_captures_activate_scenario_stability_observation(monkeypatch) -> None:
    _patch_validators(monkeypatch)
    report = observation.build_prospective_observation(
        [
            _capture(
                "capture-1",
                snapshot_id="snapshot-1",
                captured_at="2026-10-04T16:00:00+00:00",
                output_id="output-1",
                scenario_id="scenario-a",
            ),
            _capture(
                "capture-2",
                snapshot_id="snapshot-2",
                captured_at="2026-10-05T16:00:00+00:00",
                output_id="output-2",
                scenario_id="scenario-a",
            ),
        ],
        generated_at="2026-10-05T17:00:00+00:00",
    )

    assert report["status"] == "OBSERVATION_ACTIVE"
    assert report["effective_capture_count"] == 2
    assert report["comparable_dimension_count"] == 1
    assert report["latest_capture_comparison_count"] == 1
    assert report["scenario_stability"]["cumulative_counts"]["STABLE"] == 1
    assert report["scenario_stability"]["latest_capture_counts"]["STABLE"] == 1
    dimension = report["scenario_stability"]["dimensions"][0]
    assert dimension["latest_status"] == "STABLE"
    assert dimension["reaches_current_capture"] is True
    assert report["guards"]["degree_reducer_used"] is False
    assert report["guards"]["changes_portfolio_action"] is False


def test_repaired_capture_supersedes_legacy_capture(monkeypatch) -> None:
    _patch_validators(monkeypatch)
    legacy = _capture(
        "legacy-empty",
        snapshot_id="snapshot-1",
        captured_at="2026-10-04T15:00:00+00:00",
        output_id="legacy-output",
        scenario_id="legacy-scenario",
    )
    repaired = _capture(
        "repaired",
        snapshot_id="snapshot-1",
        captured_at="2026-10-04T16:00:00+00:00",
        output_id="output-1",
        scenario_id="scenario-a",
        supersedes=["legacy-empty"],
    )
    next_capture = _capture(
        "capture-2",
        snapshot_id="snapshot-2",
        captured_at="2026-10-05T16:00:00+00:00",
        output_id="output-2",
        scenario_id="scenario-b",
    )

    report = observation.build_prospective_observation(
        [legacy, repaired, next_capture],
        generated_at="2026-10-05T17:00:00+00:00",
    )

    assert report["effective_capture_count"] == 2
    assert report["superseded_capture_count"] == 1
    assert report["scenario_stability"]["cumulative_counts"]["CHANGED"] == 1
    dimension = report["scenario_stability"]["dimensions"][0]
    assert dimension["prior_output_id"] == "output-1"
    assert dimension["current_output_id"] == "output-2"


def test_non_prospective_capture_is_rejected(monkeypatch) -> None:
    _patch_validators(monkeypatch)
    capture = _capture(
        "capture-1",
        snapshot_id="snapshot-1",
        captured_at="2026-10-04T16:00:00+00:00",
        output_id="output-1",
        scenario_id="scenario-a",
    )
    capture["validation_partition"] = "legacy"

    try:
        observation.build_prospective_observation([capture])
    except observation.ProspectiveObservationError as exc:
        assert "capture_not_prospective_unspent" in str(exc)
    else:
        raise AssertionError("non-prospective capture must fail closed")
