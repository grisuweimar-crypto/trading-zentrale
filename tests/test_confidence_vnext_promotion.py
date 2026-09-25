from __future__ import annotations

from pathlib import Path

import pandas as pd

from scanner.reports.confidence_vnext_promotion import (
    POLICY_SHA256,
    POLICY_VERSION,
    _promotion_for_horizon,
    _score_directional_frame,
    run_phase5_completion,
    shadow_policy_decision,
)
from scanner.reports.confidence_vnext_progressive import MULTI_STATE_FIELDS, SINGLE_STATE_FIELDS
from scanner.reports.confidence_vnext_prospective_v2 import CLAIM_COLUMNS_V2, OUTCOME_COLUMNS_V2


def _version(signal: float) -> dict[str, object]:
    tables: dict[str, object] = {field: {} for field in (*SINGLE_STATE_FIELDS, *MULTI_STATE_FIELDS)}
    tables["selection_statistical_state"] = {
        "robust": {"directional_N": 10, "mean_signed_peer_excess": signal}
    }
    return {
        "version_id": "v1",
        "version_sha256": "a" * 64,
        "horizon_sessions": 5,
        "learned_state_reliability": tables,
    }


def _row() -> pd.Series:
    values = {field: "missing" for field in (*SINGLE_STATE_FIELDS, *MULTI_STATE_FIELDS)}
    values["selection_statistical_state"] = "robust"
    return pd.Series(values)


def test_shadow_policy_follows_positive_and_flips_negative_training_reliability():
    positive = shadow_policy_decision(_row(), _version(0.02))
    negative = shadow_policy_decision(_row(), _version(-0.02))
    assert positive["action"] == "follow"
    assert positive["multiplier"] == 1
    assert negative["action"] == "invert"
    assert negative["multiplier"] == -1
    assert POLICY_VERSION
    assert len(POLICY_SHA256) == 64


def test_shadow_policy_missing_empirical_signal_falls_back_to_frozen_direction():
    version = _version(0.01)
    version["learned_state_reliability"] = {}
    decision = shadow_policy_decision(_row(), version)
    assert decision == {
        "multiplier": 1,
        "action": "follow",
        "reason": "no_empirical_state_signal",
        "fields_used": 0,
    }


def test_scoring_is_paired_and_inversion_is_exact():
    frame = pd.DataFrame(
        [
            {
                **_row().to_dict(),
                "claim_id": "c1",
                "symbol": "AAA",
                "as_of": "2026-10-01",
                "signed_peer_excess": 0.10,
                "direction_hit": 1,
            },
            {
                **_row().to_dict(),
                "claim_id": "c2",
                "symbol": "BBB",
                "as_of": "2026-10-02",
                "signed_peer_excess": -0.04,
                "direction_hit": 0,
            },
        ]
    )
    scored = _score_directional_frame(frame, _version(-0.01))
    assert scored["adaptive_action"].tolist() == ["invert", "invert"]
    assert scored["adaptive_signed_peer_excess"].tolist() == [-0.10, 0.04]
    assert scored["adaptive_direction_hit"].tolist() == [0, 1]
    assert scored["delta_signed_peer_excess"].tolist() == [-0.20, 0.08]


def _assessment(
    version: str,
    start: str,
    end: str,
    delta: float,
    *,
    concentration_passed: bool = True,
) -> dict[str, object]:
    return {
        "version_id": version,
        "evaluation_claim_generated_start": start,
        "evaluation_claim_generated_end": end,
        "promotion_evidence_eligible_by_support": True,
        "directional": {"delta_mean_signed_peer_excess": delta},
        "concentration": {"passed": concentration_passed},
    }


def _scored_for_dates(delta: float) -> pd.DataFrame:
    rows = []
    for index in range(25):
        day = index + 1
        rows.append(
            {
                "claim_id": f"c-{index}",
                "symbol": f"S{index % 10}",
                "as_of": f"2026-10-{day:02d}",
                "baseline_signed_peer_excess": 0.01,
                "baseline_direction_hit": 1,
                "adaptive_signed_peer_excess": 0.01 + delta,
                "adaptive_direction_hit": 1,
                "delta_signed_peer_excess": delta,
                "delta_direction_hit": 0,
                "adaptive_action": "follow",
                "adaptive_reason": "nonnegative_training_reliability",
                "policy_fields_used": 1,
            }
        )
    return pd.DataFrame(rows)


def test_promotion_gate_stays_fail_closed_without_temporal_stability():
    assessments = [
        _assessment("v1", "2026-10-01", "2026-10-10", 0.01),
        _assessment("v2", "2026-10-12", "2026-10-21", -0.01),
    ]
    result = _promotion_for_horizon(
        assessments,
        [_scored_for_dates(0.01), _scored_for_dates(-0.01)],
        5,
        bootstrap_reps=20,
        random_seed=7,
    )
    assert result["status"] == "insufficient_evidence"
    assert result["gates"]["temporal_stability_check_passed"] is False
    assert result["production_change_performed"] is False


def test_promotion_gate_can_reach_separate_review_only_on_pre_registered_evidence():
    assessments = [
        _assessment("v1", "2026-10-01", "2026-10-10", 0.01),
        _assessment("v2", "2026-10-12", "2026-10-21", 0.01),
    ]
    result = _promotion_for_horizon(
        assessments,
        [_scored_for_dates(0.01), _scored_for_dates(0.01)],
        5,
        bootstrap_reps=20,
        random_seed=11,
    )
    assert result["status"] == "eligible_for_separate_promotion_review"
    assert all(result["gates"].values())
    assert result["production_change_performed"] is False


def test_empty_runner_is_technically_complete_but_collecting(tmp_path: Path):
    claims = tmp_path / "claims.csv"
    outcomes = tmp_path / "outcomes.csv"
    versions = tmp_path / "versions.jsonl"
    evaluations = tmp_path / "evaluations.jsonl"
    report = tmp_path / "report.json"

    pd.DataFrame(columns=CLAIM_COLUMNS_V2).to_csv(claims, index=False)
    pd.DataFrame(columns=OUTCOME_COLUMNS_V2).to_csv(outcomes, index=False)
    versions.write_text("", encoding="utf-8")
    evaluations.write_text("", encoding="utf-8")

    result = run_phase5_completion(
        claims,
        outcomes,
        versions,
        evaluations,
        report,
        bootstrap_reps=10,
    )
    assert result["technical_phase5_complete"] is True
    assert result["status"] == "phase5_engineering_complete_collecting_evidence"
    assert set(result["horizons"]) == {"5", "20", "40", "60"}
    assert result["semantics"]["production_change_performed"] is False
    assert report.exists()
