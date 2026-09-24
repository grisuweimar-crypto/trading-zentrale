from __future__ import annotations

import pandas as pd
import pytest

from scanner.reports.confidence_vnext_prospective import CLAIM_COLUMNS, OUTCOME_COLUMNS
from scanner.reports.confidence_vnext_walkforward import (
    evidence_fingerprint,
    frozen_baseline_evaluator,
    model_version_manifest,
    promotion_assessment,
    purged_training_pairs,
    readiness_audit,
)


def _claim(
    claim_id: str,
    snapshot: str,
    symbol: str,
    as_of: str,
    start: str,
    horizon: int = 5,
    *,
    selection_state: str = "robust",
    agreement_state: str = "compatible",
) -> dict[str, object]:
    row = {column: "" for column in CLAIM_COLUMNS}
    row.update(
        {
            "claim_id": claim_id,
            "schema_version": "phase4e_shadow_v1",
            "as_of": as_of,
            "generated_at": f"{as_of}T16:00:00+00:00",
            "run_id": f"run-{snapshot}",
            "snapshot_id": snapshot,
            "symbol": symbol,
            "currency": "USD",
            "horizon_sessions": horizon,
            "start_market_date": start,
            "start_adjusted_close": 100.0,
            "outcome_eligibility": "eligible",
            "evidence_version": "phase4_confidence_research_v1",
            "evidence_fingerprint": "e" * 64,
            "phase4_report_sha256": "a" * 64,
            "phase2_sha256": "b" * 64,
            "phase3_sha256": "c" * 64,
            "risk_scale_sha256": "missing",
            "phase2_source_as_of": f"{as_of}T00:00:00+00:00",
            "phase3_source_as_of": f"{as_of}T00:00:00+00:00",
            "selection_band": "B5",
            "selection_state": selection_state,
            "selection_direction": "positive",
            "timing_state": "robust_claim",
            "timing_direction": "positive",
            "timing_patterns": "pattern_a",
            "risk_state": "middle",
            "agreement_state": agreement_state,
            "return_claim_direction": "positive",
            "dq_selection_state": "proxy_complete",
            "dq_timing_state": "proxy_complete",
            "dq_risk_state": "proxy_complete",
            "volatility_application_status": "compatible",
        }
    )
    return row


def _outcome(
    claim_id: str,
    symbol: str,
    as_of: str,
    start: str,
    end: str,
    evaluated_at: str,
    horizon: int = 5,
    *,
    ret: float = 0.05,
    adverse: float = 0.02,
    drawdown: float = 0.03,
) -> dict[str, object]:
    row = {column: "" for column in OUTCOME_COLUMNS}
    row.update(
        {
            "claim_id": claim_id,
            "schema_version": "phase4e_shadow_v1",
            "as_of": as_of,
            "evaluated_at": evaluated_at,
            "symbol": symbol,
            "currency": "USD",
            "horizon_sessions": horizon,
            "start_market_date": start,
            "end_market_date": end,
            "start_adjusted_close": 100.0,
            "end_adjusted_close": 100.0 * (1.0 + ret),
            "return": ret,
            "adverse_excursion": adverse,
            "path_max_drawdown": drawdown,
        }
    )
    return row


def _frames():
    claims = pd.DataFrame(
        [
            _claim("c1", "s1", "AAA", "2026-10-01", "2026-10-01"),
            _claim("c2", "s1", "BBB", "2026-10-01", "2026-10-01"),
            _claim("c3", "s2", "AAA", "2026-10-20", "2026-10-20"),
            _claim("c4", "s2", "BBB", "2026-10-20", "2026-10-20"),
        ],
        columns=CLAIM_COLUMNS,
    )
    outcomes = pd.DataFrame(
        [
            _outcome("c1", "AAA", "2026-10-01", "2026-10-01", "2026-10-08", "2026-10-09T00:00:00+00:00"),
            _outcome("c2", "BBB", "2026-10-01", "2026-10-01", "2026-10-08", "2026-10-09T00:00:00+00:00", ret=-0.01),
            _outcome("c3", "AAA", "2026-10-20", "2026-10-20", "2026-10-27", "2026-10-28T00:00:00+00:00"),
            _outcome("c4", "BBB", "2026-10-20", "2026-10-20", "2026-10-27", "2026-10-28T00:00:00+00:00", ret=-0.01),
        ],
        columns=OUTCOME_COLUMNS,
    )
    return claims, outcomes


def _with_statistical_context(claims: pd.DataFrame) -> pd.DataFrame:
    enriched = claims.copy()
    enriched["timing_statistical_state"] = "robust"
    enriched["risk_statistical_state"] = "robust"
    return enriched


def _evaluation(version_id: str, start: str, end: str) -> dict[str, object]:
    return {
        "version_id": version_id,
        "evaluation_start": start,
        "evaluation_end": end,
    }


def test_empty_readiness_is_fail_closed_and_reports_archive_gap():
    result = readiness_audit(
        pd.DataFrame(columns=CLAIM_COLUMNS),
        pd.DataFrame(columns=OUTCOME_COLUMNS),
    )
    assert result["status"] == "insufficient_evidence"
    assert result["shadow_archive"]["claims"] == 0
    assert "claim_level_phase4c_timing_and_risk_states_not_archived" in result["blockers"]
    assert result["horizons"]["5"]["mature_outcomes"] == 0
    assert result["horizons"]["5"]["walkforward_evaluation_ready"] is False
    assert result["freeze"]["immutable"] is True
    assert result["freeze"]["enforced_on_claim_generated_at"] is True


def test_freeze_metadata_cannot_be_weakened_by_caller():
    empty_claims = pd.DataFrame(columns=CLAIM_COLUMNS)
    empty_outcomes = pd.DataFrame(columns=OUTCOME_COLUMNS)
    with pytest.raises(ValueError, match="freeze_time is immutable"):
        readiness_audit(
            empty_claims,
            empty_outcomes,
            freeze_time="2026-09-23T00:00:00Z",
        )
    with pytest.raises(ValueError, match="freeze_commit is immutable"):
        readiness_audit(
            empty_claims,
            empty_outcomes,
            freeze_commit="not-the-phase4e-merge",
        )


def test_readiness_derives_statistical_context_and_counts_only_mature_snapshots():
    claims, outcomes = _frames()
    blocked = readiness_audit(claims, outcomes)
    five = blocked["horizons"]["5"]
    assert five["non_overlapping_outcome_support_regions"] == 2
    assert five["mature_snapshots"] == 2
    assert five["walkforward_evaluation_ready"] is False
    assert five["claim_time_span"]["start"].startswith("2026-10-01")
    assert five["mature_outcome_time_span"]["start"].startswith("2026-10-09")
    assert five["mature_outcome_time_span"]["end"].startswith("2026-10-28")

    ready = readiness_audit(_with_statistical_context(claims), outcomes)
    assert ready["statistical_context_complete"] is True
    assert ready["horizons"]["5"]["walkforward_evaluation_ready"] is True
    assert ready["status"] == "ready_for_walkforward_evaluation"

    one_mature_snapshot = readiness_audit(
        _with_statistical_context(claims),
        outcomes.loc[outcomes["claim_id"].isin(["c1", "c2"])].copy(),
    )
    assert one_mature_snapshot["horizons"]["5"]["snapshots"] == 2
    assert one_mature_snapshot["horizons"]["5"]["mature_snapshots"] == 1
    assert one_mature_snapshot["horizons"]["5"]["walkforward_evaluation_ready"] is False


def test_pre_freeze_claims_are_rejected_from_readiness_and_training():
    claims, outcomes = _frames()
    claims.loc[claims["claim_id"].eq("c1"), "generated_at"] = "2026-09-24T01:12:14Z"

    with pytest.raises(ValueError, match="pre-freeze"):
        readiness_audit(claims, outcomes)

    with pytest.raises(ValueError, match="pre-freeze"):
        purged_training_pairs(
            claims,
            outcomes,
            horizon=5,
            training_cutoff="2026-10-15T00:00:00+00:00",
            evaluation_start="2026-10-20T00:00:00+00:00",
        )


def test_purged_training_uses_only_fully_known_pre_evaluation_outcomes():
    claims, outcomes = _frames()
    training = purged_training_pairs(
        claims,
        outcomes,
        horizon=5,
        training_cutoff="2026-10-15T00:00:00+00:00",
        evaluation_start="2026-10-20T00:00:00+00:00",
    )
    assert set(training["claim_id"]) == {"c1", "c2"}

    with pytest.raises(ValueError, match="training_cutoff"):
        purged_training_pairs(
            claims,
            outcomes,
            horizon=5,
            training_cutoff="2026-10-20T00:00:00+00:00",
            evaluation_start="2026-10-20T00:00:00+00:00",
        )


def test_model_manifest_fingerprints_every_training_column_and_revalidates_purge():
    claims, outcomes = _frames()
    training = purged_training_pairs(
        claims,
        outcomes,
        horizon=5,
        training_cutoff="2026-10-15T00:00:00+00:00",
        evaluation_start="2026-10-20T00:00:00+00:00",
    )
    first = evidence_fingerprint(training)
    second = evidence_fingerprint(training.sample(frac=1.0, random_state=7))
    assert first == second

    changed_feature = training.copy()
    changed_feature.loc[changed_feature["claim_id"].eq("c1"), "agreement_state"] = "conflict"
    assert evidence_fingerprint(changed_feature) != first

    engineered = training.copy()
    engineered["engineered_reliability_feature"] = [0.1, 0.2]
    assert evidence_fingerprint(engineered) != first

    manifest = model_version_manifest(
        version_id="phase5-candidate-0001",
        training_cutoff="2026-10-15T00:00:00+00:00",
        training_pairs=training,
        horizons=[5],
        feature_definition={"dq": True, "statistical_confidence": True, "agreement": True},
        parameters={},
        hyperparameters={"mapping": "none"},
        evaluation_start="2026-10-20T00:00:00+00:00",
        evaluation_end="2026-11-20T00:00:00+00:00",
    )
    assert manifest["evidence_fingerprint"] == first
    assert manifest["training_rows"] == 2
    assert manifest["immutable_after_evaluation_start"] is True

    late = training.copy()
    late["evaluated_at"] = "2026-10-16T00:00:00Z"
    with pytest.raises(ValueError, match="purged walk-forward"):
        model_version_manifest(
            version_id="late",
            training_cutoff="2026-10-15T00:00:00+00:00",
            training_pairs=late,
            horizons=[5],
            feature_definition={},
            parameters={},
            hyperparameters={},
            evaluation_start="2026-10-20T00:00:00+00:00",
            evaluation_end="2026-11-20T00:00:00+00:00",
        )

    wrong_horizon = training.copy()
    wrong_horizon["horizon_sessions_claim"] = 20
    with pytest.raises(ValueError, match="purged walk-forward"):
        model_version_manifest(
            version_id="wrong-horizon",
            training_cutoff="2026-10-15T00:00:00+00:00",
            training_pairs=wrong_horizon,
            horizons=[5],
            feature_definition={},
            parameters={},
            hyperparameters={},
            evaluation_start="2026-10-20T00:00:00+00:00",
            evaluation_end="2026-11-20T00:00:00+00:00",
        )


def test_frozen_baseline_is_descriptive_and_does_not_create_confidence_mapping():
    claims, outcomes = _frames()
    result = frozen_baseline_evaluator(claims, outcomes, horizon=5)
    assert result["status"] == "descriptive_only"
    assert result["N"] == 4
    assert result["notes"]["no_adaptive_weights"] is True
    assert result["notes"]["no_scalar_confidence"] is True
    assert result["groups"]["agreement_state"]["compatible"]["N"] == 4


def test_promotion_gate_requires_distinct_non_overlapping_walkforward_epochs():
    common = {
        "pit_leakage_audit_passed": True,
        "reproducible_versions": True,
        "robust_uncertainty_available": True,
        "concentration_check_passed": True,
        "temporal_stability_check_passed": True,
        "baseline_advantage_demonstrated": True,
    }

    one = promotion_assessment(
        walkforward_evaluations=[
            _evaluation("v1", "2026-10-20T00:00:00Z", "2026-11-20T00:00:00Z")
        ],
        **common,
    )
    assert one["status"] == "insufficient_evidence"

    duplicate = promotion_assessment(
        walkforward_evaluations=[
            _evaluation("v1", "2026-10-20T00:00:00Z", "2026-11-20T00:00:00Z"),
            _evaluation("v1", "2026-10-20T00:00:00Z", "2026-11-20T00:00:00Z"),
        ],
        **common,
    )
    assert duplicate["status"] == "insufficient_evidence"

    overlapping = promotion_assessment(
        walkforward_evaluations=[
            _evaluation("v1", "2026-10-20T00:00:00Z", "2026-11-20T00:00:00Z"),
            _evaluation("v2", "2026-11-15T00:00:00Z", "2026-12-15T00:00:00Z"),
        ],
        **common,
    )
    assert overlapping["status"] == "insufficient_evidence"

    eligible = promotion_assessment(
        walkforward_evaluations=[
            _evaluation("v1", "2026-10-20T00:00:00Z", "2026-11-20T00:00:00Z"),
            _evaluation("v2", "2026-11-21T00:00:00Z", "2026-12-21T00:00:00Z"),
        ],
        **common,
    )
    assert eligible["status"] == "eligible_for_separate_promotion_review"
    assert eligible["gates"]["multiple_walkforward_evaluations"] is True
    assert eligible["production_change_performed"] is False
