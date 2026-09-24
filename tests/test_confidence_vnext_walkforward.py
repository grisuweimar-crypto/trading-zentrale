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


def test_empty_readiness_is_fail_closed_and_reports_archive_gap():
    result = readiness_audit(
        pd.DataFrame(columns=CLAIM_COLUMNS),
        pd.DataFrame(columns=OUTCOME_COLUMNS),
        freeze_commit="abc",
        freeze_time="2026-09-24T01:12:14+00:00",
    )
    assert result["status"] == "insufficient_evidence"
    assert result["shadow_archive"]["claims"] == 0
    assert "claim_level_phase4c_timing_and_risk_states_not_archived" in result["blockers"]
    assert result["horizons"]["5"]["mature_outcomes"] == 0
    assert result["horizons"]["5"]["walkforward_evaluation_ready"] is False


def test_readiness_requires_explicit_statistical_context_and_time_support():
    claims, outcomes = _frames()
    blocked = readiness_audit(claims, outcomes)
    assert blocked["horizons"]["5"]["non_overlapping_outcome_support_regions"] == 2
    assert blocked["horizons"]["5"]["walkforward_evaluation_ready"] is False

    ready = readiness_audit(claims, outcomes, statistical_context_complete=True)
    assert ready["horizons"]["5"]["walkforward_evaluation_ready"] is True
    assert ready["status"] == "ready_for_walkforward_evaluation"


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


def test_model_manifest_fingerprints_exact_training_evidence():
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


def test_frozen_baseline_is_descriptive_and_does_not_create_confidence_mapping():
    claims, outcomes = _frames()
    result = frozen_baseline_evaluator(claims, outcomes, horizon=5)
    assert result["status"] == "descriptive_only"
    assert result["N"] == 4
    assert result["notes"]["no_adaptive_weights"] is True
    assert result["notes"]["no_scalar_confidence"] is True
    assert result["groups"]["agreement_state"]["compatible"]["N"] == 4


def test_promotion_gate_fails_closed_until_all_structural_requirements_pass():
    insufficient = promotion_assessment(
        walkforward_evaluations=[{"epoch": 1}],
        pit_leakage_audit_passed=True,
        reproducible_versions=True,
        robust_uncertainty_available=True,
        concentration_check_passed=True,
        temporal_stability_check_passed=True,
        baseline_advantage_demonstrated=True,
    )
    assert insufficient["status"] == "insufficient_evidence"

    eligible = promotion_assessment(
        walkforward_evaluations=[{"epoch": 1}, {"epoch": 2}],
        pit_leakage_audit_passed=True,
        reproducible_versions=True,
        robust_uncertainty_available=True,
        concentration_check_passed=True,
        temporal_stability_check_passed=True,
        baseline_advantage_demonstrated=True,
    )
    assert eligible["status"] == "eligible_for_separate_promotion_review"
    assert eligible["production_change_performed"] is False
