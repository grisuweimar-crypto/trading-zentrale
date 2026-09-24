from __future__ import annotations

import pandas as pd
import pytest

from scanner.reports.confidence_vnext_prospective import CLAIM_COLUMNS, OUTCOME_COLUMNS
from scanner.reports.confidence_vnext_walkforward import (
    Phase5WalkForwardConfig,
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


def _end_after(start: str, sessions: int = 5) -> str:
    return pd.bdate_range(start, periods=sessions + 1)[-1].strftime("%Y-%m-%d")


def _evaluated_after(end: str) -> str:
    return (pd.Timestamp(end) + pd.Timedelta(days=1)).strftime("%Y-%m-%dT00:00:00+00:00")


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


def _support_ready_frames():
    dates = pd.bdate_range("2026-10-01", periods=51)
    claims = []
    for i, day in enumerate(dates):
        iso = day.strftime("%Y-%m-%d")
        claims.append(_claim(f"r{i}", f"rs{i}", "AAA", iso, iso))
    claim_frame = pd.DataFrame(claims, columns=CLAIM_COLUMNS)
    last = dates[50].strftime("%Y-%m-%d")
    first_end = _end_after("2026-10-01")
    last_end = _end_after(last)
    outcomes = pd.DataFrame(
        [
            _outcome("r0", "AAA", "2026-10-01", "2026-10-01", first_end, _evaluated_after(first_end)),
            _outcome("r50", "AAA", last, last, last_end, _evaluated_after(last_end)),
        ],
        columns=OUTCOME_COLUMNS,
    )
    return claim_frame, outcomes


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
    assert result["statistical_context_complete"] is False
    assert "claim_level_phase4c_timing_and_risk_states_not_archived" in result["blockers"]
    assert result["horizons"]["5"]["walkforward_evaluation_ready"] is False
    assert result["freeze"]["immutable"] is True


def test_freeze_metadata_and_contract_constants_cannot_be_weakened():
    claims = pd.DataFrame(columns=CLAIM_COLUMNS)
    outcomes = pd.DataFrame(columns=OUTCOME_COLUMNS)
    with pytest.raises(ValueError, match="freeze_time is immutable"):
        readiness_audit(claims, outcomes, freeze_time="2026-09-23T00:00:00Z")
    with pytest.raises(ValueError, match="freeze_commit is immutable"):
        readiness_audit(claims, outcomes, freeze_commit="wrong")

    weakened = [
        Phase5WalkForwardConfig(require_statistical_context_for_adaptation=False),
        Phase5WalkForwardConfig(uncertainty_block_multiplier=1),
        Phase5WalkForwardConfig(minimum_time_separated_support_regions=1),
        Phase5WalkForwardConfig(fixed_event_spacing_sessions=4),
        Phase5WalkForwardConfig(schema_version="weakened"),
    ]
    for config in weakened:
        with pytest.raises(ValueError, match="contract constants are immutable"):
            readiness_audit(claims, outcomes, config=config)


def test_phase4e_v1_statistical_context_cannot_be_fabricated_with_extra_columns():
    claims, outcomes = _frames()
    enriched = claims.copy()
    enriched["timing_statistical_state"] = "robust"
    enriched["risk_statistical_state"] = "robust"
    with pytest.raises(ValueError, match="immutable Phase-4E claim schema"):
        readiness_audit(enriched, outcomes)

    plain = readiness_audit(claims, outcomes)
    assert plain["statistical_context_complete"] is False
    assert plain["status"] == "insufficient_evidence"


def test_readiness_counts_only_spaced_mature_cohorts():
    claims, outcomes = _frames()
    result = readiness_audit(claims, outcomes)
    five = result["horizons"]["5"]
    assert five["claims"] == 4
    assert five["mature_outcomes"] == 4
    assert five["spaced_evaluable_claims"] == 2
    assert five["spaced_mature_outcomes"] == 2
    assert five["snapshots"] == 2
    assert five["mature_snapshots"] == 1
    assert five["non_overlapping_outcome_support_regions"] == 1
    assert five["walkforward_evaluation_ready"] is False
    assert five["mature_outcome_time_span"]["start"].startswith("2026-10-09")
    assert five["mature_outcome_time_span"]["end"].startswith("2026-10-28")


def test_five_session_spacing_prevents_daily_occurrences_from_inflating_support():
    dates = pd.bdate_range("2026-10-01", periods=12)
    claims = pd.DataFrame(
        [
            _claim(f"d{i}", f"ds{i}", "AAA", day.strftime("%Y-%m-%d"), day.strftime("%Y-%m-%d"))
            for i, day in enumerate(dates)
        ],
        columns=CLAIM_COLUMNS,
    )
    day10 = dates[10].strftime("%Y-%m-%d")
    end0 = _end_after("2026-10-01")
    end10 = _end_after(day10)
    outcomes = pd.DataFrame(
        [
            _outcome("d0", "AAA", "2026-10-01", "2026-10-01", end0, _evaluated_after(end0)),
            _outcome("d10", "AAA", day10, day10, end10, _evaluated_after(end10)),
        ],
        columns=OUTCOME_COLUMNS,
    )
    result = readiness_audit(claims, outcomes)
    five = result["horizons"]["5"]
    assert five["fixed_event_spacing_sessions"] == 5
    assert five["spaced_evaluable_claims"] == 3
    assert five["non_overlapping_outcome_support_regions"] == 1


def test_support_regions_require_two_x_horizon_positions_after_spacing():
    claims, outcomes = _support_ready_frames()
    result = readiness_audit(claims, outcomes)
    five = result["horizons"]["5"]
    assert five["robust_uncertainty_block_length_sessions"] == 10
    assert five["spaced_evaluable_claims"] == 11
    assert five["non_overlapping_outcome_support_regions"] == 2
    assert five["walkforward_evaluation_ready"] is False  # v1 Statistical Context remains unavailable


def test_unevaluable_claim_dates_do_not_inflate_spacing_or_support():
    dates = pd.bdate_range("2026-10-01", periods=12)
    rows = []
    for i, day in enumerate(dates):
        iso = day.strftime("%Y-%m-%d")
        row = _claim(f"u{i}", f"us{i}", "AAA", iso, iso)
        if i not in {0, 11}:
            row["outcome_eligibility"] = "unevaluable"
            row["outcome_unavailable_reason"] = "claim_time_price_history_missing"
        rows.append(row)
    claims = pd.DataFrame(rows, columns=CLAIM_COLUMNS)
    last = dates[11].strftime("%Y-%m-%d")
    end0 = _end_after("2026-10-01")
    end_last = _end_after(last)
    outcomes = pd.DataFrame(
        [
            _outcome("u0", "AAA", "2026-10-01", "2026-10-01", end0, _evaluated_after(end0)),
            _outcome("u11", "AAA", last, last, end_last, _evaluated_after(end_last)),
        ],
        columns=OUTCOME_COLUMNS,
    )
    result = readiness_audit(claims, outcomes)
    assert result["horizons"]["5"]["spaced_evaluable_claims"] == 1
    assert result["horizons"]["5"]["non_overlapping_outcome_support_regions"] == 1


def test_readiness_rejects_outcomes_that_are_not_actually_mature():
    claims, outcomes = _frames()
    outcomes.loc[outcomes["claim_id"].eq("c1"), "evaluated_at"] = "2026-10-07T00:00:00Z"
    with pytest.raises(ValueError, match="outcome chronology/provenance"):
        readiness_audit(claims, outcomes)


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


def _manifest(training: pd.DataFrame, *, version_id: str = "v1"):
    return model_version_manifest(
        version_id=version_id,
        training_cutoff="2026-10-15T00:00:00+00:00",
        training_pairs=training,
        horizons=[5],
        feature_definition={"dq": True, "statistical_confidence": True, "agreement": True},
        parameters={},
        hyperparameters={"mapping": "none"},
        evaluation_start="2026-10-20T00:00:00+00:00",
        evaluation_end="2026-11-20T00:00:00+00:00",
    )


def test_model_manifest_fingerprints_every_column_and_revalidates_purge():
    claims, outcomes = _frames()
    training = purged_training_pairs(
        claims,
        outcomes,
        horizon=5,
        training_cutoff="2026-10-15T00:00:00+00:00",
        evaluation_start="2026-10-20T00:00:00+00:00",
    )
    first = evidence_fingerprint(training)
    assert first == evidence_fingerprint(training.sample(frac=1.0, random_state=7))

    changed = training.copy()
    changed.loc[changed["claim_id"].eq("c1"), "agreement_state"] = "conflict"
    assert evidence_fingerprint(changed) != first

    missing_feature = training.copy()
    missing_feature["engineered_state"] = [pd.NA, "x"]
    literal_feature = training.copy()
    literal_feature["engineered_state"] = ["<NA>", "x"]
    assert evidence_fingerprint(missing_feature) != evidence_fingerprint(literal_feature)

    numeric_feature = training.copy()
    numeric_feature["typed_feature"] = [1, 2]
    string_feature = training.copy()
    string_feature["typed_feature"] = ["1", "2"]
    assert evidence_fingerprint(numeric_feature) != evidence_fingerprint(string_feature)

    manifest = _manifest(training)
    assert manifest["evidence_fingerprint"] == first
    assert manifest["training_rows"] == 2
    assert manifest["immutable_after_evaluation_start"] is True

    late = training.copy()
    late["evaluated_at"] = "2026-10-16T00:00:00Z"
    with pytest.raises(ValueError, match="purged walk-forward"):
        _manifest(late, version_id="late")

    future_generated = training.copy()
    future_generated["generated_at"] = "2026-10-15T01:00:00Z"
    with pytest.raises(ValueError, match="purged walk-forward"):
        _manifest(future_generated, version_id="future-generated")

    premature = training.copy()
    premature["evaluated_at"] = "2026-10-07T00:00:00Z"
    with pytest.raises(ValueError, match="purged walk-forward"):
        _manifest(premature, version_id="premature")

    duplicate = pd.concat([training, training.iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match="duplicate claim_id"):
        _manifest(duplicate, version_id="duplicate")

    wrong_claim_horizon = training.copy()
    wrong_claim_horizon["horizon_sessions_claim"] = 20
    with pytest.raises(ValueError, match="purged walk-forward"):
        _manifest(wrong_claim_horizon, version_id="wrong-claim-horizon")

    wrong_outcome_horizon = training.copy()
    wrong_outcome_horizon["horizon_sessions_outcome"] = 20
    with pytest.raises(ValueError, match="purged walk-forward"):
        _manifest(wrong_outcome_horizon, version_id="wrong-outcome-horizon")


def test_manifest_rejects_preclaim_outcome_and_missing_start_provenance():
    claims, outcomes = _frames()
    training = purged_training_pairs(
        claims,
        outcomes,
        horizon=5,
        training_cutoff="2026-10-15T00:00:00+00:00",
        evaluation_start="2026-10-20T00:00:00+00:00",
    )

    preclaim = training.copy()
    preclaim["as_of_claim"] = "2026-10-10"
    preclaim["as_of_outcome"] = "2026-10-10"
    with pytest.raises(ValueError, match="purged walk-forward"):
        _manifest(preclaim, version_id="preclaim")

    mismatched_start = training.copy()
    mismatched_start["start_market_date_outcome"] = "2026-09-30"
    with pytest.raises(ValueError, match="purged walk-forward"):
        _manifest(mismatched_start, version_id="start-mismatch")

    missing_start = training.drop(columns=["start_market_date_outcome"])
    with pytest.raises(ValueError, match="cannot prove purging"):
        _manifest(missing_start, version_id="missing-start")


def test_empty_evidence_fingerprint_attests_column_names_and_dtypes():
    empty_a = pd.DataFrame({"feature_a": pd.Series(dtype="float64")})
    empty_b = pd.DataFrame({"feature_b": pd.Series(dtype="float64")})
    empty_int = pd.DataFrame({"feature_a": pd.Series(dtype="int64")})
    empty_string = pd.DataFrame({"feature_a": pd.Series(dtype="string")})
    assert evidence_fingerprint(empty_a) != evidence_fingerprint(empty_b)
    assert evidence_fingerprint(empty_a) != evidence_fingerprint(empty_int)
    assert evidence_fingerprint(empty_a) != evidence_fingerprint(empty_string)
    assert evidence_fingerprint(empty_a) == evidence_fingerprint(empty_a.copy())


def test_frozen_baseline_is_descriptive_and_applies_spacing():
    claims, outcomes = _frames()
    result = frozen_baseline_evaluator(claims, outcomes, horizon=5)
    assert result["status"] == "descriptive_only"
    assert result["N"] == 2
    assert result["support_regions"] == 1
    assert result["notes"]["fixed_event_spacing_sessions"] == 5
    assert result["notes"]["peer_labels_use_complete_snapshot_cohort_before_spacing"] is True
    assert result["notes"]["no_adaptive_weights"] is True
    assert result["notes"]["no_scalar_confidence"] is True
    assert result["groups"]["agreement_state"]["compatible"]["N"] == 2


def test_promotion_gate_requires_distinct_non_overlapping_market_date_epochs():
    common = {
        "pit_leakage_audit_passed": True,
        "reproducible_versions": True,
        "robust_uncertainty_available": True,
        "concentration_check_passed": True,
        "temporal_stability_check_passed": True,
        "baseline_advantage_demonstrated": True,
    }

    one = promotion_assessment(
        walkforward_evaluations=[_evaluation("v1", "2026-10-20T00:00:00Z", "2026-11-20T00:00:00Z")],
        **common,
    )
    assert one["status"] == "insufficient_evidence"

    duplicate = promotion_assessment(
        walkforward_evaluations=[
            _evaluation("v1", "2026-10-20T00:00:00Z", "2026-11-20T00:00:00Z"),
            _evaluation("v1", "2026-11-21T00:00:00Z", "2026-12-21T00:00:00Z"),
        ],
        **common,
    )
    assert duplicate["status"] == "insufficient_evidence"

    same_market_date = promotion_assessment(
        walkforward_evaluations=[
            _evaluation("v1", "2026-10-20T00:00:00Z", "2026-11-20T00:00:00Z"),
            _evaluation("v2", "2026-11-20T16:00:00Z", "2026-12-20T16:00:00Z"),
        ],
        **common,
    )
    assert same_market_date["status"] == "insufficient_evidence"

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
