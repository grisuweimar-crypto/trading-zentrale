from __future__ import annotations

from hashlib import sha256
import json

import pandas as pd
import pytest

from scanner.reports.confidence_vnext_prospective import CLAIM_COLUMNS, OUTCOME_COLUMNS
from scanner.reports.confidence_vnext_walkforward import (
    Phase5WalkForwardConfig,
    V1_HORIZON_PROVENANCE_BLOCKER,
    evidence_fingerprint,
    frozen_baseline_evaluator,
    model_version_manifest,
    promotion_assessment,
    purged_training_pairs,
    readiness_audit,
)


def _claim(
    snapshot: str = "s1",
    symbol: str = "AAA",
    as_of: str = "2026-10-01",
    start: str = "2026-10-01",
    horizon: int = 5,
    *,
    generated_at: str | None = None,
    selection_state: str = "robust",
) -> dict[str, object]:
    row = {column: "" for column in CLAIM_COLUMNS}
    row.update(
        {
            "schema_version": "phase4e_shadow_v1",
            "as_of": as_of,
            "generated_at": generated_at or f"{as_of}T16:00:00+00:00",
            "run_id": f"run-{snapshot}",
            "snapshot_id": snapshot,
            "symbol": symbol,
            "currency": "USD",
            "horizon_sessions": horizon,
            "start_market_date": start,
            "start_adjusted_close": 100.0,
            "outcome_eligibility": "eligible",
            "outcome_unavailable_reason": "",
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
            "agreement_state": "compatible",
            "agreement_conflicts": "",
            "return_claim_direction": "positive",
            "dq_selection_state": "proxy_complete",
            "dq_timing_state": "proxy_complete",
            "dq_risk_state": "proxy_complete",
            "volatility_application_status": "compatible",
        }
    )
    payload = {key: value for key, value in row.items() if key != "claim_id"}
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    row["claim_id"] = sha256(canonical.encode("utf-8")).hexdigest()
    return row


def _outcome(
    claim: dict[str, object],
    *,
    end: str = "2026-10-08",
    evaluated_at: str = "2026-10-09T00:00:00+00:00",
    start_price: object = 100.0,
    end_price: object = 105.0,
    ret: object = 0.05,
    adverse: object = 0.02,
    drawdown: object = 0.03,
) -> dict[str, object]:
    row = {column: "" for column in OUTCOME_COLUMNS}
    row.update(
        {
            "claim_id": claim["claim_id"],
            "schema_version": "phase4e_shadow_v1",
            "as_of": claim["as_of"],
            "evaluated_at": evaluated_at,
            "symbol": claim["symbol"],
            "currency": claim["currency"],
            "horizon_sessions": claim["horizon_sessions"],
            "start_market_date": claim["start_market_date"],
            "end_market_date": end,
            "start_adjusted_close": start_price,
            "end_adjusted_close": end_price,
            "return": ret,
            "adverse_excursion": adverse,
            "path_max_drawdown": drawdown,
        }
    )
    return row


def _frames(claims: list[dict[str, object]], outcomes: list[dict[str, object]]):
    return (
        pd.DataFrame(claims, columns=CLAIM_COLUMNS),
        pd.DataFrame(outcomes, columns=OUTCOME_COLUMNS),
    )


def test_empty_readiness_is_fail_closed():
    result = readiness_audit(
        pd.DataFrame(columns=CLAIM_COLUMNS),
        pd.DataFrame(columns=OUTCOME_COLUMNS),
    )
    assert result["status"] == "insufficient_evidence"
    assert result["shadow_archive"]["claims"] == 0
    assert result["shadow_archive"]["mature_outcomes"] == 0
    assert result["statistical_context_complete"] is False
    assert V1_HORIZON_PROVENANCE_BLOCKER in result["blockers"]
    assert result["horizons"]["5"]["walkforward_evaluation_ready"] is False


def test_freeze_and_contract_constants_are_immutable():
    claims, outcomes = _frames([], [])
    with pytest.raises(ValueError, match="freeze_time is immutable"):
        readiness_audit(claims, outcomes, freeze_time="2026-09-23T00:00:00Z")
    with pytest.raises(ValueError, match="freeze_commit is immutable"):
        readiness_audit(claims, outcomes, freeze_commit="wrong")
    for config in (
        Phase5WalkForwardConfig(uncertainty_block_multiplier=1),
        Phase5WalkForwardConfig(minimum_time_separated_support_regions=1),
        Phase5WalkForwardConfig(fixed_event_spacing_sessions=4),
        Phase5WalkForwardConfig(require_statistical_context_for_adaptation=False),
    ):
        with pytest.raises(ValueError, match="contract constants are immutable"):
            readiness_audit(claims, outcomes, config=config)


def test_claim_hash_and_schema_are_authenticated():
    claim = _claim()
    claims, outcomes = _frames([claim], [])
    assert readiness_audit(claims, outcomes)["shadow_archive"]["claims"] == 1

    edited = claims.copy()
    edited.loc[0, "selection_state"] = "mixed"
    with pytest.raises(ValueError, match="claim hash mismatch"):
        readiness_audit(edited, outcomes)

    wrong_schema = claims.copy()
    wrong_schema.loc[0, "schema_version"] = "forged"
    with pytest.raises(ValueError, match="claim schema_version"):
        readiness_audit(wrong_schema, outcomes)


def test_claim_archive_rejects_unsupported_horizon_without_outcome():
    claim = _claim(horizon=10)
    claims, outcomes = _frames([claim], [])
    with pytest.raises(ValueError, match="unsupported Phase 4E claim horizon_sessions"):
        readiness_audit(claims, outcomes)


def test_claim_generation_must_be_contemporaneous_not_retroactive():
    retro = _claim(generated_at="2026-10-14T16:00:00+00:00")
    claims, outcomes = _frames([retro], [])
    with pytest.raises(ValueError, match="claim chronology/provenance"):
        readiness_audit(claims, outcomes)


def test_statistical_context_cannot_be_fabricated_with_extra_columns():
    claim = _claim()
    claims, outcomes = _frames([claim], [])
    claims["timing_statistical_state"] = "robust"
    claims["risk_statistical_state"] = "robust"
    with pytest.raises(ValueError, match="immutable Phase-4E claim schema"):
        readiness_audit(claims, outcomes)


def test_raw_outcome_labels_require_finite_consistent_values():
    claim = _claim()
    bad = _outcome(claim, ret="")
    claims, outcomes = _frames([claim], [bad])
    with pytest.raises(ValueError, match="outcome chronology/labels"):
        readiness_audit(claims, outcomes)

    inconsistent = _outcome(claim, ret=0.2)
    claims, outcomes = _frames([claim], [inconsistent])
    with pytest.raises(ValueError, match="outcome chronology/labels"):
        readiness_audit(claims, outcomes)

    impossible_risk = _outcome(claim, adverse=1.5)
    claims, outcomes = _frames([claim], [impossible_risk])
    with pytest.raises(ValueError, match="outcome chronology/labels"):
        readiness_audit(claims, outcomes)


def test_raw_outcome_path_risk_labels_must_be_internally_possible():
    claim = _claim()

    missing_endpoint_loss = _outcome(
        claim,
        end_price=50.0,
        ret=-0.5,
        adverse=0.0,
        drawdown=0.0,
    )
    claims, outcomes = _frames([claim], [missing_endpoint_loss])
    with pytest.raises(ValueError, match="outcome chronology/labels"):
        readiness_audit(claims, outcomes)

    drawdown_below_adverse = _outcome(
        claim,
        adverse=0.04,
        drawdown=0.03,
    )
    claims, outcomes = _frames([claim], [drawdown_below_adverse])
    with pytest.raises(ValueError, match="outcome chronology/labels"):
        readiness_audit(claims, outcomes)


def test_obviously_too_short_declared_horizon_is_rejected():
    claim = _claim(horizon=60)
    outcome = _outcome(
        claim,
        end="2026-10-02",
        evaluated_at="2026-10-03T00:00:00+00:00",
    )
    claims, outcomes = _frames([claim], [outcome])
    with pytest.raises(ValueError, match="outcome chronology/labels"):
        readiness_audit(claims, outcomes)


def test_valid_looking_v1_outcome_remains_unverified_and_untrainable():
    claim = _claim()
    outcome = _outcome(claim)
    claims, outcomes = _frames([claim], [outcome])

    result = readiness_audit(claims, outcomes)
    five = result["horizons"]["5"]
    assert five["raw_archive_outcomes"] == 1
    assert five["mature_outcomes"] == 0
    assert five["verified_mature_outcomes"] == 0
    assert five["horizon_session_provenance_verified"] is False
    assert five["walkforward_evaluation_ready"] is False
    assert V1_HORIZON_PROVENANCE_BLOCKER in result["blockers"]

    baseline = frozen_baseline_evaluator(claims, outcomes, horizon=5)
    assert baseline["status"] == "insufficient_evidence"
    assert baseline["N"] == 0
    assert baseline["raw_archive_outcomes"] == 1
    assert V1_HORIZON_PROVENANCE_BLOCKER in baseline["blockers"]

    with pytest.raises(ValueError, match=V1_HORIZON_PROVENANCE_BLOCKER):
        purged_training_pairs(
            claims,
            outcomes,
            horizon=5,
            training_cutoff="2026-10-15T00:00:00Z",
            evaluation_start="2026-10-20T00:00:00Z",
        )


def test_evidence_fingerprint_is_typed_null_safe_and_schema_bound():
    missing = pd.DataFrame({"x": [pd.NA], "y": [1]})
    literal = pd.DataFrame({"x": ["<NA>"], "y": [1]})
    numeric = pd.DataFrame({"x": [1]})
    string = pd.DataFrame({"x": ["1"]})
    assert evidence_fingerprint(missing) != evidence_fingerprint(literal)
    assert evidence_fingerprint(numeric) != evidence_fingerprint(string)

    empty_float = pd.DataFrame({"x": pd.Series(dtype="float64")})
    empty_int = pd.DataFrame({"x": pd.Series(dtype="int64")})
    empty_other = pd.DataFrame({"z": pd.Series(dtype="float64")})
    assert evidence_fingerprint(empty_float) != evidence_fingerprint(empty_int)
    assert evidence_fingerprint(empty_float) != evidence_fingerprint(empty_other)
    assert evidence_fingerprint(empty_float) == evidence_fingerprint(empty_float.copy())


def test_manifest_is_framework_only_until_verified_provenance_exists():
    empty = pd.DataFrame()
    manifest = model_version_manifest(
        version_id="v-empty",
        training_cutoff="2026-10-15T00:00:00Z",
        training_pairs=empty,
        horizons=[5],
        feature_definition={"research_only": True},
        parameters={},
        hyperparameters={},
        evaluation_start="2026-10-20T00:00:00Z",
        evaluation_end="2026-11-20T00:00:00Z",
    )
    assert manifest["training_rows"] == 0
    assert manifest["shadow_v1_training_allowed"] is False

    with pytest.raises(ValueError, match=V1_HORIZON_PROVENANCE_BLOCKER):
        model_version_manifest(
            version_id="v-bad",
            training_cutoff="2026-10-15T00:00:00Z",
            training_pairs=pd.DataFrame({"claim_id": ["x"]}),
            horizons=[5],
            feature_definition={},
            parameters={},
            hyperparameters={},
            evaluation_start="2026-10-20T00:00:00Z",
            evaluation_end="2026-11-20T00:00:00Z",
        )


def _evaluation(version: str, start: str, end: str) -> dict[str, str]:
    return {"version_id": version, "evaluation_start": start, "evaluation_end": end}


def test_promotion_requires_distinct_nonoverlapping_market_date_epochs():
    gates = dict(
        pit_leakage_audit_passed=True,
        reproducible_versions=True,
        robust_uncertainty_available=True,
        concentration_check_passed=True,
        temporal_stability_check_passed=True,
        baseline_advantage_demonstrated=True,
    )
    same_day = promotion_assessment(
        walkforward_evaluations=[
            _evaluation("v1", "2026-10-20T00:00:00Z", "2026-11-20T00:00:00Z"),
            _evaluation("v2", "2026-11-20T16:00:00Z", "2026-12-20T16:00:00Z"),
        ],
        **gates,
    )
    assert same_day["status"] == "insufficient_evidence"

    same_declared_market_day_with_offset = promotion_assessment(
        walkforward_evaluations=[
            _evaluation("v1", "2026-10-20T00:00:00-05:00", "2026-11-20T00:00:00-05:00"),
            _evaluation("v2", "2026-11-20T23:00:00-05:00", "2026-12-20T23:00:00-05:00"),
        ],
        **gates,
    )
    assert same_declared_market_day_with_offset["status"] == "insufficient_evidence"

    good = promotion_assessment(
        walkforward_evaluations=[
            _evaluation("v1", "2026-10-20T00:00:00Z", "2026-11-20T00:00:00Z"),
            _evaluation("v2", "2026-11-21T00:00:00Z", "2026-12-21T00:00:00Z"),
        ],
        **gates,
    )
    assert good["status"] == "eligible_for_separate_promotion_review"
    assert good["production_change_performed"] is False
