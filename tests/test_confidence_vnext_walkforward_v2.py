from __future__ import annotations

import json

import pandas as pd
import pytest

from scanner.reports.confidence_vnext_progressive import (
    SCHEMA_VERSION as PHASE5C_SCHEMA_VERSION,
    _version_id,
    _version_sha256,
)
from scanner.reports.confidence_vnext_prospective_v2 import (
    CLAIM_COLUMNS_V2,
    OUTCOME_COLUMNS_V2,
    SCHEMA_VERSION as V2_SCHEMA_VERSION,
    _sha_payload,
)
from scanner.reports.confidence_vnext_walkforward_v2 import (
    _epoch_frame,
    run_walkforward_evaluation,
)


def _claim(
    symbol: str,
    snapshot: str,
    as_of: str,
    horizon: int,
    *,
    generated_at: str | None = None,
):
    payload = {
        column: ""
        for column in CLAIM_COLUMNS_V2
        if column != "claim_id"
    }
    payload.update(
        {
            "schema_version": V2_SCHEMA_VERSION,
            "source_commit": "a" * 40,
            "as_of": as_of,
            "generated_at": generated_at or f"{as_of}T16:00:00+00:00",
            "run_id": f"run-{snapshot}",
            "snapshot_id": snapshot,
            "symbol": symbol,
            "currency": "USD",
            "horizon_sessions": horizon,
            "start_market_date": as_of,
            "start_adjusted_close": 100.0,
            "outcome_eligibility": "eligible",
            "outcome_unavailable_reason": "",
            "cooldown_forbidden_start_dates": json.dumps(
                [
                    as_of,
                    as_of,
                    as_of,
                    as_of,
                    as_of,
                ],
                separators=(",", ":"),
            ),
            "cooldown_context_complete": True,
            "evidence_version": "phase4_confidence_research_v1",
            "evidence_fingerprint": "e" * 64,
            "phase4_report_sha256": "1" * 64,
            "phase2_sha256": "2" * 64,
            "phase3_sha256": "3" * 64,
            "risk_scale_sha256": "missing",
            "phase2_source_as_of": f"{as_of}T00:00:00+00:00",
            "phase3_source_as_of": f"{as_of}T00:00:00+00:00",
            "selection_band": "B5",
            "selection_statistical_state": "robust",
            "selection_statistical_direction": "positive",
            "selection_statistical_context": "{}",
            "timing_statistical_context": "[]",
            "timing_matched_states": "robust",
            "timing_unevaluable_state_counts": "{}",
            "risk_statistical_context": "[]",
            "risk_evidence_states": "volatility:robust:middle",
            "statistical_context_sha256": "4" * 64,
            "timing_model_state": "robust_claim",
            "timing_model_direction": "positive",
            "risk_model_state": "middle",
            "agreement_state": "compatible",
            "agreement_conflicts": "",
            "return_claim_direction": "positive",
            "dq_selection_state": "proxy_complete",
            "dq_timing_state": "proxy_complete",
            "dq_risk_state": "proxy_complete",
            "volatility_application_status": "compatible",
        }
    )
    payload["claim_id"] = _sha_payload(payload)
    return payload


def _outcome(
    claim: dict[str, object],
    end: str,
    ret: float,
    evaluated_at: str,
):
    start = 100.0
    horizon = int(claim["horizon_sessions"])
    payload = {
        "claim_id": claim["claim_id"],
        "schema_version": V2_SCHEMA_VERSION,
        "source_commit": claim["source_commit"],
        "as_of": claim["as_of"],
        "evaluated_at": evaluated_at,
        "symbol": claim["symbol"],
        "currency": claim["currency"],
        "horizon_sessions": horizon,
        "start_market_date": claim["start_market_date"],
        "end_market_date": end,
        "elapsed_market_sessions": horizon,
        "path_session_count": horizon + 1,
        "session_dates_sha256": "5" * 64,
        "path_sha256": "6" * 64,
        "start_adjusted_close": start,
        "end_adjusted_close": start * (1.0 + ret),
        "return": ret,
        "adverse_excursion": max(0.0, -ret),
        "path_max_drawdown": max(0.0, -ret),
    }
    payload["outcome_id"] = _sha_payload(payload)
    return payload


def _version(
    horizon: int,
    cutoff: str,
    fingerprint: str,
    *,
    previous_hash: str = "",
):
    payload = {
        "schema_version": PHASE5C_SCHEMA_VERSION,
        "version_id": _version_id(horizon, fingerprint),
        "previous_version_sha256": previous_hash,
        "horizon_sessions": horizon,
        "source_schema_version": V2_SCHEMA_VERSION,
        "training_cutoff": cutoff,
        "training_claim_start": "2026-09-01",
        "training_claim_end": "2026-09-30",
        "training_outcome_end": "2026-10-01",
        "evidence_fingerprint": fingerprint,
        "training_rows": 20,
        "readiness": {
            "stage": "pilot_learning",
            "pilot_ready": True,
            "robust_learning_base": False,
        },
        "learned_state_reliability": {},
        "evaluation": {
            "status": "awaiting_strictly_future_evidence",
            "training_rows_may_never_be_reused_for_evaluation": True,
            "evaluation_claim_generated_at_must_be_after_training_cutoff": True,
        },
        "semantics": {
            "research_only": True,
            "horizon_specific_labels_only": True,
            "shorter_horizon_labels_borrowed": False,
        },
    }
    payload["version_sha256"] = _version_sha256(payload)
    return payload


def _write_versions(path, versions):
    path.write_text(
        "".join(json.dumps(item, sort_keys=True) + "\n" for item in versions),
        encoding="utf-8",
    )


def _future_fixture():
    a = _claim("AAA", "future", "2026-10-02", 5)
    b = _claim("BBB", "future", "2026-10-02", 5)
    claims = pd.DataFrame([a, b], columns=CLAIM_COLUMNS_V2)
    outcomes = pd.DataFrame(
        [
            _outcome(a, "2026-10-09", 0.05, "2026-10-10T16:00:00Z"),
            _outcome(b, "2026-10-09", 0.01, "2026-10-10T16:00:00Z"),
        ],
        columns=OUTCOME_COLUMNS_V2,
    )
    return claims, outcomes


def _write_sources(tmp_path, claims, outcomes, versions):
    claims_path = tmp_path / "claims.csv"
    outcomes_path = tmp_path / "outcomes.csv"
    versions_path = tmp_path / "versions.jsonl"
    evaluations_path = tmp_path / "evaluations.jsonl"
    report_path = tmp_path / "report.json"
    claims.to_csv(claims_path, index=False)
    outcomes.to_csv(outcomes_path, index=False)
    _write_versions(versions_path, versions)
    return (
        claims_path,
        outcomes_path,
        versions_path,
        evaluations_path,
        report_path,
    )


def test_no_versions_fails_closed_without_inventing_learning(tmp_path):
    claims = pd.DataFrame(columns=CLAIM_COLUMNS_V2)
    outcomes = pd.DataFrame(columns=OUTCOME_COLUMNS_V2)
    paths = _write_sources(tmp_path, claims, outcomes, [])

    result = run_walkforward_evaluation(*paths, bootstrap_reps=5)

    assert result["status"] == "awaiting_phase5c_candidate_versions"
    assert result["finalized_evaluations"] == 0
    assert result["promotion"]["status"] == "insufficient_evidence"
    assert result["semantics"]["production_confidence_changed"] is False


def test_latest_version_is_provisional_and_not_archived(tmp_path):
    claims, outcomes = _future_fixture()
    v1 = _version(5, "2026-10-01T16:00:00Z", "a" * 64)
    paths = _write_sources(tmp_path, claims, outcomes, [v1])

    result = run_walkforward_evaluation(*paths, bootstrap_reps=5)

    five = result["horizons"]["5"]
    assert result["finalized_evaluations"] == 0
    assert five["latest_provisional"]["status"] == "provisional_not_promotion_evidence"
    assert five["latest_provisional"]["N"] == 2
    assert result["semantics"]["provisional_counts_as_promotion_evidence"] is False


def test_successor_finalizes_strictly_future_epoch(tmp_path):
    claims, outcomes = _future_fixture()
    v1 = _version(5, "2026-10-01T16:00:00Z", "a" * 64)
    v2 = _version(
        5,
        "2026-10-12T16:00:00Z",
        "b" * 64,
        previous_hash=v1["version_sha256"],
    )
    paths = _write_sources(tmp_path, claims, outcomes, [v1, v2])

    result = run_walkforward_evaluation(*paths, bootstrap_reps=5)

    assert result["finalized_evaluations"] == 1
    five = result["horizons"]["5"]
    assert five["finalized_evaluation_epochs"] == 1
    assert five["finalized_status"][0]["N"] == 2
    assert result["new_finalized_evaluations"]
    lines = paths[3].read_text(encoding="utf-8").splitlines()
    record = json.loads(lines[0])
    assert record["version_id"] == v1["version_id"]
    assert record["successor_version_id"] == v2["version_id"]
    assert record["semantics"]["strictly_future_claims_only"] is True
    assert record["semantics"]["production_change_performed"] is False


def test_claim_generated_exactly_at_training_cutoff_is_excluded():
    at_cutoff_a = _claim(
        "AAA",
        "boundary",
        "2026-10-01",
        5,
        generated_at="2026-10-01T16:00:00Z",
    )
    at_cutoff_b = _claim(
        "BBB",
        "boundary",
        "2026-10-01",
        5,
        generated_at="2026-10-01T16:00:00Z",
    )
    future_a = _claim("CCC", "future", "2026-10-02", 5)
    future_b = _claim("DDD", "future", "2026-10-02", 5)
    claims = pd.DataFrame(
        [at_cutoff_a, at_cutoff_b, future_a, future_b],
        columns=CLAIM_COLUMNS_V2,
    )
    outcomes = pd.DataFrame(
        [
            _outcome(at_cutoff_a, "2026-10-08", 0.02, "2026-10-09T16:00:00Z"),
            _outcome(at_cutoff_b, "2026-10-08", 0.01, "2026-10-09T16:00:00Z"),
            _outcome(future_a, "2026-10-09", 0.03, "2026-10-10T16:00:00Z"),
            _outcome(future_b, "2026-10-09", 0.01, "2026-10-10T16:00:00Z"),
        ],
        columns=OUTCOME_COLUMNS_V2,
    )

    frame = _epoch_frame(
        claims,
        outcomes,
        horizon=5,
        training_cutoff="2026-10-01T16:00:00Z",
        evaluation_known_by="2026-10-12T16:00:00Z",
    )

    assert set(frame["symbol"]) == {"CCC", "DDD"}


def test_horizon_labels_are_not_borrowed():
    five_a = _claim("AAA", "five", "2026-10-02", 5)
    five_b = _claim("BBB", "five", "2026-10-02", 5)
    twenty_a = _claim("CCC", "twenty", "2026-10-02", 20)
    twenty_b = _claim("DDD", "twenty", "2026-10-02", 20)
    claims = pd.DataFrame(
        [five_a, five_b, twenty_a, twenty_b], columns=CLAIM_COLUMNS_V2
    )
    outcomes = pd.DataFrame(
        [
            _outcome(five_a, "2026-10-09", 0.03, "2026-10-10T16:00:00Z"),
            _outcome(five_b, "2026-10-09", 0.01, "2026-10-10T16:00:00Z"),
            _outcome(twenty_a, "2026-10-30", 0.08, "2026-10-31T16:00:00Z"),
            _outcome(twenty_b, "2026-10-30", 0.04, "2026-10-31T16:00:00Z"),
        ],
        columns=OUTCOME_COLUMNS_V2,
    )

    five = _epoch_frame(
        claims,
        outcomes,
        horizon=5,
        training_cutoff="2026-10-01T16:00:00Z",
        evaluation_known_by="2026-11-01T16:00:00Z",
    )

    assert set(pd.to_numeric(five["horizon_sessions"])) == {5}
    assert set(five["symbol"]) == {"AAA", "BBB"}


def test_evaluation_archive_hash_detects_mutation(tmp_path):
    claims, outcomes = _future_fixture()
    v1 = _version(5, "2026-10-01T16:00:00Z", "a" * 64)
    v2 = _version(
        5,
        "2026-10-12T16:00:00Z",
        "b" * 64,
        previous_hash=v1["version_sha256"],
    )
    paths = _write_sources(tmp_path, claims, outcomes, [v1, v2])
    run_walkforward_evaluation(*paths, bootstrap_reps=5)

    record = json.loads(paths[3].read_text(encoding="utf-8").splitlines()[0])
    record["evaluation_rows"] += 1
    paths[3].write_text(json.dumps(record) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="evaluation integrity failure"):
        run_walkforward_evaluation(*paths, bootstrap_reps=5)


def test_archived_epoch_rerun_and_append_preserves_chain(tmp_path):
    claims, outcomes = _future_fixture()
    v1 = _version(5, "2026-10-01T16:00:00Z", "a" * 64)
    v2 = _version(
        5,
        "2026-10-12T16:00:00Z",
        "b" * 64,
        previous_hash=v1["version_sha256"],
    )
    paths = _write_sources(tmp_path, claims, outcomes, [v1, v2])

    first = run_walkforward_evaluation(*paths, bootstrap_reps=5)
    assert first["finalized_evaluations"] == 1
    first_text = paths[3].read_text(encoding="utf-8")
    first_record = json.loads(first_text.splitlines()[0])
    first_hash = first_record["evaluation_sha256"]

    second = run_walkforward_evaluation(*paths, bootstrap_reps=5)
    assert second["new_finalized_evaluations"] == []
    assert paths[3].read_text(encoding="utf-8") == first_text

    c = _claim("CCC", "future-2", "2026-10-13", 5)
    d = _claim("DDD", "future-2", "2026-10-13", 5)
    claims = pd.concat(
        [claims, pd.DataFrame([c, d], columns=CLAIM_COLUMNS_V2)],
        ignore_index=True,
    )
    outcomes = pd.concat(
        [
            outcomes,
            pd.DataFrame(
                [
                    _outcome(c, "2026-10-20", 0.04, "2026-10-21T16:00:00Z"),
                    _outcome(d, "2026-10-20", 0.02, "2026-10-21T16:00:00Z"),
                ],
                columns=OUTCOME_COLUMNS_V2,
            ),
        ],
        ignore_index=True,
    )
    claims.to_csv(paths[0], index=False)
    outcomes.to_csv(paths[1], index=False)

    v3 = _version(
        5,
        "2026-10-25T16:00:00Z",
        "c" * 64,
        previous_hash=v2["version_sha256"],
    )
    _write_versions(paths[2], [v1, v2, v3])

    third = run_walkforward_evaluation(*paths, bootstrap_reps=5)
    assert len(third["new_finalized_evaluations"]) == 1
    records = [
        json.loads(line)
        for line in paths[3].read_text(encoding="utf-8").splitlines()
    ]
    assert len(records) == 2
    assert records[0] == first_record
    assert records[1]["previous_evaluation_sha256"] == first_hash

    stable_text = paths[3].read_text(encoding="utf-8")
    fourth = run_walkforward_evaluation(*paths, bootstrap_reps=5)
    assert fourth["new_finalized_evaluations"] == []
    assert paths[3].read_text(encoding="utf-8") == stable_text
