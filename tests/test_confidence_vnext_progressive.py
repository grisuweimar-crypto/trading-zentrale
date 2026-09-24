from __future__ import annotations

import json

import pandas as pd
import pytest

from scanner.reports.confidence_vnext_progressive import (
    build_candidate_version,
    claim_spacing_membership,
    horizon_readiness,
    progressive_training_frame,
    run_progressive_learning,
)
from scanner.reports.confidence_vnext_prospective_v2 import (
    CLAIM_COLUMNS_V2,
    OUTCOME_COLUMNS_V2,
    SCHEMA_VERSION,
    _sha_payload,
)


def _claim(symbol: str, snapshot: str, as_of: str, horizon: int, forbidden: list[str]):
    payload = {column: "" for column in CLAIM_COLUMNS_V2 if column != "claim_id"}
    payload.update(
        {
            "schema_version": SCHEMA_VERSION,
            "source_commit": "a" * 40,
            "as_of": as_of,
            "generated_at": f"{as_of}T16:00:00+00:00",
            "run_id": f"run-{snapshot}",
            "snapshot_id": snapshot,
            "symbol": symbol,
            "currency": "USD",
            "horizon_sessions": horizon,
            "start_market_date": as_of,
            "start_adjusted_close": 100.0,
            "outcome_eligibility": "eligible",
            "outcome_unavailable_reason": "",
            "cooldown_forbidden_start_dates": json.dumps(forbidden, separators=(",", ":")),
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
    *,
    evaluated_at: str | None = None,
):
    start = 100.0
    end_price = start * (1.0 + ret)
    evaluated = evaluated_at or (
        (pd.Timestamp(end) + pd.Timedelta(days=1)).tz_localize("UTC").isoformat()
    )
    horizon = int(claim["horizon_sessions"])
    payload = {
        "claim_id": claim["claim_id"],
        "schema_version": SCHEMA_VERSION,
        "source_commit": claim["source_commit"],
        "as_of": claim["as_of"],
        "evaluated_at": evaluated,
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
        "end_adjusted_close": end_price,
        "return": ret,
        "adverse_excursion": max(0.0, -ret),
        "path_max_drawdown": max(0.0, -ret),
    }
    payload["outcome_id"] = _sha_payload(payload)
    return payload


def _two_date_fixture(include_20t_outcomes: bool = False):
    dates = [
        (
            "2026-10-01",
            ["2026-09-25", "2026-09-28", "2026-09-29", "2026-09-30", "2026-10-01"],
            "2026-10-08",
            "2026-10-29",
        ),
        (
            "2026-10-08",
            ["2026-10-02", "2026-10-05", "2026-10-06", "2026-10-07", "2026-10-08"],
            "2026-10-15",
            "2026-11-05",
        ),
    ]
    claims = []
    outcomes = []
    for day_idx, (as_of, forbidden, end5, end20) in enumerate(dates):
        for symbol_idx in range(10):
            symbol = f"S{symbol_idx:02d}"
            ret = 0.01 + symbol_idx * 0.002 + day_idx * 0.001
            c5 = _claim(symbol, f"s5-{day_idx}", as_of, 5, forbidden)
            c20 = _claim(symbol, f"s20-{day_idx}", as_of, 20, forbidden)
            claims.extend([c5, c20])
            outcomes.append(_outcome(c5, end5, ret))
            if include_20t_outcomes:
                outcomes.append(_outcome(c20, end20, ret / 2.0))
    return (
        pd.DataFrame(claims, columns=CLAIM_COLUMNS_V2),
        pd.DataFrame(outcomes, columns=OUTCOME_COLUMNS_V2),
    )


def _write_fixture(tmp_path, include_20t_outcomes: bool = False):
    claims, outcomes = _two_date_fixture(include_20t_outcomes=include_20t_outcomes)
    claims_path = tmp_path / "claims.csv"
    outcomes_path = tmp_path / "outcomes.csv"
    versions_path = tmp_path / "versions.jsonl"
    report_path = tmp_path / "report.json"
    claims.to_csv(claims_path, index=False)
    outcomes.to_csv(outcomes_path, index=False)
    return claims, outcomes, claims_path, outcomes_path, versions_path, report_path


def test_spacing_membership_is_fixed_from_claims_not_available_outcomes():
    c0 = _claim(
        "AAA", "s0", "2026-10-01", 5,
        ["2026-09-25", "2026-09-28", "2026-09-29", "2026-09-30", "2026-10-01"],
    )
    c1 = _claim(
        "AAA", "s1", "2026-10-02", 5,
        ["2026-09-28", "2026-09-29", "2026-09-30", "2026-10-01", "2026-10-02"],
    )
    c2 = _claim(
        "AAA", "s2", "2026-10-08", 5,
        ["2026-10-02", "2026-10-05", "2026-10-06", "2026-10-07", "2026-10-08"],
    )
    claims = pd.DataFrame([c0, c1, c2], columns=CLAIM_COLUMNS_V2)
    outcomes = pd.DataFrame(
        [
            _outcome(c1, "2026-10-09", 0.01),
            _outcome(c2, "2026-10-15", 0.02),
        ],
        columns=OUTCOME_COLUMNS_V2,
    )

    membership = claim_spacing_membership(claims)
    assert c0["claim_id"] in membership
    assert c1["claim_id"] not in membership
    assert c2["claim_id"] in membership

    frame = progressive_training_frame(claims, outcomes, horizon=5)
    assert list(frame["claim_id"]) == [c2["claim_id"]]


def test_spacing_tie_uses_claim_chronology_before_snapshot_id():
    earlier = _claim(
        "AAA", "z-snapshot", "2026-10-01", 5,
        ["2026-09-25", "2026-09-28", "2026-09-29", "2026-09-30", "2026-10-01"],
    )
    later = _claim(
        "AAA", "a-snapshot", "2026-10-02", 5,
        ["2026-09-28", "2026-09-29", "2026-09-30", "2026-10-01", "2026-10-02"],
    )
    # The later claim points to the same stale market session but has a snapshot
    # ID that sorts before the earlier claim. Claim chronology must still win.
    later["start_market_date"] = "2026-10-01"
    later_payload = dict(later)
    later_payload.pop("claim_id")
    later["claim_id"] = _sha_payload(later_payload)

    claims = pd.DataFrame([later, earlier], columns=CLAIM_COLUMNS_V2)
    membership = claim_spacing_membership(claims)

    assert earlier["claim_id"] in membership
    assert later["claim_id"] not in membership


def test_historical_cutoff_filters_late_peer_before_peer_baseline_derivation():
    forbidden = ["2026-09-25", "2026-09-28", "2026-09-29", "2026-09-30", "2026-10-01"]
    a = _claim("AAA", "s0", "2026-10-01", 5, forbidden)
    b = _claim("BBB", "s0", "2026-10-01", 5, forbidden)
    c = _claim("CCC", "s0", "2026-10-01", 5, forbidden)
    claims = pd.DataFrame([a, b, c], columns=CLAIM_COLUMNS_V2)
    outcomes = pd.DataFrame(
        [
            _outcome(a, "2026-10-08", 0.01, evaluated_at="2026-10-09T16:00:00Z"),
            _outcome(b, "2026-10-08", 0.03, evaluated_at="2026-10-09T16:00:00Z"),
            _outcome(c, "2026-10-09", 0.50, evaluated_at="2026-10-10T16:00:00Z"),
        ],
        columns=OUTCOME_COLUMNS_V2,
    )

    historical = progressive_training_frame(
        claims,
        outcomes,
        horizon=5,
        knowable_by="2026-10-09T23:00:00Z",
    )
    assert set(historical["symbol"]) == {"AAA", "BBB"}
    by_symbol = historical.set_index("symbol")
    assert float(by_symbol.loc["AAA", "peer_median_return"]) == pytest.approx(0.03)
    assert float(by_symbol.loc["BBB", "peer_median_return"]) == pytest.approx(0.01)


def test_5t_can_start_learning_while_20t_is_still_collecting():
    claims, outcomes = _two_date_fixture(include_20t_outcomes=False)
    five = progressive_training_frame(claims, outcomes, horizon=5)
    twenty = progressive_training_frame(claims, outcomes, horizon=20)

    five_ready = horizon_readiness(five, 5)
    twenty_ready = horizon_readiness(twenty, 20)

    assert five_ready["pilot_ready"] is True
    assert five_ready["stage"] == "pilot_learning"
    assert five_ready["training_rows"] == 20
    assert five_ready["symbols"] == 10
    assert twenty_ready["pilot_ready"] is False
    assert twenty_ready["stage"] == "collecting"


def test_candidate_is_hash_chained_research_only_and_does_not_borrow_horizons():
    claims, outcomes = _two_date_fixture(include_20t_outcomes=False)
    five = progressive_training_frame(claims, outcomes, horizon=5)
    candidate = build_candidate_version(five, horizon=5)

    assert candidate is not None
    assert candidate["version_id"].startswith("phase5c-5T-")
    assert candidate["previous_version_sha256"] == ""
    assert len(candidate["version_sha256"]) == 64
    assert candidate["semantics"]["horizon_specific_labels_only"] is True
    assert candidate["semantics"]["shorter_horizon_labels_borrowed"] is False
    assert candidate["semantics"]["scalar_confidence_mapping_created"] is False
    assert candidate["semantics"]["production_confidence_changed"] is False
    assert candidate["evaluation"]["status"] == "awaiting_strictly_future_evidence"
    assert candidate["evaluation"]["evaluation_claim_generated_at_must_be_after_training_cutoff"] is True


def test_progressive_runner_is_idempotent_for_unchanged_evidence(tmp_path):
    _, _, claims_path, outcomes_path, versions_path, report_path = _write_fixture(tmp_path)

    first = run_progressive_learning(claims_path, outcomes_path, versions_path, report_path)
    second = run_progressive_learning(claims_path, outcomes_path, versions_path, report_path)

    assert first["horizons"]["5"]["pilot_ready"] is True
    assert first["horizons"]["20"]["pilot_ready"] is False
    assert len(first["new_model_versions"]) == 1
    assert second["new_model_versions"] == []
    assert second["model_versions"] == first["model_versions"] == 1
    assert second["version_chain_tip"] == first["version_chain_tip"]
    assert second["semantics"]["dynamic_horizon_specific_learning"] is True
    assert second["semantics"]["historical_cutoff_applied_before_peer_label_derivation"] is True
    assert second["semantics"]["model_version_archive_hash_chained"] is True
    assert second["semantics"]["production_confidence_changed"] is False


def test_version_hash_detects_mutation(tmp_path):
    _, _, claims_path, outcomes_path, versions_path, report_path = _write_fixture(tmp_path)
    run_progressive_learning(claims_path, outcomes_path, versions_path, report_path)

    item = json.loads(versions_path.read_text(encoding="utf-8").splitlines()[0])
    item["training_rows"] = int(item["training_rows"]) + 1
    versions_path.write_text(json.dumps(item) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="version integrity failure"):
        run_progressive_learning(claims_path, outcomes_path, versions_path, report_path)


def test_previous_report_detects_version_archive_truncation(tmp_path):
    _, _, claims_path, outcomes_path, versions_path, report_path = _write_fixture(
        tmp_path,
        include_20t_outcomes=True,
    )
    first = run_progressive_learning(claims_path, outcomes_path, versions_path, report_path)
    assert first["model_versions"] == 2

    lines = versions_path.read_text(encoding="utf-8").splitlines()
    versions_path.write_text(lines[0] + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="archive was truncated"):
        run_progressive_learning(claims_path, outcomes_path, versions_path, report_path)
