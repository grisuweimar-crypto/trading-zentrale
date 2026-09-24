from __future__ import annotations

import json

import pandas as pd

from scanner.reports.confidence_vnext_frozen_baseline import (
    apply_fixed_event_spacing,
    evaluate_frozen_baseline,
)
from scanner.reports.confidence_vnext_prospective_v2 import (
    CLAIM_COLUMNS_V2,
    OUTCOME_COLUMNS_V2,
    SCHEMA_VERSION,
    _sha_payload,
)


def _claim(claim_id: str, symbol: str, snapshot: str, as_of: str, forbidden: list[str], *, selection_state: str = "robust"):
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
            "horizon_sessions": 5,
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
            "selection_statistical_state": selection_state,
            "selection_statistical_direction": "positive",
            "selection_statistical_context": "{}",
            "timing_statistical_context": "[]",
            "timing_matched_states": "robust" if selection_state == "robust" else "directional_only",
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


def _outcome(claim: dict[str, object], end: str, ret: float, adverse: float, drawdown: float):
    start_price = 100.0
    end_price = start_price * (1.0 + ret)
    payload = {
        "claim_id": claim["claim_id"],
        "schema_version": SCHEMA_VERSION,
        "source_commit": claim["source_commit"],
        "as_of": claim["as_of"],
        "evaluated_at": f"{end}T23:00:00+00:00",
        "symbol": claim["symbol"],
        "currency": "USD",
        "horizon_sessions": 5,
        "start_market_date": claim["start_market_date"],
        "end_market_date": end,
        "elapsed_market_sessions": 5,
        "path_session_count": 6,
        "session_dates_sha256": "5" * 64,
        "path_sha256": "6" * 64,
        "start_adjusted_close": start_price,
        "end_adjusted_close": end_price,
        "return": ret,
        "adverse_excursion": adverse,
        "path_max_drawdown": drawdown,
    }
    payload["outcome_id"] = _sha_payload(payload)
    return payload


def test_fixed_spacing_rejects_same_symbol_occurrence_inside_five_sessions():
    rows = [
        {
            "claim_id": "c0",
            "snapshot_id": "s0",
            "as_of": "2026-10-01",
            "symbol": "AAA",
            "start_market_date": "2026-10-01",
            "cooldown_forbidden_start_dates": json.dumps(
                ["2026-09-25", "2026-09-28", "2026-09-29", "2026-09-30", "2026-10-01"]
            ),
            "cooldown_context_complete": True,
        },
        {
            "claim_id": "c1",
            "snapshot_id": "s1",
            "as_of": "2026-10-02",
            "symbol": "AAA",
            "start_market_date": "2026-10-02",
            "cooldown_forbidden_start_dates": json.dumps(
                ["2026-09-28", "2026-09-29", "2026-09-30", "2026-10-01", "2026-10-02"]
            ),
            "cooldown_context_complete": True,
        },
        {
            "claim_id": "c2",
            "snapshot_id": "s2",
            "as_of": "2026-10-08",
            "symbol": "AAA",
            "start_market_date": "2026-10-08",
            "cooldown_forbidden_start_dates": json.dumps(
                ["2026-10-02", "2026-10-05", "2026-10-06", "2026-10-07", "2026-10-08"]
            ),
            "cooldown_context_complete": True,
        },
    ]
    spaced = apply_fixed_event_spacing(pd.DataFrame(rows))
    assert list(spaced["claim_id"]) == ["c0", "c2"]


def test_frozen_baseline_reports_exact_states_without_assuming_ordinal_order():
    first = _claim(
        "ignored", "AAA", "s1", "2026-10-01",
        ["2026-09-25", "2026-09-28", "2026-09-29", "2026-09-30", "2026-10-01"],
        selection_state="robust",
    )
    second = _claim(
        "ignored", "BBB", "s1", "2026-10-01",
        ["2026-09-25", "2026-09-28", "2026-09-29", "2026-09-30", "2026-10-01"],
        selection_state="directional_only",
    )
    claims = pd.DataFrame([first, second], columns=CLAIM_COLUMNS_V2)
    outcomes = pd.DataFrame(
        [
            _outcome(first, "2026-10-08", 0.05, 0.01, 0.02),
            _outcome(second, "2026-10-08", -0.02, 0.03, 0.04),
        ],
        columns=OUTCOME_COLUMNS_V2,
    )
    result = evaluate_frozen_baseline(claims, outcomes, bootstrap_reps=20)
    five = result["horizons"]["5"]
    assert result["semantics"]["state_names_assumed_ordinal"] is False
    assert five["status"] == "descriptive_only"
    assert set(five["groups"]["selection_statistical_state"]) == {"robust", "directional_only"}
    assert five["overall"]["robust_uncertainty"]["direction_hit_rate_95"] is None
    assert result["semantics"]["scalar_confidence_mapping_created"] is False
