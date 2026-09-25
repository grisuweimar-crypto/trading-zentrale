import pandas as pd
import pytest

from scanner.research.elliott_vnext.cross_system import (
    CrossSystemConfig,
    CrossSystemInputError,
    build_event_windows,
    extract_elliott_events,
    join_model_claims,
    nearest_scanner_transitions,
    normalize_model_claims,
    scanner_feature_rows,
    summarize_cross_system,
)


def _route_snapshot(*, context="entry_or_add_review", trigger="EW_W2_CORE", available="2026-08-10", as_of="2026-08-10", stage="wave_2_complete"):
    return {
        "symbol": "TEST",
        "as_of": as_of,
        "timeframe": "daily",
        "degree": "intermediate",
        "research_only": True,
        "routing_is_trade_decision": False,
        "primary_scenario": {
            "scenario_id": "scenario-1",
            "pattern_class": "impulse",
            "family": "motive",
            "direction": "up",
            "degree": "intermediate",
            "timeframe": "daily",
            "stage": stage,
            "status": "valid_structural_candidate",
            "research_only": True,
        },
        "alternative_scenarios": [],
        "swing_routing": [
            {
                "trigger": trigger,
                "review_context": context,
                "scenario_id": "scenario-1",
                "scenario_role": "primary",
                "available_from": available,
                "source": "unit_test_6d_route",
                "evidence": {"test": True},
                "requires_external_confirmation": True,
                "actionability": "review_only_not_trade_instruction",
                "historical_outperformance_claimed": False,
                "research_only": True,
            }
        ],
    }


def _history():
    dates = pd.date_range("2026-07-20", periods=28, freq="B")
    rows = []
    for i, day in enumerate(dates):
        rows.append(
            {
                "date": day.date().isoformat(),
                "symbol": "TEST",
                "name": "Test Corp",
                "sector": "Industrials",
                "observation_type": "observed_scanner",
                "score": 40 + i,
                "opportunity": 45 + i * 0.5,
                "risk": 30 + ((-1) ** i) * 2,
                "RS3M": -2 + i * 0.25,
                "Trend200": -4 + i * 0.4,
                "cycle_pct": 20 + i * 2.5,
                "r_code": "R3" if i < 12 else "R4",
            }
        )
    # Same-day rerun: later appended row must win deterministically.
    duplicate = dict(rows[10])
    duplicate["score"] = 91.0
    duplicate["opportunity"] = 88.0
    rows.insert(11, duplicate)
    return pd.DataFrame(rows)


def test_extracts_only_observed_6d_routes_and_keeps_causality():
    events = extract_elliott_events([_route_snapshot()])
    assert len(events) == 1
    event = events[0]
    assert event["available_from"] == "2026-08-10"
    assert event["review_orientation"] == "supportive_review"
    assert event["partition"] == "legacy_spent_validation_descriptive_only"
    assert event["causal_event_source"] == "observed_6d_route_only"
    assert event["incremental_value_claimed"] is False
    assert "trade_decision" not in event
    assert "order_instruction" not in event


def test_future_route_is_rejected_instead_of_backdated():
    with pytest.raises(CrossSystemInputError, match="route_available_from_after_snapshot_as_of"):
        extract_elliott_events([
            _route_snapshot(available="2026-08-11", as_of="2026-08-10")
        ])


def test_no_historical_elliott_stream_means_no_reconstruction():
    scanner, _ = scanner_feature_rows(_history())
    summary = summarize_cross_system([], [], [], [])
    assert summary["historical_pit_elliott_stream_present"] is False
    assert summary["present_day_elliott_reconstruction_permitted"] is False
    assert summary["stage_specific_incremental_performance_evaluated"] is False
    assert not scanner.empty


def test_scanner_features_preserve_missingness_and_last_same_day_rerun():
    history = _history().drop(columns=["risk"])
    scanner, coverage = scanner_feature_rows(history)
    duplicate_day = pd.Timestamp(_history().iloc[10]["date"])
    row = scanner.loc[scanner["date"].eq(duplicate_day)].iloc[0]
    assert row["score"] == 91.0
    assert scanner["risk"].isna().all()
    assert coverage["sources"]["risk"] is None
    assert coverage["historical_missingness_preserved"] is True
    assert coverage["present_day_reconstruction_used"] is False


def test_event_window_uses_observed_session_offsets_and_marks_future_rows():
    scanner, _ = scanner_feature_rows(_history())
    event_day = scanner.iloc[13]["date"].date().isoformat()
    events = extract_elliott_events([
        _route_snapshot(available=event_day, as_of=event_day)
    ])
    windows = build_event_windows(events, scanner)
    event_rows = [row for row in windows if row["event_id"] == events[0]["event_id"]]
    offsets = [row["relative_session"] for row in event_rows]
    assert 0 in offsets
    assert min(offsets) <= -13
    assert max(offsets) >= 14
    assert all(row["post_event_analysis_only"] == (row["relative_session"] > 0) for row in event_rows)


def test_lead_lag_sign_convention_is_explicit():
    event_id = "event-1"
    rows = []
    for offset in range(-3, 4):
        row = {
            "event_id": event_id,
            "symbol": "TEST",
            "relative_session": offset,
            "scanner_date": f"2026-08-{10 + offset:02d}",
            "score_turn_up": offset == -2,
            "risk_turn_up": offset == 2,
        }
        rows.append(row)
    transitions = nearest_scanner_transitions(rows)
    by_name = {item["transition"]: item for item in transitions}
    assert by_name["score_turn_up"]["relative_session"] == -2
    assert by_name["score_turn_up"]["lead_lag"] == "scanner_leads"
    assert by_name["risk_turn_up"]["relative_session"] == 2
    assert by_name["risk_turn_up"]["lead_lag"] == "elliott_leads"
    assert by_name["risk_turn_up"]["post_event_analysis_only"] is True


def test_model_claims_require_own_asof_provenance_and_future_claims_are_invisible():
    events = extract_elliott_events([_route_snapshot()])
    claims = normalize_model_claims([
        {
            "symbol": "TEST",
            "model": "timing",
            "claim_id": "timing-old",
            "model_version": "phase2-frozen-v1",
            "available_from": "2026-08-08",
            "stance": "supportive",
            "maturity": "mature",
        },
        {
            "symbol": "TEST",
            "model": "risk",
            "claim_id": "risk-future",
            "model_version": "phase3-v1",
            "available_from": "2026-08-12",
            "stance": "cautionary",
            "maturity": "mature",
        },
    ])
    relations = join_model_claims(events, claims)
    by_model = {item["model"]: item for item in relations}
    assert by_model["timing"]["relation"] == "redundancy_candidate"
    assert by_model["timing"]["claim_preexisting"] is True
    assert by_model["risk"]["claim_id"] is None
    assert by_model["risk"]["relation"] == "elliott_rescue_candidate"


def test_same_day_compatible_claim_is_confirmation_not_redundancy():
    events = extract_elliott_events([_route_snapshot()])
    claims = normalize_model_claims([
        {
            "symbol": "TEST",
            "model": "probability",
            "claim_id": "p1",
            "model_version": "frozen-v1",
            "available_from": "2026-08-10",
            "stance": "supportive",
            "maturity": "mature",
        }
    ])
    relation = join_model_claims(events, claims)[0]
    assert relation["relation"] == "confirmation_candidate"
    assert relation["relation_is_performance_claim"] is False
    assert relation["incremental_value_evaluated"] is False


def test_opposite_mature_claim_is_preserved_as_conflict():
    events = extract_elliott_events([_route_snapshot()])
    claims = normalize_model_claims([
        {
            "symbol": "TEST",
            "model": "timing",
            "claim_id": "t1",
            "model_version": "frozen-v1",
            "available_from": "2026-08-10",
            "stance": "cautionary",
            "maturity": "mature",
        }
    ])
    assert join_model_claims(events, claims)[0]["relation"] == "conflict"


def test_neutral_elliott_review_allows_scanner_rescue_candidate():
    events = extract_elliott_events([
        _route_snapshot(context="hold_review", trigger="EW_W2_DANGER")
    ])
    claims = normalize_model_claims([
        {
            "symbol": "TEST",
            "model": "selection",
            "claim_id": "s1",
            "model_version": "selection-v1",
            "available_from": "2026-08-10",
            "stance": "supportive",
            "maturity": "mature",
        }
    ])
    assert join_model_claims(events, claims)[0]["relation"] == "scanner_rescue_candidate"


def test_immature_claim_is_unknown_not_agreement():
    events = extract_elliott_events([_route_snapshot()])
    claims = normalize_model_claims([
        {
            "symbol": "TEST",
            "model": "confidence",
            "claim_id": "c1",
            "model_version": "phase5-shadow",
            "available_from": "2026-08-10",
            "stance": "supportive",
            "maturity": "directional_but_immature",
        }
    ])
    assert join_model_claims(events, claims)[0]["relation"] == "elliott_rescue_candidate"


def test_summary_keeps_spent_holdout_and_6g_boundaries_explicit():
    events = extract_elliott_events([_route_snapshot()])
    summary = summarize_cross_system(events, [], [], [])
    policy = summary["spent_holdout_policy"]
    assert policy["legacy_phase1b_2_validation_is_spent"] is True
    assert policy["may_be_used_to_select_or_tune_6e_rules"] is False
    assert policy["new_unspent_or_prospective_evidence_required_for_incremental_claim"] is True
    assert summary["outcome_validation_deferred_to"] == "6G"
    assert summary["incremental_predictive_value_claimed"] is False
    assert summary["trade_decision_emitted"] is False
    assert summary["order_instruction_emitted"] is False


def test_window_must_cover_at_least_twenty_sessions_each_side():
    with pytest.raises(ValueError, match="event_window_sessions must be >= 20"):
        CrossSystemConfig(event_window_sessions=19)
