from __future__ import annotations

import pandas as pd

from scanner.research.elliott_vnext.cross_system import TRANSITION_COLUMNS
from scanner.research.elliott_vnext.stage5_incremental import (
    Stage5Config,
    model_relation_validation,
    transition_context,
    transition_incremental_validation,
)


def _scanner() -> pd.DataFrame:
    dates = pd.date_range("2026-09-01", periods=45, freq="B")
    rows = []
    for i, day in enumerate(dates):
        row = {
            "symbol": "TEST",
            "date": day,
        }
        for transition in TRANSITION_COLUMNS:
            row[transition] = False
        if i == 18:
            row["score_turn_up"] = True
        if i == 22:
            row["risk_turn_up"] = True
        rows.append(row)
    return pd.DataFrame(rows)


def _event(event_id: str, available_from: str) -> dict[str, object]:
    return {
        "event_id": event_id,
        "symbol": "TEST",
        "available_from": available_from,
        "wave_stage": "wave_2_complete",
        "degree": "intermediate",
        "elliott_direction": "up",
        "review_orientation": "supportive_review",
    }


def test_transition_context_separates_event_time_from_post_event_lead_lag() -> None:
    scanner = _scanner()
    anchor_day = scanner.iloc[20]["date"].date().isoformat()
    masks, rows, coverage = transition_context(
        [_event("e1", anchor_day)],
        scanner,
        Stage5Config(bootstrap_reps=0),
    )

    score_bit = 1 << TRANSITION_COLUMNS.index("score_turn_up")
    risk_bit = 1 << TRANSITION_COLUMNS.index("risk_turn_up")
    assert masks["e1"] & score_bit
    assert not (masks["e1"] & risk_bit)

    by_transition = {row["transition"]: row for row in rows}
    assert by_transition["score_turn_up"]["relative_session"] == -2
    assert by_transition["score_turn_up"]["post_event_analysis_only"] is False
    assert by_transition["risk_turn_up"]["relative_session"] == 2
    assert by_transition["risk_turn_up"]["post_event_analysis_only"] is True
    assert coverage["post_event_rows_used_for_incremental_test"] is False


def _outcome(
    event_id: str,
    day: str,
    signed_return: float,
    correct: bool,
) -> dict[str, object]:
    return {
        "claim_type": "route_review",
        "source_event_id": event_id,
        "outcome_available": True,
        "available_from": day,
        "partition": "legacy_development_descriptive_only",
        "horizon_sessions": 5,
        "review_orientation": "supportive_review",
        "wave_stage": "wave_2_complete",
        "degree": "intermediate",
        "elliott_direction": "up",
        "signed_forward_return": signed_return,
        "review_correct": correct,
    }


def test_transition_incremental_compares_same_class_with_and_without_transition() -> None:
    bit = 1 << TRANSITION_COLUMNS.index("score_turn_up")
    masks = {
        "a": bit,
        "b": bit,
        "c": 0,
        "d": 0,
    }
    outcomes = [
        _outcome("a", "2026-07-01", 0.10, True),
        _outcome("b", "2026-07-08", 0.06, True),
        _outcome("c", "2026-07-15", -0.02, False),
        _outcome("d", "2026-07-22", 0.00, False),
    ]

    rows = transition_incremental_validation(
        outcomes,
        masks,
        Stage5Config(bootstrap_reps=0),
    )
    score = [
        row for row in rows
        if row["transition"] == "score_turn_up"
    ]
    assert len(score) == 1
    row = score[0]
    signed = row["signed_return_difference_vs_same_class_without_transition"]
    correct = row["review_correctness_difference_vs_same_class_without_transition"]
    assert signed["N"] == 2
    assert signed["baseline_N"] == 2
    assert abs(float(signed["difference"]) - 0.09) < 1e-12
    assert abs(float(correct["difference"]) - 1.0) < 1e-12
    assert row["rule_selected_on_this_partition"] is False
    assert row["formal_promotion_evidence"] is False


def test_model_relations_fail_closed_when_no_asof_claims_exist() -> None:
    result = model_relation_validation(
        [_event("e1", "2026-09-30")],
        [],
        [],
        Stage5Config(bootstrap_reps=0),
    )
    assert result["status"] == "not_testable_no_historical_frozen_directional_model_claims_supplied"
    assert result["claims_retrojected"] is False
    assert result["validation_rows"] == []


def test_model_relation_validation_uses_only_explicit_claims() -> None:
    events = [{
        **_event("e1", "2026-09-30"),
        "trigger": "EW_W2_CORE",
    }]
    outcomes = [{
        **_outcome("e1", "2026-09-30", 0.08, True),
        "partition": "prospective_unspent",
        "claim_id": "e1",
    }]
    claims = [{
        "symbol": "TEST",
        "model": "timing",
        "claim_id": "timing-1",
        "model_version": "frozen-v1",
        "available_from": "2026-09-30",
        "stance": "supportive",
        "maturity": "mature",
    }]

    result = model_relation_validation(
        events,
        outcomes,
        claims,
        Stage5Config(bootstrap_reps=0),
    )
    assert result["status"] == "evaluated_from_explicit_asof_claims"
    assert result["claim_count"] == 1
    assert result["relation_counts"]["confirmation_candidate"] == 1
    assert result["claims_retrojected"] is False
