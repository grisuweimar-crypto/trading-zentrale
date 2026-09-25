from __future__ import annotations

import copy

import pytest

from scanner.research.decision_layer.universal_stance import (
    UniversalStanceError,
    compute_universal_stance,
    validate_universal_stance,
)


def _common(family: str, claim_id: str, payload: dict, **extra) -> dict:
    row = {
        "family": family,
        "claim_id": claim_id,
        "as_of": "2026-09-26T18:00:00Z",
        "available_from": "2026-09-26T18:00:00Z",
        "source_version": "test-v1",
        "coverage_state": "available",
        "maturity_state": "robust",
        "pit_state": "verified",
        "integration_mode": "research_only",
        "payload": payload,
    }
    row.update(extra)
    return row


def _timing(claim_id: str, direction: str | None, horizon: int = 20) -> dict:
    payload = {
        "pattern_id": f"p-{claim_id}",
        "horizon_sessions": horizon,
        "pattern_frozen": True,
        "match_from_pit_features": True,
    }
    if direction is not None:
        payload["direction"] = direction
    return _common("timing", claim_id, payload)


def _packet(evidence: list[dict], as_of: str = "2026-09-26T18:00:00Z") -> dict:
    adjusted = copy.deepcopy(evidence)
    for row in adjusted:
        row["as_of"] = as_of
        row["available_from"] = as_of
    return {
        "schema_version": "decision_layer_input_contract_v1",
        "symbol": "TEST",
        "as_of": as_of,
        "source_snapshot_id": "snap-7d",
        "evidence": adjusted,
    }


def test_single_positive_claim_produces_positive_stance() -> None:
    result = compute_universal_stance(_packet([_timing("tim", "positive")]))
    assert result["universal_stance"]["state"] == "positive"
    assert result["evidence_structure"]["support_structure"] == "single_direction_or_unopposed"
    assert result["validation"]["status"] == "prospective_unconfirmed"
    assert result["semantics"]["portfolio_action_computed"] is False


def test_single_negative_claim_produces_negative_stance() -> None:
    result = compute_universal_stance(
        _packet([_common("selection", "sel", {"direction": "negative"})])
    )
    assert result["universal_stance"] == {
        "state": "negative",
        "direction": "negative",
        "portfolio_independent": True,
        "research_only": True,
    }


def test_cross_family_confirmation_is_structure_not_extra_vote() -> None:
    result = compute_universal_stance(
        _packet([
            _common("selection", "sel", {"direction": "positive"}),
            _timing("tim", "positive"),
        ])
    )
    assert result["universal_stance"]["state"] == "positive"
    assert result["evidence_structure"]["support_structure"] == "cross_family_confirmation"
    assert result["semantics"]["weighted_super_score_used"] is False


def test_opposite_claims_remain_conflicted_not_neutral() -> None:
    result = compute_universal_stance(
        _packet([
            _common("selection", "sel", {"direction": "positive"}),
            _timing("tim", "negative"),
        ])
    )
    assert result["universal_stance"]["state"] == "conflicted"
    assert result["universal_stance"]["direction"] is None
    assert result["evidence_structure"]["support_structure"] == "unresolved_conflict"
    assert result["semantics"]["conflict_resolved"] is False
    assert result["semantics"]["conflict_treated_as_neutral"] is False


def test_missing_direction_is_insufficient_not_neutral() -> None:
    result = compute_universal_stance(_packet([_timing("tim", None)]))
    assert result["universal_stance"]["state"] == "insufficient_evidence"
    assert result["evidence_structure"]["unknown_direction_claim_ids"] == ["tim"]
    assert result["semantics"]["neutral_inferred_from_missing_evidence"] is False


def test_probability_and_confidence_do_not_change_direction() -> None:
    evidence = [
        _timing("tim", "positive", 5),
        _common(
            "probability",
            "prob",
            {"horizon_sessions": 5, "probability": 0.2},
            claim_ref="tim",
        ),
        _common("confidence", "conf", {"reliability": 0.1}, claim_ref="tim"),
    ]
    result = compute_universal_stance(_packet(evidence))
    assert result["universal_stance"]["state"] == "positive"
    assert result["evidence_structure"]["annotation_count"] == 2
    assert result["semantics"]["probability_and_confidence_count_as_votes"] is False


def test_ineligible_opposite_claim_does_not_create_conflict() -> None:
    selection = _common("selection", "sel", {"direction": "positive"})
    selection["coverage_state"] = "unavailable"
    result = compute_universal_stance(_packet([selection, _timing("tim", "negative")]))
    assert result["universal_stance"]["state"] == "negative"
    assert result["evidence_structure"]["conflicts"] == []
    assert result["evidence_structure"]["ineligible_directional_claims"][0]["claim_id"] == "sel"


def test_spent_packet_is_explicitly_replay_only() -> None:
    result = compute_universal_stance(
        _packet([_timing("tim", "positive")], as_of="2026-09-25T18:00:00Z")
    )
    assert result["research_partition"] == "legacy_replay_spent"
    assert result["validation"]["status"] == "spent_replay_only"
    assert result["validation"]["promotion_eligible"] is False


def test_partition_boundary_uses_packet_calendar_date_not_utc_conversion() -> None:
    result = compute_universal_stance(
        _packet(
            [_timing("tim", "positive")],
            as_of="2026-09-26T00:30:00+02:00",
        )
    )
    assert result["research_partition"] == "prospective_unspent"
    assert result["validation"]["status"] == "prospective_unconfirmed"


def test_validator_rejects_portfolio_or_action_fields() -> None:
    result = compute_universal_stance(_packet([_timing("tim", "positive")]))
    bad = copy.deepcopy(result)
    bad["portfolio_action"] = "BUY"
    with pytest.raises(UniversalStanceError, match="forbidden_portfolio_or_action_fields"):
        validate_universal_stance(bad)
