from __future__ import annotations

import copy

import pytest

from scanner.research.decision_layer.input_contract import build_input_packet
from scanner.research.decision_layer.portfolio_action import compute_portfolio_action
from scanner.research.decision_layer.reliability_explainability import (
    ReliabilityExplainabilityError,
    build_reliability_explanation,
    validate_reliability_explanation,
)
from scanner.research.decision_layer.state_transition import build_state_transition_history
from scanner.research.decision_layer.universal_stance import compute_universal_stance


def _row(
    family: str,
    claim_id: str,
    *,
    direction: str | None = None,
    claim_ref: str | None = None,
    coverage: str = "available",
    maturity: str = "robust",
    pit: str = "verified",
    integration: str = "production_existing",
    extra_payload: dict[str, object] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {}
    if direction is not None:
        payload["direction"] = direction
    if family == "timing":
        payload.update({
            "pattern_id": f"pattern-{claim_id}",
            "horizon_sessions": 5,
            "pattern_frozen": True,
            "match_from_pit_features": True,
        })
    elif family == "probability":
        payload.update({"horizon_sessions": 5, "estimated_probability": 0.62})
    elif family == "risk":
        payload.update({"risk_state": "elevated"})
    elif family == "confidence":
        payload.update({"confidence_band": "medium"})
    if extra_payload:
        payload.update(extra_payload)

    row: dict[str, object] = {
        "family": family,
        "claim_id": claim_id,
        "as_of": "2026-09-28T18:00:00+02:00",
        "available_from": "2026-09-28T17:59:00+02:00",
        "source_version": f"{family}-v1",
        "coverage_state": coverage,
        "maturity_state": maturity,
        "pit_state": pit,
        "integration_mode": integration,
        "payload": payload,
    }
    if claim_ref is not None:
        row["claim_ref"] = claim_ref
    return row


def _packet(
    day: int,
    snapshot: str,
    rows: list[dict[str, object]],
) -> dict[str, object]:
    as_of = f"2026-09-{day:02d}T18:00:00+02:00"
    normalized = copy.deepcopy(rows)
    for row in normalized:
        row["as_of"] = as_of
        row["available_from"] = f"2026-09-{day:02d}T17:59:00+02:00"
    return build_input_packet(
        symbol="TEST",
        as_of=as_of,
        source_snapshot_id=snapshot,
        evidence=normalized,
    )


def _positive_rows() -> list[dict[str, object]]:
    return [
        _row("selection", "sel", direction="positive"),
        _row("timing", "tim", direction="positive"),
    ]


def _negative_rows() -> list[dict[str, object]]:
    return [
        _row("selection", "sel", direction="negative"),
        _row("timing", "tim", direction="negative"),
    ]


def _position(state: str = "flat", **extra: object) -> dict[str, object]:
    base: dict[str, object] = {
        "schema_version": "decision_position_snapshot_v1",
        "symbol": "TEST",
        "as_of": "2026-09-28T17:58:00+02:00",
        "source_snapshot_id": "position-snap",
        "position_state": state,
    }
    if state == "long":
        base["quantity"] = 10
    base.update(extra)
    return base


def _swing(*contexts: str) -> dict[str, object]:
    return {
        "source": "elliott_vnext_6h",
        "source_output_id": "elliott-1",
        "as_of": "2026-09-28T17:50:00+02:00",
        "review_contexts": list(contexts),
        "routing_is_trade_decision": False,
        "research_only": True,
    }


def _chain(
    latest: dict[str, object],
    history: list[dict[str, object]],
    *,
    position: dict[str, object] | None = None,
    swing: dict[str, object] | None = None,
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    stances = [compute_universal_stance(packet) for packet in history + [latest]]
    transition = build_state_transition_history(stances)
    action = compute_portfolio_action(transition, position or _position(), swing)
    return stances[-1], transition, action


def test_cross_family_positive_explains_existing_hold_without_new_score():
    previous = _packet(27, "snap-27", _positive_rows())
    latest = _packet(28, "snap-28", _positive_rows())
    stance, transition, action = _chain(latest, [previous], position=_position("long"))

    result = build_reliability_explanation(latest, stance, transition, action)

    assert result["decision_context"]["portfolio_action_state"] == "HOLD"
    assert result["reliability"]["assessment"] == "provisional_cross_family_support"
    assert result["reliability"]["numeric_reliability_score"] is None
    assert len(result["explanation"]["directional_evidence"]["supporting"]) == 2
    assert result["explanation"]["directional_evidence"]["counter"] == []
    assert result["semantics"]["portfolio_action_changed"] is False


def test_same_family_support_is_explicitly_correlated_not_cross_family():
    rows = [
        _row("timing", "tim-a", direction="positive"),
        _row("timing", "tim-b", direction="positive"),
    ]
    previous = _packet(27, "snap-27", rows)
    latest = _packet(28, "snap-28", rows)
    stance, transition, action = _chain(latest, [previous])

    result = build_reliability_explanation(latest, stance, transition, action)
    assert result["reliability"]["assessment"] == "provisional_same_family_support"
    assert result["decision_context"]["support_structure"] == "same_family_correlated_support"


def test_single_unopposed_direction_remains_provisional():
    rows = [_row("selection", "sel", direction="positive")]
    previous = _packet(27, "snap-27", rows)
    latest = _packet(28, "snap-28", rows)
    stance, transition, action = _chain(latest, [previous])

    result = build_reliability_explanation(latest, stance, transition, action)
    assert result["reliability"]["assessment"] == "provisional_unopposed_support"


def test_conflict_is_explained_not_resolved():
    conflict_rows = [
        _row("selection", "sel", direction="positive"),
        _row("timing", "tim", direction="negative"),
    ]
    latest = _packet(28, "snap-28", conflict_rows)
    stance, transition, action = _chain(latest, [], position=_position("long"))

    result = build_reliability_explanation(latest, stance, transition, action)
    directional = result["explanation"]["directional_evidence"]
    assert result["reliability"]["assessment"] == "blocked_conflict"
    assert result["decision_context"]["portfolio_action_state"] == "HOLD"
    assert directional["selected_direction"] is None
    assert len(directional["positive"]) == 1
    assert len(directional["negative"]) == 1
    assert directional["unresolved_conflicts"]
    assert result["semantics"]["conflict_resolved"] is False


def test_insufficient_evidence_is_blocked_and_missing_not_neutral():
    rows = [
        _row(
            "selection",
            "sel",
            direction="positive",
            coverage="unavailable",
            maturity="insufficient_evidence",
        )
    ]
    latest = _packet(28, "snap-28", rows)
    stance, transition, action = _chain(latest, [])

    result = build_reliability_explanation(latest, stance, transition, action)
    assert result["reliability"]["assessment"] == "blocked_insufficient"
    assert result["semantics"]["missing_evidence_treated_as_neutral"] is False
    assert any(
        gap["issue"] == "directional_claim_ineligible"
        for gap in result["explanation"]["missing_or_limited_evidence"]
    )


def test_pending_opposite_transition_surfaces_exact_confirmation_trigger():
    first = _packet(26, "snap-26", _positive_rows())
    second = _packet(27, "snap-27", _positive_rows())
    latest = _packet(28, "snap-28", _negative_rows())
    stance, transition, action = _chain(latest, [first, second], position=_position("long"))

    result = build_reliability_explanation(latest, stance, transition, action)
    assert result["decision_context"]["portfolio_action_state"] == "WAIT_CONFIRMATION"
    assert result["reliability"]["assessment"] == "pending_confirmation"
    trigger = next(
        row for row in result["change_triggers"]["decision_change_triggers"]
        if row["trigger_id"] == "pending_direction_reaches_confirmation_depth"
    )
    assert trigger["current_confirmation_count"] == 1
    assert trigger["required_confirmation_count"] == 2
    assert trigger["requires_recompute_from_source"] is True


def test_probability_confidence_risk_are_visible_but_not_votes():
    rows = [
        _row("selection", "sel", direction="positive"),
        _row("probability", "prob", claim_ref="sel", integration="research_only"),
        _row("confidence", "conf", claim_ref="sel", integration="research_only"),
        _row("risk", "risk", integration="research_only"),
    ]
    previous = _packet(27, "snap-27", rows)
    latest = _packet(28, "snap-28", rows)
    stance, transition, action = _chain(latest, [previous])

    result = build_reliability_explanation(latest, stance, transition, action)
    assert {row["family"] for row in result["explanation"]["annotations"]} == {"probability", "confidence"}
    assert {row["family"] for row in result["explanation"]["non_directional_context"]} == {"risk"}
    assert all(row["directional_vote"] is False for row in result["explanation"]["annotations"])
    assert all(row["directional_vote"] is False for row in result["explanation"]["non_directional_context"])


def test_missing_cost_and_pnl_are_information_gaps_not_direction_changes():
    previous = _packet(27, "snap-27", _positive_rows())
    latest = _packet(28, "snap-28", _positive_rows())
    stance, transition, action = _chain(latest, [previous], position=_position("long"), swing=_swing("profit_protection_review"))

    result = build_reliability_explanation(latest, stance, transition, action)
    assert result["decision_context"]["portfolio_action_state"] == "REDUCE_REVIEW"
    info_ids = {row["trigger_id"] for row in result["change_triggers"]["information_completion_triggers"]}
    assert "transaction_cost_model_supplied" in info_ids
    assert "pnl_context_completed" in info_ids
    assert all(row["changes_decision_directly"] is False for row in result["change_triggers"]["information_completion_triggers"])


def test_add_review_explains_capacity_and_change_trigger():
    previous = _packet(27, "snap-27", _positive_rows())
    latest = _packet(28, "snap-28", _positive_rows())
    stance, transition, action = _chain(
        latest,
        [previous],
        position=_position("long", can_add=True, remaining_adds=2),
        swing=_swing("entry_or_add_review"),
    )

    result = build_reliability_explanation(latest, stance, transition, action)
    assert result["decision_context"]["portfolio_action_state"] == "ADD_REVIEW"
    assert any(
        row["trigger_id"] == "add_context_or_capacity_clears"
        for row in result["change_triggers"]["decision_change_triggers"]
    )


def test_source_snapshot_mismatch_fails_closed():
    previous = _packet(27, "snap-27", _positive_rows())
    latest = _packet(28, "snap-28", _positive_rows())
    stance, transition, action = _chain(latest, [previous])
    bad = copy.deepcopy(action)
    bad["source_snapshot_id"] = "other-snapshot"

    with pytest.raises(ReliabilityExplainabilityError, match="source_source_snapshot_id_mismatch"):
        build_reliability_explanation(latest, stance, transition, bad)


def test_validator_rejects_numeric_reliability_score():
    previous = _packet(27, "snap-27", _positive_rows())
    latest = _packet(28, "snap-28", _positive_rows())
    stance, transition, action = _chain(latest, [previous])
    result = build_reliability_explanation(latest, stance, transition, action)
    bad = copy.deepcopy(result)
    bad["reliability"]["numeric_reliability_score"] = 0.9

    with pytest.raises(ReliabilityExplainabilityError, match="numeric_reliability_score_forbidden_v1"):
        validate_reliability_explanation(bad)


def test_validator_rejects_generated_action_field():
    previous = _packet(27, "snap-27", _positive_rows())
    latest = _packet(28, "snap-28", _positive_rows())
    stance, transition, action = _chain(latest, [previous])
    result = build_reliability_explanation(latest, stance, transition, action)
    bad = copy.deepcopy(result)
    bad["new_portfolio_action"] = "EXIT_REVIEW"

    with pytest.raises(ReliabilityExplainabilityError, match="forbidden_decision_or_execution_fields"):
        validate_reliability_explanation(bad)


def test_explanation_id_detects_tampering():
    previous = _packet(27, "snap-27", _positive_rows())
    latest = _packet(28, "snap-28", _positive_rows())
    stance, transition, action = _chain(latest, [previous])
    result = build_reliability_explanation(latest, stance, transition, action)
    bad = copy.deepcopy(result)
    bad["decision_context"]["portfolio_action_reason_code"] = "tampered"

    with pytest.raises(ReliabilityExplainabilityError, match="explanation_id_integrity_failure"):
        validate_reliability_explanation(bad)
