from __future__ import annotations

import copy

import pytest

from scanner.research.decision_layer.depot_watch import (
    DepotWatchError,
    build_depot_watch,
    validate_depot_watch,
    validate_position_book,
)
from scanner.research.decision_layer.input_contract import build_input_packet
from scanner.research.decision_layer.portfolio_action import compute_portfolio_action
from scanner.research.decision_layer.reliability_explainability import build_reliability_explanation
from scanner.research.decision_layer.state_transition import build_state_transition_history
from scanner.research.decision_layer.universal_stance import compute_universal_stance


AS_OF = "2026-09-28T18:00:00+02:00"
SNAPSHOT = "daily-snap-28"


def _row(family, claim_id, direction=None):
    payload = {}
    if direction is not None:
        payload["direction"] = direction
    if family == "timing":
        payload.update({
            "pattern_id": "pattern-1",
            "horizon_sessions": 5,
            "pattern_frozen": True,
            "match_from_pit_features": True,
        })
    return {
        "family": family,
        "claim_id": claim_id,
        "as_of": AS_OF,
        "available_from": "2026-09-28T17:59:00+02:00",
        "source_version": f"{family}-v1",
        "coverage_state": "available",
        "maturity_state": "robust",
        "pit_state": "verified",
        "integration_mode": "production_existing",
        "payload": payload,
    }


def _packet(symbol="TEST", snapshot=SNAPSHOT, as_of=AS_OF, direction="positive"):
    rows = [_row("selection", f"{symbol}-sel", direction), _row("timing", f"{symbol}-tim", direction)]
    for row in rows:
        row["as_of"] = as_of
        row["available_from"] = as_of.replace("18:00:00", "17:59:00")
    return build_input_packet(symbol=symbol, as_of=as_of, source_snapshot_id=snapshot, evidence=rows)


def _position(symbol="TEST", state="long", source="broker-snap", as_of="2026-09-28T17:58:00+02:00", **extra):
    value = {
        "schema_version": "decision_position_snapshot_v1",
        "symbol": symbol,
        "as_of": as_of,
        "source_snapshot_id": source,
        "position_state": state,
    }
    if state == "long":
        value["quantity"] = 10
    value.update(extra)
    return value


def _position_book(*positions, as_of="2026-09-28T17:59:00+02:00"):
    return {
        "schema_version": "decision_depot_position_book_v1",
        "source_snapshot_id": "portfolio-snap-1",
        "as_of": as_of,
        "positions": list(positions),
    }


def _daily(*symbols):
    return {
        "snapshot_id": SNAPSHOT,
        "as_of": AS_OF,
        "universe_size": len(symbols),
        "symbols": {
            symbol: {
                "current": {
                    "name": f"{symbol} Corp",
                    "score": 71.2,
                    "r_code": "R5",
                    "close": 123.45,
                    "currency": "USD",
                },
                "classifications": {"top_20_percent": True},
            }
            for symbol in symbols
        },
    }


def _bundle(symbol="TEST", position=None, snapshot=SNAPSHOT, as_of=AS_OF, direction="positive"):
    previous_as_of = "2026-09-27T18:00:00+02:00"
    previous = _packet(symbol, "prev-snap", previous_as_of, direction)
    current = _packet(symbol, snapshot, as_of, direction)
    stances = [compute_universal_stance(previous), compute_universal_stance(current)]
    transition = build_state_transition_history(stances)
    action = compute_portfolio_action(transition, position or _position(symbol))
    explanation = build_reliability_explanation(current, stances[-1], transition, action)
    return {
        "schema_version": "decision_chain_bundle_v1",
        "packet": current,
        "stance": stances[-1],
        "transition": transition,
        "action": action,
        "explanation": explanation,
    }


def _bundles(*bundles):
    return {"schema_version": "decision_chain_bundle_set_v1", "bundles": list(bundles)}


def test_complete_watch_surfaces_decision_layer_not_raw_indicator_recommendation():
    position = _position(currency="USD", average_entry_price=100, current_price=120)
    result = build_depot_watch(_daily("TEST"), _position_book(position), _bundles(_bundle(position=position)))

    assert result["watch_status"] == "complete"
    row = result["rows"][0]
    assert row["availability"] == "decision_available"
    assert row["decision"]["universal_stance_state"] == "positive"
    assert row["decision"]["portfolio_action_state"] == "HOLD"
    assert row["decision"]["reliability_assessment"] == "provisional_cross_family_support"
    assert row["decision"]["numeric_reliability_score"] is None
    assert row["daily_scanner_context"]["score"] == 71.2
    assert result["semantics"]["scanner_scalar_replaced_missing_decision_evidence"] is False
    assert result["semantics"]["holdings_ranked"] is False
    assert result["validation"]["execution_allowed"] is False


def test_review_action_is_grouped_not_ranked():
    position = _position(state="flat")
    result = build_depot_watch(_daily("TEST"), _position_book(position), _bundles(_bundle(position=position)))
    row = result["rows"][0]
    assert row["decision"]["portfolio_action_state"] == "ENTER_REVIEW"
    assert row["presentation_group"] == "review_now"
    assert row["attention_required"] is True
    assert result["presentation"]["grouping_is_ranking"] is False
    assert result["presentation"]["action_or_reliability_ranking_performed"] is False


def test_missing_bundle_is_explicitly_unavailable_and_score_does_not_substitute():
    position = _position()
    result = build_depot_watch(_daily("TEST"), _position_book(position), _bundles())
    row = result["rows"][0]
    assert result["watch_status"] == "unavailable"
    assert row["availability"] == "decision_bundle_missing"
    assert row["decision"] is None
    assert row["daily_scanner_context"]["score"] == 71.2
    assert row["presentation_group"] == "unavailable"


def test_exact_symbol_absence_is_not_fuzzy_matched():
    position = _position("TEST.DE")
    result = build_depot_watch(_daily("TEST"), _position_book(position), _bundles())
    row = result["rows"][0]
    assert row["availability"] == "symbol_not_in_daily_research"
    assert result["semantics"]["fuzzy_symbol_matching_used"] is False


def test_bundle_from_other_daily_snapshot_is_not_joined_by_date_only():
    position = _position()
    foreign = _bundle(position=position, snapshot="other-snapshot")
    result = build_depot_watch(_daily("TEST"), _position_book(position), _bundles(foreign))
    row = result["rows"][0]
    assert row["availability"] == "decision_bundle_snapshot_mismatch"
    assert row["decision"] is None


def test_position_context_must_match_the_position_used_by_7f():
    supplied = _position(source="broker-new", current_price=120, currency="USD")
    old = _position(source="broker-old", current_price=115, currency="USD")
    result = build_depot_watch(_daily("TEST"), _position_book(supplied), _bundles(_bundle(position=old)))
    row = result["rows"][0]
    assert row["availability"] == "position_context_mismatch"
    assert row["decision"] is None


def test_invalid_bundle_fails_closed_per_symbol_without_hiding_other_valid_positions():
    first = _position("TEST")
    second = _position("GOOD", source="good-broker")
    bad = _bundle("TEST", first)
    bad["explanation"] = copy.deepcopy(bad["explanation"])
    bad["explanation"]["decision_context"]["portfolio_action_state"] = "EXIT_REVIEW"
    good = _bundle("GOOD", second)

    result = build_depot_watch(
        _daily("TEST", "GOOD"),
        _position_book(first, second),
        _bundles(bad, good),
    )
    rows = {row["symbol"]: row for row in result["rows"]}
    assert result["watch_status"] == "partial"
    assert rows["TEST"]["availability"] == "decision_bundle_invalid"
    assert rows["GOOD"]["availability"] == "decision_available"


def test_partial_watch_keeps_missing_and_available_titles_separate():
    first = _position("TEST")
    second = _position("MISSING", source="broker-2")
    result = build_depot_watch(
        _daily("TEST", "MISSING"),
        _position_book(first, second),
        _bundles(_bundle("TEST", first)),
    )
    assert result["watch_status"] == "partial"
    assert result["summary"]["decision_available_count"] == 1
    assert result["summary"]["unavailable_count"] == 1
    assert result["presentation"]["groups"]["unavailable"] == ["MISSING"]


def test_duplicate_position_symbol_fails_hard_as_ambiguous_input():
    with pytest.raises(DepotWatchError, match="duplicate_position_symbol"):
        validate_position_book(_position_book(_position(), _position(source="broker-2")), decision_as_of=AS_OF)


def test_future_position_book_fails_closed():
    with pytest.raises(DepotWatchError, match="future_position_book"):
        build_depot_watch(
            _daily("TEST"),
            _position_book(_position(), as_of="2026-09-28T19:00:00+02:00"),
            _bundles(_bundle(position=_position())),
        )


def test_model_and_legacy_portfolio_sources_are_never_implied():
    result = build_depot_watch(_daily("TEST"), _position_book(_position()), _bundles(_bundle(position=_position())))
    assert result["semantics"]["model_portfolio_used_as_actual_position_source"] is False
    assert result["semantics"]["legacy_holdings_used_as_actual_position_source"] is False
    assert result["privacy"]["public_repository_persistence_default"] is False
    assert result["privacy"]["autopilot_public_commit_enabled"] is False


def test_watch_id_detects_post_build_tampering():
    result = build_depot_watch(_daily("TEST"), _position_book(_position()), _bundles(_bundle(position=_position())))
    bad = copy.deepcopy(result)
    bad["rows"][0]["decision"]["portfolio_action_reason_code"] = "tampered"
    with pytest.raises(DepotWatchError, match="watch_id_integrity_failure"):
        validate_depot_watch(bad)


def test_unavailable_row_cannot_be_given_an_action_afterwards():
    result = build_depot_watch(_daily("TEST"), _position_book(_position()), _bundles())
    bad = copy.deepcopy(result)
    bad["rows"][0]["decision"] = {"portfolio_action_state": "HOLD"}
    with pytest.raises(DepotWatchError, match="unavailable_row_must_not_expose_decision"):
        validate_depot_watch(bad)
