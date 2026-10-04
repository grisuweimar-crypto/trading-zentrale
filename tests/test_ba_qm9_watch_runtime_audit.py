from __future__ import annotations

import copy

import pytest

from scanner.research.decision_layer.depot_watch import build_depot_watch
from scanner.research.decision_layer.input_contract import build_input_packet
from scanner.research.decision_layer.portfolio_action import compute_portfolio_action
from scanner.research.decision_layer.reliability_explainability import (
    build_reliability_explanation,
)
from scanner.research.decision_layer.state_transition import build_state_transition_history
from scanner.research.decision_layer.universal_stance import compute_universal_stance
from scanner.research.governance.ba_qm9_watch_runtime_audit import (
    BAQM9WatchAuditError,
    audit_depot_watch,
)


AS_OF = "2026-10-04T18:00:00+00:00"
SNAPSHOT = "ba-qm9-snapshot"


def _evidence(family: str, claim_id: str, direction: str = "positive") -> dict:
    payload: dict[str, object] = {"direction": direction}
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
        "available_from": "2026-10-04T17:59:00+00:00",
        "source_version": f"{family}-v1",
        "coverage_state": "available",
        "maturity_state": "robust",
        "pit_state": "verified",
        "integration_mode": "production_existing",
        "payload": payload,
    }


def _packet(symbol: str, snapshot: str = SNAPSHOT, as_of: str = AS_OF) -> dict:
    rows = [
        _evidence("selection", f"{symbol}-sel"),
        _evidence("timing", f"{symbol}-tim"),
    ]
    for row in rows:
        row["as_of"] = as_of
        row["available_from"] = as_of
    return build_input_packet(
        symbol=symbol,
        as_of=as_of,
        source_snapshot_id=snapshot,
        evidence=rows,
    )


def _position(
    symbol: str = "TEST",
    *,
    source: str = "broker-snapshot",
    can_add=None,
    remaining_adds=None,
) -> dict:
    return {
        "schema_version": "decision_position_snapshot_v1",
        "symbol": symbol,
        "as_of": "2026-10-04T17:58:00+00:00",
        "source_snapshot_id": source,
        "position_state": "long",
        "quantity": 10,
        "currency": "USD",
        "average_entry_price": 100,
        "current_price": 110,
        "can_add": can_add,
        "remaining_adds": remaining_adds,
    }


def _position_book(*positions: dict) -> dict:
    return {
        "schema_version": "decision_depot_position_book_v1",
        "source_snapshot_id": "private-depot-snapshot",
        "as_of": "2026-10-04T17:59:00+00:00",
        "positions": list(positions),
    }


def _daily(*symbols: str) -> dict:
    return {
        "snapshot_id": SNAPSHOT,
        "as_of": AS_OF,
        "universe_size": len(symbols),
        "symbols": {
            symbol: {
                "current": {
                    "name": symbol,
                    "score": 70.0,
                    "r_code": "R5",
                    "close": 110.0,
                    "currency": "USD",
                }
            }
            for symbol in symbols
        },
    }


def _bundle(
    symbol: str = "TEST",
    *,
    position: dict | None = None,
    snapshot: str = SNAPSHOT,
    as_of: str = AS_OF,
) -> dict:
    previous = _packet(
        symbol,
        snapshot="previous-snapshot",
        as_of="2026-10-03T18:00:00+00:00",
    )
    current = _packet(symbol, snapshot=snapshot, as_of=as_of)
    stances = [compute_universal_stance(previous), compute_universal_stance(current)]
    transition = build_state_transition_history(stances)
    action = compute_portfolio_action(transition, position or _position(symbol))
    explanation = build_reliability_explanation(
        current,
        stances[-1],
        transition,
        action,
    )
    return {
        "schema_version": "decision_chain_bundle_v1",
        "packet": current,
        "stance": stances[-1],
        "transition": transition,
        "action": action,
        "explanation": explanation,
    }


def _bundle_set(*bundles: dict) -> dict:
    return {
        "schema_version": "decision_chain_bundle_set_v1",
        "bundles": list(bundles),
    }


def test_complete_watch_passes_all_eleven_ba_qm9_checks() -> None:
    position = _position(can_add=True, remaining_adds=2)
    daily = _daily("TEST")
    bundles = _bundle_set(_bundle(position=position))
    book = _position_book(position)
    watch = build_depot_watch(daily, book, bundles)

    result = audit_depot_watch(daily, book, bundles, watch)

    assert result["status"] == "PASSED"
    assert result["check_count"] == 11
    assert result["all_required_checks_passed"] is True
    assert all(item["passed"] is True for item in result["checks"].values())
    assert result["checks"]["ADD_CAPACITY"]["details"]["states"]["TEST"] == "available"
    assert result["checks"]["REPRODUCIBILITY"]["details"]["exact_rebuild_equal"] is True
    assert result["old_scanner_heuristic_fallback_used"] is False
    assert result["depot_wall_clock_freshness_claimed"] is False
    assert result["execution_allowed"] is False


def test_missing_decision_stays_unavailable_without_scanner_fallback() -> None:
    position = _position()
    daily = _daily("TEST")
    bundles = _bundle_set()
    book = _position_book(position)
    watch = build_depot_watch(daily, book, bundles)

    result = audit_depot_watch(daily, book, bundles, watch)

    assert result["status"] == "PASSED"
    assert result["checks"]["MISSING_EVIDENCE"]["details"][
        "symbols_without_available_decision"
    ] == ["TEST"]
    assert result["checks"]["MISSING_EVIDENCE"]["details"][
        "scanner_scalar_fallback_used"
    ] is False
    assert watch["rows"][0]["decision"] is None
    assert watch["rows"][0]["daily_scanner_context"]["score"] == 70.0


def test_unknown_add_capacity_remains_unknown() -> None:
    position = _position(can_add=None, remaining_adds=None)
    daily = _daily("TEST")
    bundles = _bundle_set(_bundle(position=position))
    book = _position_book(position)
    watch = build_depot_watch(daily, book, bundles)

    result = audit_depot_watch(daily, book, bundles, watch)

    assert result["checks"]["ADD_CAPACITY"]["details"]["states"]["TEST"] == "unknown"
    assert watch["rows"][0]["position"]["add_capacity_state"] == "unknown"


def test_structurally_stale_decision_bundle_is_blocked_but_depot_age_is_not_invented() -> None:
    position = _position()
    daily = _daily("TEST")
    bundles = _bundle_set(
        _bundle(
            position=position,
            snapshot="old-scanner-snapshot",
            as_of=AS_OF,
        )
    )
    book = _position_book(position)
    watch = build_depot_watch(daily, book, bundles)

    result = audit_depot_watch(daily, book, bundles, watch)

    assert watch["rows"][0]["availability"] == "decision_bundle_snapshot_mismatch"
    assert watch["rows"][0]["decision"] is None
    stale = result["checks"]["STALE_DATA"]["details"]
    assert stale["structural_stale_symbols"] == ["TEST"]
    assert stale["depot_wall_clock_freshness_policy"] == "UNDEFINED"
    assert stale["depot_wall_clock_freshness_state"] == "UNKNOWN_NOT_CLAIMED_FRESH"


def test_tampered_watch_cannot_pass_reproducibility() -> None:
    position = _position()
    daily = _daily("TEST")
    bundles = _bundle_set(_bundle(position=position))
    book = _position_book(position)
    watch = build_depot_watch(daily, book, bundles)
    changed = copy.deepcopy(watch)
    changed["rows"][0]["attention_required"] = not changed["rows"][0]["attention_required"]

    with pytest.raises(Exception):
        audit_depot_watch(daily, book, bundles, changed)


def test_watch_built_from_different_position_state_is_rejected() -> None:
    current = _position(source="broker-new", can_add=False)
    old = _position(source="broker-old", can_add=True)
    daily = _daily("TEST")
    bundles = _bundle_set(_bundle(position=old))
    book = _position_book(current)
    watch = build_depot_watch(daily, book, bundles)

    result = audit_depot_watch(daily, book, bundles, watch)
    assert watch["rows"][0]["availability"] == "position_context_mismatch"
    assert result["checks"]["FAIL_CLOSED"]["passed"] is True
