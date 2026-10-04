from __future__ import annotations

from copy import deepcopy

import pytest

from scanner.research.decision_layer.depot_watch import (
    BUNDLE_SET_SCHEMA_VERSION,
    DepotWatchError,
    build_depot_watch,
    validate_depot_watch,
)
from scanner.research.decision_layer.depot_watch_orchestrator import (
    build_decision_bundle_set,
    build_orchestrated_depot_watch,
)
from scanner.research.decision_layer.input_contract import build_input_packet
from scanner.research.decision_layer.portfolio_action import PortfolioActionError
from scanner.research.governance.ba_qm9_end_application_audit import (
    BAQM9AuditError,
    EXPECTED_CHECKS,
    audit_current_public_boundaries,
    evaluate_ba_qm9_closure,
    load_contract,
    validate_contract,
)


SNAPSHOT = "snapshot-qm9"
DECISION_TIME = "2026-10-04T18:00:00+00:00"


def _daily() -> dict:
    return {
        "schema_version": "daily_research_v1",
        "snapshot_id": SNAPSHOT,
        "source_snapshot_id": SNAPSHOT,
        "as_of": DECISION_TIME,
        "generated_at": DECISION_TIME,
        "universe_size": 1,
        "symbols": {
            "TEST": {
                "current": {
                    "name": "Test Corp",
                    "score": 30.0,
                    "rank": 1,
                    "rank_percentile": 0.01,
                    "r_code": "R4",
                    "rs3m": 0.12,
                    "trend200": 0.15,
                    "confidence": 70.0,
                    "confidence_label": "HIGH",
                    "close": 100.0,
                    "currency": "USD",
                },
                "dynamics": {},
                "persistence": {},
                "classification": {},
                "historical_matches": {},
            }
        },
    }


def _position(
    *,
    symbol: str = "TEST",
    as_of: str = "2026-10-04T17:59:00+00:00",
    can_add: bool | None = None,
    remaining_adds: int | None = None,
) -> dict:
    return {
        "schema_version": "decision_position_snapshot_v1",
        "symbol": symbol,
        "source_snapshot_id": f"private-{symbol}",
        "as_of": as_of,
        "position_state": "long",
        "quantity": 10,
        "currency": "USD",
        "average_entry_price": 90.0,
        "current_price": 100.0,
        "can_add": can_add,
        "remaining_adds": remaining_adds,
    }


def _book(*positions: dict) -> dict:
    rows = list(positions) if positions else [_position()]
    return {
        "schema_version": "decision_depot_position_book_v1",
        "source_snapshot_id": "private-qm9-book",
        "as_of": max(str(row["as_of"]) for row in rows),
        "positions": rows,
    }


def _packet() -> dict:
    return build_input_packet(
        symbol="TEST",
        as_of=DECISION_TIME,
        source_snapshot_id=SNAPSHOT,
        evidence=[
            {
                "family": "selection",
                "claim_id": f"selection:TEST:{SNAPSHOT}",
                "as_of": DECISION_TIME,
                "available_from": DECISION_TIME,
                "source_version": "scanner:v1:qm9",
                "coverage_state": "available",
                "maturity_state": "not_applicable",
                "pit_state": "verified",
                "integration_mode": "production_existing",
                "payload": {"score": 30.0, "quality_band": "R4"},
            },
            {
                "family": "timing",
                "claim_id": f"timing:TEST:{SNAPSHOT}:5T",
                "as_of": DECISION_TIME,
                "available_from": DECISION_TIME,
                "source_version": "phase1b_frozen_patterns_v1:qm9",
                "coverage_state": "available",
                "maturity_state": "directional_but_immature",
                "pit_state": "verified",
                "integration_mode": "research_only",
                "payload": {
                    "pattern_id": "qm9",
                    "horizon_sessions": 5,
                    "pattern_frozen": True,
                    "match_from_pit_features": True,
                    "direction": "positive",
                },
            },
        ],
    )


def _bundle_set(position_book: dict | None = None) -> dict:
    bundles, _ = build_decision_bundle_set(
        _daily(),
        position_book or _book(),
        [_packet()],
    )
    return bundles


def _watch(position_book: dict | None = None) -> dict:
    watch, _ = build_orchestrated_depot_watch(
        _daily(),
        position_book or _book(),
        [_packet()],
    )
    return watch


def test_ba_qm9_contract_is_exactly_quality_management_only() -> None:
    value = load_contract()
    assert value["status"] == "COMPLETE"
    assert value["engineering_status"] == "COMPLETE"
    assert value["closure_claimed"] is True
    assert value["scope"] == "QUALITY_MANAGEMENT_ONLY"
    assert tuple(value["required_checks"]) == EXPECTED_CHECKS
    assert value["product_logic_changed"] is False
    assert value["investment_logic_changed"] is False
    assert value["execution_enabled"] is False
    assert value["masterplan_guard"][
        "missing_decision_state_may_use_old_scanner_heuristic"
    ] is False


def test_ba_qm9_contract_rejects_old_scanner_heuristic_fallback() -> None:
    changed = deepcopy(load_contract())
    changed["masterplan_guard"][
        "missing_decision_state_may_use_old_scanner_heuristic"
    ] = True
    with pytest.raises(
        BAQM9AuditError,
        match="ba_qm9_masterplan_must_be_false:missing_decision_state_may_use_old_scanner_heuristic",
    ):
        validate_contract(changed)


def test_current_public_boundary_audit_preserves_ba_qm8_and_rejects_stale_runtime() -> None:
    result = audit_current_public_boundaries()
    assert result["status"] == "PASSED_PUBLIC_BOUNDARY_AUDIT"
    assert result["w10_status"] == "sealed"
    assert result["w10_snapshot_id"] == result["snapshot_id"]
    assert result["private_depot_data_loaded"] is False
    assert result["product_logic_changed"] is False
    assert result["investment_logic_changed"] is False
    assert result["execution_enabled"] is False
    assert result["lag1_evidence_impact"] == "PROMOTION_BLOCKED"
    assert result["ba_qm9_may_release_lag1_block"] is False
    assert result["stale_runtime_silently_accepted"] is False
    if result["runtime_snapshot_id"] != result["snapshot_id"]:
        assert result["runtime_transport_accepted"] is False
        assert result["runtime_transport_state"] == "STALE_OR_INVALID_FAIL_CLOSED"
        assert result["operational_finding"]["operational_followup_owner"] == "BA-QM10"


def test_identity_exact_symbol_only_and_no_fuzzy_substitution() -> None:
    watch, _ = build_orchestrated_depot_watch(
        _daily(),
        _book(_position(symbol="test")),
        [_packet()],
    )
    row = watch["rows"][0]
    assert row["symbol"] == "test"
    assert row["availability"] == "symbol_not_in_daily_research"
    assert row["decision"] is None
    assert watch["semantics"]["fuzzy_symbol_matching_used"] is False


def test_identity_snapshot_mismatch_is_unavailable_not_repaired() -> None:
    bundles = _bundle_set()
    changed = deepcopy(bundles)
    changed["bundles"][0]["packet"]["source_snapshot_id"] = "stale-snapshot"
    watch = build_depot_watch(_daily(), _book(), changed)
    row = watch["rows"][0]
    assert row["availability"] == "decision_bundle_snapshot_mismatch"
    assert row["decision"] is None


def test_time_future_position_snapshot_is_rejected() -> None:
    with pytest.raises(DepotWatchError, match="future_position"):
        build_orchestrated_depot_watch(
            _daily(),
            _book(_position(as_of="2026-10-04T18:01:00+00:00")),
            [_packet()],
        )


def test_completeness_duplicate_positions_fail_hard_and_missing_bundle_stays_visible() -> None:
    pos = _position()
    with pytest.raises(DepotWatchError, match="duplicate_position_symbol"):
        build_depot_watch(_daily(), _book(pos, deepcopy(pos)), _bundle_set())

    missing = build_depot_watch(
        _daily(),
        _book(),
        {"schema_version": BUNDLE_SET_SCHEMA_VERSION, "bundles": []},
    )
    assert missing["watch_status"] == "unavailable"
    assert missing["summary"]["position_count"] == 1
    assert missing["summary"]["unavailable_count"] == 1
    assert missing["rows"][0]["availability"] == "decision_bundle_missing"


def test_position_state_comes_only_from_supplied_private_snapshot() -> None:
    position = _position()
    watch = _watch(_book(position))
    row = watch["rows"][0]
    assert row["availability"] == "decision_available"
    assert row["position"]["source_snapshot_id"] == position["source_snapshot_id"]
    assert row["position"]["position_state"] == "long"
    assert row["position"]["quantity"] == 10.0
    assert watch["semantics"]["model_portfolio_used_as_actual_position_source"] is False
    assert watch["semantics"]["legacy_holdings_used_as_actual_position_source"] is False
    assert watch["semantics"]["universal_stance_recomputed"] is False


def test_add_capacity_is_position_only_unknown_or_blocked_and_contradictions_fail() -> None:
    unknown_bundles = _bundle_set(_book(_position()))
    unknown_action = unknown_bundles["bundles"][0]["action"]
    assert unknown_action["position_context"]["add_capacity_state"] == "unknown"

    blocked_bundles = _bundle_set(
        _book(_position(can_add=False, remaining_adds=0))
    )
    blocked_action = blocked_bundles["bundles"][0]["action"]
    assert blocked_action["position_context"]["add_capacity_state"] == "blocked"

    with pytest.raises(PortfolioActionError, match="contradictory_add_capacity"):
        _bundle_set(_book(_position(can_add=False, remaining_adds=2)))


def test_action_semantics_are_preserved_and_never_become_execution() -> None:
    bundles = _bundle_set()
    action = bundles["bundles"][0]["action"]["portfolio_action"]
    watch = build_depot_watch(_daily(), _book(), bundles)
    row = watch["rows"][0]
    assert row["decision"]["portfolio_action_state"] == action["state"]
    assert row["decision"]["portfolio_action_reason_code"] == action["reason_code"]
    assert row["execution_allowed"] is False
    assert watch["semantics"]["universal_stance_recomputed"] is False
    assert watch["semantics"]["transition_recomputed"] is False
    assert watch["semantics"]["portfolio_action_changed"] is False
    assert watch["semantics"]["broker_order_generated"] is False


def test_missing_decision_never_falls_back_to_scanner_scalar() -> None:
    watch = build_depot_watch(
        _daily(),
        _book(),
        {"schema_version": BUNDLE_SET_SCHEMA_VERSION, "bundles": []},
    )
    row = watch["rows"][0]
    assert row["availability"] == "decision_bundle_missing"
    assert row["decision"] is None
    assert row["daily_scanner_context"]["score"] == 30.0
    assert watch["semantics"]["scanner_scalar_replaced_missing_decision_evidence"] is False


def test_stale_as_of_is_unavailable_and_not_silently_refreshed() -> None:
    bundles = _bundle_set()
    changed = deepcopy(bundles)
    changed["bundles"][0]["packet"]["as_of"] = "2026-10-03T18:00:00+00:00"
    watch = build_depot_watch(_daily(), _book(), changed)
    row = watch["rows"][0]
    assert row["availability"] == "decision_bundle_snapshot_mismatch"
    assert row["decision"] is None


def test_invalid_bundle_fails_closed_per_position() -> None:
    bundles = _bundle_set()
    changed = deepcopy(bundles)
    changed["bundles"][0]["action"]["schema_version"] = "invalid"
    watch = build_depot_watch(_daily(), _book(), changed)
    row = watch["rows"][0]
    assert row["availability"] == "decision_bundle_invalid"
    assert row["decision"] is None
    assert row["execution_allowed"] is False


def test_forbidden_execution_field_is_rejected() -> None:
    watch = _watch()
    changed = deepcopy(watch)
    changed["rows"][0]["order_instruction"] = "BUY"
    with pytest.raises(
        DepotWatchError, match="forbidden_execution_or_sizing_fields"
    ):
        validate_depot_watch(changed)


def test_explanation_fields_are_preserved_from_7g() -> None:
    bundles = _bundle_set()
    bundle = bundles["bundles"][0]
    explanation = bundle["explanation"]
    watch = build_depot_watch(_daily(), _book(), bundles)
    decision = watch["rows"][0]["decision"]

    assert decision["explanation_id"] == explanation["explanation_id"]
    assert decision["missing_or_limited_evidence"] == explanation["explanation"][
        "missing_or_limited_evidence"
    ]
    assert decision["decision_change_triggers"] == explanation["change_triggers"][
        "decision_change_triggers"
    ]
    assert decision["numeric_reliability_score"] is None


def test_same_inputs_are_reproducible_and_tampering_breaks_watch_id() -> None:
    first = _watch()
    second = _watch()
    assert first["watch_id"] == second["watch_id"]
    assert first == second

    changed = deepcopy(first)
    changed["summary"]["attention_required_count"] = 999
    with pytest.raises(DepotWatchError, match="watch_id_integrity_failure"):
        validate_depot_watch(changed)


def test_ba_qm9_formal_closure_is_qm_only_and_routes_operations_to_ba_qm10() -> None:
    result = evaluate_ba_qm9_closure()
    assert result["status"] == "BA_QM9_ENGINEERING_COMPLETE"
    assert result["scope"] == "QUALITY_MANAGEMENT_ONLY"
    assert result["required_check_count"] == 11
    assert result["all_required_checks_passed"] is True
    assert result["manipulation_and_regression_tests_passed"] == 55
    assert result["stale_runtime_silently_accepted"] is False
    assert result["product_logic_changed"] is False
    assert result["investment_logic_changed"] is False
    assert result["execution_enabled"] is False
    assert result["empirical_promotion_performed"] is False
    assert result["lag1_evidence_impact"] == "PROMOTION_BLOCKED"
    assert result["ba_qm9_may_release_lag1_block"] is False
    assert result["next_mandatory_work_package"] == "BA-QM10 – Produktions- und Betriebs-QM"
