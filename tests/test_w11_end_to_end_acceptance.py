from __future__ import annotations

from copy import deepcopy

import pytest

from scanner.research.decision_layer.depot_watch_orchestrator import build_orchestrated_depot_watch
from scanner.research.decision_layer.input_contract import build_input_packet
from scanner.research.decision_layer.w11_end_to_end import (
    W11EndToEndError,
    validate_w11_end_to_end,
)


SNAPSHOT = "snapshot-w11"
DECISION_TIME = "2026-10-01T10:30:00+00:00"


def _daily():
    return {
        "schema_version": "daily_research_v1",
        "snapshot_id": SNAPSHOT,
        "source_snapshot_id": SNAPSHOT,
        "as_of": "2026-10-01",
        "generated_at": DECISION_TIME,
        "universe_size": 1,
        "symbols": {
            "TEST": {
                "current": {
                    "name": "Test Corp",
                    "score": 30.0,
                    "r_code": "R4",
                    "close": 100.0,
                    "currency": "USD",
                }
            }
        },
    }


def _position(symbol: str):
    return {
        "schema_version": "decision_position_snapshot_v1",
        "symbol": symbol,
        "source_snapshot_id": f"private-{symbol}",
        "as_of": "2026-10-01T10:28:00+00:00",
        "position_state": "long",
        "quantity": 10,
        "currency": "USD",
        "average_entry_price": 90.0,
        "current_price": 100.0,
    }


def _position_book(symbol: str = "TEST"):
    return {
        "schema_version": "decision_depot_position_book_v1",
        "source_snapshot_id": "private-w11-book",
        "as_of": "2026-10-01T10:29:00+00:00",
        "positions": [_position(symbol)],
    }


def _position_book_with_unmapped():
    return {
        "schema_version": "decision_depot_position_book_v1",
        "source_snapshot_id": "private-w11-book",
        "as_of": "2026-10-01T10:29:00+00:00",
        "positions": [
            _position("TEST"),
            _position("UNMAPPED:PRIVATE_HOLDING"),
        ],
    }


def _packet():
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
                "source_version": "scanner:v1:research_views_v1",
                "coverage_state": "available",
                "maturity_state": "not_applicable",
                "pit_state": "verified",
                "integration_mode": "production_existing",
                "payload": {"score": 30.0, "quality_band": "R4"},
            },
            {
                "family": "timing",
                "claim_id": "timing:TEST:w11:5T",
                "as_of": DECISION_TIME,
                "available_from": DECISION_TIME,
                "source_version": "phase1b_frozen_patterns_v1:test",
                "coverage_state": "available",
                "maturity_state": "directional_but_immature",
                "pit_state": "verified",
                "integration_mode": "research_only",
                "payload": {
                    "pattern_id": "w11",
                    "horizon_sessions": 5,
                    "pattern_frozen": True,
                    "match_from_pit_features": True,
                    "direction": "positive",
                },
            },
        ],
    )


def _manifest():
    available = "2026-10-01T10:29:00+00:00"
    common = {
        "status": "available",
        "available_from": available,
        "availability_source": "test",
        "snapshot_identity_state": "verified",
    }
    return {
        "schema_version": "decision_snapshot_orchestration_w10_v1",
        "snapshot_id": SNAPSHOT,
        "snapshot_as_of": "2026-10-01",
        "status": "sealed",
        "started_at": available,
        "pre_7a_frozen_at": DECISION_TIME,
        "sealed_at": "2026-10-01T10:31:00+00:00",
        "stages": {
            "scanner_daily_research": deepcopy(common),
            "phase2_probability": deepcopy(common),
            "phase3_risk": deepcopy(common),
            "phase4_confidence": deepcopy(common),
            "phase5_governance": deepcopy(common),
            "phase6_elliott": {
                "status": "not_supplied",
                "resolved_at": DECISION_TIME,
                "availability_source": None,
                "snapshot_identity_state": "not_applicable",
                "decision_effect": "none",
                "missing_is_neutral_evidence": False,
            },
            "final_7a": {
                **deepcopy(common),
                "available_from": DECISION_TIME,
            },
            "phase7a_archive": {
                **deepcopy(common),
                "available_from": "2026-10-01T10:31:00+00:00",
            },
        },
        "guards": {
            "final_7a_requires_all_upstream_stages_resolved": True,
            "runtime_stage_available_from_is_recorded_not_supplied": True,
            "later_evidence_backdating_allowed": False,
            "phase6_missing_is_neutral_evidence": False,
            "private_position_data_persisted": False,
        },
        "downstream_contract": {
            "private_runner": "scripts/run_depot_watch_orchestrated.py",
            "chain": ["7D", "7E", "7F", "7G", "7H"],
            "requires_same_snapshot_id": True,
            "phase6_effect_boundary": "7F_review_context_only",
            "private_position_data_persisted": False,
        },
    }


def _valid_inputs():
    packets = [_packet()]
    watch, diagnostics = build_orchestrated_depot_watch(
        _daily(), _position_book(), packets
    )
    assert watch["watch_status"] == "complete"
    return _manifest(), packets, watch, diagnostics


def test_w11_accepts_complete_same_snapshot_chain():
    manifest, packets, watch, diagnostics = _valid_inputs()
    receipt = validate_w11_end_to_end(
        manifest=manifest,
        archive_packets=packets,
        watch=watch,
        diagnostics=diagnostics,
    )
    assert receipt["status"] == "passed"
    assert receipt["snapshot_id"] == SNAPSHOT
    assert receipt["watch_status"] == "complete"
    assert receipt["expected_unmapped_position_count"] == 0
    assert all(receipt["checks"].values())
    assert receipt["receipt_contains_position_rows"] is False


def test_w11_accepts_partial_watch_when_only_explicit_unmapped_positions_fail_closed():
    packet = _packet()
    watch, diagnostics = build_orchestrated_depot_watch(
        _daily(), _position_book_with_unmapped(), [packet]
    )
    assert watch["watch_status"] == "partial"
    receipt = validate_w11_end_to_end(
        manifest=_manifest(),
        archive_packets=[packet],
        watch=watch,
        diagnostics=diagnostics,
    )
    assert receipt["status"] == "passed"
    assert receipt["watch_status"] == "partial"
    assert receipt["expected_unmapped_position_count"] == 1
    assert receipt["expected_unmapped_symbols"] == ["UNMAPPED:PRIVATE_HOLDING"]
    assert receipt["checks"]["expected_unmapped_positions_fail_closed"] is True
    assert receipt["checks"]["all_mapped_positions_have_decisions"] is True


def test_w11_rejects_unexpected_missing_mapped_position():
    packet = _packet()
    watch, diagnostics = build_orchestrated_depot_watch(
        _daily(), _position_book("MISSING"), [packet]
    )
    with pytest.raises(W11EndToEndError, match="unexpected_unavailable_position"):
        validate_w11_end_to_end(
            manifest=_manifest(),
            archive_packets=[packet],
            watch=watch,
            diagnostics=diagnostics,
        )


def test_w11_rejects_duplicate_current_packet_symbol():
    manifest, packets, watch, diagnostics = _valid_inputs()
    with pytest.raises(W11EndToEndError, match="duplicate_current_packet_symbol"):
        validate_w11_end_to_end(
            manifest=manifest,
            archive_packets=[packets[0], deepcopy(packets[0])],
            watch=watch,
            diagnostics=diagnostics,
        )


def test_w11_rejects_unacceptable_pit_state():
    manifest, packets, watch, diagnostics = _valid_inputs()
    bad = deepcopy(packets[0])
    bad["evidence"][0]["pit_state"] = "unverified"
    with pytest.raises(W11EndToEndError, match="pit_not_acceptable"):
        validate_w11_end_to_end(
            manifest=manifest,
            archive_packets=[bad],
            watch=watch,
            diagnostics=diagnostics,
        )


def test_w11_rejects_phase6_missing_as_neutral():
    manifest, packets, watch, diagnostics = _valid_inputs()
    manifest["stages"]["phase6_elliott"]["missing_is_neutral_evidence"] = True
    with pytest.raises(W11EndToEndError, match="phase6_missing_treated_as_neutral"):
        validate_w11_end_to_end(
            manifest=manifest,
            archive_packets=packets,
            watch=watch,
            diagnostics=diagnostics,
        )


def test_w11_rejects_unauthorized_phase8_effect():
    manifest, packets, watch, diagnostics = _valid_inputs()
    diagnostics["phase8_external_evidence_activated"] = True
    with pytest.raises(W11EndToEndError, match="phase8_external_evidence_activated"):
        validate_w11_end_to_end(
            manifest=manifest,
            archive_packets=packets,
            watch=watch,
            diagnostics=diagnostics,
        )


def test_w11_rejects_super_score():
    manifest, packets, watch, diagnostics = _valid_inputs()
    diagnostics["super_score"] = 99
    with pytest.raises(W11EndToEndError, match="super_score_forbidden"):
        validate_w11_end_to_end(
            manifest=manifest,
            archive_packets=packets,
            watch=watch,
            diagnostics=diagnostics,
        )
