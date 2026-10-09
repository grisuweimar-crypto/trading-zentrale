from __future__ import annotations

import pandas as pd
import pytest
from copy import deepcopy

from scanner.research.decision_layer.current_evidence import (
    build_current_packet_set_from_frames,
    merge_packet_set_into_archive,
)
from scanner.research.decision_layer.depot_watch import (
    DepotWatchError,
    _canonical_hash,
    seal_depot_watch,
    validate_depot_watch,
)
from scanner.research.decision_layer.depot_watch_orchestrator import (
    _attach_path_reviews,
    build_orchestrated_depot_watch,
)
from scanner.research.decision_layer.evidence_archive import load_evidence_archive
from scanner.research.decision_layer.input_contract import build_input_packet


CURRENT_SNAPSHOT = "snapshot-current"
CURRENT_TIME = "2026-09-29T18:00:00+00:00"


def _daily():
    return {
        "schema_version": "daily_research_v1",
        "snapshot_id": CURRENT_SNAPSHOT,
        "source_snapshot_id": CURRENT_SNAPSHOT,
        "as_of": "2026-09-29",
        "generated_at": CURRENT_TIME,
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


def _history_and_latest():
    rows = []
    for day, score in enumerate(range(10, 21), start=18):
        rows.append({
            "date": f"2026-09-{day:02d}",
            "symbol": "TEST",
            "name": "Test Corp",
            "score": float(score),
            "opportunity": 50.0,
            "risk": 30.0,
            "rs3m": 0.10,
            "trend200": 0.05,
            "cycle": 50.0,
            "r_code": "R3",
            "observation_type": "observed_scanner",
        })
    history = pd.DataFrame(rows)
    latest = pd.DataFrame([{
        "date": "2026-09-29",
        "symbol": "TEST",
        "name": "Test Corp",
        "score": 30.0,
        "opportunity": 52.0,
        "risk": 29.0,
        "rs3m": 0.11,
        "trend200": 0.06,
        "cycle": 51.0,
        "r_code": "R4",
        "observation_type": "observed_scanner",
        "snapshot_id": CURRENT_SNAPSHOT,
        "generated_at": "2026-09-29T17:59:00+00:00",
        "scoring_version": "v1",
        "schema_version": "research_views_v1",
    }])
    return history, latest


def _timing_catalog():
    return {
        "schema_version": "phase1b_frozen_patterns_v1",
        "horizons": {
            "5": {
                "frozen_patterns": [{
                    "pattern": "score_d1_up",
                    "conditions": ["score_d1_up"],
                    "discovery_direction": "positive",
                }]
            },
            "20": {"frozen_patterns": []},
            "40": {"frozen_patterns": []},
            "60": {"frozen_patterns": []},
        },
    }


def test_current_packet_builder_preserves_selection_without_inventing_direction():
    history, latest = _history_and_latest()
    packet_set = build_current_packet_set_from_frames(
        daily=_daily(),
        latest=latest,
        history=history,
        timing_catalog=_timing_catalog(),
    )
    assert packet_set["snapshot_id"] == CURRENT_SNAPSHOT
    assert packet_set["as_of"] == CURRENT_TIME
    assert packet_set["packet_count"] == 1
    packet = packet_set["packets"][0]
    selection = next(row for row in packet["evidence"] if row["family"] == "selection")
    timing = next(row for row in packet["evidence"] if row["family"] == "timing")
    assert "direction" not in selection["payload"]
    assert selection["integration_mode"] == "production_existing"
    assert timing["payload"]["direction"] == "positive"
    assert timing["payload"]["pattern_frozen"] is True
    assert timing["payload"]["match_from_pit_features"] is True
    assert timing["integration_mode"] == "research_only"
    assert packet_set["semantics"]["external_evidence_phase8_activated"] is False


def test_current_packet_archive_merge_is_idempotent(tmp_path):
    history, latest = _history_and_latest()
    packet_set = build_current_packet_set_from_frames(
        daily=_daily(), latest=latest, history=history, timing_catalog=_timing_catalog()
    )
    archive = tmp_path / "decision_evidence_7a.jsonl"
    first = merge_packet_set_into_archive(archive, packet_set)
    second = merge_packet_set_into_archive(archive, packet_set)
    packets, metadata = load_evidence_archive(archive)
    assert first["current_packets_added"] == 1
    assert second["current_packets_added"] == 0
    assert second["current_packets_already_present"] == 1
    assert len(packets) == 1
    assert metadata["snapshot_count"] == 1


def _timing_packet(symbol: str, snapshot: str, as_of: str):
    row = {
        "family": "timing",
        "claim_id": f"timing:{symbol}:frozen-positive:5T",
        "as_of": as_of,
        "available_from": as_of,
        "source_version": "phase1b_frozen_patterns_v1:test",
        "coverage_state": "available",
        "maturity_state": "directional_but_immature",
        "pit_state": "verified",
        "integration_mode": "research_only",
        "payload": {
            "pattern_id": "frozen-positive",
            "horizon_sessions": 5,
            "pattern_frozen": True,
            "match_from_pit_features": True,
            "direction": "positive",
        },
    }
    return build_input_packet(
        symbol=symbol,
        as_of=as_of,
        source_snapshot_id=snapshot,
        evidence=[row],
    )


def _position_book():
    return {
        "schema_version": "decision_depot_position_book_v1",
        "source_snapshot_id": "private-portfolio-1",
        "as_of": "2026-09-29T17:58:30+00:00",
        "positions": [{
            "schema_version": "decision_position_snapshot_v1",
            "symbol": "TEST",
            "source_snapshot_id": "private-position-1",
            "as_of": "2026-09-29T17:58:00+00:00",
            "position_state": "long",
            "quantity": 10,
            "currency": "USD",
            "average_entry_price": 90.0,
            "current_price": 100.0,
        }],
    }


def test_orchestrator_uses_real_distinct_snapshot_history_for_hysteresis():
    packets = [
        _timing_packet("TEST", "snapshot-old", "2026-09-28T18:00:00+00:00"),
        _timing_packet("TEST", CURRENT_SNAPSHOT, CURRENT_TIME),
    ]
    watch, diagnostics = build_orchestrated_depot_watch(_daily(), _position_book(), packets)
    assert watch["watch_status"] == "complete"
    row = watch["rows"][0]
    assert row["availability"] == "decision_available"
    assert row["decision"]["universal_stance_state"] == "positive"
    assert row["decision"]["transition_status"] == "bootstrap_confirmed"
    assert row["decision"]["portfolio_action_state"] == "HOLD"
    assert diagnostics["bundle_count"] == 1
    assert diagnostics["phase8_external_evidence_activated"] is False
    assert diagnostics["scanner_scalar_fallback_used"] is False


def test_orchestrator_does_not_fake_second_hysteresis_observation():
    packets = [_timing_packet("TEST", CURRENT_SNAPSHOT, CURRENT_TIME)]
    watch, _ = build_orchestrated_depot_watch(_daily(), _position_book(), packets)
    row = watch["rows"][0]
    assert row["decision"]["transition_status"] == "bootstrap_pending"
    assert row["decision"]["portfolio_action_state"] == "WAIT_CONFIRMATION"


def test_missing_current_packet_remains_unavailable_not_scanner_fallback():
    watch, diagnostics = build_orchestrated_depot_watch(_daily(), _position_book(), [])
    row = watch["rows"][0]
    assert watch["watch_status"] == "unavailable"
    assert row["availability"] == "decision_bundle_missing"
    assert row["decision"] is None
    assert row["daily_scanner_context"]["score"] == 30.0
    assert diagnostics["missing_current_packet_symbols"] == ["TEST"]
    assert diagnostics["scanner_scalar_fallback_used"] is False


def test_active_overextension_monitor_is_surfaced_as_attention_without_changing_hold():
    watch = {
        "rows": [{
            "symbol": "ACB.TO",
            "attention_required": False,
            "decision": {
                "portfolio_action_state": "HOLD",
            },
        }],
    }
    bundle_set = {
        "bundles": [{
            "packet": {"symbol": "ACB.TO"},
            "path_review": {
                "review_state": "monitor",
                "sequence_state": "active_overextension",
            },
        }],
    }

    result = _attach_path_reviews(watch, bundle_set)
    row = result["rows"][0]
    assert row["attention_required"] is True
    assert row["decision"]["portfolio_action_state"] == "HOLD"
    assert row["decision"]["path_review_state"] == "monitor"
    assert row["decision"]["path_sequence_state"] == "active_overextension"
    assert row["decision"]["path_review_is_trade_decision"] is False


def test_post_overextension_monitor_memory_does_not_force_attention():
    watch = {
        "rows": [{
            "symbol": "TEST",
            "attention_required": False,
            "decision": {
                "portfolio_action_state": "HOLD",
            },
        }],
    }
    bundle_set = {
        "bundles": [{
            "packet": {"symbol": "TEST"},
            "path_review": {
                "review_state": "monitor",
                "sequence_state": "post_overextension_memory",
            },
        }],
    }

    result = _attach_path_reviews(watch, bundle_set)
    assert result["rows"][0]["attention_required"] is False


def test_fully_orchestrated_watch_is_resealed_after_review_contexts():
    packets = [
        _timing_packet("TEST", "snapshot-old", "2026-09-28T18:00:00+00:00"),
        _timing_packet("TEST", CURRENT_SNAPSHOT, CURRENT_TIME),
    ]
    watch, diagnostics = build_orchestrated_depot_watch(
        _daily(), _position_book(), packets,
    )
    assert diagnostics["bundle_count"] == 1
    assert validate_depot_watch(watch) == watch
    assert seal_depot_watch(watch) == watch  # deterministic, idempotent
    assert watch["rows"][0]["decision"]["portfolio_action_state"] == "HOLD"
    assert watch["summary"]["attention_required_count"] == sum(
        row["attention_required"] for row in watch["rows"]
    )
    assert watch["validation"]["execution_allowed"] is False
    assert watch["validation"]["promotion_eligible"] is False


def test_path_review_monitor_in_final_watch_updates_attention_and_integrity(monkeypatch):
    # Keep the real complete orchestration; inject only a synthetic path
    # review payload at the already-existing research-only adapter boundary.
    monkeypatch.setattr(
        "scanner.research.decision_layer.depot_watch_orchestrator._path_review",
        lambda packet: {
            "review_state": "monitor",
            "sequence_state": "active_overextension",
        },
    )
    packets = [
        _timing_packet("TEST", "snapshot-old", "2026-09-28T18:00:00+00:00"),
        _timing_packet("TEST", CURRENT_SNAPSHOT, CURRENT_TIME),
    ]
    watch, _ = build_orchestrated_depot_watch(_daily(), _position_book(), packets)
    row = watch["rows"][0]
    assert row["decision"]["portfolio_action_state"] == "HOLD"
    assert row["attention_required"] is True
    assert row["decision"]["path_review_state"] == "monitor"
    assert row["path_review"]["sequence_state"] == "active_overextension"
    assert watch["summary"]["attention_required_count"] == 1
    assert validate_depot_watch(watch) == watch


def test_watch_integrity_rejects_post_seal_mutation_and_stale_attention_summary():
    packets = [
        _timing_packet("TEST", "snapshot-old", "2026-09-28T18:00:00+00:00"),
        _timing_packet("TEST", CURRENT_SNAPSHOT, CURRENT_TIME),
    ]
    watch, _ = build_orchestrated_depot_watch(_daily(), _position_book(), packets)
    assert validate_depot_watch(watch) == watch
    tampered = deepcopy(watch)
    tampered["rows"][0]["decision"]["path_review_state"] = "arbitrary"
    with pytest.raises(DepotWatchError, match="watch_id_integrity_failure"):
        validate_depot_watch(tampered)

    stale = deepcopy(watch)
    stale["rows"][0]["attention_required"] = not stale["rows"][0]["attention_required"]
    # Even a formally recomputed hash must not hide a stale count.
    unsigned = deepcopy(stale)
    unsigned.pop("watch_id")
    stale["watch_id"] = _canonical_hash(unsigned)
    with pytest.raises(DepotWatchError, match="watch_attention_required_count_mismatch"):
        validate_depot_watch(stale)

    repaired = seal_depot_watch(stale)
    assert validate_depot_watch(repaired) == repaired
    assert repaired["summary"]["attention_required_count"] == 1


def test_watch_final_seal_does_not_bypass_forbidden_execution_or_action_controls():
    packets = [
        _timing_packet("TEST", "snapshot-old", "2026-09-28T18:00:00+00:00"),
        _timing_packet("TEST", CURRENT_SNAPSHOT, CURRENT_TIME),
    ]
    watch, _ = build_orchestrated_depot_watch(_daily(), _position_book(), packets)
    forbidden = deepcopy(watch)
    forbidden["validation"]["execution_allowed"] = True
    with pytest.raises(DepotWatchError, match="watch_execution_must_remain_disabled"):
        seal_depot_watch(forbidden)

    changed_action = deepcopy(watch)
    changed_action["rows"][0]["decision"]["portfolio_action_state"] = "ADD_REVIEW"
    with pytest.raises(DepotWatchError, match="action_presentation_group_mismatch"):
        seal_depot_watch(changed_action)
