from __future__ import annotations

import json

from scanner.research.decision_layer.depot_watch_orchestrator import build_orchestrated_depot_watch
from scanner.research.decision_layer.input_contract import build_input_packet
from scanner.research.decision_layer.integrated_evidence import PATH_CONTEXT_TYPE
from scanner.research.decision_layer.universal_stance import compute_universal_stance
from scanner.research.decision_layer.watch_runtime import (
    MANIFEST_SCHEMA_VERSION,
    SHARD_IDS,
    SHARD_SCHEMA_VERSION,
    compact_packet,
    load_runtime_packets_for_symbols,
    write_watch_runtime,
)


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
                "current": {"name": "Test Corp", "score": 30.0, "r_code": "R4", "close": 100.0, "currency": "USD"},
                "dynamics": {},
                "persistence": {},
                "classification": {},
                "historical_matches": {},
            }
        },
    }


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


def _timing_packet(snapshot: str, as_of: str, *, with_path: bool = False):
    timing = {
        "family": "timing",
        "claim_id": "timing:TEST:frozen-positive:5T",
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
            "diagnostic_nested_payload_not_used_downstream": {"large": [1, 2, 3]},
        },
    }
    rows = [timing]
    if with_path:
        rows.append({
            "family": "risk",
            "claim_id": "risk:TEST:scanner-path",
            "as_of": as_of,
            "available_from": as_of,
            "source_version": PATH_CONTEXT_TYPE,
            "coverage_state": "available",
            "maturity_state": "not_yet_mature",
            "pit_state": "verified",
            "integration_mode": "research_only",
            "payload": {
                "context_type": PATH_CONTEXT_TYPE,
                "overextension_active": False,
                "last_overextension_date": None,
                "last_overextension_rs3m": None,
                "sessions_since_last_overextension": None,
                "recent_overextension": False,
                "sequence_state": "no_recent_overextension_context",
                "review_state": "none",
                "deterioration": {
                    "score_falling_5t": False,
                    "rank_worsening_5t": False,
                    "rs3m_falling_5t": False,
                    "trend200_falling_5t": False,
                    "r_code_downgrade": False,
                },
                "review_is_trade_decision": False,
                "execution_allowed": False,
            },
        })
    return build_input_packet(
        symbol="TEST",
        as_of=as_of,
        source_snapshot_id=snapshot,
        evidence=rows,
    )


def test_compaction_preserves_7d_and_final_watch_semantics():
    full_packets = [
        _timing_packet("snapshot-old", "2026-09-28T18:00:00+00:00"),
        _timing_packet(CURRENT_SNAPSHOT, CURRENT_TIME, with_path=True),
    ]
    compact_packets = [compact_packet(packet) for packet in full_packets]

    assert "diagnostic_nested_payload_not_used_downstream" in full_packets[0]["evidence"][0]["payload"]
    assert "diagnostic_nested_payload_not_used_downstream" not in compact_packets[0]["evidence"][0]["payload"]
    for full, compact in zip(full_packets, compact_packets, strict=True):
        assert compute_universal_stance(full) == compute_universal_stance(compact)

    full_watch, full_diag = build_orchestrated_depot_watch(_daily(), _position_book(), full_packets)
    compact_watch, compact_diag = build_orchestrated_depot_watch(_daily(), _position_book(), compact_packets)

    assert compact_watch["rows"] == full_watch["rows"]
    assert compact_watch["watch_status"] == full_watch["watch_status"] == "complete"
    assert compact_diag["w8_changed_action_symbols"] == full_diag["w8_changed_action_symbols"]
    assert compact_diag["state_history_counts"] == full_diag["state_history_counts"]


def test_compaction_preserves_complete_scanner_path_context():
    full = _timing_packet(CURRENT_SNAPSHOT, CURRENT_TIME, with_path=True)
    compact = compact_packet(full)
    full_path = next(row for row in full["evidence"] if row["family"] == "risk")["payload"]
    compact_path = next(row for row in compact["evidence"] if row["family"] == "risk")["payload"]
    assert compact_path == full_path


def test_runtime_loader_reads_only_shards_needed_by_private_positions(tmp_path):
    runtime_dir = tmp_path / "watch_runtime"
    packet = compact_packet(_timing_packet(CURRENT_SNAPSHOT, CURRENT_TIME))
    shard_id = SHARD_IDS[0]
    shard_name = f"shard_{shard_id}.json"
    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "snapshot_id": CURRENT_SNAPSHOT,
        "decision_as_of": CURRENT_TIME,
        "w10_status": "sealed",
        "w10_sealed_at": CURRENT_TIME,
        "source_current_7a_sha256": "a" * 64,
        "source_archive_7a_sha256": "b" * 64,
        "symbol_count": 1,
        "packet_count": 1,
        "shard_count": len(SHARD_IDS),
        "symbol_shards": {"TEST": shard_name},
        "private_position_data_included": False,
        "decision_logic_changed": False,
        "runtime_projection_sha256": "c" * 64,
    }
    shards = {
        shard_id: {
            "schema_version": SHARD_SCHEMA_VERSION,
            "snapshot_id": CURRENT_SNAPSHOT,
            "decision_as_of": CURRENT_TIME,
            "shard_id": shard_id,
            "symbols": ["TEST"] if shard_id == SHARD_IDS[0] else [],
            "packet_count": 1 if shard_id == SHARD_IDS[0] else 0,
            "packets": [packet] if shard_id == SHARD_IDS[0] else [],
            "private_position_data_included": False,
        }
        for shard_id in SHARD_IDS
    }
    write_watch_runtime(runtime_dir, manifest=manifest, shards=shards)

    packets, metadata = load_runtime_packets_for_symbols(
        runtime_dir,
        ["TEST", "UNMAPPED:ETF"],
        expected_snapshot_id=CURRENT_SNAPSHOT,
    )
    assert {row["symbol"] for row in packets} == {"TEST"}
    assert metadata["mapped_symbol_count"] == 1
    assert metadata["loaded_shard_count"] == 1
    assert metadata["private_position_data_included"] is False
    assert json.loads((runtime_dir / "manifest.json").read_text())["symbol_count"] == 1


def test_runtime_shard_contract_uses_64_deterministic_buckets() -> None:
    assert len(SHARD_IDS) == 64
    assert len(set(SHARD_IDS)) == 64
    assert SHARD_IDS[0] == "00"
    assert SHARD_IDS[-1] == "3f"
