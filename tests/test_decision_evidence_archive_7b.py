import json
from pathlib import Path

import pytest

from scanner.research.decision_layer.evidence_archive import (
    EvidenceArchiveError,
    append_prospective_packet,
    load_evidence_archive,
    validate_archive_packets,
)


def _selection_packet(as_of="2026-09-26T18:00:00+00:00", snapshot="snap-1"):
    return {
        "schema_version": "decision_layer_input_contract_v1",
        "symbol": "TEST",
        "as_of": as_of,
        "source_snapshot_id": snapshot,
        "evidence": [
            {
                "family": "selection",
                "claim_id": f"selection:TEST:{snapshot}",
                "as_of": as_of,
                "available_from": as_of,
                "source_version": "scanner_vnext",
                "coverage_state": "available",
                "maturity_state": "not_applicable",
                "pit_state": "verified",
                "integration_mode": "production_existing",
                "payload": {
                    "score": 72.0,
                    "score_percentile": 0.93,
                    "quality_band": "B5",
                    "score_status": "OK",
                    "trend_ok": True,
                    "liquidity_ok": True,
                },
            }
        ],
    }


def test_missing_archive_is_explicit_zero_coverage(tmp_path):
    packets, metadata = load_evidence_archive(tmp_path / "missing.jsonl")
    assert packets == []
    assert metadata["status"] == "archive_not_present_yet"
    assert metadata["packet_count"] == 0
    assert metadata["historical_retrojection_permitted"] is False
    assert metadata["evidence_fusion_performed"] is False


def test_prospective_packet_is_validated_and_counted():
    packets, metadata = validate_archive_packets([_selection_packet()])
    assert len(packets) == 1
    assert packets[0]["archive_partition"] == "prospective_unspent"
    assert metadata["partitions"] == {"prospective_unspent": 1}
    assert metadata["families"] == {"selection": 1}
    assert metadata["universal_stance_computed"] is False


def test_duplicate_snapshot_symbol_time_identity_fails_closed():
    packet = _selection_packet()
    with pytest.raises(EvidenceArchiveError, match="duplicate_archive_packet"):
        validate_archive_packets([packet, dict(packet)])


def test_spent_packet_cannot_enter_prospective_archive_by_default(tmp_path):
    old = _selection_packet(as_of="2026-09-25T18:00:00+00:00", snapshot="old")
    with pytest.raises(EvidenceArchiveError, match="spent_packet_cannot_enter_prospective_archive"):
        append_prospective_packet(tmp_path / "archive.jsonl", old)


def test_append_and_reload_preserves_validated_packet(tmp_path):
    path = tmp_path / "archive.jsonl"
    metadata = append_prospective_packet(path, _selection_packet())
    assert metadata["packet_count"] == 1
    packets, loaded = load_evidence_archive(path)
    assert loaded["status"] == "available"
    assert loaded["packet_count"] == 1
    assert packets[0]["source_snapshot_id"] == "snap-1"
    line = json.loads(path.read_text(encoding="utf-8").strip())
    assert line["coverage"]["stance_computed"] is False
    assert line["archive_partition"] == "prospective_unspent"


def test_archive_never_turns_research_packet_into_action():
    packet = _selection_packet()
    packet["portfolio_action"] = "ADD"
    with pytest.raises(Exception, match="forbidden_decision_or_portfolio_fields"):
        validate_archive_packets([packet])
