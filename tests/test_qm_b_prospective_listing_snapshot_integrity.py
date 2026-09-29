from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from scanner.research.governance.qm_b_prospective_listing_snapshots import (
    ProspectiveListingSnapshotError,
    archive_snapshot,
    build_snapshot_envelope,
    parse_nasdaq_symbol_directory,
)


RAW = b"Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares\nAAPL|Apple Inc.|Q|N|N|100|N|N\nFile Creation Time: 0929202617:03|||||||\n"


def make_envelope(retrieved_at: str = "2026-09-29T21:00:00+00:00") -> dict:
    parsed = parse_nasdaq_symbol_directory(RAW, filename="nasdaqlisted.txt")
    envelope = build_snapshot_envelope(
        source_id="nasdaq_symbol_directory",
        source_url="https://example.invalid/nasdaqlisted.txt",
        retrieved_at=retrieved_at,
        raw_payload=RAW,
        parser_result=parsed,
        raw_archive_path="raw/placeholder.bin",
        normalized_archive_path="normalized/placeholder.json",
    )
    envelope["raw_archive_path"] = f"raw/{envelope['snapshot_id']}.bin"
    envelope["normalized_archive_path"] = f"normalized/{envelope['snapshot_id']}.json"
    return envelope


def test_normalized_archive_bytes_match_declared_hash(tmp_path: Path):
    envelope = make_envelope()
    ledger = tmp_path / "ledger.jsonl"
    archive_snapshot(ledger_path=ledger, raw_payload=RAW, envelope=envelope, repository_root=tmp_path)
    normalized = (tmp_path / envelope["normalized_archive_path"]).read_bytes()
    assert hashlib.sha256(normalized).hexdigest() == envelope["normalized_payload_sha256"]


def test_same_payload_at_different_retrieval_time_is_new_snapshot():
    first = make_envelope("2026-09-29T21:00:00+00:00")
    second = make_envelope("2026-09-29T21:01:00+00:00")
    assert first["raw_payload_sha256"] == second["raw_payload_sha256"]
    assert first["snapshot_id"] != second["snapshot_id"]
    assert first["valid_from"] != second["valid_from"]


def test_valid_from_cannot_be_manually_backdated(tmp_path: Path):
    envelope = make_envelope()
    envelope["valid_from"] = "2026-02-01T00:00:00+00:00"
    with pytest.raises(ProspectiveListingSnapshotError, match="valid_from_must_equal_retrieved_at"):
        archive_snapshot(
            ledger_path=tmp_path / "ledger.jsonl",
            raw_payload=RAW,
            envelope=envelope,
            repository_root=tmp_path,
        )


def test_archive_paths_cannot_escape_repository_root(tmp_path: Path):
    envelope = make_envelope()
    envelope["raw_archive_path"] = "../escape.bin"
    with pytest.raises(ProspectiveListingSnapshotError, match="archive_path_outside_root"):
        archive_snapshot(
            ledger_path=tmp_path / "ledger.jsonl",
            raw_payload=RAW,
            envelope=envelope,
            repository_root=tmp_path,
        )


def test_existing_lock_fails_closed_before_append(tmp_path: Path):
    envelope = make_envelope()
    ledger = tmp_path / "ledger.jsonl"
    lock = ledger.with_suffix(ledger.suffix + ".lock")
    lock.write_text("busy", encoding="utf-8")
    with pytest.raises(ProspectiveListingSnapshotError, match="ledger_lock_exists"):
        archive_snapshot(
            ledger_path=ledger,
            raw_payload=RAW,
            envelope=envelope,
            repository_root=tmp_path,
        )
    assert not ledger.exists()
