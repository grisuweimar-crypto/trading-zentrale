from __future__ import annotations

import json
from pathlib import Path

import pytest

from scanner.research.governance.qm_b_prospective_listing_snapshots import (
    ProspectiveListingSnapshotError,
    archive_snapshot,
    build_snapshot_envelope,
    load_snapshot_contract,
    parse_nasdaq_symbol_directory,
    verify_snapshot_ledger,
)


NASDAQ_LISTED = b"Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares\nAAPL|Apple Inc.|Q|N|N|100|N|N\nFile Creation Time: 0929202617:03|||||||\n"
OTHER_LISTED = b"ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol\nIBM|International Business Machines|N|IBM|N|100|N|IBM\nFile Creation Time: 0929202617:04|||||||\n"


def _envelope(raw: bytes, parser_result: dict, *, retrieved_at: str = "2026-09-29T20:55:00+00:00") -> dict:
    snapshot_id_seed = "ignored"
    return build_snapshot_envelope(
        source_id="nasdaq_symbol_directory",
        source_url="https://example.invalid/source.txt",
        retrieved_at=retrieved_at,
        raw_payload=raw,
        parser_result=parser_result,
        raw_archive_path=f"raw/{snapshot_id_seed}.bin",
        normalized_archive_path=f"normalized/{snapshot_id_seed}.json",
    )


def test_contract_keeps_production_disabled_and_retrojection_forbidden():
    contract = load_snapshot_contract()
    assert contract["research_only"] is True
    assert contract["productive_integration_enabled"] is False
    assert contract["time_rules"]["historical_retrojection_permitted"] is False
    assert contract["promotion"]["strict_historical_listing_ledger"] is False
    assert contract["promotion"]["productive_scanner_integration"] is False


def test_nasdaq_listed_parser_preserves_raw_creation_marker_without_timezone_inference():
    parsed = parse_nasdaq_symbol_directory(NASDAQ_LISTED, filename="nasdaqlisted.txt")
    assert parsed["source_generated_at_raw"] == "0929202617:03"
    assert parsed["source_generated_timezone"] is None
    assert parsed["record_count"] == 1
    row = parsed["records"][0]
    assert row["source_symbol"] == "AAPL"
    assert row["venue_namespace"] == "nasdaq_symbol_directory"
    assert row["venue_code"] == "NASDAQ"
    assert row["listing_state"] == "LISTED_IN_SNAPSHOT"
    assert row["listing_start"] is None
    assert row["listing_end"] is None
    assert row["market_tradability"] == "UNKNOWN"
    assert row["project_investability"] == "UNKNOWN"
    assert parsed["inferences"]["file_creation_timezone_inferred"] is False


def test_otherlisted_keeps_source_exchange_namespace_without_mic_inference():
    parsed = parse_nasdaq_symbol_directory(OTHER_LISTED, filename="otherlisted.txt")
    row = parsed["records"][0]
    assert row["source_symbol"] == "IBM"
    assert row["venue_namespace"] == "nasdaq_symbol_directory_exchange_code"
    assert row["venue_code"] == "N"
    assert row["market_tradability"] == "UNKNOWN"
    assert row["project_investability"] == "UNKNOWN"


def test_envelope_valid_from_equals_actual_retrieval_not_source_marker():
    parsed = parse_nasdaq_symbol_directory(NASDAQ_LISTED, filename="nasdaqlisted.txt")
    envelope = _envelope(NASDAQ_LISTED, parsed, retrieved_at="2026-09-30T01:15:00+02:00")
    assert envelope["retrieved_at"] == "2026-09-29T23:15:00+00:00"
    assert envelope["valid_from"] == envelope["retrieved_at"]
    assert envelope["source_generated_at_raw"] == "0929202617:03"
    assert envelope["historical_retrojection_permitted"] is False
    assert envelope["effective_dates_can_move_valid_from_backward"] is False


def test_retrieved_at_without_timezone_fails_closed():
    parsed = parse_nasdaq_symbol_directory(NASDAQ_LISTED, filename="nasdaqlisted.txt")
    with pytest.raises(ProspectiveListingSnapshotError, match="timestamp_timezone_required"):
        _envelope(NASDAQ_LISTED, parsed, retrieved_at="2026-09-29T20:55:00")


def test_snapshot_archive_is_hash_chained_and_duplicate_fails(tmp_path: Path):
    parsed = parse_nasdaq_symbol_directory(NASDAQ_LISTED, filename="nasdaqlisted.txt")
    envelope = _envelope(NASDAQ_LISTED, parsed)
    envelope["raw_archive_path"] = f"raw/{envelope['snapshot_id']}.bin"
    envelope["normalized_archive_path"] = f"normalized/{envelope['snapshot_id']}.json"
    ledger = tmp_path / "ledger.jsonl"

    result = archive_snapshot(
        ledger_path=ledger,
        raw_payload=NASDAQ_LISTED,
        envelope=envelope,
        repository_root=tmp_path,
    )
    assert result["event_count"] == 1
    assert result["historical_retrojection_permitted"] is False
    assert (tmp_path / envelope["raw_archive_path"]).read_bytes() == NASDAQ_LISTED
    state = verify_snapshot_ledger(ledger)
    assert state["valid"] is True
    assert state["snapshot_ids"] == [envelope["snapshot_id"]]

    with pytest.raises(ProspectiveListingSnapshotError, match="duplicate_snapshot_identity"):
        archive_snapshot(
            ledger_path=ledger,
            raw_payload=NASDAQ_LISTED,
            envelope=envelope,
            repository_root=tmp_path,
        )


def test_ledger_tamper_is_detected(tmp_path: Path):
    parsed = parse_nasdaq_symbol_directory(NASDAQ_LISTED, filename="nasdaqlisted.txt")
    envelope = _envelope(NASDAQ_LISTED, parsed)
    envelope["raw_archive_path"] = f"raw/{envelope['snapshot_id']}.bin"
    envelope["normalized_archive_path"] = f"normalized/{envelope['snapshot_id']}.json"
    ledger = tmp_path / "ledger.jsonl"
    archive_snapshot(ledger_path=ledger, raw_payload=NASDAQ_LISTED, envelope=envelope, repository_root=tmp_path)

    row = json.loads(ledger.read_text(encoding="utf-8"))
    row["record_count"] = 999
    ledger.write_text(json.dumps(row) + "\n", encoding="utf-8")
    with pytest.raises(ProspectiveListingSnapshotError, match="ledger_hash_invalid"):
        verify_snapshot_ledger(ledger)


def test_raw_payload_mismatch_fails_before_ledger_append(tmp_path: Path):
    parsed = parse_nasdaq_symbol_directory(NASDAQ_LISTED, filename="nasdaqlisted.txt")
    envelope = _envelope(NASDAQ_LISTED, parsed)
    envelope["raw_archive_path"] = f"raw/{envelope['snapshot_id']}.bin"
    envelope["normalized_archive_path"] = f"normalized/{envelope['snapshot_id']}.json"
    ledger = tmp_path / "ledger.jsonl"
    with pytest.raises(ProspectiveListingSnapshotError, match="raw_payload_hash_mismatch"):
        archive_snapshot(ledger_path=ledger, raw_payload=b"different", envelope=envelope, repository_root=tmp_path)
    assert not ledger.exists()


def test_unknown_or_missing_listing_fields_are_not_filled_by_parser():
    parsed = parse_nasdaq_symbol_directory(NASDAQ_LISTED, filename="nasdaqlisted.txt")
    row = parsed["records"][0]
    assert row["listing_start"] is None
    assert row["listing_end"] is None
    assert row["market_tradability"] == "UNKNOWN"
    assert row["project_investability"] == "UNKNOWN"
