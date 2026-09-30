from __future__ import annotations

import json
from pathlib import Path

import pytest

from scanner.research.governance.qm_b_durable_membership_capture import (
    DurableMembershipCaptureError,
    capture_universe_membership,
)
from scanner.research.governance.qm_b_prospective_membership import verify_membership_ledger


HEADER = "active,symbol,name,isin,asset_type,country,currency\n"
ROW_A = "1,AAPL,Apple Inc.,US0378331005,stock,United States,USD\n"
ROW_B = "1,MSFT,Microsoft Corp.,US5949181045,stock,United States,USD\n"
COMMIT_A = "a" * 40
COMMIT_B = "b" * 40


def _write(path: Path, body: str) -> bytes:
    raw = body.encode("utf-8")
    path.write_bytes(raw)
    return raw


def _capture(source: Path, root: Path, *, when: str, commit: str, trigger: str = "test"):
    return capture_universe_membership(
        source,
        repo_root=root,
        observed_at=when,
        source_commit_sha=commit,
        trigger=trigger,
    )


def test_first_capture_persists_exact_raw_snapshot_and_ledger(tmp_path: Path) -> None:
    source = tmp_path / "input.csv"
    raw = _write(source, HEADER + ROW_A)
    result = _capture(source, tmp_path, when="2026-09-30T06:00:00+00:00", commit=COMMIT_A)
    assert result["capture_status"] == "ARCHIVED"
    assert result["ledger_event_appended"] is True
    assert result["ledger_event_count"] == 1
    assert result["stable_instrument_claim_count"] == 1

    archive = tmp_path / "artifacts/research/qm/qm_b_membership"
    latest = json.loads((archive / "latest.json").read_text(encoding="utf-8"))
    assert latest["source_commit_sha"] == COMMIT_A
    assert latest["observed_at"] == "2026-09-30T06:00:00+00:00"
    assert latest["historical_retrojection_permitted"] is False
    assert (tmp_path / latest["raw_archive_path"]).read_bytes() == raw
    normalized = json.loads((tmp_path / latest["normalized_snapshot_path"]).read_text(encoding="utf-8"))
    assert normalized["universe_snapshot_id"] == f"git:{COMMIT_A}:data/inputs/universe_master.csv"
    assert normalized["membership_valid_from"] == normalized["universe_observed_at"]
    assert normalized["listing_status_promotion_performed"] is False
    assert normalized["tradability_promotion_performed"] is False
    assert normalized["project_investability_promotion_performed"] is False
    state = verify_membership_ledger(archive / "membership_events.jsonl")
    assert state["valid"] is True
    assert state["event_count"] == 1


def test_identical_content_is_no_change_even_with_later_commit_and_time(tmp_path: Path) -> None:
    source = tmp_path / "input.csv"
    _write(source, HEADER + ROW_A)
    first = _capture(source, tmp_path, when="2026-09-30T06:00:00Z", commit=COMMIT_A)
    second = _capture(source, tmp_path, when="2026-09-30T07:00:00Z", commit=COMMIT_B)
    assert first["capture_status"] == "ARCHIVED"
    assert second["capture_status"] == "NO_CHANGE"
    assert second["reason_code"] == "UNIVERSE_SHA256_ALREADY_ARCHIVED"
    assert second["ledger_event_appended"] is False
    archive = tmp_path / "artifacts/research/qm/qm_b_membership"
    assert verify_membership_ledger(archive / "membership_events.jsonl")["event_count"] == 1
    latest = json.loads((archive / "latest.json").read_text(encoding="utf-8"))
    assert latest["source_commit_sha"] == COMMIT_A


def test_changed_content_appends_second_prospective_state(tmp_path: Path) -> None:
    source = tmp_path / "input.csv"
    _write(source, HEADER + ROW_A)
    first = _capture(source, tmp_path, when="2026-09-30T06:00:00Z", commit=COMMIT_A)
    _write(source, HEADER + ROW_A + ROW_B)
    second = _capture(source, tmp_path, when="2026-09-30T07:00:00Z", commit=COMMIT_B)
    assert first["membership_snapshot_id"] != second["membership_snapshot_id"]
    assert second["capture_status"] == "ARCHIVED"
    assert second["stable_instrument_claim_count"] == 2
    archive = tmp_path / "artifacts/research/qm/qm_b_membership"
    state = verify_membership_ledger(archive / "membership_events.jsonl")
    assert state["event_count"] == 2
    assert len(list((archive / "raw").glob("*.csv"))) == 2
    assert len(list((archive / "snapshots").glob("*.json"))) == 2


def test_invalid_commit_sha_fails_closed(tmp_path: Path) -> None:
    source = tmp_path / "input.csv"
    _write(source, HEADER + ROW_A)
    with pytest.raises(DurableMembershipCaptureError, match="source_commit_sha_invalid"):
        _capture(source, tmp_path, when="2026-09-30T06:00:00Z", commit="not-a-git-sha")


def test_naive_observation_time_fails_closed(tmp_path: Path) -> None:
    source = tmp_path / "input.csv"
    _write(source, HEADER + ROW_A)
    with pytest.raises(DurableMembershipCaptureError, match="timestamp_timezone_required"):
        _capture(source, tmp_path, when="2026-09-30T06:00:00", commit=COMMIT_A)


def test_existing_tampered_ledger_blocks_new_capture(tmp_path: Path) -> None:
    source = tmp_path / "input.csv"
    _write(source, HEADER + ROW_A)
    _capture(source, tmp_path, when="2026-09-30T06:00:00Z", commit=COMMIT_A)
    ledger = tmp_path / "artifacts/research/qm/qm_b_membership/membership_events.jsonl"
    text = ledger.read_text(encoding="utf-8")
    ledger.write_text(text.replace("AAPL", "APPL", 1), encoding="utf-8")
    _write(source, HEADER + ROW_A + ROW_B)
    with pytest.raises(Exception, match="hash_invalid"):
        _capture(source, tmp_path, when="2026-09-30T07:00:00Z", commit=COMMIT_B)
