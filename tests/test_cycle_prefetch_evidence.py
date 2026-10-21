"""Future-only CY-02/CY-03 cache prefetch witnesses; NEVER source/PIT approval."""
from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json

import pandas as pd
import pytest

from scanner.reports.cycle_prefetch_evidence import (
    ARCHIVE_DIR, MARKET_CACHE, REPORT, attempt_record, archive_current_witness,
    canonical, digest, inspect_archived_witnesses, verify_witness, write_current_witness,
)


SID = "11111111-2222-4333-8444-555555555555"
START = "2026-10-21T15:01:00+00:00"
FINISH = "2026-10-21T15:01:11+00:00"
GENERATED = "2026-10-21T15:15:00+00:00"
RUN = "github-999-1"


def fixture(tmp_path, monkeypatch):
    monkeypatch.setenv("GITHUB_RUN_ID", "999")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    path = tmp_path / MARKET_CACHE
    path.parent.mkdir(parents=True)
    path.write_bytes(b"canonical closed OHLCV cache")
    frame = pd.DataFrame([{
        "date": "2026-10-20", "symbol": "SPY", "open": "1", "high": "2",
        "low": "0.5", "close": "1.5", "adj_close": "1.25", "volume": "100",
    }])
    attempt = attempt_record(
        requested=["SPY"], request={"start": "2026-10-15", "period": None, "end": "2026-10-21"},
        started=START, finished=FINISH, frame=frame)
    witness = write_current_witness(tmp_path, [attempt], cache=path.read_bytes())
    bars = b"immutable per-snapshot gzip bytes"
    barsha = sha256(bars).hexdigest()
    bar_path = tmp_path / "artifacts/cycle_history/bars" / (SID + ".csv.gz")
    bar_path.parent.mkdir(parents=True)
    bar_path.write_bytes(bars)
    records = [dict(snapshot_id=SID, as_of="2026-10-21", run_id=RUN,
                    bars_sha256=barsha, generated_at=GENERATED)]
    documents = {
        "artifacts/cycle_history/manifest.json": {
            "latest_snapshot_id": SID, "as_of": "2026-10-21",
            "run_id": RUN, "archive_bars_sha256": barsha,
        },
        "artifacts/research/history_metadata.json": {
            "snapshot_id": SID, "as_of": "2026-10-21", "generated_at": GENERATED,
            "daily_run": {"run_id": RUN}, "latest_run_complete": True,
            "validation": {"status": "ok"},
        },
    }
    for name, doc in documents.items():
        p = tmp_path / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(doc))
    return witness, records


def test_real_prefetch_fetch_batch_captures_request_before_after(monkeypatch):
    from scripts import prefetch_market_history as p
    frame = pd.DataFrame([{"date": "2026-10-20", "symbol": "SPY", "close": "10"}])
    monkeypatch.setattr(p, "_download", lambda *args, **kwargs: frame)
    attempts = []
    rows, errors = p.fetch_batch(["SPY"], {"period": "2y", "start": None, "end": "2026-10-21"},
                                 retries=0, sleep=lambda _: None, witness_attempts=attempts)
    assert len(rows) == 1 and errors == {}
    assert len(attempts) == 1
    assert datetime.fromisoformat(attempts[0]["started_at_utc"]) <= datetime.fromisoformat(
        attempts[0]["finished_at_utc"])
    assert attempts[0]["frame_sha256"] == digest(canonical(attempts[0]["frame_rows"]))


def test_archive_is_single_run_bound_and_stays_unverified(tmp_path, monkeypatch):
    witness, ledger = fixture(tmp_path, monkeypatch)
    assert witness["publisher_bar_availability_verified"] is False
    assert witness["research_released"] is False
    first = archive_current_witness(tmp_path)
    assert first == archive_current_witness(tmp_path)
    assert first["status"] == "CLIENT_CAPTURE_ONLY_NO_PUBLISHER_PIT"
    audit = inspect_archived_witnesses(tmp_path, ledger)
    assert audit["witness_snapshots"] == 1
    assert audit["research_released"] is False
    assert audit["independent_publisher_pit_verified"] is False


def test_mutation_of_response_or_forged_pit_is_rejected(tmp_path, monkeypatch):
    witness, ledger = fixture(tmp_path, monkeypatch)
    witness["attempts"][0]["frame_rows"][0]["close"] = "999"
    with pytest.raises(ValueError, match="digest_mismatch"):
        verify_witness(witness)
    doc = json.loads((tmp_path / REPORT).read_text())
    doc["publisher_bar_availability_verified"] = True
    with pytest.raises(ValueError, match="digest_mismatch|false_authority"):
        verify_witness(doc)


def test_changed_cache_and_stale_run_cannot_bind(tmp_path, monkeypatch):
    _, _ = fixture(tmp_path, monkeypatch)
    (tmp_path / MARKET_CACHE).write_bytes(b"tampered")
    with pytest.raises(ValueError, match="cache_changed"):
        archive_current_witness(tmp_path)
    (tmp_path / MARKET_CACHE).write_bytes(b"canonical closed OHLCV cache")
    meta = tmp_path / "artifacts/research/history_metadata.json"
    doc = json.loads(meta.read_text())
    doc["daily_run"]["run_id"] = "github-1000-1"
    meta.write_text(json.dumps(doc))
    with pytest.raises(ValueError, match="identity_mismatch"):
        archive_current_witness(tmp_path)


def test_past_snapshots_without_proof_remain_excluded(tmp_path, monkeypatch):
    _, ledger = fixture(tmp_path, monkeypatch)
    audit = inspect_archived_witnesses(tmp_path, ledger)
    assert audit["witness_snapshots"] == 0
    assert audit["snapshots_without_prefetch_witness"] == 1
    assert not (tmp_path / ARCHIVE_DIR).exists()


def test_immutable_archived_payload_tampering_is_rejected(tmp_path, monkeypatch):
    _, ledger = fixture(tmp_path, monkeypatch)
    archive_current_witness(tmp_path)
    path = tmp_path / ARCHIVE_DIR / (SID + ".json")
    data = json.loads(path.read_text())
    data["research_released"] = True
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="digest_mismatch"):
        inspect_archived_witnesses(tmp_path, ledger)


def test_future_clock_and_wrong_execution_rejected(tmp_path, monkeypatch):
    _, _ = fixture(tmp_path, monkeypatch)
    path = tmp_path / REPORT
    doc = json.loads(path.read_text())
    doc["execution_id"] = "github-other-1"
    doc["witness_sha256"] = digest(canonical({k: v for k, v in doc.items()
                                               if k != "witness_sha256"}))
    path.write_text(json.dumps(doc))
    with pytest.raises(ValueError, match="identity_mismatch"):
        archive_current_witness(tmp_path)
