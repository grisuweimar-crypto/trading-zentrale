"""CY-02 contemporaneous (not provider-certified) fetch evidence regressions."""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

from scanner.reports.cycle_provider_receipt import (
    CycleProviderReceiptError,
    make_fetch_receipt, persist_current_fetch_receipt, record_snapshot_receipt,
    verify_fetch_receipt,
)


BARS = b"immutable canonical gzip evidence fixture"
START = "2026-10-12T15:01:00+00:00"
FINISH = "2026-10-12T15:01:11+00:00"
FRAME_HASH = "a" * 64


def receipt(**updates):
    params = dict(started_utc=START, finished_utc=FINISH,
                  frame_sha256=FRAME_HASH, frame_rows=120, input_bars_gzip=BARS)
    params.update(updates)
    return make_fetch_receipt(**params)


def test_receipt_hash_and_claims_are_strictly_unverified():
    doc = receipt()
    verify_fetch_receipt(doc, bars=BARS)
    assert doc["clock_authority"] == "CLIENT_RUNNER_UTC_UNATTESTED"
    assert doc["research_released"] is False
    assert doc["provider_signed_bar_availability"] is False
    assert doc["input_bars_gzip_sha256"] == sha256(BARS).hexdigest()


def test_fetch_window_fails_closed_on_reversal_and_timezone():
    with pytest.raises(CycleProviderReceiptError, match="reversed"):
        receipt(started_utc=FINISH, finished_utc=START)
    with pytest.raises(CycleProviderReceiptError, match="bad_utc_timestamp"):
        receipt(started_utc="2026-10-12T15:01:00")
    with pytest.raises(CycleProviderReceiptError, match="bad_utc_timestamp"):
        receipt(finished_utc="2026-10-12T17:01:00+02:00")


def test_hash_mismatch_and_faked_authority_rejected():
    doc = receipt()
    with pytest.raises(CycleProviderReceiptError, match="bars_digest_mismatch"):
        verify_fetch_receipt(doc, bars=b"other bars")
    doc["research_released"] = True
    with pytest.raises(CycleProviderReceiptError, match="untrusted_provider"):
        verify_fetch_receipt(doc, bars=BARS)
    doc = receipt()
    doc["provider_frame_sha256"] = "b" * 64
    with pytest.raises(CycleProviderReceiptError, match="digest_mismatch"):
        verify_fetch_receipt(doc, bars=BARS)


def test_provider_disabled_removes_preexisting_stale_receipt(tmp_path):
    from scanner.reports.cycle_provider_receipt import REPORT
    target = tmp_path / REPORT
    target.parent.mkdir(parents=True)
    target.write_text("stale receipt")
    assert persist_current_fetch_receipt(None, bars=BARS, root=tmp_path) is None
    assert not target.exists()


def test_persist_capture_from_actual_report(tmp_path):
    from scanner.reports.cycle_provider_receipt import REPORT
    report = SimpleNamespace(enabled=True, provider_fetch_started_at_utc=START,
                             provider_fetch_completed_at_utc=FINISH,
                             provider_frame_sha256=FRAME_HASH, provider_frame_rows=120)
    doc = persist_current_fetch_receipt(report, bars=BARS, root=tmp_path)
    assert doc is not None
    assert json.loads((tmp_path / REPORT).read_text())["receipt_sha256"] == doc["receipt_sha256"]


def _snapshot(tmp_path, *, finished=FINISH, created="2026-10-12T15:15:00Z", valid=215):
    from scanner.reports.cycle_provider_receipt import REPORT
    sid = "11111111-2222-4333-8444-555555555555"
    report = SimpleNamespace(enabled=True, provider_fetch_started_at_utc=START,
                             provider_fetch_completed_at_utc=finished,
                             provider_frame_sha256=FRAME_HASH, provider_frame_rows=120)
    persist_current_fetch_receipt(report, bars=BARS, root=tmp_path)
    archived = tmp_path / "artifacts/cycle_history/bars" / (sid + ".csv.gz")
    archived.parent.mkdir(parents=True, exist_ok=True)
    archived.write_bytes(BARS)
    manifest = {
        "latest_snapshot_id": sid, "as_of": "2026-10-12", "run_id": "github-01",
        "archive_bars_sha256": sha256(BARS).hexdigest(), "new_current_valid": valid,
    }
    metadata = {
        "snapshot_id": sid, "latest_run_complete": True,
        "validation": {"status": "ok"}, "generated_at": created,
    }
    for path, payload in [
        ("artifacts/cycle_history/manifest.json", manifest),
        ("artifacts/research/history_metadata.json", metadata),
    ]:
        target = tmp_path / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payload))
    return sid


def test_snapshot_receipt_is_identity_bound_immutable_and_idempotent(tmp_path):
    sid = _snapshot(tmp_path)
    first = record_snapshot_receipt(tmp_path)
    second = record_snapshot_receipt(tmp_path)
    assert first == second
    assert first["status"] == "CAPTURED_UNVERIFIED_PROVIDER_PIT"
    path = tmp_path / "artifacts/cycle_history/provider_receipts" / (sid + ".json")
    original = path.read_bytes()
    assert json.loads(original)["research_released"] is False
    path.write_bytes(original + b"CORRUPTION")
    with pytest.raises(CycleProviderReceiptError, match="immutable_snapshot"):
        record_snapshot_receipt(tmp_path)


def test_snapshot_receipt_rejects_future_stamped_provider(tmp_path):
    _snapshot(tmp_path, created="2026-10-12T15:00:00Z")
    with pytest.raises(CycleProviderReceiptError, match="after_snapshot_publication"):
        record_snapshot_receipt(tmp_path)


def test_snapshot_receipt_requires_provider_if_valid_cycle(tmp_path):
    from scanner.reports.cycle_provider_receipt import REPORT
    _snapshot(tmp_path)
    (tmp_path / REPORT).unlink()
    with pytest.raises(CycleProviderReceiptError, match="valid_bars_without_download"):
        record_snapshot_receipt(tmp_path)


def test_yahoo_download_gets_real_clock_boundaries(monkeypatch):
    import scanner.data.enrich.yahoo_prices as yahoo
    monkeypatch.setitem(sys.modules, "yfinance", SimpleNamespace(download=lambda **_: pd.DataFrame()))
    table = pd.DataFrame({"Yahoo": ["SPY"], "Währung": ["USD"]})
    _, report = yahoo.enrich_watchlist_with_yahoo(table, enabled=True)
    start = datetime.fromisoformat(report.provider_fetch_started_at_utc)
    finish = datetime.fromisoformat(report.provider_fetch_completed_at_utc)
    assert start.tzinfo == timezone.utc
    assert start <= finish
    assert report.provider_frame_sha256 and len(report.provider_frame_sha256) == 64
    assert "NOT PROVIDER-SIGNED OR PIT CERTIFIED" in report.to_text()


def test_workflow_stages_receipt_only_after_cycle_history():
    from pathlib import Path
    text = (Path(__file__).resolve().parents[1] / ".github/workflows/run_scanner.yml").read_text(encoding="utf-8")
    assert "python scripts/record_cycle_history.py" in text
    assert "python scripts/record_cycle_provider_receipt.py" in text
    assert text.index("python scripts/record_cycle_history.py") < text.index("python scripts/record_cycle_provider_receipt.py")
