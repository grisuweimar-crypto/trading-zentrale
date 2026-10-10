"""CY-02/CY-03 client-side provider fetch receipts; not publisher PIT certification.

Only future observations are recorded. No reconstruction of older Yahoo
response times, no retroactive research release, and no source-currency claims.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Mapping

SCHEMA = "cycle_provider_fetch_receipt_v1"
ARCHIVE = "cycle_provider_snapshot_receipt_v1"
HASH_FIELDS = "receipt_sha256"
REPORT = "artifacts/reports/cycle_provider_fetch_receipt_v1.json"


class CycleProviderReceiptError(ValueError):
    pass


def _canon(v: Mapping[str, Any]) -> bytes:
    return json.dumps(dict(v), sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("utf-8")


def _sha(v: bytes) -> str:
    return sha256(v).hexdigest()


def _datetime(v: Any, key: str) -> datetime:
    if not isinstance(v, str):
        raise CycleProviderReceiptError("bad_utc_timestamp:" + key)
    try:
        dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CycleProviderReceiptError("bad_utc_timestamp:" + key) from exc
    if dt.tzinfo is None or dt.utcoffset() is None or dt.utcoffset().total_seconds() != 0:
        raise CycleProviderReceiptError("bad_utc_timestamp:" + key)
    return dt.astimezone(timezone.utc)


def _digest(v: Mapping[str, Any], field: str) -> None:
    copy = dict(v)
    digest = copy.pop(field, None)
    if not isinstance(digest, str) or digest != _sha(_canon(copy)):
        raise CycleProviderReceiptError("receipt_digest_mismatch:" + field)


def _hex(v: Any, name: str) -> str:
    if not isinstance(v, str) or len(v) != 64 or any(ch not in "0123456789abcdef" for ch in v):
        raise CycleProviderReceiptError("invalid_sha256:" + name)
    return v


def make_fetch_receipt(*, started_utc: str, finished_utc: str, frame_sha256: str,
                       frame_rows: int, input_bars_gzip: bytes) -> dict[str, Any]:
    before, after = _datetime(started_utc, "started"), _datetime(finished_utc, "finished")
    if before > after:
        raise CycleProviderReceiptError("fetch_window_reversed")
    if type(frame_rows) is not int or frame_rows < 0:
        raise CycleProviderReceiptError("invalid_frame_rows")
    _hex(frame_sha256, "provider_frame")
    if not input_bars_gzip:
        raise CycleProviderReceiptError("missing_cycle_input_bars")
    doc = {
        "schema_version": SCHEMA,
        "provider": "YAHOO_FINANCE_VIA_YFINANCE_DOWNLOAD",
        "quote_frequency": "1d",
        "auto_adjust": True,
        "clock_authority": "CLIENT_RUNNER_UTC_UNATTESTED",
        "provider_signed_bar_availability": False,
        "original_quote_currency_verified": False,
        "independent_exchange_session_verified": False,
        "research_released": False,
        "started_at_utc": before.isoformat().replace("+00:00", "Z"),
        "finished_at_utc": after.isoformat().replace("+00:00", "Z"),
        "provider_frame_sha256": frame_sha256,
        "provider_frame_rows": frame_rows,
        "input_bars_gzip_sha256": _sha(input_bars_gzip),
    }
    doc[HASH_FIELDS] = _sha(_canon(doc))
    return doc


def verify_fetch_receipt(receipt: Mapping[str, Any], *, bars: bytes) -> None:
    if (receipt.get("schema_version") != SCHEMA or
        receipt.get("provider") != "YAHOO_FINANCE_VIA_YFINANCE_DOWNLOAD" or
        receipt.get("clock_authority") != "CLIENT_RUNNER_UTC_UNATTESTED" or
        receipt.get("research_released") is not False or
        receipt.get("provider_signed_bar_availability") is not False or
        receipt.get("original_quote_currency_verified") is not False or
        receipt.get("independent_exchange_session_verified") is not False or
        receipt.get("quote_frequency") != "1d" or
        receipt.get("auto_adjust") is not True):
        raise CycleProviderReceiptError("untrusted_provider_receipt_claim")
    if _datetime(receipt.get("started_at_utc"), "started") > _datetime(receipt.get("finished_at_utc"), "finished"):
        raise CycleProviderReceiptError("fetch_window_reversed")
    _hex(receipt.get("provider_frame_sha256"), "provider_frame")
    if type(receipt.get("provider_frame_rows")) is not int or receipt["provider_frame_rows"] < 0:
        raise CycleProviderReceiptError("invalid_frame_rows")
    if _hex(receipt.get("input_bars_gzip_sha256"), "input_bars") != _sha(bars):
        raise CycleProviderReceiptError("bars_digest_mismatch")
    _digest(receipt, HASH_FIELDS)


def persist_current_fetch_receipt(report: Any | None, *, bars: bytes, root: Path) -> dict[str, Any] | None:
    """Write once after the *actual* download, or delete stale previous-run report."""
    target = Path(root) / REPORT
    target.unlink(missing_ok=True)  # never reuse old receipt after a failed/offline provider call
    if report is None or not report.enabled:
        return None
    if (report.provider_fetch_started_at_utc is None
            or report.provider_fetch_completed_at_utc is None):
        raise CycleProviderReceiptError("missing_provider_fetch_window")
    receipt = make_fetch_receipt(
        started_utc=report.provider_fetch_started_at_utc,
        finished_utc=report.provider_fetch_completed_at_utc,
        frame_sha256=report.provider_frame_sha256,
        frame_rows=report.provider_frame_rows,
        input_bars_gzip=bars,
    )
    verify_fetch_receipt(receipt, bars=bars)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(json.dumps(receipt, sort_keys=True, indent=2, ensure_ascii=False).encode("utf-8") + b"\n")
    return receipt


def record_snapshot_receipt(root: Path) -> dict[str, Any]:
    """Bind current receipt to a *new* CY-03 snapshot; keep original bytes."""
    root = Path(root)
    manifest_path = root / "artifacts/cycle_history/manifest.json"
    meta_path = root / "artifacts/research/history_metadata.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    if not meta.get("latest_run_complete") or meta.get("validation", {}).get("status") != "ok":
        raise CycleProviderReceiptError("incomplete_scanner_snapshot")
    sid = str(manifest.get("latest_snapshot_id") or "")
    if not sid or sid != meta.get("snapshot_id"):
        raise CycleProviderReceiptError("snapshot_identity_mismatch")
    bars_path = root / "artifacts/cycle_history/bars" / (sid + ".csv.gz")
    bars = bars_path.read_bytes()
    if _sha(bars) != manifest.get("archive_bars_sha256"):
        raise CycleProviderReceiptError("snapshot_archived_bars_mismatch")
    current = root / REPORT
    if not current.is_file() or current.is_symlink():
        if manifest.get("new_current_valid") == 0:
            return {"status": "NO_VALID_CYCLE_BARS", "snapshot_id": sid}
        raise CycleProviderReceiptError("valid_bars_without_download_receipt")
    receipt = json.loads(current.read_text(encoding="utf-8"))
    verify_fetch_receipt(receipt, bars=bars)
    if _datetime(receipt["finished_at_utc"], "finished") > _datetime(meta.get("generated_at"), "generated_at"):
        raise CycleProviderReceiptError("download_finished_after_snapshot_publication")
    doc = {
        "schema_version": ARCHIVE,
        "snapshot_id": sid,
        "scanner_run_id": str(manifest.get("run_id")),
        "scanner_as_of": str(manifest.get("as_of")),
        "observed_source_report": REPORT,
        "fetch_receipt": receipt,
        "historical_pit_certification": False,
        "research_released": False,
        "recorded_from_current_source_only": True,
    }
    doc["snapshot_receipt_sha256"] = _sha(_canon(doc))
    path = root / "artifacts/cycle_history/provider_receipts" / (sid + ".json")
    serialized = json.dumps(doc, sort_keys=True, indent=2, ensure_ascii=False).encode("utf-8") + b"\n"
    if path.exists():
        if path.read_bytes() != serialized:
            raise CycleProviderReceiptError("immutable_snapshot_receipt_changed")
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(serialized)
    return {"status": "CAPTURED_UNVERIFIED_PROVIDER_PIT", "snapshot_id": sid, "snapshot_receipt_sha256": doc["snapshot_receipt_sha256"]}
