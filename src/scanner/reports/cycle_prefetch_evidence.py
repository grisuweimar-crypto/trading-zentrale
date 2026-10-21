"""Witness yfinance-normalized daily-bar responses from the pre-scoring prefetch.

These are CLIENT-side UTC observations, NOT signed Yahoo HTTP response bytes,
publisher-bar timestamps, quote-currency verification or PIT research approval.
Only NEW observations may be stamped. Historical v1 remains unchanged.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Any

SCHEMA = "cycle_prefetch_client_witness_v1"
ARCHIVE_SCHEMA = "cycle_prefetch_snapshot_witness_v1"
REPORT = Path("artifacts/reports/cycle_prefetch_client_witness_v1.json")
ARCHIVE_DIR = Path("artifacts/cycle_history/prefetch_receipts")
MARKET_CACHE = Path("artifacts/market_data/yahoo_ohlcv.csv")
COLUMNS = ("date", "symbol", "open", "high", "low", "close", "adj_close", "volume")


def canonical(data: Any) -> bytes:
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
                      allow_nan=False).encode("utf-8")


def digest(data: bytes) -> str:
    return sha256(data).hexdigest()


def stamped(data: dict, field: str) -> dict:
    result = dict(data)
    result[field] = digest(canonical(result))
    return result


def check_stamp(data: dict, field: str) -> None:
    if not isinstance(data, dict) or data.get(field) != digest(canonical(
            {k: v for k, v in data.items() if k != field})):
        raise ValueError("cy_prefetch:evidence_digest_mismatch")


def parse_utc(s: str) -> datetime:
    if not isinstance(s, str) or not s.endswith("+00:00"):
        raise ValueError("cy_prefetch:utc_clock_required")
    try:
        t = datetime.fromisoformat(s)
    except ValueError as exc:
        raise ValueError("cy_prefetch:bad_clock") from exc
    if t.utcoffset() is None or t.utcoffset().total_seconds() != 0:
        raise ValueError("cy_prefetch:utc_clock_required")
    return t


def attempt_record(*, requested: list[str], request: dict, started: str,
                   finished: str, frame: Any, exception_type: str | None = None) -> dict:
    """Store the actual normalized frame rows, not merely an unverifiable hash."""
    if parse_utc(started) > parse_utc(finished):
        raise ValueError("cy_prefetch:reversed_clock")
    values = [] if frame is None else frame.to_dict("records")
    rows = [{col: ("" if value.get(col) is None else str(value.get(col)))
             for col in COLUMNS} for value in values]
    rows.sort(key=lambda x: (x["symbol"], x["date"]))
    return {
        "requested_provider_symbols": list(requested),
        "request": {k: request.get(k) for k in ("period", "start", "end")},
        "started_at_utc": started, "finished_at_utc": finished,
        "exception_type": exception_type,
        "frame_representation": "YFINANCE_NORMALIZED_DAILY_ROWS_NOT_HTTP_RESPONSE",
        "frame_rows": rows, "frame_sha256": digest(canonical(rows)),
    }


def write_current_witness(root: Path, attempts: list[dict], *, cache: bytes) -> dict:
    if not attempts:
        raise ValueError("cy_prefetch:no_actual_download_attempt")
    document = stamped({
        "schema_version": SCHEMA,
        "provider": "YAHOO_FINANCE_VIA_YFINANCE_DOWNLOAD",
        "execution_id": ("github-" + os.environ["GITHUB_RUN_ID"] + "-" +
                         os.environ.get("GITHUB_RUN_ATTEMPT", "1"))
                        if os.environ.get("GITHUB_RUN_ID") else None,
        "clock_authority": "CLIENT_RUNNER_UTC_UNATTESTED",
        "response_scope": "YFINANCE_NORMALIZED_FRAME_NOT_ORIGINAL_HTTP",
        "source_quote_currency_verified": False,
        "publisher_bar_availability_verified": False,
        "exchange_session_independently_verified": False,
        "research_released": False,
        "market_cache_sha256": digest(cache),
        "market_cache_path": str(MARKET_CACHE),
        "attempts": attempts,
    }, "witness_sha256")
    verify_witness(document, cache=cache)
    target = root / REPORT
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(json.dumps(document, indent=2, sort_keys=True, ensure_ascii=False).encode() + b"\n")
    return document


def verify_witness(data: dict, *, cache: bytes | None = None) -> None:
    check_stamp(data, "witness_sha256")
    if (data.get("schema_version") != SCHEMA or
            data.get("provider") != "YAHOO_FINANCE_VIA_YFINANCE_DOWNLOAD" or
            data.get("clock_authority") != "CLIENT_RUNNER_UTC_UNATTESTED" or
            data.get("response_scope") != "YFINANCE_NORMALIZED_FRAME_NOT_ORIGINAL_HTTP" or
            data.get("market_cache_path") != str(MARKET_CACHE) or
            any(data.get(field) is not False for field in (
                "source_quote_currency_verified", "publisher_bar_availability_verified",
                "exchange_session_independently_verified", "research_released"))):
        raise ValueError("cy_prefetch:false_authority_claim")
    attempts = data.get("attempts")
    if not isinstance(attempts, list) or not attempts:
        raise ValueError("cy_prefetch:missing_attempts")
    for item in attempts:
        if parse_utc(item["started_at_utc"]) > parse_utc(item["finished_at_utc"]):
            raise ValueError("cy_prefetch:reversed_clock")
        rows = item.get("frame_rows")
        if (not isinstance(rows, list) or
                any(not isinstance(r, dict) or set(r) != set(COLUMNS) for r in rows) or
                item.get("frame_sha256") != digest(canonical(rows)) or
                item.get("frame_representation") != "YFINANCE_NORMALIZED_DAILY_ROWS_NOT_HTTP_RESPONSE"):
            raise ValueError("cy_prefetch:frame_digest_or_schema_changed")
    if cache is not None and data.get("market_cache_sha256") != digest(cache):
        raise ValueError("cy_prefetch:cache_changed")


def archive_current_witness(root: Path) -> dict:
    """Bind actual pre-scoring response record to ONE newly completed snapshot."""
    root = Path(root)
    report = json.loads((root / REPORT).read_text(encoding="utf-8"))
    cache = (root / MARKET_CACHE).read_bytes()
    verify_witness(report, cache=cache)
    meta = json.loads((root / "artifacts/research/history_metadata.json").read_text())
    manifest = json.loads((root / "artifacts/cycle_history/manifest.json").read_text())
    sid = meta["snapshot_id"]
    if (not meta.get("latest_run_complete") or
            meta.get("validation", {}).get("status") != "ok" or
            sid != manifest.get("latest_snapshot_id") or
            meta.get("as_of") != manifest.get("as_of") or
            meta.get("daily_run", {}).get("run_id") != manifest.get("run_id") or
            report.get("execution_id") != manifest.get("run_id")):
        raise ValueError("cy_prefetch:snapshot_identity_mismatch")
    generated = datetime.fromisoformat(meta["generated_at"].replace("Z", "+00:00"))
    if generated.utcoffset() is None or any(
            parse_utc(a["finished_at_utc"]) > generated
            for a in report["attempts"]):
        raise ValueError("cy_prefetch:fetch_after_snapshot")
    barsha = manifest["archive_bars_sha256"]
    if digest((root / "artifacts/cycle_history/bars" / (sid + ".csv.gz")).read_bytes()) != barsha:
        raise ValueError("cy_prefetch:archived_bars_changed")
    wrapped = stamped({
        "schema_version": ARCHIVE_SCHEMA, "snapshot_id": sid,
        "run_id": manifest["run_id"], "as_of": manifest["as_of"],
        "generated_at": meta["generated_at"], "bars_sha256": barsha,
        "witness": report, "research_released": False,
    }, "archive_sha256")
    target = root / ARCHIVE_DIR / (sid + ".json")
    target.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(wrapped, indent=2, sort_keys=True, ensure_ascii=False).encode() + b"\n"
    if target.exists() and target.read_bytes() != raw:
        raise ValueError("cy_prefetch:immutable_snapshot_witness_changed")
    if not target.exists():
        target.write_bytes(raw)
    return {"snapshot_id": sid, "status": "CLIENT_CAPTURE_ONLY_NO_PUBLISHER_PIT",
            "archive_sha256": wrapped["archive_sha256"]}


def inspect_archived_witnesses(root: Path, ledger: list[dict]) -> dict:
    """Read-only tamper/identity audit; missing older witnesses are NOT recreated."""
    root = Path(root)
    by_sid: dict[str, list[dict]] = {}
    for row in ledger:
        by_sid.setdefault(row["snapshot_id"], []).append(row)
    found: set[str] = set()
    directory = root / ARCHIVE_DIR
    if directory.is_symlink() or (directory.exists() and not directory.is_dir()):
        raise ValueError("cy_prefetch:invalid_archive_directory")
    for path in sorted(directory.glob("*")) if directory.exists() else []:
        if path.is_symlink() or path.suffix != ".json":
            raise ValueError("cy_prefetch:invalid_archive_file")
        doc = json.loads(path.read_text(encoding="utf-8"))
        check_stamp(doc, "archive_sha256")
        sid = doc.get("snapshot_id")
        if (sid not in by_sid or path.name != sid + ".json" or sid in found or
                doc.get("schema_version") != ARCHIVE_SCHEMA or
                doc.get("research_released") is not False):
            raise ValueError("cy_prefetch:archive_identity_or_claim")
        found.add(sid)
        verify_witness(doc["witness"])
        if doc["witness"].get("execution_id") != doc["run_id"]:
            raise ValueError("cy_prefetch:execution_identity_mismatch")
        records = by_sid[sid]
        if (any(row["run_id"] != doc["run_id"] or row["as_of"] != doc["as_of"] or
                row["bars_sha256"] != doc["bars_sha256"] or
                row["generated_at"] != doc["generated_at"] for row in records) or
                digest((root / "artifacts/cycle_history/bars" / (sid + ".csv.gz")).read_bytes())
                != doc["bars_sha256"] or
                any(parse_utc(a["finished_at_utc"]) >
                    datetime.fromisoformat(doc["generated_at"].replace("Z", "+00:00"))
                    for a in doc["witness"]["attempts"])):
            raise ValueError("cy_prefetch:archive_ledger_or_bar_mismatch")
    return {"witness_snapshots": len(found),
            "snapshots_without_prefetch_witness": len(by_sid) - len(found),
            "independent_publisher_pit_verified": False, "research_released": False}
