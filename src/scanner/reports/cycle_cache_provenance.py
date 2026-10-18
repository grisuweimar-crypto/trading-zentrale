"""Fail-closed integrity validation for cached daily-bar acquisition chains.

This module never grants research eligibility. It validates a chain only when
the acquisition and consumption documents are supplied by the actual producers.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path


class ProvenanceError(ValueError):
    """Missing, inconsistent, or unverifiable acquisition evidence."""


def canonical_hash(document: dict) -> str:
    payload = {k: v for k, v in document.items() if k != "record_sha256"}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def _read(path: Path) -> dict:
    if path.is_symlink() or not path.is_file():
        raise ProvenanceError("missing_or_symlink:" + str(path))
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("record_sha256") != canonical_hash(value):
        raise ProvenanceError("invalid_record_hash:" + str(path))
    return value


def _utc(value: str) -> datetime:
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError) as exc:
        raise ProvenanceError("invalid_utc_timestamp") from exc
    if dt.tzinfo is None or dt.utcoffset().total_seconds() != 0:
        raise ProvenanceError("timestamp_not_utc")
    return dt.astimezone(timezone.utc)


def verify_chain(root: Path, snapshot_id: str, *, observed_at: datetime | None = None) -> dict:
    """Verify immutable cache -> scanner -> archive evidence for one snapshot.

    Acquisition records are content-addressed. No timestamps are inferred.
    This proves *client-side* chain integrity, not provider publisher PIT.
    """
    base = root / "artifacts" / "cycle_history" / "cache_provenance"
    consumer = _read(base / "snapshots" / (snapshot_id + ".json"))
    acquisition_hash = consumer.get("acquisition_record_sha256")
    if not isinstance(acquisition_hash, str) or len(acquisition_hash) != 64 or any(c not in "0123456789abcdef" for c in acquisition_hash):
        raise ProvenanceError("invalid_acquisition_reference")
    acquisition = _read(base / "acquisitions" / (acquisition_hash + ".json"))
    if acquisition["record_sha256"] != acquisition_hash:
        raise ProvenanceError("acquisition_reference_mismatch")
    if consumer.get("snapshot_id") != snapshot_id:
        raise ProvenanceError("snapshot_mismatch")
    required = ("provider", "request", "cache_sha256", "bars_sha256", "started_at_utc", "finished_at_utc")
    if any(not acquisition.get(k) for k in required):
        raise ProvenanceError("incomplete_acquisition")
    if consumer.get("cache_sha256") != acquisition["cache_sha256"]:
        raise ProvenanceError("cache_hash_mismatch")
    if consumer.get("bars_sha256") != acquisition["bars_sha256"]:
        raise ProvenanceError("bar_hash_mismatch")
    start, finish = _utc(acquisition["started_at_utc"]), _utc(acquisition["finished_at_utc"])
    consumed = _utc(consumer["consumed_at_utc"])
    archived = _utc(consumer["archived_at_utc"])
    now = (observed_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    if not start <= finish <= consumed <= archived <= now:
        raise ProvenanceError("invalid_temporal_order")
    # The record must also bind to the archived bars independently.
    archived_bars = root / "artifacts" / "cycle_history" / "bars" / (snapshot_id + ".csv.gz")
    if archived_bars.is_symlink() or not archived_bars.is_file():
        raise ProvenanceError("missing_archived_bars")
    actual = hashlib.sha256(archived_bars.read_bytes()).hexdigest()
    if consumer.get("archive_bars_sha256") != actual:
        raise ProvenanceError("archive_bar_hash_mismatch")
    return {"snapshot_id": snapshot_id, "status": "CLIENT_CACHE_CHAIN_VERIFIED_ONLY",
            "acquisition_record_sha256": acquisition_hash, "provider_pit_verified": False,
            "research_eligible": False}
