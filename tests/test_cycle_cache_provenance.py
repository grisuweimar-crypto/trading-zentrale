"""Negative checks for cached daily-bar provenance."""
from datetime import datetime, timezone
import json
import pytest

from scanner.reports.cycle_cache_provenance import ProvenanceError, canonical_hash, verify_chain, verify_prefetch_receipt


def write_record(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    data["record_sha256"] = canonical_hash(data)
    path.write_text(json.dumps(data), encoding="utf-8")
    return data["record_sha256"]


def test_missing_chain_fails_closed(tmp_path):
    with pytest.raises(ProvenanceError, match="missing_or_symlink"):
        verify_chain(tmp_path, "snapshot")


def test_tampered_record_fails_closed(tmp_path):
    p = tmp_path / "artifacts/cycle_history/cache_provenance/snapshots/snapshot.json"
    write_record(p, {"snapshot_id": "snapshot"})
    data = json.loads(p.read_text())
    data["snapshot_id"] = "other"
    p.write_text(json.dumps(data))
    with pytest.raises(ProvenanceError, match="invalid_record_hash"):
        verify_chain(tmp_path, "snapshot")


def test_hash_is_key_order_independent():
    assert canonical_hash({"b": 2, "a": 1}) == canonical_hash({"a": 1, "b": 2})


def test_prefetch_cache_receipt_roundtrip_and_tamper(tmp_path):
    import hashlib
    cache = tmp_path / "artifacts/market_data/yahoo_ohlcv.csv"
    cache.parent.mkdir(parents=True)
    cache.write_bytes(b"symbol,date,close\\nSPY,2026-10-16,100\\n")
    receipt = cache.parent / "latest_cache_provenance.json"
    write_record(receipt, {
        "schema_version": "cycle_cache_prefetch_v1",
        "cache_sha256": hashlib.sha256(cache.read_bytes()).hexdigest(),
        "recorded_at_utc": "2026-10-17T10:00:00+00:00",
        "fetch_batches": [], "legacy_cache_rows_unattested": True,
    })
    assert verify_prefetch_receipt(tmp_path)["research_eligible"] is False
    cache.write_bytes(cache.read_bytes() + b"tampered")
    with pytest.raises(ProvenanceError, match="market_cache_modified"):
        verify_prefetch_receipt(tmp_path)
