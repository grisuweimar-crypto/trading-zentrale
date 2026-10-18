"""Negative checks for cached daily-bar provenance."""
from datetime import datetime, timezone
import json
import pytest

from scanner.reports.cycle_cache_provenance import ProvenanceError, canonical_hash, verify_chain


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
