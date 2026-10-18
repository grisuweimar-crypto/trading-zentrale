"""Byte-exact multi-part Elliott shadow archive regression and adversarial tests."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scanner.research.elliott_vnext import shadow_shards as shards


def sample_archive() -> bytes:
    return b"".join(
        (json.dumps({"schema_version": "elliott_vnext_prospective_capture_v1",
                     "run_id": f"github-{i}-1", "event": "x" * (i % 13)},
                    separators=(",", ":")).encode() + (b"\r\n" if i % 3 == 0 else b"\n"))
        for i in range(300)
    )


def test_immutable_lossless_multi_part_roundtrip_and_determinism(tmp_path, monkeypatch):
    monkeypatch.setattr(shards, "SHARD_BYTES", 109)
    raw = sample_archive()
    archive = tmp_path / "history.jsonl"
    manifest = tmp_path / "history.jsonl.manifest.json"
    restored = tmp_path / "restored.jsonl"
    archive.write_bytes(raw)
    first = shards.pack_sharded(archive, manifest)
    assert len(first["parts"]) > 2
    assert first["original_sha256"] == hashlib.sha256(raw).hexdigest()
    assert first["original_bytes"] == len(raw)
    assert all(part["compressed_size"] < shards.MAX_GIT_PAYLOAD_BYTES for part in first["parts"])
    part_hashes = {p["file"]: (tmp_path / p["file"]).read_bytes() for p in first["parts"]}
    original_manifest = manifest.read_bytes()
    receipt = shards.unpack_sharded(manifest, restored)
    assert receipt["roundtrip_verified"] is True
    assert restored.read_bytes() == raw
    assert shards.pack_sharded(archive, manifest) == first
    assert manifest.read_bytes() == original_manifest
    assert {p["file"]: (tmp_path / p["file"]).read_bytes() for p in first["parts"]} == part_hashes


@pytest.mark.parametrize("corruption", ("missing", "truncated", "wrong_blob", "manifest_order", "manifest_size", "missing_part"))
def test_corrupt_or_missing_shard_cannot_replace_existing_archive(tmp_path, monkeypatch, corruption):
    monkeypatch.setattr(shards, "SHARD_BYTES", 160)
    raw = sample_archive()
    archive = tmp_path / "history.jsonl"
    manifest_path = tmp_path / "history.jsonl.manifest.json"
    target = tmp_path / "old-validated.jsonl"
    archive.write_bytes(raw)
    manifest = shards.pack_sharded(archive, manifest_path)
    target.write_bytes(b"old-safe-history-stays-byte-identical")
    part = tmp_path / manifest["parts"][0]["file"]
    if corruption == "missing":
        part.unlink()
    elif corruption == "truncated":
        part.write_bytes(part.read_bytes()[:-5])
    elif corruption == "wrong_blob":
        part.write_bytes(b"x" + part.read_bytes()[1:])
    elif corruption == "manifest_order":
        manifest["parts"][0]["index"] = 900
        manifest_path.write_text(json.dumps(manifest))
    elif corruption == "manifest_size":
        manifest["original_bytes"] += 1
        manifest_path.write_text(json.dumps(manifest))
    else:
        manifest["parts"].append({
            **manifest["parts"][-1],
            "index": len(manifest["parts"]),
            "file": f"history.jsonl.part-{len(manifest['parts']):05d}.gz",
        })
        manifest["original_bytes"] += manifest["parts"][-1]["original_size"]
        manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(shards.ShadowTransportError):
        shards.unpack_sharded(manifest_path, target)
    assert target.read_bytes() == b"old-safe-history-stays-byte-identical"


def test_shard_size_guard_never_lifts_github_payload_limit(tmp_path, monkeypatch):
    monkeypatch.setattr(shards, "SHARD_BYTES", 500)
    monkeypatch.setattr(shards, "MAX_GIT_PAYLOAD_BYTES", 10)
    source = tmp_path / "history.jsonl"
    manifest = tmp_path / "history.jsonl.manifest.json"
    source.write_bytes(b"original-content-" * 100)
    with pytest.raises(shards.ShadowTransportError, match="compressed_shadow_payload_exceeds_review_limit"):
        shards.pack_sharded(source, manifest)
    assert not manifest.exists()


def test_full_reassembled_archive_enforces_append_only_before_publication(tmp_path, monkeypatch):
    from scanner.research.elliott_vnext.shadow_transport import verify_shadow_archive_append_only
    monkeypatch.setattr(shards, "SHARD_BYTES", 99)
    original = tmp_path / "before.jsonl"
    new = tmp_path / "after.jsonl"
    packed = tmp_path / "after.jsonl.manifest.json"
    restored = tmp_path / "restored.jsonl"
    original.write_bytes(sample_archive())
    new.write_bytes(original.read_bytes() + b'{"run_id":"new"}\n')
    shards.pack_sharded(new, packed)
    shards.unpack_sharded(packed, restored)
    result = verify_shadow_archive_append_only(original, restored)
    assert result["append_only_verified"]
    assert result["appended_bytes"] == len(b'{"run_id":"new"}\n')


def test_workflow_has_manifest_first_restore_and_legacy_fallbacks():
    w = (Path(__file__).resolve().parents[1] / ".github/workflows/elliott_vnext_prospective_capture.yml").read_text(encoding="utf-8")
    assert "shadow_shards pack" in w
    assert "shadow_shards unpack" in w
    assert "unpack_sharded(manifest_file, archive_path)" in w
    assert "shadow_transport verify-append" in w
    assert "shadow_transport unpack" in w
    assert "shadow_transport pack" in w
    assert "elliott_vnext_prospective_history_6h.jsonl.manifest.json" in w
    assert "elliott_vnext_prospective_history_6h.jsonl.part-*.gz" in w
    assert "elliott-vnext-shadow-data" in w
    assert "changes_universal_stance" in w
    assert "changes_portfolio_action" in w
