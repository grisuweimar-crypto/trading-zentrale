"""Transport-only tests: no Elliott predictions or scorer inputs are altered."""
from __future__ import annotations

import gzip
from pathlib import Path

import pytest

import scanner.research.elliott_vnext.shadow_transport as transport


@pytest.mark.parametrize("suffix,body", [
    ("history.jsonl", b'{"capture_id":"a","value":[1,2]}\n{"capture_id":"b","value":[3]}\n'),
    ("current.json", b'{"capture_id":"b","outputs":[{"scenario":"W1"}]}\n'),
    ("many.jsonl", (b'{"result":"identical raw bytes","outputs":[1,2,3]}\n') * 4000),
])
def test_lossless_deterministic_gzip_roundtrip(tmp_path, suffix, body):
    source = tmp_path / suffix
    packed = tmp_path / (suffix + ".gz")
    restored = tmp_path / ("restored_" + suffix)
    source.write_bytes(body)
    receipt = transport.pack_shadow_payload(source, packed)
    assert receipt["roundtrip_verified"] is True
    assert receipt["original_bytes"] == len(body)
    assert receipt["compressed_bytes"] < transport.MAX_GIT_PAYLOAD_BYTES
    first_gzip = packed.read_bytes()
    assert gzip.decompress(first_gzip) == body
    assert transport.pack_shadow_payload(source, packed)["compressed_sha256"] == receipt["compressed_sha256"]
    assert packed.read_bytes() == first_gzip  # no per-run timestamp or path header
    unpack = transport.unpack_shadow_payload(packed, restored)
    assert unpack["original_sha256"] == receipt["original_sha256"]
    assert restored.read_bytes() == body


@pytest.mark.parametrize("mutation", ("bad_magic", "bad_crc", "truncated"))
def test_corrupted_gzip_is_fail_closed(tmp_path, mutation):
    source = tmp_path / "archive.jsonl"
    packed = tmp_path / "archive.jsonl.gz"
    target = tmp_path / "restored.jsonl"
    source.write_bytes((b'{"capture_id":"immutable","hash":"a"}\n') * 200)
    transport.pack_shadow_payload(source, packed)
    altered = bytearray(packed.read_bytes())
    if mutation == "bad_magic":
        altered[0] ^= 0xFF
    elif mutation == "bad_crc":
        altered[-5] ^= 0xFF
    else:
        altered = altered[:-6]
    packed.write_bytes(bytes(altered))
    with pytest.raises(transport.ShadowTransportError, match="shadow_gzip_corrupt"):
        transport.unpack_shadow_payload(packed, target)
    assert not target.exists()


def test_oversize_compressed_transport_stops_before_publication(tmp_path, monkeypatch):
    source = tmp_path / "large.jsonl"
    target = tmp_path / "large.jsonl.gz"
    source.write_bytes(b'{"a":1}\n' * 100)
    monkeypatch.setattr(transport, "MAX_GIT_PAYLOAD_BYTES", 15)
    with pytest.raises(transport.ShadowTransportError, match="compressed_shadow_payload_exceeds_review_limit"):
        transport.pack_shadow_payload(source, target)
    assert not target.exists()


def test_missing_or_empty_source_never_generates_fake_archive(tmp_path):
    raw = tmp_path / "missing.jsonl"
    target = tmp_path / "missing.jsonl.gz"
    with pytest.raises(transport.ShadowTransportError, match="shadow_source_missing_or_empty"):
        transport.pack_shadow_payload(raw, target)
    raw.write_bytes(b"")
    with pytest.raises(transport.ShadowTransportError, match="shadow_source_missing_or_empty"):
        transport.pack_shadow_payload(raw, target)


def test_shadow_workflow_preserves_legacy_read_and_source_publication_binding():
    workflow = (Path(__file__).resolve().parents[1] / ".github/workflows/elliott_vnext_prospective_capture.yml").read_text(encoding="utf-8")
    assert "zipped_archive = git_bytes(" in workflow
    assert "gzip.decompress(zipped_archive).decode('utf-8')" in workflow
    assert "shadow_transport unpack" in workflow
    assert "shadow_transport pack" in workflow
    assert "git rm -f --ignore-unmatch" in workflow
    assert "elliott_vnext_prospective_history_6h.jsonl.gz" in workflow
    assert "elliott_vnext_prospective_current_6h.json.gz" in workflow
    assert 'git show "${{ steps.bind.outputs.commit }}:$path" > "$path"' in workflow
    assert "tests/test_elliott_shadow_transport.py" in workflow


@pytest.mark.parametrize("kind", ("no_change", "append"))
def test_full_history_bytes_preserved_before_archive_transport(tmp_path, kind):
    before = tmp_path / "previous.jsonl"
    current = tmp_path / "new.jsonl"
    legacy = b'{"capture_id":"a","identity":"old"}\\n' * 50
    before.write_bytes(legacy)
    current.write_bytes(legacy + (b'{"capture_id":"b","identity":"new"}\\n' if kind == "append" else b""))
    result = transport.verify_shadow_archive_append_only(before, current)
    assert result["append_only_verified"] is True
    assert result["baseline_bytes"] == len(legacy)
    assert result["appended_bytes"] == current.stat().st_size - len(legacy)


@pytest.mark.parametrize("kind,expected", (
    ("truncate", "shadow_archive_truncated"),
    ("mutation", "shadow_archive_prior_bytes_changed"),
    ("empty", "shadow_archive_truncated"),
))
def test_history_loss_or_rewrite_rejected_even_if_workflow_would_be_green(tmp_path, kind, expected):
    old = tmp_path / "previous.jsonl"
    new = tmp_path / "current.jsonl"
    raw = b'{"capture_id":"full-archive"}\\n' * 500
    old.write_bytes(raw)
    if kind == "truncate":
        new.write_bytes(raw[:-20])
    elif kind == "empty":
        new.write_bytes(b"")
    else:
        new.write_bytes(b"X" + raw[1:] + b'{"capture_id":"new"}\\n')
    with pytest.raises(transport.ShadowTransportError, match=expected):
        transport.verify_shadow_archive_append_only(old, new)


def test_shadow_workflow_restores_correct_runtime_symbol_paths_and_validates_prior_sha():
    workflow = (Path(__file__).resolve().parents[1] / ".github/workflows/elliott_vnext_prospective_capture.yml").read_text(encoding="utf-8")
    assert 'origin/elliott-vnext-shadow-data:$path.gz' in workflow
    assert 'shadow_transport unpack "$path.gz" "$path"' in workflow
    assert "elliott_restored_archive_hash_mismatch" in workflow
    assert "shadow_transport verify-append" in workflow
    assert "archive_sha256=" in workflow
    assert "elliott-vnext-shadow-data:.github/workflows/elliott_vnext_prospective_capture.yml.gz" not in workflow
