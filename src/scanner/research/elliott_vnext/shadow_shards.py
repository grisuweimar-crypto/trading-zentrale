"""Lossless, deterministic, bounded multi-part transport for Elliott 6H history.

The archive stays an append-only byte stream in the capture workspace.  This
module changes only how it is stored on the isolated Git shadow-data branch.
All chunks are SHA-256 checked before any restored file replaces its target.
"""
from __future__ import annotations

import argparse
import gzip
from hashlib import sha256
import json
from pathlib import Path
import shutil
import tempfile

from .shadow_transport import MAX_GIT_PAYLOAD_BYTES, ShadowTransportError

SHARD_BYTES = 48 * 1024 * 1024
SCHEMA = "elliott_shadow_shards_v1"
MAX_PARTS = 10000


def _digest(path: Path) -> tuple[str, int]:
    digest = sha256()
    size = 0
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _load_manifest(path: Path) -> dict:
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ShadowTransportError("shadow_shard_manifest_invalid") from exc
    if (not isinstance(manifest, dict)
            or manifest.get("schema_version") != SCHEMA
            or not isinstance(manifest.get("parts"), list)
            or not 1 <= len(manifest["parts"]) <= MAX_PARTS
            or not isinstance(manifest.get("original_sha256"), str)
            or len(manifest["original_sha256"]) != 64
            or not isinstance(manifest.get("original_bytes"), int)
            or manifest["original_bytes"] <= 0):
        raise ShadowTransportError("shadow_shard_manifest_invalid")
    prefix = path.name.removesuffix(".manifest.json")
    if prefix == path.name:
        raise ShadowTransportError("shadow_shard_manifest_path_invalid")
    for index, part in enumerate(manifest["parts"]):
        if not isinstance(part, dict) or part.get("index") != index:
            raise ShadowTransportError("shadow_shard_manifest_invalid")
        expected = f"{prefix}.part-{index:05d}.gz"
        if (part.get("file") != expected or
                not isinstance(part.get("original_size"), int) or
                not 0 < part["original_size"] <= SHARD_BYTES or
                not isinstance(part.get("compressed_size"), int) or
                not 0 < part["compressed_size"] < MAX_GIT_PAYLOAD_BYTES or
                not isinstance(part.get("original_sha256"), str) or
                len(part["original_sha256"]) != 64 or
                not isinstance(part.get("compressed_sha256"), str) or
                len(part["compressed_sha256"]) != 64):
            raise ShadowTransportError("shadow_shard_manifest_invalid")
    if sum(p["original_size"] for p in manifest["parts"]) != manifest["original_bytes"]:
        raise ShadowTransportError("shadow_shard_manifest_size_mismatch")
    return manifest


def pack_sharded(source: Path, manifest_path: Path) -> dict:
    """Prepare all shards first; write manifest last, after exact reassembly."""
    if not source.is_file() or source.stat().st_size == 0:
        raise ShadowTransportError("shadow_source_missing_or_empty")
    if source.resolve() == manifest_path.resolve() or not manifest_path.name.endswith(".manifest.json"):
        raise ShadowTransportError("shadow_shard_manifest_path_invalid")
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    prefix = manifest_path.name.removesuffix(".manifest.json")
    manifest = {
        "schema_version": SCHEMA,
        "source_name": source.name,
        "original_bytes": source.stat().st_size,
        "original_sha256": "",
        "part_size_bytes": SHARD_BYTES,
        "parts": [],
    }
    with tempfile.TemporaryDirectory(prefix="elliott-shards-", dir=manifest_path.parent) as temp_name:
        temp = Path(temp_name)
        total = sha256()
        with source.open("rb") as stream:
            for index in range(MAX_PARTS):
                chunk = stream.read(SHARD_BYTES)
                if not chunk:
                    break
                total.update(chunk)
                name = f"{prefix}.part-{index:05d}.gz"
                compressed = temp / name
                with compressed.open("wb") as raw:
                    with gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0, compresslevel=6) as gz:
                        gz.write(chunk)
                compressed_sha, compressed_size = _digest(compressed)
                if compressed_size >= MAX_GIT_PAYLOAD_BYTES:
                    raise ShadowTransportError("compressed_shadow_payload_exceeds_review_limit")
                manifest["parts"].append({
                    "index": index,
                    "file": name,
                    "original_size": len(chunk),
                    "original_sha256": sha256(chunk).hexdigest(),
                    "compressed_size": compressed_size,
                    "compressed_sha256": compressed_sha,
                })
            if stream.read(1):
                raise ShadowTransportError("shadow_shard_count_exceeded")
        manifest["original_sha256"] = total.hexdigest()
        if not manifest["parts"]:
            raise ShadowTransportError("shadow_source_missing_or_empty")
        for part in manifest["parts"]:
            (temp / part["file"]).replace(manifest_path.parent / part["file"])
        temporary_manifest = temp / manifest_path.name
        temporary_manifest.write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        temporary_manifest.replace(manifest_path)
    return manifest


def unpack_sharded(manifest_path: Path, target: Path) -> dict:
    """Reassemble exact source bytes; corruption leaves prior target unchanged."""
    manifest = _load_manifest(manifest_path)
    if manifest_path.resolve() == target.resolve():
        raise ShadowTransportError("shadow_transport_path_invalid")
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="elliott-restore-", dir=target.parent) as temp_name:
        pending = Path(temp_name) / "restored"
        digest = sha256()
        restored = 0
        with pending.open("wb") as output:
            for part in manifest["parts"]:
                zipped = manifest_path.parent / part["file"]
                try:
                    zip_digest, zip_size = _digest(zipped)
                except OSError as exc:
                    raise ShadowTransportError("shadow_shard_missing") from exc
                if (zip_digest != part["compressed_sha256"]
                        or zip_size != part["compressed_size"]):
                    raise ShadowTransportError("shadow_shard_compressed_hash_mismatch")
                chunk_hash = sha256()
                chunk_size = 0
                try:
                    with gzip.open(zipped, "rb") as stream:
                        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                            chunk_size += len(chunk)
                            if chunk_size > part["original_size"]:
                                raise ShadowTransportError("shadow_shard_size_mismatch")
                            chunk_hash.update(chunk)
                            digest.update(chunk)
                            output.write(chunk)
                except (OSError, EOFError, gzip.BadGzipFile) as exc:
                    raise ShadowTransportError("shadow_shard_gzip_corrupt") from exc
                if (chunk_size != part["original_size"]
                        or chunk_hash.hexdigest() != part["original_sha256"]):
                    raise ShadowTransportError("shadow_shard_original_hash_mismatch")
                restored += chunk_size
        if (restored != manifest["original_bytes"]
                or digest.hexdigest() != manifest["original_sha256"]):
            raise ShadowTransportError("shadow_shard_archive_hash_mismatch")
        pending.replace(target)
    return {
        "original_bytes": restored, "original_sha256": digest.hexdigest(),
        "part_count": len(manifest["parts"]), "roundtrip_verified": True,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("pack", "unpack"))
    parser.add_argument("source", type=Path)
    parser.add_argument("target", type=Path)
    args = parser.parse_args()
    if args.operation == "pack":
        result = pack_sharded(args.source, args.target)
    else:
        result = unpack_sharded(args.source, args.target)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
