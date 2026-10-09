"""Lossless, deterministic gzip transport for isolated Elliott 6H shadow payloads.

The capture computation remains on the original JSON/JSONL byte streams.
Only the git shadow-data branch stores gzip. No result, event or run is pruned.
"""
from __future__ import annotations

import argparse
import gzip
from hashlib import sha256
import json
from pathlib import Path
import shutil

# Stop before GitHub's 100-MB single-file limit. Exceeding this requires a
# separately reviewed lossless sharding design, never truncation.
MAX_GIT_PAYLOAD_BYTES = 80_000_000


class ShadowTransportError(ValueError):
    """Fail-closed shadow payload transport failure."""


def _hash_and_size(path: Path) -> tuple[str, int]:
    digest = sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def pack_shadow_payload(source: Path, target: Path) -> dict[str, object]:
    """Encode exact original bytes, with a fixed gzip header and verified roundtrip."""
    if not source.is_file() or source.stat().st_size == 0:
        raise ShadowTransportError("shadow_source_missing_or_empty")
    if source.resolve() == target.resolve() or target.suffix != ".gz":
        raise ShadowTransportError("shadow_transport_path_invalid")
    original_sha, original_size = _hash_and_size(source)
    target.parent.mkdir(parents=True, exist_ok=True)
    pending = target.with_name(target.name + ".tmp")
    try:
        with source.open("rb") as src, pending.open("wb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0, compresslevel=6) as zipped:
                shutil.copyfileobj(src, zipped, length=1024 * 1024)
        zipped_sha, zipped_size = _hash_and_size(pending)
        if zipped_size >= MAX_GIT_PAYLOAD_BYTES:
            raise ShadowTransportError("compressed_shadow_payload_exceeds_review_limit")
        _verify_roundtrip(pending, expected_sha=original_sha, expected_size=original_size)
        pending.replace(target)
        return {
            "source": str(source), "target": str(target), "original_bytes": original_size,
            "original_sha256": original_sha, "compressed_bytes": zipped_size,
            "compressed_sha256": zipped_sha, "roundtrip_verified": True,
        }
    finally:
        pending.unlink(missing_ok=True)


def _verify_roundtrip(compressed: Path, *, expected_sha: str, expected_size: int) -> None:
    digest = sha256()
    size = 0
    try:
        with gzip.open(compressed, "rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
                size += len(chunk)
    except (OSError, EOFError, gzip.BadGzipFile) as exc:
        raise ShadowTransportError("shadow_gzip_corrupt") from exc
    if size != expected_size or digest.hexdigest() != expected_sha:
        raise ShadowTransportError("shadow_gzip_roundtrip_mismatch")


def unpack_shadow_payload(source: Path, target: Path) -> dict[str, object]:
    """Restore the original byte stream; invalid/truncated gzip fails closed."""
    if not source.is_file() or source.suffix != ".gz" or source.resolve() == target.resolve():
        raise ShadowTransportError("shadow_compressed_source_invalid")
    target.parent.mkdir(parents=True, exist_ok=True)
    pending = target.with_name(target.name + ".tmp")
    try:
        try:
            with gzip.open(source, "rb") as zipped, pending.open("wb") as dst:
                shutil.copyfileobj(zipped, dst, length=1024 * 1024)
        except (OSError, EOFError, gzip.BadGzipFile) as exc:
            raise ShadowTransportError("shadow_gzip_corrupt") from exc
        digest, size = _hash_and_size(pending)
        if size == 0:
            raise ShadowTransportError("shadow_restored_empty")
        pending.replace(target)
        return {"source": str(source), "target": str(target), "original_sha256": digest, "original_bytes": size}
    finally:
        pending.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Lossless Elliott 6H shadow-data transport")
    parser.add_argument("operation", choices=("pack", "unpack"))
    parser.add_argument("source", type=Path)
    parser.add_argument("target", type=Path)
    args = parser.parse_args()
    result = (
        pack_shadow_payload(args.source, args.target)
        if args.operation == "pack"
        else unpack_shadow_payload(args.source, args.target)
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
