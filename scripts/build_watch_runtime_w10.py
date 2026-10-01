#!/usr/bin/env python3
"""Build the compact public runtime transport for Depot-Watch consumption."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.decision_layer.current_evidence import DEFAULT_ARCHIVE
from scanner.research.decision_layer.evidence_archive import load_evidence_archive
from scanner.research.decision_layer.watch_runtime import (
    DEFAULT_RUNTIME_DIR,
    build_watch_runtime,
    write_watch_runtime,
)


DEFAULT_CURRENT = "artifacts/research/current_decision_packets_7a.json"
DEFAULT_W10 = "artifacts/research/decision_snapshot_w10.json"


def _load(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--current", default=DEFAULT_CURRENT)
    parser.add_argument("--archive", default=DEFAULT_ARCHIVE)
    parser.add_argument("--w10-manifest", default=DEFAULT_W10)
    parser.add_argument("--output-dir", default=DEFAULT_RUNTIME_DIR)
    args = parser.parse_args()

    root = args.root.resolve()
    current_path = root / args.current
    archive_path = root / args.archive
    w10_path = root / args.w10_manifest
    output_dir = root / args.output_dir

    current = _load(current_path)
    w10 = _load(w10_path)
    packets, archive_metadata = load_evidence_archive(archive_path, missing_ok=False)
    manifest, shards = build_watch_runtime(
        current_packet_set=current,
        archive_packets=packets,
        w10_manifest=w10,
    )
    write_watch_runtime(output_dir, manifest=manifest, shards=shards)

    sizes = {
        path.name: path.stat().st_size
        for path in sorted(output_dir.glob("shard_*.json"))
    }
    print(json.dumps({
        "status": "ok",
        "snapshot_id": manifest["snapshot_id"],
        "symbol_count": manifest["symbol_count"],
        "packet_count": manifest["packet_count"],
        "archive_status": archive_metadata.get("status"),
        "runtime_manifest": str(output_dir / "manifest.json"),
        "max_shard_bytes": max(sizes.values(), default=0),
        "total_shard_bytes": sum(sizes.values()),
        "private_position_data_included": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
