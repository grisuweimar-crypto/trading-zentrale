#!/usr/bin/env python3
"""Publish small per-symbol views from the already validated Watch runtime shards.

Transport only: no Decision evidence, stance, hysteresis, portfolio action,
reliability, or Depot-Watch logic is recomputed.
"""
from __future__ import annotations

import argparse
from hashlib import sha256
import json
from pathlib import Path

SCHEMA_VERSION = "decision_watch_symbol_runtime_v1"
INDEX_SCHEMA_VERSION = "decision_watch_symbol_runtime_index_v1"
DEFAULT_RUNTIME_DIR = "artifacts/research/watch_runtime"


def _load(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _filename(symbol: str) -> str:
    return f"symbol_{sha256(symbol.encode('utf-8')).hexdigest()[:16]}.json"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--runtime-dir", default=DEFAULT_RUNTIME_DIR)
    args = parser.parse_args()

    runtime_dir = (args.root / args.runtime_dir).resolve()
    manifest = _load(runtime_dir / "manifest.json")
    if manifest.get("schema_version") != "decision_watch_runtime_manifest_v1":
        raise ValueError("unsupported runtime manifest")
    if manifest.get("private_position_data_included") is not False:
        raise ValueError("runtime privacy guard invalid")
    if manifest.get("decision_logic_changed") is not False:
        raise ValueError("runtime decision-logic guard invalid")

    snapshot_id = str(manifest.get("snapshot_id") or "")
    decision_as_of = str(manifest.get("decision_as_of") or "")
    symbol_shards = manifest.get("symbol_shards")
    if not snapshot_id or not decision_as_of or not isinstance(symbol_shards, dict):
        raise ValueError("runtime manifest identity incomplete")

    shard_cache: dict[str, dict] = {}
    symbol_dir = runtime_dir / "symbols"
    symbol_dir.mkdir(parents=True, exist_ok=True)
    for old in symbol_dir.glob("symbol_*.json"):
        old.unlink()

    index: dict[str, str] = {}
    total_packets = 0
    for symbol in sorted(map(str, symbol_shards)):
        shard_name = str(symbol_shards[symbol])
        if shard_name not in shard_cache:
            shard = _load(runtime_dir / shard_name)
            if shard.get("schema_version") != "decision_watch_runtime_shard_v1":
                raise ValueError(f"invalid runtime shard: {shard_name}")
            if str(shard.get("snapshot_id") or "") != snapshot_id:
                raise ValueError(f"runtime shard snapshot mismatch: {shard_name}")
            shard_cache[shard_name] = shard
        raw_packets = shard_cache[shard_name].get("packets")
        if not isinstance(raw_packets, list):
            raise ValueError(f"runtime shard packets invalid: {shard_name}")
        packets = [row for row in raw_packets if isinstance(row, dict) and str(row.get("symbol") or "") == symbol]
        if not packets:
            raise ValueError(f"runtime symbol packets missing: {symbol}")
        if str(packets[-1].get("source_snapshot_id") or "") != snapshot_id:
            raise ValueError(f"runtime latest packet snapshot mismatch: {symbol}")
        filename = _filename(symbol)
        payload = {
            "schema_version": SCHEMA_VERSION,
            "symbol": symbol,
            "snapshot_id": snapshot_id,
            "decision_as_of": decision_as_of,
            "packet_count": len(packets),
            "packets": packets,
            "source_runtime_projection_sha256": str(manifest.get("runtime_projection_sha256") or ""),
            "private_position_data_included": False,
            "decision_logic_changed": False,
        }
        (symbol_dir / filename).write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        index[symbol] = filename
        total_packets += len(packets)

    index_payload = {
        "schema_version": INDEX_SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "decision_as_of": decision_as_of,
        "symbol_count": len(index),
        "packet_count": total_packets,
        "symbol_files": index,
        "source_runtime_projection_sha256": str(manifest.get("runtime_projection_sha256") or ""),
        "private_position_data_included": False,
        "decision_logic_changed": False,
    }
    (symbol_dir / "index.json").write_text(
        json.dumps(index_payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({
        "status": "ok",
        "snapshot_id": snapshot_id,
        "symbol_count": len(index),
        "packet_count": total_packets,
        "private_position_data_included": False,
        "decision_logic_changed": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
