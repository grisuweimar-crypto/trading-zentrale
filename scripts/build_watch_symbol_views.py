#!/usr/bin/env python3
"""Publish small per-symbol views from the already validated Watch runtime shards.

Transport only: Decision logic is imported from the production modules. The
public summaries remain portfolio-independent and contain no private holdings.
"""
from __future__ import annotations

import argparse
from hashlib import sha256
import json
from pathlib import Path
import sys

SCHEMA_VERSION = "decision_watch_symbol_runtime_v1"
SUMMARY_SCHEMA_VERSION = "decision_watch_symbol_state_summary_v1"
INDEX_SCHEMA_VERSION = "decision_watch_symbol_runtime_index_v1"
DEFAULT_RUNTIME_DIR = "artifacts/research/watch_runtime"


def _load(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _token(symbol: str) -> str:
    return sha256(symbol.encode("utf-8")).hexdigest()[:16]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--runtime-dir", default=DEFAULT_RUNTIME_DIR)
    parser.add_argument(
        "--elliott-capture",
        help="Optional research-only elliott_vnext_prospective_capture_v1 JSON for read-only display.",
    )
    args = parser.parse_args()

    root = args.root.resolve()
    sys.path.insert(0, str(root / "src"))
    from scanner.research.decision_layer.universal_stance import compute_universal_stance
    from scanner.research.decision_layer.state_transition import build_state_transition_history
    from scanner.research.decision_layer.phase7_state_history import build_state_history_context
    from scanner.research.decision_layer.elliott_readonly_display import (
        build_prepared_elliott_watch_display,
        prepare_elliott_watch_display_capture,
    )

    runtime_dir = (root / args.runtime_dir).resolve()
    manifest = _load(runtime_dir / "manifest.json")
    daily = _load(root / "artifacts/research/daily_research.json")
    elliott_capture = None
    prepared_elliott_capture = None
    if args.elliott_capture:
        elliott_path = Path(args.elliott_capture)
        if not elliott_path.is_absolute():
            elliott_path = root / elliott_path
        if not elliott_path.exists():
            raise ValueError(f"explicit Elliott capture missing: {elliott_path}")
        elliott_capture = _load(elliott_path)
        prepared_elliott_capture = prepare_elliott_watch_display_capture(
            elliott_capture
        )
    if manifest.get("schema_version") != "decision_watch_runtime_manifest_v1":
        raise ValueError("unsupported runtime manifest")
    if manifest.get("private_position_data_included") is not False:
        raise ValueError("runtime privacy guard invalid")
    if manifest.get("decision_logic_changed") is not False:
        raise ValueError("runtime decision-logic guard invalid")

    snapshot_id = str(manifest.get("snapshot_id") or "")
    decision_as_of = str(manifest.get("decision_as_of") or "")
    symbol_shards = manifest.get("symbol_shards")
    daily_symbols = daily.get("symbols")
    if not snapshot_id or not decision_as_of or not isinstance(symbol_shards, dict):
        raise ValueError("runtime manifest identity incomplete")
    if str(daily.get("snapshot_id") or "") != snapshot_id or not isinstance(daily_symbols, dict):
        raise ValueError("daily/runtime snapshot mismatch")

    shard_cache: dict[str, dict] = {}
    symbol_dir = runtime_dir / "symbols"
    symbol_dir.mkdir(parents=True, exist_ok=True)
    for old in symbol_dir.glob("symbol_*.json"):
        old.unlink()
    for old in symbol_dir.glob("summary_*.json"):
        old.unlink()

    packet_files: dict[str, str] = {}
    summary_files: dict[str, str] = {}
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

        token = _token(symbol)
        packet_filename = f"symbol_{token}.json"
        packet_payload = {
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
        (symbol_dir / packet_filename).write_text(
            json.dumps(packet_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

        stances = [compute_universal_stance(packet) for packet in packets]
        transition = build_state_transition_history(stances)
        latest_stance = stances[-1]
        structure = latest_stance["evidence_structure"]
        universal = latest_stance["universal_stance"]
        state_history = build_state_history_context(packets[-1], daily_symbols.get(symbol, {}))
        stance_history = []
        for stance in stances:
            u = stance["universal_stance"]
            s = stance["evidence_structure"]
            stance_history.append({
                "as_of": stance["as_of"],
                "source_snapshot_id": stance["source_snapshot_id"],
                "state": u["state"],
                "direction": u["direction"],
                "relation_state": s["relation_state"],
                "support_structure": s["support_structure"],
            })
        elliott_display = build_prepared_elliott_watch_display(
            prepared_elliott_capture,
            symbol=symbol,
            expected_snapshot_id=snapshot_id,
            expected_as_of=str(daily.get("as_of") or ""),
        )
        summary_filename = f"summary_{token}.json"
        summary_payload = {
            "schema_version": SUMMARY_SCHEMA_VERSION,
            "symbol": symbol,
            "snapshot_id": snapshot_id,
            "decision_as_of": decision_as_of,
            "latest_universal_stance": {
                "state": universal["state"],
                "direction": universal["direction"],
                "relation_state": structure["relation_state"],
                "support_structure": structure["support_structure"],
                "known_directional_claim_ids": structure["known_directional_claim_ids"],
                "known_directional_family_counts": structure["known_directional_family_counts"],
                "conflicts": structure["conflicts"],
            },
            "stance_history": stance_history,
            "transition_state": transition["transition_state"],
            "state_history_state": None if state_history is None else state_history["state"],
            "state_history_path_memory": None if state_history is None else state_history["path_memory"],
            "elliott_vnext_research": elliott_display,
            "private_position_data_included": False,
            "portfolio_action_computed": False,
            "decision_logic_changed": False,
        }
        (symbol_dir / summary_filename).write_text(
            json.dumps(summary_payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        packet_files[symbol] = packet_filename
        summary_files[symbol] = summary_filename
        total_packets += len(packets)

    index_payload = {
        "schema_version": INDEX_SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "decision_as_of": decision_as_of,
        "symbol_count": len(packet_files),
        "packet_count": total_packets,
        "symbol_files": packet_files,
        "summary_files": summary_files,
        "source_runtime_projection_sha256": str(manifest.get("runtime_projection_sha256") or ""),
        "elliott_vnext_display": {
            "capture_supplied": elliott_capture is not None,
            "source_capture_id": None if elliott_capture is None else elliott_capture.get("capture_id"),
            "source_snapshot_id": None if elliott_capture is None else elliott_capture.get("snapshot_id"),
            "source_as_of": None if elliott_capture is None else elliott_capture.get("as_of"),
            "research_only": True,
            "read_only_presentation": True,
            "w10_source_emitted": False,
            "decision_logic_changed": False,
        },
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
        "symbol_count": len(packet_files),
        "packet_count": total_packets,
        "elliott_capture_supplied": elliott_capture is not None,
        "elliott_source_capture_id": None if elliott_capture is None else elliott_capture.get("capture_id"),
        "elliott_source_snapshot_id": None if elliott_capture is None else elliott_capture.get("snapshot_id"),
        "private_position_data_included": False,
        "decision_logic_changed": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
