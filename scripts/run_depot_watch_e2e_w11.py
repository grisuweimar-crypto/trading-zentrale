#!/usr/bin/env python3
"""Run the real W11 Depot-Watch end-to-end acceptance on one sealed snapshot."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.reports.daily_research import validate_daily_research
from scanner.research.decision_layer.current_evidence import DEFAULT_ARCHIVE
from scanner.research.decision_layer.depot_watch_orchestrator import build_orchestrated_depot_watch
from scanner.research.decision_layer.evidence_archive import load_evidence_archive
from scanner.research.decision_layer.watch_runtime import (
    DEFAULT_RUNTIME_DIR,
    load_runtime_packets_for_symbols,
)
from scanner.research.decision_layer.w10_orchestration import validate_sealed_manifest
from scanner.research.decision_layer.w11_end_to_end import validate_w11_end_to_end


DEFAULT_W10_MANIFEST = "artifacts/research/decision_snapshot_w10.json"


def _load(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _outside_repo(root: Path, path: Path, label: str) -> Path:
    root_resolved = root.resolve()
    resolved = path.resolve()
    try:
        resolved.relative_to(root_resolved)
    except ValueError:
        return resolved
    raise ValueError(f"{label}_must_be_outside_repository")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run W11 against one sealed W10 snapshot and a private runtime-only "
            "position book. Public evidence is consumed through the compact Watch "
            "runtime by default; no private Watch output is written into the repository."
        )
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Repository root containing the public research artifacts",
    )
    parser.add_argument(
        "--positions",
        required=True,
        type=Path,
        help="Private decision_depot_position_book_v1 JSON; must be outside the repository",
    )
    parser.add_argument(
        "--private-output-dir",
        required=True,
        type=Path,
        help="Directory outside the repository for Watch, diagnostics and W11 receipt",
    )
    parser.add_argument("--runtime-dir", default=DEFAULT_RUNTIME_DIR)
    parser.add_argument("--archive", default=DEFAULT_ARCHIVE)
    parser.add_argument(
        "--legacy-archive",
        action="store_true",
        help="Explicitly bypass compact runtime and use the full archive",
    )
    parser.add_argument("--w10-manifest", default=DEFAULT_W10_MANIFEST)
    parser.add_argument(
        "--elliott-6h-source",
        type=Path,
        help="Optional PIT-stamped decision_elliott_6h_source_v1 JSON",
    )
    args = parser.parse_args()

    root = args.root.resolve()
    positions_path = _outside_repo(root, args.positions, "positions")
    output_dir = _outside_repo(root, args.private_output_dir, "private_output_dir")
    output_dir.mkdir(parents=True, exist_ok=True)

    daily = validate_daily_research(root)
    manifest_path = Path(args.w10_manifest)
    if not manifest_path.is_absolute():
        manifest_path = root / manifest_path
    manifest = validate_sealed_manifest(
        _load(manifest_path), expected_snapshot_id=str(daily["snapshot_id"])
    )
    positions = _load(positions_path)
    elliott = _load(args.elliott_6h_source) if args.elliott_6h_source else None

    if args.legacy_archive:
        archive_path = Path(args.archive)
        if not archive_path.is_absolute():
            archive_path = root / archive_path
        packets, transport_metadata = load_evidence_archive(archive_path, missing_ok=False)
        transport_metadata = {**transport_metadata, "transport": "legacy_full_archive"}
    else:
        raw_positions = positions.get("positions")
        if not isinstance(raw_positions, list):
            raise ValueError("position_book_positions_must_be_list")
        symbols = [
            str(row.get("symbol") or "")
            for row in raw_positions
            if isinstance(row, dict) and str(row.get("symbol") or "").strip()
        ]
        packets, transport_metadata = load_runtime_packets_for_symbols(
            root / args.runtime_dir,
            symbols,
            w10_manifest=manifest,
            expected_snapshot_id=str(daily["snapshot_id"]),
        )
        transport_metadata = {**transport_metadata, "transport": "compact_watch_runtime"}

    watch, diagnostics = build_orchestrated_depot_watch(
        daily,
        positions,
        packets,
        elliott_6h_source=elliott,
    )
    diagnostics = {
        **diagnostics,
        "archive_status": transport_metadata.get("status"),
        "evidence_transport": transport_metadata.get("transport"),
        "runtime_loaded_shard_count": transport_metadata.get("loaded_shard_count"),
        "runtime_loaded_packet_count": transport_metadata.get("loaded_packet_count"),
        "w10_manifest_status": manifest["status"],
        "w10_snapshot_id": manifest["snapshot_id"],
        "w10_pre_7a_frozen_at": manifest["pre_7a_frozen_at"],
        "w10_sealed_at": manifest["sealed_at"],
        "w10_no_backdating_guard": True,
    }
    receipt = validate_w11_end_to_end(
        manifest=manifest,
        archive_packets=packets,
        watch=watch,
        diagnostics=diagnostics,
    )

    (output_dir / "depot_watch_7h.json").write_text(
        json.dumps(watch, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    (output_dir / "w11_diagnostics.json").write_text(
        json.dumps(diagnostics, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    receipt_path = output_dir / "w11_acceptance_receipt.json"
    receipt_path.write_text(
        json.dumps(receipt, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    print(json.dumps({
        "status": receipt["status"],
        "snapshot_id": receipt["snapshot_id"],
        "evidence_transport": transport_metadata.get("transport"),
        "receipt": str(receipt_path),
        "private_outputs_inside_repository": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
