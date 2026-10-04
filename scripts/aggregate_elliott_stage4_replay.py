#!/usr/bin/env python3
from __future__ import annotations

"""Aggregate parallel Elliott Stage-4 replay chunks into one 6G result."""

import argparse
import gzip
from hashlib import sha256
import json
from pathlib import Path
import subprocess

import pandas as pd

from scanner.research.elliott_vnext.stage4_historical import (
    build_stage4_historical_validation_from_replay,
)
from scanner.research.elliott_vnext.validation import ValidationConfig


DEFAULT_PRICES = "artifacts/market_data/yahoo_ohlcv.csv"
DEFAULT_OUTPUT = "artifacts/research/elliott_vnext_stage4_historical_validation.json"


def _resolve(root: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else root / path


def _sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _git_head(root: Path) -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        text=True,
    ).strip()


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    temp.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--prices", default=DEFAULT_PRICES)
    parser.add_argument("--chunks-dir", required=True)
    parser.add_argument("--chunk-count", type=int, required=True)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--bootstrap-reps", type=int, default=1000)
    args = parser.parse_args()

    root = args.root.resolve()
    prices_path = _resolve(root, args.prices)
    chunks_dir = _resolve(root, args.chunks_dir)
    output_path = _resolve(root, args.output)

    source_commit = _git_head(root)
    source_hash = _sha256(prices_path)
    chunks = sorted(chunks_dir.glob("stage4-chunk-*.json.gz"))
    if len(chunks) != args.chunk_count:
        raise ValueError(f"stage4_chunk_count_mismatch:{len(chunks)}:{args.chunk_count}")

    seen_indices: set[int] = set()
    seen_symbols: set[str] = set()
    all_snapshots: list[dict] = []
    coverage_details: list[dict] = []
    symbols_requested = 0
    symbols_with_snapshots = 0
    snapshot_count = 0

    for path in chunks:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
        if payload.get("schema_version") != "elliott_vnext_stage4_replay_chunk_v1":
            raise ValueError(f"stage4_chunk_schema_invalid:{path.name}")
        if payload.get("source_commit") != source_commit:
            raise ValueError(f"stage4_chunk_source_commit_mismatch:{path.name}")
        if payload.get("price_source_sha256") != source_hash:
            raise ValueError(f"stage4_chunk_source_hash_mismatch:{path.name}")
        if int(payload.get("chunk_count") or -1) != args.chunk_count:
            raise ValueError(f"stage4_chunk_partition_mismatch:{path.name}")
        index = int(payload.get("chunk_index"))
        if index in seen_indices:
            raise ValueError(f"stage4_chunk_duplicate_index:{index}")
        seen_indices.add(index)
        if payload.get("guard_review", {}).get("valid") is not True:
            raise ValueError(f"stage4_chunk_guard_invalid:{index}")

        symbols = [str(value) for value in payload.get("symbols", [])]
        overlap = seen_symbols.intersection(symbols)
        if overlap:
            raise ValueError("stage4_symbol_overlap:" + ",".join(sorted(overlap)))
        seen_symbols.update(symbols)

        snapshots = payload.get("snapshots")
        coverage = payload.get("coverage")
        if not isinstance(snapshots, list) or not isinstance(coverage, dict):
            raise ValueError(f"stage4_chunk_payload_invalid:{index}")
        all_snapshots.extend(snapshots)
        details = coverage.get("details")
        if isinstance(details, list):
            coverage_details.extend(details)
        symbols_requested += int(coverage.get("symbols_requested") or 0)
        symbols_with_snapshots += int(coverage.get("symbols_with_snapshots") or 0)
        snapshot_count += int(coverage.get("snapshots") or 0)

    if seen_indices != set(range(args.chunk_count)):
        raise ValueError("stage4_chunk_index_set_incomplete")

    prices = pd.read_csv(prices_path, low_memory=False)
    source_symbols = set(prices["symbol"].dropna().astype(str).unique())
    if seen_symbols != source_symbols:
        missing = sorted(source_symbols - seen_symbols)
        extra = sorted(seen_symbols - source_symbols)
        raise ValueError(
            f"stage4_chunk_universe_mismatch:missing={missing[:20]}:extra={extra[:20]}"
        )

    combined_coverage = {
        "symbols_requested": symbols_requested,
        "symbols_with_snapshots": symbols_with_snapshots,
        "snapshots": snapshot_count,
        "details": coverage_details,
        "failures_are_missing_evidence_not_imputed": True,
        "research_only": True,
    }
    result = build_stage4_historical_validation_from_replay(
        prices,
        all_snapshots,
        combined_coverage,
        price_source_sha256=source_hash,
        source_commit=source_commit,
        replay_chunk_count=args.chunk_count,
        config=ValidationConfig(bootstrap_reps=args.bootstrap_reps),
    )
    _atomic_json(output_path, result)

    print(json.dumps({
        "technical_stage_status": result["technical_stage_status"],
        "empirical_promotion_status": result["empirical_promotion_status"],
        "replay": result["replay"],
        "coverage": result["coverage"],
        "structure_resolution_counts": result["structure_summary"]["resolution_counts"],
        "projection_summary_rows": len(result["projection_summary"]),
        "route_summary_rows": len(result["route_summary"]),
        "output": str(output_path),
    }, indent=2, sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
