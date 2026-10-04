#!/usr/bin/env python3
from __future__ import annotations

"""Build one deterministic parallel replay chunk for Elliott Stage 4."""

import argparse
import gzip
from hashlib import sha256
import json
from pathlib import Path
import subprocess

import pandas as pd

from scanner.research.elliott_vnext.stage4_historical import (
    _replay_guard_review,
)
from scanner.research.elliott_vnext.validation import ValidationConfig
from scanner.research.elliott_vnext.validation_replay import replay_universe_states


DEFAULT_PRICES = "artifacts/market_data/yahoo_ohlcv.csv"


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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--prices", default=DEFAULT_PRICES)
    parser.add_argument("--chunk-index", type=int, required=True)
    parser.add_argument("--chunk-count", type=int, required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    if args.chunk_count <= 0:
        raise ValueError("chunk_count_must_be_positive")
    if args.chunk_index < 0 or args.chunk_index >= args.chunk_count:
        raise ValueError("chunk_index_out_of_range")

    root = args.root.resolve()
    prices_path = _resolve(root, args.prices)
    output_path = _resolve(root, args.output)
    frame = pd.read_csv(prices_path, low_memory=False)

    symbols = sorted(frame["symbol"].dropna().astype(str).unique().tolist())
    selected = symbols[args.chunk_index :: args.chunk_count]
    if not selected:
        raise ValueError("empty_stage4_chunk")

    config = ValidationConfig(bootstrap_reps=0)
    routed, coverage = replay_universe_states(
        frame,
        symbols=selected,
        config=config,
        keep_unchanged=False,
    )
    guard = _replay_guard_review(routed, coverage)
    if guard["valid"] is not True:
        raise ValueError("stage4_chunk_guard_violation:" + ";".join(guard["violations"]))

    payload = {
        "schema_version": "elliott_vnext_stage4_replay_chunk_v1",
        "source_commit": _git_head(root),
        "price_source_sha256": _sha256(prices_path),
        "chunk_index": args.chunk_index,
        "chunk_count": args.chunk_count,
        "symbols": selected,
        "snapshots": routed,
        "coverage": coverage,
        "guard_review": guard,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(output_path, "wt", encoding="utf-8", compresslevel=6) as handle:
        json.dump(payload, handle, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)

    print(json.dumps({
        "chunk_index": args.chunk_index,
        "chunk_count": args.chunk_count,
        "symbol_count": len(selected),
        "snapshot_count": len(routed),
        "guard_valid": guard["valid"],
        "output": str(output_path),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
