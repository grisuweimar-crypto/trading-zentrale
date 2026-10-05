#!/usr/bin/env python3
from __future__ import annotations

"""Distill one completed Stage-4 replay chunk into compact 6G components."""

import argparse
import gzip
from hashlib import sha256
import json
from pathlib import Path

import pandas as pd

from scanner.research.elliott_vnext.stage4_historical import _replay_guard_review
from scanner.research.elliott_vnext.validation import (
    ValidationConfig,
    attach_forward_outcomes,
    evaluate_structure_progression,
    extract_projection_claims,
    extract_route_claims,
    extract_structure_claims,
)


DEFAULT_PRICES = "artifacts/market_data/yahoo_ohlcv.csv"


def _resolve(root: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else root / path


def _sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--prices", default=DEFAULT_PRICES)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    root = args.root.resolve()
    prices_path = _resolve(root, args.prices)
    input_path = _resolve(root, args.input)
    output_path = _resolve(root, args.output)

    with gzip.open(input_path, "rt", encoding="utf-8") as handle:
        payload = json.load(handle)
    if payload.get("schema_version") != "elliott_vnext_stage4_replay_chunk_v1":
        raise ValueError("stage4_replay_chunk_schema_invalid")

    source_hash = _sha256(prices_path)
    if payload.get("price_source_sha256") != source_hash:
        raise ValueError("stage4_replay_chunk_price_hash_mismatch")

    snapshots = payload.get("snapshots")
    coverage = payload.get("coverage")
    symbols = [str(value) for value in payload.get("symbols", [])]
    if not isinstance(snapshots, list) or not isinstance(coverage, dict) or not symbols:
        raise ValueError("stage4_replay_chunk_payload_invalid")

    guard = _replay_guard_review(snapshots, coverage)
    if guard.get("valid") is not True:
        raise ValueError("stage4_replay_chunk_guard_invalid:" + ";".join(guard.get("violations") or []))

    prices = pd.read_csv(prices_path, low_memory=False)
    selected_prices = prices.loc[prices["symbol"].astype(str).isin(symbols)].copy()
    config = ValidationConfig(bootstrap_reps=0, replay_price_basis="raw")

    structure_claims = extract_structure_claims(snapshots, config)
    structure_validation = evaluate_structure_progression(structure_claims, snapshots)
    projection_claims = extract_projection_claims(snapshots, config)
    route_claims = extract_route_claims(snapshots, config)
    projection_outcomes = attach_forward_outcomes(
        projection_claims,
        selected_prices,
        config,
    )
    route_outcomes = attach_forward_outcomes(
        route_claims,
        selected_prices,
        config,
    )

    result = {
        "schema_version": "elliott_vnext_stage4_validation_component_v1",
        "source_commit": payload.get("source_commit"),
        "price_source_sha256": source_hash,
        "chunk_index": int(payload.get("chunk_index")),
        "chunk_count": int(payload.get("chunk_count")),
        "symbols": symbols,
        "replay_coverage": coverage,
        "replay_guard_review": guard,
        "structure_validation": structure_validation,
        "projection_claim_count": len(projection_claims),
        "route_claim_count": len(route_claims),
        "projection_outcomes": projection_outcomes,
        "route_outcomes": route_outcomes,
        "research_only": True,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(output_path, "wt", encoding="utf-8", compresslevel=6) as handle:
        json.dump(
            result,
            handle,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            default=str,
        )

    print(json.dumps({
        "chunk_index": result["chunk_index"],
        "symbols": len(symbols),
        "snapshots": int(coverage.get("snapshots") or 0),
        "structure_claims": len(structure_validation),
        "projection_claims": len(projection_claims),
        "route_claims": len(route_claims),
        "projection_outcome_rows": len(projection_outcomes),
        "route_outcome_rows": len(route_outcomes),
        "output": str(output_path),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
