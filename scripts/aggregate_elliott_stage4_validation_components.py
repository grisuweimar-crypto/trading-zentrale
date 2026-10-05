#!/usr/bin/env python3
from __future__ import annotations

"""Aggregate compact Stage-4 validation components into the canonical result."""

import argparse
import gzip
from hashlib import sha256
import json
from pathlib import Path

import pandas as pd

from scanner.research.elliott_vnext.stage4_historical import (
    build_stage4_historical_validation_from_components,
)
from scanner.research.elliott_vnext.validation import ValidationConfig


DEFAULT_PRICES = "artifacts/market_data/yahoo_ohlcv.csv"
DEFAULT_OUTPUT = "artifacts/research/elliott_vnext_stage4_historical_validation.json"


def _resolve(root: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else root / path


def _sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


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
    parser.add_argument("--components-dir", required=True)
    parser.add_argument("--chunk-count", type=int, required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--source-workflow-run-id", type=int)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--bootstrap-reps", type=int, default=1000)
    args = parser.parse_args()

    root = args.root.resolve()
    prices_path = _resolve(root, args.prices)
    components_dir = _resolve(root, args.components_dir)
    output_path = _resolve(root, args.output)

    source_hash = _sha256(prices_path)
    paths = sorted(components_dir.glob("stage4-validation-chunk-*.json.gz"))
    if len(paths) != args.chunk_count:
        raise ValueError(
            f"stage4_validation_component_count_mismatch:{len(paths)}:{args.chunk_count}"
        )

    seen_indices: set[int] = set()
    seen_symbols: set[str] = set()
    coverage_details: list[dict] = []
    structure_validation: list[dict] = []
    projection_outcomes: list[dict] = []
    route_outcomes: list[dict] = []
    symbols_requested = 0
    symbols_with_snapshots = 0
    snapshot_count = 0
    projection_claim_count = 0
    route_claim_count = 0
    guard_violations: list[str] = []
    replay_error_count = 0
    coverage_complete = True

    for path in paths:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
        if payload.get("schema_version") != "elliott_vnext_stage4_validation_component_v1":
            raise ValueError(f"stage4_validation_component_schema_invalid:{path.name}")
        if payload.get("source_commit") != args.source_commit:
            raise ValueError(f"stage4_validation_component_source_commit_mismatch:{path.name}")
        if payload.get("price_source_sha256") != source_hash:
            raise ValueError(f"stage4_validation_component_price_hash_mismatch:{path.name}")
        if int(payload.get("chunk_count") or -1) != args.chunk_count:
            raise ValueError(f"stage4_validation_component_partition_mismatch:{path.name}")

        index = int(payload.get("chunk_index"))
        if index in seen_indices:
            raise ValueError(f"stage4_validation_component_duplicate_index:{index}")
        seen_indices.add(index)

        symbols = [str(value) for value in payload.get("symbols", [])]
        overlap = seen_symbols.intersection(symbols)
        if overlap:
            raise ValueError("stage4_validation_component_symbol_overlap:" + ",".join(sorted(overlap)))
        seen_symbols.update(symbols)

        coverage = payload.get("replay_coverage")
        guard = payload.get("replay_guard_review")
        if not isinstance(coverage, dict) or not isinstance(guard, dict):
            raise ValueError(f"stage4_validation_component_replay_metadata_invalid:{index}")
        if guard.get("valid") is not True:
            guard_violations.extend(str(value) for value in (guard.get("violations") or []))
        replay_error_count += int(guard.get("replay_error_count") or 0)
        coverage_complete = coverage_complete and guard.get("coverage_complete") is True

        details = coverage.get("details")
        if isinstance(details, list):
            coverage_details.extend(item for item in details if isinstance(item, dict))
        else:
            coverage_complete = False

        symbols_requested += int(coverage.get("symbols_requested") or 0)
        symbols_with_snapshots += int(coverage.get("symbols_with_snapshots") or 0)
        snapshot_count += int(coverage.get("snapshots") or 0)
        projection_claim_count += int(payload.get("projection_claim_count") or 0)
        route_claim_count += int(payload.get("route_claim_count") or 0)

        for key, target in (
            ("structure_validation", structure_validation),
            ("projection_outcomes", projection_outcomes),
            ("route_outcomes", route_outcomes),
        ):
            rows = payload.get(key)
            if not isinstance(rows, list):
                raise ValueError(f"stage4_validation_component_rows_invalid:{index}:{key}")
            target.extend(row for row in rows if isinstance(row, dict))

    if seen_indices != set(range(args.chunk_count)):
        raise ValueError("stage4_validation_component_index_set_incomplete")

    prices = pd.read_csv(prices_path, low_memory=False)
    source_symbols = set(prices["symbol"].dropna().astype(str).unique())
    if seen_symbols != source_symbols:
        missing = sorted(source_symbols - seen_symbols)
        extra = sorted(seen_symbols - source_symbols)
        raise ValueError(
            f"stage4_validation_component_universe_mismatch:missing={missing[:20]}:extra={extra[:20]}"
        )

    combined_coverage = {
        "symbols_requested": symbols_requested,
        "symbols_with_snapshots": symbols_with_snapshots,
        "snapshots": snapshot_count,
        "details": coverage_details,
        "failures_are_missing_evidence_not_imputed": True,
        "research_only": True,
    }
    combined_guard = {
        "valid": not guard_violations and replay_error_count == 0 and coverage_complete,
        "violation_count": len(guard_violations),
        "violations": guard_violations[:100],
        "coverage_complete": coverage_complete,
        "coverage_violation_count": 0 if coverage_complete else 1,
        "replay_error_count": replay_error_count,
        "replay_errors_are_missing_evidence_not_imputed": True,
        "snapshot_guard_verified_per_chunk": True,
    }

    result = build_stage4_historical_validation_from_components(
        prices,
        structure_validation=structure_validation,
        projection_outcomes=projection_outcomes,
        route_outcomes=route_outcomes,
        replay_coverage=combined_coverage,
        replay_guard_review=combined_guard,
        projection_claim_count=projection_claim_count,
        route_claim_count=route_claim_count,
        price_source_sha256=source_hash,
        source_commit=args.source_commit,
        replay_chunk_count=args.chunk_count,
        source_workflow_run_id=args.source_workflow_run_id,
        config=ValidationConfig(
            bootstrap_reps=args.bootstrap_reps,
            replay_price_basis="raw",
        ),
    )
    _atomic_json(output_path, result)

    print(json.dumps({
        "technical_stage_status": result["technical_stage_status"],
        "empirical_promotion_status": result["empirical_promotion_status"],
        "source": result["source"],
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
