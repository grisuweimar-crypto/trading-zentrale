#!/usr/bin/env python3
from __future__ import annotations

"""Build one compact Stage-5 validation chunk from one completed Stage-4 replay chunk."""

import argparse
import gzip
from hashlib import sha256
import json
from pathlib import Path

import pandas as pd

from scanner.research.elliott_vnext.cross_system import (
    extract_elliott_events,
    scanner_feature_rows,
)
from scanner.research.elliott_vnext.stage5_incremental import (
    Stage5Config,
    lead_lag_sufficient_stats,
    transition_context,
    transition_daily_sufficient_stats,
)
from scanner.research.elliott_vnext.validation import (
    attach_forward_outcomes,
    extract_route_claims,
)


DEFAULT_HISTORY = "artifacts/research/history_analysis.csv"
DEFAULT_PRICES = "artifacts/market_data/yahoo_ohlcv.csv"


def _resolve(root: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else root / path


def _sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--history", default=DEFAULT_HISTORY)
    parser.add_argument("--prices", default=DEFAULT_PRICES)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--source-workflow-run-id", type=int, required=True)
    args = parser.parse_args()

    root = args.root.resolve()
    input_path = _resolve(root, args.input)
    output_path = _resolve(root, args.output)
    history_path = _resolve(root, args.history)
    prices_path = _resolve(root, args.prices)

    with gzip.open(input_path, "rt", encoding="utf-8") as handle:
        payload = json.load(handle)
    if payload.get("schema_version") != "elliott_vnext_stage4_replay_chunk_v1":
        raise ValueError("stage4_replay_chunk_schema_invalid")
    if payload.get("source_commit") != args.source_commit:
        raise ValueError("stage4_replay_chunk_source_commit_mismatch")

    snapshots = payload.get("snapshots")
    symbols = [str(value) for value in payload.get("symbols", [])]
    coverage = payload.get("coverage")
    if not isinstance(snapshots, list) or not symbols or not isinstance(coverage, dict):
        raise ValueError("stage4_replay_chunk_payload_invalid")
    if coverage.get("research_only") is not True:
        raise ValueError("stage4_replay_chunk_not_research_only")

    history = pd.read_csv(history_path, low_memory=False)
    prices = pd.read_csv(prices_path, low_memory=False)
    history = history.loc[history["symbol"].astype(str).isin(symbols)].copy()
    prices = prices.loc[prices["symbol"].astype(str).isin(symbols)].copy()

    config = Stage5Config(bootstrap_reps=0)
    scanner, scanner_coverage = scanner_feature_rows(history, config.cross_system())
    events = extract_elliott_events(snapshots, config.cross_system())
    event_masks, lead_lag_rows, transition_coverage = transition_context(
        events,
        scanner,
        config,
    )

    route_claims = extract_route_claims(snapshots, config.validation())
    route_outcomes = attach_forward_outcomes(
        route_claims,
        prices,
        config.validation(),
    )

    sufficient = transition_daily_sufficient_stats(route_outcomes, event_masks)
    lead_lag = lead_lag_sufficient_stats(lead_lag_rows)
    mature = sum(row.get("outcome_available") is True for row in route_outcomes)
    prospective_mature = sum(
        row.get("outcome_available") is True
        and row.get("partition") == "prospective_unspent"
        for row in route_outcomes
    )

    result = {
        "schema_version": "elliott_vnext_stage5_chunk_v1",
        "stage": "STAGE_5_INCREMENTAL_CROSS_SYSTEM",
        "source_commit": args.source_commit,
        "source_workflow_run_id": int(args.source_workflow_run_id),
        "chunk_index": int(payload.get("chunk_index")),
        "chunk_count": int(payload.get("chunk_count")),
        "history_sha256": _sha256(history_path),
        "price_sha256": _sha256(prices_path),
        "symbols": symbols,
        "stage4_replay_coverage": coverage,
        "scanner_coverage": scanner_coverage,
        "transition_coverage": transition_coverage,
        "elliott_event_count": len(events),
        "route_claim_count": len(route_claims),
        "route_outcome_rows": len(route_outcomes),
        "mature_route_outcomes": int(mature),
        "prospective_unspent_mature_route_outcomes": int(prospective_mature),
        "transition_daily_sufficient_stats": sufficient,
        "lead_lag_sufficient_stats": lead_lag,
        "model_relation_status": "not_testable_no_historical_frozen_directional_model_claims_supplied",
        "model_claims_retrojected": False,
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
        "elliott_events": len(events),
        "route_claims": len(route_claims),
        "route_outcome_rows": len(route_outcomes),
        "mature_route_outcomes": mature,
        "sufficient_rows": len(sufficient),
        "lead_lag_rows": len(lead_lag),
        "output": str(output_path),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
