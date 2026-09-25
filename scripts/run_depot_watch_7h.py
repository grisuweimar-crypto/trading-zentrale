#!/usr/bin/env python3
"""Build a Phase-7H research-only Depot-Watch from private runtime inputs."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.reports.daily_research import validate_daily_research
from scanner.research.decision_layer.depot_watch import build_depot_watch


def _load(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build Phase-7H Depot-Watch without persisting private position data by default"
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Repository root containing the authoritative artifacts/research/daily_research.json",
    )
    parser.add_argument("--positions", required=True, type=Path, help="Private decision_depot_position_book_v1 JSON")
    parser.add_argument("--bundles", required=True, type=Path, help="decision_chain_bundle_set_v1 JSON")
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional output path. Omit to print JSON to stdout; no public-repo persistence is performed automatically.",
    )
    args = parser.parse_args()

    daily = validate_daily_research(args.root)
    positions = _load(args.positions)
    bundles = _load(args.bundles)
    result = build_depot_watch(daily, positions, bundles)
    text = json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")


if __name__ == "__main__":
    main()
