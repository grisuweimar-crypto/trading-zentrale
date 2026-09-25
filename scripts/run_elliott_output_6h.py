#!/usr/bin/env python3
"""Build one Elliott vNext 6H research output from explicit JSON inputs.

This script is intentionally not wired into the productive Scanner or Depot-Watch.
It is an integration/export utility for research artifacts and later Decision-Layer
work only.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.elliott_vnext.output import build_module_output


def _load_json(path: str | None):
    if not path:
        return None
    return json.loads(Path(path).read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Build research-only Elliott vNext Module 6H output")
    parser.add_argument("--snapshot", required=True, help="6D routed snapshot JSON object")
    parser.add_argument("--pivots", help="6A source pivots JSON array")
    parser.add_argument("--validation", help="6G validation report JSON object")
    parser.add_argument("--market-context", help="6F context snapshot JSON array")
    parser.add_argument("--relative-strength", help="optional PIT relative-strength JSON object")
    parser.add_argument("--cross-system", help="optional 6E cross-system summary JSON object")
    parser.add_argument("--output", required=True, help="destination JSON path")
    args = parser.parse_args()

    snapshot = _load_json(args.snapshot)
    if not isinstance(snapshot, dict):
        raise SystemExit("--snapshot must contain one JSON object")
    pivots = _load_json(args.pivots)
    if pivots is not None and not isinstance(pivots, list):
        raise SystemExit("--pivots must contain a JSON array")
    validation = _load_json(args.validation)
    if validation is not None and not isinstance(validation, dict):
        raise SystemExit("--validation must contain one JSON object")
    context = _load_json(args.market_context)
    if context is not None and not isinstance(context, list):
        raise SystemExit("--market-context must contain a JSON array")
    relative_strength = _load_json(args.relative_strength)
    if relative_strength is not None and not isinstance(relative_strength, dict):
        raise SystemExit("--relative-strength must contain one JSON object")
    cross_system = _load_json(args.cross_system)
    if cross_system is not None and not isinstance(cross_system, dict):
        raise SystemExit("--cross-system must contain one JSON object")

    result = build_module_output(
        snapshot,
        source_pivots=pivots,
        validation_report=validation,
        market_context_snapshot=context,
        relative_strength=relative_strength,
        cross_system_summary=cross_system,
    )
    destination = Path(args.output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
