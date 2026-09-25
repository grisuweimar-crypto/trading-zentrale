#!/usr/bin/env python3
"""Compute one Phase-7F research-only Portfolio Action review state."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.decision_layer.portfolio_action import compute_portfolio_action


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute Phase-7F Portfolio Action and Swing Management")
    parser.add_argument("--transition", required=True, help="Phase-7E transition JSON")
    parser.add_argument("--position", required=True, help="PIT position snapshot JSON")
    parser.add_argument("--swing-context", help="Optional Elliott-6H review-context JSON")
    parser.add_argument("--output", help="Optional output JSON path")
    args = parser.parse_args()

    transition = json.loads(Path(args.transition).read_text(encoding="utf-8"))
    position = json.loads(Path(args.position).read_text(encoding="utf-8"))
    swing = json.loads(Path(args.swing_context).read_text(encoding="utf-8")) if args.swing_context else None
    result = compute_portfolio_action(transition, position, swing)
    text = json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")
    else:
        print(text, end="")


if __name__ == "__main__":
    main()
