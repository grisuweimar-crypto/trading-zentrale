#!/usr/bin/env python3
"""Build one Phase-7G reliability/explainability record."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.decision_layer.reliability_explainability import (
    build_reliability_explanation,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Phase-7G Reliability & Explainability output")
    parser.add_argument("--packet", required=True, help="Phase-7A evidence packet JSON")
    parser.add_argument("--stance", required=True, help="Phase-7D Universal Stance JSON")
    parser.add_argument("--transition", required=True, help="Phase-7E transition JSON")
    parser.add_argument("--action", required=True, help="Phase-7F Portfolio Action JSON")
    parser.add_argument("--output", help="Optional output JSON path")
    args = parser.parse_args()

    packet = json.loads(Path(args.packet).read_text(encoding="utf-8"))
    stance = json.loads(Path(args.stance).read_text(encoding="utf-8"))
    transition = json.loads(Path(args.transition).read_text(encoding="utf-8"))
    action = json.loads(Path(args.action).read_text(encoding="utf-8"))

    result = build_reliability_explanation(packet, stance, transition, action)
    text = json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")
    else:
        print(text, end="")


if __name__ == "__main__":
    main()
