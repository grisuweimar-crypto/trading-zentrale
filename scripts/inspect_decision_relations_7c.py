#!/usr/bin/env python3
"""Inspect Phase-7C confirmation/conflict topology for one validated 7A packet."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.decision_layer import packet_relation_graph


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect Phase-7C relation topology for one 7A packet")
    parser.add_argument("--input", required=True, help="Phase-7A packet JSON")
    parser.add_argument("--output", help="Optional normalized relation-graph JSON")
    args = parser.parse_args()

    packet = json.loads(Path(args.input).read_text(encoding="utf-8"))
    graph = packet_relation_graph(packet)
    text = json.dumps(graph, indent=2, ensure_ascii=False) + "\n"
    if args.output:
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    else:
        print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
