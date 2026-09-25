#!/usr/bin/env python3
"""Compute one Phase-7D research-only Universal Stance from a 7A packet."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.decision_layer.universal_stance import compute_universal_stance


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute Phase-7D Universal Stance")
    parser.add_argument("--input", required=True, help="Validated or raw Phase-7A packet JSON")
    parser.add_argument("--output", help="Optional output JSON path")
    args = parser.parse_args()

    packet = json.loads(Path(args.input).read_text(encoding="utf-8"))
    result = compute_universal_stance(packet)
    text = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    else:
        print(text)


if __name__ == "__main__":
    main()
