#!/usr/bin/env python3
"""Validate a Phase-7A Decision-Layer research input packet."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.decision_layer import validate_input_packet


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate a Phase-7A typed evidence packet")
    parser.add_argument("--input", required=True, type=Path, help="Input JSON packet")
    parser.add_argument("--output", type=Path, help="Optional normalized output JSON with coverage summary")
    args = parser.parse_args()

    packet = json.loads(args.input.read_text(encoding="utf-8"))
    validated = validate_input_packet(packet)
    text = json.dumps(validated, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")


if __name__ == "__main__":
    main()
