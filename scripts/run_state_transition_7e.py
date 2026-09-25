#!/usr/bin/env python3
"""Build Phase-7E state transitions from an ordered JSON array of 7D outputs."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.decision_layer.state_transition import (
    build_state_transition_history,
    compare_confirmation_depths,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Phase-7E Universal Stance transition state")
    parser.add_argument("--input", required=True, help="JSON file containing an ordered array of 7D outputs")
    parser.add_argument("--output", help="Optional output JSON path")
    parser.add_argument("--compare", action="store_true", help="Compare frozen research candidate depths without selecting a winner")
    args = parser.parse_args()

    history = json.loads(Path(args.input).read_text(encoding="utf-8"))
    if not isinstance(history, list):
        raise SystemExit("input_must_be_json_array")
    result = compare_confirmation_depths(history) if args.compare else build_state_transition_history(history)
    text = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    else:
        print(text)


if __name__ == "__main__":
    main()
