from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.decision_layer.w8_shadow_trace import (
    append_private_trace,
    extract_w8_trace_rows,
    load_private_trace,
    summarize_w8_trace,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Capture privacy-safe W8 shadow rows from a private Depot-Watch bundle set."
    )
    parser.add_argument("--bundle-set", required=True, type=Path)
    parser.add_argument("--private-trace", required=True, type=Path)
    parser.add_argument("--summary-output", type=Path)
    args = parser.parse_args()

    bundle_set = json.loads(args.bundle_set.read_text(encoding="utf-8"))
    rows = extract_w8_trace_rows(bundle_set)
    receipt = append_private_trace(args.private_trace, rows)
    summary = summarize_w8_trace(load_private_trace(args.private_trace))

    if args.summary_output:
        args.summary_output.write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    print(json.dumps({"capture": receipt, "summary": summary}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
