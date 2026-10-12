#!/usr/bin/env python3
"""Run private ZERO imports and the existing sealed W10 Depot Watch in one process.

Runs on a private host with local broker exports, never on public GitHub Actions.
The public research pipeline is not altered and receives no private inputs.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys

from import_zero_private_positions import convert
from import_zero_private_ledger import parse, rows, reconcile
from scanner.research.decision_layer.depot_watch import validate_position_book


def main():
    parser = argparse.ArgumentParser()
    for arg in ("stocks", "crypto", "transactions", "symbol-map", "private-dir"):
        parser.add_argument("--" + arg, required=True, type=Path)
    parser.add_argument("--as-of", required=True, help="Broker position snapshot timestamp with timezone")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    private = args.private_dir.resolve()
    if private.is_relative_to(root):
        parser.error("private directory must be outside public repository")
    mapping = json.loads(args.symbol_map.read_text(encoding="utf-8"))
    book = convert(args.stocks, args.crypto, mapping, args.as_of)
    validate_position_book(book)
    events = parse(rows(args.transactions))
    report = reconcile(events, rows(args.stocks), rows(args.crypto))
    if report["mismatches"] or report["unexplained_open_positions"]:
        raise SystemExit("STOP: private ZERO holdings ledger does not reconcile")
    if report["current_position_count"] != len(book["positions"]):
        raise SystemExit("STOP: private book missing broker holdings")
    private.mkdir(parents=True, exist_ok=True)
    files = {
        "positions": (private / "positions.json", book),
        "ledger": (private / "ledger.json", {"report": report, "events": events}),
    }
    for name, (path, data) in files.items():
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        path.chmod(0o600)
    cmd = [
        sys.executable, str(root / "scripts/run_depot_watch_orchestrated.py"),
        "--root", str(root),
        "--positions", str(files["positions"][0]),
        "--output", str(private / "watch.json"),
        "--diagnostics-output", str(private / "watch_diagnostics.json"),
    ]
    subprocess.run(cmd, check=True)
    for path in (private / "watch.json", private / "watch_diagnostics.json"):
        if path.exists():
            path.chmod(0o600)
    print(json.dumps({
        "positions_reconciled": report["matching_position_count"],
        "event_count": report["event_count"],
        "private_runtime": str(private),
        "watch_output": str(private / "watch.json"),
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
