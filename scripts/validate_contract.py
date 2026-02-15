"""Validate the watchlist UI contract.

This is a lightweight gate to prevent the UI from silently breaking when
columns/types/rules change.

Usage
-----
  python -m scanner.app.run_daily
  python scripts/validate_contract.py

Options
-------
  --csv       Which CSV to validate (default: watchlist_ALL.csv)
  --contract  Which contract JSON to use (default: configs/watchlist_contract.json)
  --strict-optional  Treat missing optional columns as errors

Exit codes
----------
0 = OK
1 = FAIL
2 = Not configured (missing csv/contract)
"""

from __future__ import annotations

import argparse
from pathlib import Path

from scanner.data.schema.contract import validate_csv


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=r"artifacts/watchlist/watchlist_ALL.csv")
    ap.add_argument("--contract", default=r"configs/watchlist_contract.json")
    ap.add_argument("--strict-optional", action="store_true")
    args = ap.parse_args()

    res = validate_csv(args.csv, args.contract, strict_optional=args.strict_optional)

    # Not configured / missing inputs
    missing = [e for e in res.errors if e.startswith("missing CSV") or e.startswith("missing contract")]
    if missing:
        print("❌ Not configured")
        for e in res.errors:
            print(" -", e)
        print("Run: python -m scanner.app.run_daily")
        return 2

    if res.ok:
        print(f"✅ Contract OK: {Path(args.csv).as_posix()} ({res.summary()})")
        for w in res.warnings:
            print("⚠️", w)
        return 0

    print(f"❌ Contract FAIL: {Path(args.csv).as_posix()} ({res.summary()})")
    for e in res.errors:
        print(" -", e)
    for w in res.warnings:
        print("⚠️", w)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
