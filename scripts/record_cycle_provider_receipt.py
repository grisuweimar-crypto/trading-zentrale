"""Bind a true future Yahoo fetch receipt to the just-archived CY-03 snapshot.

No provider-signed PIT claim, research release, scoring or trade effect.
"""
import json
from pathlib import Path
from scanner.reports.cycle_provider_receipt import record_snapshot_receipt


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    result = record_snapshot_receipt(root)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
