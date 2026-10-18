"""Fail-closed validation of the pre-scoring canonical cache provenance receipt.

This validates client-observed integrity only. It does not assert provider PIT
or make archived CY-03 observations research eligible.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from scanner.reports.cycle_cache_provenance import verify_prefetch_receipt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    result = verify_prefetch_receipt(args.root)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
