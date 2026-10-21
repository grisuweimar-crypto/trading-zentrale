"""Bind the pre-scoring fetch witness to the actual CY-03 snapshot; NO research release."""
from __future__ import annotations

import json
from pathlib import Path

from scanner.reports.cycle_prefetch_evidence import archive_current_witness


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    print(json.dumps(archive_current_witness(root), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
