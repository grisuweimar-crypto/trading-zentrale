#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from scanner.research.governance.qm_b_project_investability import audit_membership_snapshot


def main() -> int:
    p = argparse.ArgumentParser(description="QM-B project investability audit")
    p.add_argument("--membership", required=True)
    p.add_argument("--evidence", help="optional JSON array of additional PIT evidence records")
    p.add_argument("--output")
    args = p.parse_args()
    membership = json.loads(Path(args.membership).read_text(encoding="utf-8"))
    evidence = []
    if args.evidence:
        evidence = json.loads(Path(args.evidence).read_text(encoding="utf-8"))
        if not isinstance(evidence, list):
            raise SystemExit("--evidence must contain a JSON array")
    result = audit_membership_snapshot(membership, additional_evidence=evidence)
    text = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
