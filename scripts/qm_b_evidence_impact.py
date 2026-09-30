#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.governance.qm_b_evidence_impact import evaluate, require_pass


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate QM-B evidence impact on prior research")
    parser.add_argument("--root", default=".")
    parser.add_argument("--contract", default="configs/qm_b_evidence_impact_v1.json")
    parser.add_argument("--output")
    args = parser.parse_args()
    result = evaluate(args.root, contract_path=args.contract)
    require_pass(result)
    payload = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
