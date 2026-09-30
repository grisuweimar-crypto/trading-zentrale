#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.governance.qm_b_historical_taxonomy_integrity import (
    evaluate,
    load_contract,
    require_pass,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate frozen QM-B historical-taxonomy dispositions")
    parser.add_argument("--scan", required=True)
    parser.add_argument("--contract", default="configs/qm_b_historical_taxonomy_integrity_v1.json")
    parser.add_argument("--output")
    args = parser.parse_args()

    scan = json.loads(Path(args.scan).read_text(encoding="utf-8"))
    contract = load_contract(args.contract)
    result = evaluate(scan, contract)
    payload = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).write_text(payload + "\n", encoding="utf-8")
    print(payload)
    require_pass(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
