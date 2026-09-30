#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.governance.qm_b_crypto_object_semantics import audit


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit QM-B crypto stable-object semantics")
    parser.add_argument("--root", default=".")
    parser.add_argument("--contract", default="configs/qm_b_crypto_object_semantics_v1.json")
    parser.add_argument("--output")
    args = parser.parse_args()
    result = audit(Path(args.root), contract_path=args.contract)
    payload = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
