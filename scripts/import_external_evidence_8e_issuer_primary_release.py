#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.external_evidence.issuer_primary_release_8e import build_issuer_primary_release_evidence

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "configs" / "external_evidence_8e_issuer_primary_release_v1.json"


def main() -> int:
    parser = argparse.ArgumentParser(description="Normalize prospective issuer primary-release events for Phase 8E.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    records = json.loads(Path(args.input).read_text(encoding="utf-8"))
    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    if not isinstance(records, list):
        raise SystemExit("input JSON must be an array")
    payload = build_issuer_primary_release_evidence(records=records, config=config)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"row_count": payload["row_count"], "output": str(output)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
