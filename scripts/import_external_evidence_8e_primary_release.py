#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from scanner.research.external_evidence.primary_release_8e import (
    build_primary_release_evidence,
    write_primary_release_evidence,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "configs" / "external_evidence_8e_primary_release_v1.json"
DEFAULT_OUTPUT = (
    ROOT / "artifacts" / "external_evidence" / "8e_primary_releases" / "primary_release_evidence.json"
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Normalize prospectively observed FTC/DOJ/issuer primary releases under the "
            "outcome-blind Phase 8E First Public Release contract. Input is a JSON array "
            "of explicitly structured release records; free-text event classification is not performed."
        )
    )
    parser.add_argument("--input", required=True, help="JSON array of structured release records")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument(
        "--ingested-at",
        default=None,
        help="Timezone-aware ISO timestamp. Defaults to current UTC time.",
    )
    args = parser.parse_args()

    records = json.loads(Path(args.input).read_text(encoding="utf-8"))
    if not isinstance(records, list):
        raise ValueError("--input JSON root must be an array")
    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    ingested_at = args.ingested_at or datetime.now(timezone.utc).isoformat()
    payload = build_primary_release_evidence(
        records=records,
        ingested_at=ingested_at,
        config=config,
    )
    output = Path(args.output)
    write_primary_release_evidence(payload, output)
    print(
        json.dumps(
            {
                "schema_version": payload["schema_version"],
                "status": payload["status"],
                "row_count": payload["row_count"],
                "ingested_at": payload["ingested_at"],
                "output": str(output),
                "market_outcomes_read": payload["guards"]["market_outcomes_read"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
