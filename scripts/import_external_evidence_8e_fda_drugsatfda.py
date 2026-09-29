#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.external_evidence.fda_drugsatfda_8e import (
    build_fda_approval_evidence,
    write_fda_approval_evidence,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "configs" / "external_evidence_8e_fda_approval_v1.json"
DEFAULT_OUTPUT = ROOT / "artifacts" / "research" / "external_evidence_8e_fda_approvals.json"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Import an operator-downloaded official Drugs@FDA bulk ZIP into the outcome-blind Phase 8E-B2 regulatory-approval evidence contract."
    )
    parser.add_argument("--zip", required=True, help="Path to the official Drugs@FDA drugsatfda.zip file.")
    parser.add_argument(
        "--ingested-at",
        required=True,
        help="Timezone-aware timestamp when this exact snapshot became available to the project.",
    )
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()

    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    payload = build_fda_approval_evidence(
        zip_path=Path(args.zip),
        ingested_at=args.ingested_at,
        config=config,
    )
    write_fda_approval_evidence(payload, Path(args.output))
    print(
        json.dumps(
            {
                "status": payload["status"],
                "row_count": payload["row_count"],
                "source_zip_sha256": payload["source_zip_sha256"],
                "output": args.output,
                "market_outcomes_read": payload["guards"]["market_outcomes_read"],
                "historical_first_public_release_reconstructed": payload["guards"][
                    "historical_first_public_release_reconstructed"
                ],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
