#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from scanner.research.external_evidence.structured_events_8e_phase8c_adapter import (
    adapt_phase8c_structured_events,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "artifacts" / "research" / "external_evidence_8e_phase8c_reuse.json"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Adapt validated Phase 8C filing-metadata events into conservative Phase 8E evidence."
    )
    parser.add_argument("--input", required=True, help="JSON list or object with rows containing Phase 8C-D structured-event rows.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()

    raw = json.loads(Path(args.input).read_text(encoding="utf-8"))
    rows = raw.get("rows") if isinstance(raw, dict) else raw
    if not isinstance(rows, list):
        raise ValueError("input JSON must be a list or an object with a rows list")

    payload = adapt_phase8c_structured_events(
        rows,
        adapter_ingested_at=datetime.now(timezone.utc),
    )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(
        json.dumps(
            {
                "status": payload["status"],
                "row_count": payload["row_count"],
                "unmapped_candidate_event_type_counts": payload[
                    "unmapped_candidate_event_type_counts"
                ],
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
