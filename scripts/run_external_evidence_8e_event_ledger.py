#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from scanner.research.external_evidence.structured_events_8e import build_event_ledger


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "configs" / "external_evidence_8e_structured_events_v1.json"
DEFAULT_OUTPUT = ROOT / "artifacts" / "research" / "external_evidence_8e_event_ledger.json"


def _aware_datetime(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("--as-of must be timezone-aware")
    return parsed


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build the outcome-blind Phase 8E structured-event ledger from normalized source evidence."
    )
    parser.add_argument("--evidence", required=True, help="JSON file containing a list or {'rows': [...]} normalized 8E evidence rows.")
    parser.add_argument("--as-of", required=True, help="Timezone-aware research as-of timestamp.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()

    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    if config.get("schema_version") != "external_evidence_8e_structured_events_v1":
        raise ValueError("unsupported Phase 8E config")
    if config.get("principles", {}).get("market_outcomes_may_be_read") is not False:
        raise ValueError("Phase 8E event ledger must remain outcome-blind")

    raw = json.loads(Path(args.evidence).read_text(encoding="utf-8"))
    rows = raw.get("rows") if isinstance(raw, dict) else raw
    if not isinstance(rows, list):
        raise ValueError("evidence JSON must be a list or an object with a rows list")

    source_ranks = {
        key: int(value["authority_rank"])
        for key, value in config["source_classes"].items()
    }
    payload = build_event_ledger(
        evidence_rows=rows,
        as_of=_aware_datetime(args.as_of),
        allowed_event_types=config["initial_event_taxonomy"],
        source_ranks=source_ranks,
    )

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(
        json.dumps(
            {
                "status": payload["status"],
                "event_count": payload["event_count"],
                "output": str(output),
                "market_outcomes_read": payload["guards"]["market_outcomes_read"],
                "generic_sentiment_enabled": payload["guards"]["generic_sentiment_enabled"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
