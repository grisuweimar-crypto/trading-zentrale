#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from scanner.research.external_evidence.news_discovery_8e import (
    build_news_discovery_snapshot,
    write_news_discovery_snapshot,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "configs" / "external_evidence_8e_news_discovery_v1.json"
DEFAULT_OUT = ROOT / "artifacts" / "external_evidence" / "8e_news_discovery"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Import an official FTC/DOJ RSS snapshot as discovery-only Phase 8E evidence."
    )
    parser.add_argument("--xml", required=True)
    parser.add_argument(
        "--feed-kind",
        required=True,
        choices=["FTC_COMPETITION_PRESS_RELEASES", "DOJ_ANTITRUST_PRESS_RELEASES"],
    )
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--ingested-at", default=None)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    ingested_at = args.ingested_at or datetime.now(timezone.utc).isoformat()
    output = Path(args.output) if args.output else DEFAULT_OUT / f"{args.feed_kind.lower()}.json"
    payload = build_news_discovery_snapshot(
        xml_path=Path(args.xml),
        feed_kind=args.feed_kind,
        ingested_at=ingested_at,
        config=config,
    )
    write_news_discovery_snapshot(payload, output)
    print(json.dumps({
        "status": payload["status"],
        "feed_kind": payload["feed_kind"],
        "row_count": payload["row_count"],
        "output": str(output),
        "canonical_events_created": False,
        "market_outcomes_read": False,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
