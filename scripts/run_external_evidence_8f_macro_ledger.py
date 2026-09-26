from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from scanner.research.external_evidence.macro_ledger_8f import build_macro_ledger


DEFAULT_MACRO_CONFIG = Path("configs/external_evidence_8f_macro_exposure_v1.json")


def _aware_datetime(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("--as-of must be timezone-aware")
    return parsed


def _load_rows(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    if text.startswith("["):
        value = json.loads(text)
        if not isinstance(value, list):
            raise ValueError(f"expected JSON array in {path}")
        return [dict(row) for row in value]
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build or extend the append-only, outcome-blind Phase 8F macro ledger."
    )
    parser.add_argument("--macro-config", type=Path, default=DEFAULT_MACRO_CONFIG)
    parser.add_argument("--existing", type=Path)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--as-of", type=_aware_datetime, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    macro_config = json.loads(args.macro_config.read_text(encoding="utf-8"))
    allowed_series_ids = {
        str(item["series_id"])
        for item in macro_config.get("series_catalog") or []
        if item.get("series_id")
    }
    if not allowed_series_ids:
        raise SystemExit("Phase 8F macro config contains no enabled series")

    ledger = build_macro_ledger(
        existing_rows=_load_rows(args.existing),
        new_rows=_load_rows(args.input),
        allowed_series_ids=allowed_series_ids,
        as_of=args.as_of,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(ledger, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
