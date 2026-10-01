#!/usr/bin/env python3
from __future__ import annotations

"""Capture one published scanner snapshot through frozen Elliott 6A->6H."""

import argparse
import csv
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path

from scanner.reports.daily_research import validate_daily_research
from scanner.research.elliott_vnext.prospective_capture import (
    archive_capture,
    build_prospective_capture,
)


DEFAULT_PRICES = "artifacts/market_data/yahoo_ohlcv.csv"
DEFAULT_CURRENT = "artifacts/research/elliott_vnext_prospective_current_6h.json"
DEFAULT_ARCHIVE = "artifacts/research/elliott_vnext_prospective_history_6h.jsonl"


def _sha(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _atomic_json(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    temp.replace(path)


def _resolve(root: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else root / path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--prices", default=DEFAULT_PRICES)
    parser.add_argument("--current", default=DEFAULT_CURRENT)
    parser.add_argument("--archive", default=DEFAULT_ARCHIVE)
    parser.add_argument("--publication-commit", required=True)
    parser.add_argument("--publication-available-from", required=True)
    parser.add_argument("--expected-run-id")
    args = parser.parse_args()

    root = args.root.resolve()
    daily_path = root / "artifacts/research/daily_research.json"
    metadata_path = root / "artifacts/research/history_metadata.json"
    prices_path = _resolve(root, args.prices)
    current_path = _resolve(root, args.current)
    archive_path = _resolve(root, args.archive)

    daily = validate_daily_research(root)
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    daily_run = metadata.get("daily_run") or {}
    run_id = str(daily_run.get("run_id") or "").strip()
    if metadata.get("latest_run_complete") is not True:
        raise ValueError("elliott_capture_requires_complete_scanner_publication")
    if daily_run.get("scanner_status") != "success":
        raise ValueError("elliott_capture_requires_successful_scanner_publication")
    if args.expected_run_id and run_id != args.expected_run_id:
        raise ValueError(
            f"elliott_capture_run_id_mismatch:{run_id}:{args.expected_run_id}"
        )
    if not prices_path.exists():
        raise FileNotFoundError(f"elliott_price_history_missing:{prices_path}")

    with prices_path.open("r", encoding="utf-8", newline="") as handle:
        price_rows = list(csv.DictReader(handle))

    capture = build_prospective_capture(
        price_rows,
        daily,
        source_publication_commit=args.publication_commit,
        scanner_published_at=args.publication_available_from,
        run_id=run_id,
        price_source_sha256=_sha(prices_path),
        daily_source_sha256=_sha(daily_path),
        captured_at=datetime.now(timezone.utc).isoformat(),
    )
    appended, stored_capture = archive_capture(archive_path, capture)
    _atomic_json(current_path, stored_capture)

    print(
        json.dumps(
            {
                "schema_version": stored_capture["schema_version"],
                "snapshot_id": stored_capture["snapshot_id"],
                "as_of": stored_capture["as_of"],
                "run_id": stored_capture["run_id"],
                "validation_partition": stored_capture["validation_partition"],
                "symbols_with_outputs": stored_capture["symbols_with_outputs"],
                "output_count": stored_capture["output_count"],
                "archive_appended": appended,
                "current": str(current_path),
                "archive": str(archive_path),
                "w10_source_emitted": False,
            },
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
