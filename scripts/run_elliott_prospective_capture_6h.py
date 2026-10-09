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
from scanner.research.elliott_vnext.stage4_historical import (
    adapt_stage4_validation_for_6h,
)


DEFAULT_PRICES = "artifacts/market_data/yahoo_ohlcv.csv"
DEFAULT_CURRENT = "artifacts/research/elliott_vnext_prospective_current_6h.json"
DEFAULT_ARCHIVE = "artifacts/research/elliott_vnext_prospective_history_6h.jsonl"
DEFAULT_STAGE4_VALIDATION = "artifacts/research/elliott_vnext_stage4_historical_validation.json"


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


# All scanner-side sources consumed by validate_daily_research must be from
# the SAME immutable publication. An older metadata + current history_recent
# silently mixing snapshots must never be accepted by the prospective sidecar.
FROZEN_RESEARCH_SOURCES = {
    "latest_scanner": "artifacts/research/latest_scanner.csv",
    "history_recent": "artifacts/research/history_recent.csv",
    "price_backfill": "artifacts/research/price_backfill.csv",
    "daily_research": "artifacts/research/daily_research.json",
    "scanner_input_provenance": "artifacts/research/scanner_input_provenance.json",
}


def verify_frozen_research_sources(root: Path, metadata: dict[str, object]) -> None:
    """Fail closed unless each exact frozen artifact matches its bound SHA-256."""
    for identity_key, relative in FROZEN_RESEARCH_SOURCES.items():
        identity = metadata.get(identity_key)
        if not isinstance(identity, dict):
            raise ValueError(f"elliott_source_identity_missing:{identity_key}")
        if identity.get("path") != relative:
            raise ValueError(f"elliott_source_path_mismatch:{identity_key}")
        expected = str(identity.get("sha256") or "")
        if len(expected) != 64 or any(c not in "0123456789abcdef" for c in expected):
            raise ValueError(f"elliott_source_hash_missing:{identity_key}")
        path = root / relative
        if not path.is_file():
            raise ValueError(f"elliott_source_file_missing:{identity_key}")
        if _sha(path) != expected:
            raise ValueError(f"elliott_source_hash_mismatch:{identity_key}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--prices", default=DEFAULT_PRICES)
    parser.add_argument("--current", default=DEFAULT_CURRENT)
    parser.add_argument("--archive", default=DEFAULT_ARCHIVE)
    parser.add_argument("--validation", default=DEFAULT_STAGE4_VALIDATION)
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
    validation_path = _resolve(root, args.validation)

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    verify_frozen_research_sources(root, metadata)
    daily = validate_daily_research(root)
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
    if not validation_path.exists():
        raise FileNotFoundError(
            f"elliott_stage4_validation_missing:{validation_path}"
        )

    stage4_validation = json.loads(validation_path.read_text(encoding="utf-8"))
    validation_report = adapt_stage4_validation_for_6h(stage4_validation)

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
        validation_report=validation_report,
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
                "validation_report_supplied": stored_capture.get("validation_source") is not None,
                "validation_source": stored_capture.get("validation_source"),
            },
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
