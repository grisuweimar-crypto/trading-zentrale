#!/usr/bin/env python3
"""Locate potential current-taxonomy leakage into historical research paths.

This scanner is intentionally syntactic. A candidate is not a defect; every
candidate requires an explicit disposition before the QM-B gate can pass.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

TEXT_SUFFIXES = {".py", ".json", ".yml", ".yaml", ".toml"}
ROOTS = ("src", "scripts", "configs", ".github/workflows")
EXCLUDED_PARTS = {"__pycache__", ".git"}
EXCLUDED_NAMES = {
    "qm_b_historical_taxonomy_scan.py",
    "qm_b_historical_taxonomy_integrity.py",
    "qm_b_historical_taxonomy_integrity_v1.json",
}

TAXONOMY_TERMS = (
    "category",
    "sector",
    "industry",
    "pillar_primary",
    "pillar_tags",
    "pillar_confidence",
    "pillar_reason",
    "bucket_type",
)
HISTORICAL_TERMS = (
    "history_analysis",
    "history_recent",
    "historical",
    "history_rows",
    "price_backfill",
    "obs_date",
    "start_market_date",
    "forward_",
    "build_events",
    "HistoricalMatcher",
)
CURRENT_METADATA_TERMS = (
    "universe_master",
    "latest_scanner",
    "data/inputs/universe_master.csv",
)


def _matches(text: str, terms: tuple[str, ...]) -> list[str]:
    lower = text.lower()
    return sorted({term for term in terms if term.lower() in lower})


def scan(root: Path) -> dict:
    files_scanned = 0
    candidates: list[dict] = []
    for base in ROOTS:
        path = root / base
        if not path.exists():
            continue
        paths = [path] if path.is_file() else path.rglob("*")
        for file in paths:
            if not file.is_file() or file.suffix.lower() not in TEXT_SUFFIXES:
                continue
            rel = file.relative_to(root)
            if file.name in EXCLUDED_NAMES or any(part in EXCLUDED_PARTS for part in rel.parts):
                continue
            try:
                text = file.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError):
                continue
            files_scanned += 1
            taxonomy = _matches(text, TAXONOMY_TERMS)
            historical = _matches(text, HISTORICAL_TERMS)
            if not taxonomy or not historical:
                continue
            current = _matches(text, CURRENT_METADATA_TERMS)
            candidates.append(
                {
                    "path": rel.as_posix(),
                    "taxonomy_terms": taxonomy,
                    "historical_terms": historical,
                    "current_metadata_terms": current,
                    "priority": "HIGH" if current else "NORMAL",
                    "status": "REVIEW_REQUIRED",
                }
            )
    candidates.sort(key=lambda row: (row["priority"] != "HIGH", row["path"]))
    return {
        "schema_version": "qm_b_historical_taxonomy_scan_v1",
        "files_scanned": files_scanned,
        "candidate_count": len(candidates),
        "high_priority_count": sum(row["priority"] == "HIGH" for row in candidates),
        "candidates": candidates,
        "interpretation": "syntactic candidates only; presence is not evidence of leakage",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    parser.add_argument("--output")
    args = parser.parse_args()
    result = scan(Path(args.root).resolve())
    payload = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
