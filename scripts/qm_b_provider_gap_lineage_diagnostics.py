#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

TARGETS = {
    "DV.V": "CA2568277834",
    "SLVR.V": "CA82831T1093",
}
TEXT_SUFFIXES = {".py", ".json", ".csv", ".md", ".txt", ".yml", ".yaml", ".toml"}
SCAN_ROOTS = ("data", "artifacts", "configs", "docs", "src", "scripts", ".github")


def main() -> int:
    parser = argparse.ArgumentParser(description="Trace repository evidence for current provider coverage gaps")
    parser.add_argument("--root", default=".")
    parser.add_argument("--output")
    args = parser.parse_args()
    root = Path(args.root).resolve()
    hits = {symbol: [] for symbol in TARGETS}
    scanned = 0
    for relroot in SCAN_ROOTS:
        base = root / relroot
        if not base.exists():
            continue
        paths = [base] if base.is_file() else base.rglob("*")
        for path in paths:
            if not path.is_file() or path.suffix.lower() not in TEXT_SUFFIXES:
                continue
            scanned += 1
            try:
                lines = path.read_text(encoding="utf-8-sig").splitlines()
            except (UnicodeDecodeError, OSError):
                continue
            for lineno, line in enumerate(lines, start=1):
                for symbol, isin in TARGETS.items():
                    matched = []
                    if symbol in line:
                        matched.append("symbol")
                    if isin in line:
                        matched.append("isin")
                    if matched:
                        hits[symbol].append({
                            "path": path.relative_to(root).as_posix(),
                            "line": lineno,
                            "matched": matched,
                            "excerpt": line.strip()[:800],
                        })
    report = {
        "schema_version": "qm_b_provider_gap_lineage_diagnostic_v1",
        "files_scanned": scanned,
        "targets": {
            symbol: {
                "isin": isin,
                "hit_count": len(hits[symbol]),
                "matched_files": sorted({row["path"] for row in hits[symbol]}),
                "hits": hits[symbol],
            }
            for symbol, isin in TARGETS.items()
        },
        "interpretation": {
            "repository_presence_proves_listing": False,
            "current_universe_presence_proves_historical_listing": False,
            "provider_error_proves_delisting": False,
            "purpose": "locate existing identity/listing/provider evidence before classifying the coverage gap",
        },
    }
    if args.output:
        Path(args.output).write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
