#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

NEEDLES = ("market_date", "MarketDate")
SCAN_ROOTS = ("src", "scripts", "configs", ".github/workflows")
SUFFIXES = {".py", ".json", ".yml", ".yaml", ".toml", ".md", ".txt"}


def scan(root: Path) -> dict:
    matches = []
    files_scanned = 0
    for relative_root in SCAN_ROOTS:
        base = root / relative_root
        if not base.exists():
            continue
        paths = [base] if base.is_file() else base.rglob("*")
        for path in paths:
            if not path.is_file() or path.suffix.lower() not in SUFFIXES:
                continue
            files_scanned += 1
            try:
                lines = path.read_text(encoding="utf-8").splitlines()
            except UnicodeDecodeError:
                continue
            for lineno, line in enumerate(lines, start=1):
                found = [needle for needle in NEEDLES if needle in line]
                if found:
                    matches.append(
                        {
                            "path": path.relative_to(root).as_posix(),
                            "line": lineno,
                            "needles": found,
                            "excerpt": line.strip()[:500],
                        }
                    )
    return {
        "files_scanned": files_scanned,
        "match_count": len(matches),
        "matched_files": sorted({row["path"] for row in matches}),
        "matches": matches,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Locate productive and research market_date semantics")
    parser.add_argument("--root", default=".")
    parser.add_argument("--output")
    args = parser.parse_args()
    report = scan(Path(args.root).resolve())
    if args.output:
        Path(args.output).write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
