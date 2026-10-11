"""Read-only inventory of tracked Scanner_vNext artifacts.

This script NEVER deletes, regenerates, or rewrites repository files.
Identical Git blobs are already deduplicated in Git object storage.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess

PROTECTED_EXACT = {
    "artifacts/research/history_analysis.csv",
    "artifacts/snapshots/score_history.csv",
    "artifacts/research/price_backfill.csv",
    "artifacts/market_data/yahoo_ohlcv.csv",
    "artifacts/research/decision_evidence_7a.jsonl.gz",
}
PROTECTED_PREFIXES = (
    "artifacts/research/qm/",
    "artifacts/cycle_history/",
)
PUBLISHED_PREFIXES = (
    "artifacts/research/watch_runtime/",
    "artifacts/ui/",
)
PUBLISHED_EXACT = {
    "artifacts/research/latest_scanner.csv",
    "artifacts/research/daily_research.json",
    "artifacts/research/history_metadata.json",
    "artifacts/research/current_decision_packets_7a.json",
    "artifacts/research/decision_snapshot_w10.json",
    "artifacts/watchlist/watchlist.csv",
    "artifacts/watchlist/watchlist_full.csv",
}
REVIEW_EXACT = {
    "artifacts/research/history_research.csv",
    "artifacts/snapshots/score_history_recent.csv",
    "artifacts/research/history_recent.csv",
}
BACKUP_PATTERN = re.compile(r"^artifacts/watchlist/watchlist \([0-9]+\)\.csv$")


def classify(path: str) -> str:
    """Conservative categories: 'review' never authorizes deletion."""
    if path in PROTECTED_EXACT or path.startswith(PROTECTED_PREFIXES):
        return "retain_source_or_audit"
    if path in PUBLISHED_EXACT or path.startswith(PUBLISHED_PREFIXES):
        return "retain_current_publication"
    if path in REVIEW_EXACT or path.startswith("artifacts/research/monthly/"):
        return "derived_review_required"
    if BACKUP_PATTERN.fullmatch(path):
        return "possible_copy_review_required"
    return "unclassified_review_required"


def inventory(root: Path) -> dict:
    """Read Git's index and local sizes without changing either."""
    cmd = ["git", "ls-files", "--stage", "-z", "--", "artifacts/"]
    data = subprocess.run(cmd, cwd=root, capture_output=True, check=True).stdout
    tracked = []
    for item in data.split(b"\0"):
        if not item:
            continue
        meta, path_bytes = item.split(b"\t", 1)
        mode, blob, stage = meta.decode("ascii").split()
        if stage != "0":
            raise ValueError("Unmerged Git index: audit requires stage 0")
        path = path_bytes.decode("utf-8", "surrogateescape")
        local = root / path
        try:
            size = os.stat(local, follow_symlinks=False).st_size
        except FileNotFoundError:
            size = None
        tracked.append({
            "path": path,
            "bytes": size,
            "blob": blob,
            "category": classify(path),
            "mode": mode,
        })
    tracked.sort(key=lambda x: x["path"])
    duplicates: dict[str, list[dict]] = {}
    totals: dict[str, dict] = {}
    for row in tracked:
        totals.setdefault(row["category"], {"files": 0, "bytes": 0})
        totals[row["category"]]["files"] += 1
        totals[row["category"]]["bytes"] += row["bytes"] or 0
        duplicates.setdefault(row["blob"], []).append(row)
    groups = [
        {
            "blob": blob,
            "paths": [x["path"] for x in rows],
            "logical_extra_bytes": (len(rows) - 1) * (rows[0]["bytes"] or 0),
        }
        for blob, rows in duplicates.items() if len(rows) > 1
    ]
    groups.sort(key=lambda x: (-x["logical_extra_bytes"], x["paths"]))
    return {
        "schema_version": 1,
        "mode": "read_only",
        "tracked_artifact_count": len(tracked),
        "tracked_worktree_bytes": sum(x["bytes"] or 0 for x in tracked),
        "missing_worktree_files": [x["path"] for x in tracked if x["bytes"] is None],
        "by_class": totals,
        "possible_copy_paths": [
            x["path"] for x in tracked
            if x["category"] == "possible_copy_review_required"
        ],
        "identical_blob_groups": groups,
        "largest": sorted(
            tracked, key=lambda x: (-(x["bytes"] or 0), x["path"])
        )[:20],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument(
        "--json", action="store_true",
        help="Emit structured JSON instead of a short report"
    )
    args = parser.parse_args()
    try:
        result = inventory(args.root.resolve())
    except (subprocess.CalledProcessError, OSError, ValueError) as exc:
        parser.exit(2, f"Artifact inventory failed: {exc}\n")
    if args.json:
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print(
            f"Tracked artifacts: {result['tracked_artifact_count']} files, "
            f"{result['tracked_worktree_bytes']:,} bytes"
        )
        for kind, count in sorted(result["by_class"].items()):
            print(
                f"  {kind}: {count['files']} files, "
                f"{count['bytes']:,} bytes"
            )
        print(
            "Exact-blob duplicate groups: "
            f"{len(result['identical_blob_groups'])} (NOT Git space savings)"
        )
        for item in result["possible_copy_paths"]:
            print(f"Review possible copy: {item}")
        for item in result["largest"][:10]:
            print(
                f"  {item['bytes'] or 0:>12,}  {item['path']} "
                f"[{item['category']}]"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
