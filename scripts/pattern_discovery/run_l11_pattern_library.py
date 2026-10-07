from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.pattern_discovery import (
    build_pattern_library,
    persist_pattern_library,
)


def _json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _path(root: Path, value: str) -> Path:
    candidate = Path(value)
    return candidate if candidate.is_absolute() else root / candidate


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build and persist Pattern Discovery L11 Pattern Library / "
            "Research UI from governed L5/L6/L9/L10 artifacts."
        )
    )
    parser.add_argument(
        "--l5-snapshot",
        action="append",
        required=True,
        help="Repeat for every L5 freeze snapshot included in the library.",
    )
    parser.add_argument(
        "--rating-history",
        action="append",
        required=True,
        help="Repeat for every L10 rating history included in the library.",
    )
    parser.add_argument(
        "--l9-report",
        action="append",
        default=[],
        help="Optional L9 confirmation-look JSON; repeat as needed.",
    )
    parser.add_argument(
        "--dependency-graph",
        help="Optional L6 dependency graph JSON.",
    )
    parser.add_argument("--generated-at", required=True)
    parser.add_argument("--repo-root", default=".")
    args = parser.parse_args()

    root = Path(args.repo_root).resolve()
    l5_snapshots = [
        _json(_path(root, value))
        for value in args.l5_snapshot
    ]
    rating_histories = [
        _json(_path(root, value))
        for value in args.rating_history
    ]
    l9_reports = [
        _json(_path(root, value))
        for value in args.l9_report
    ]
    dependency_graph = (
        _json(_path(root, args.dependency_graph))
        if args.dependency_graph
        else None
    )

    library = build_pattern_library(
        l5_snapshots,
        rating_histories=rating_histories,
        confirmation_reports=l9_reports,
        dependency_graph=dependency_graph,
        generated_at=args.generated_at,
    )
    persisted = persist_pattern_library(root, library)
    print(
        json.dumps(
            {
                "phase": "L11",
                "library_hash": library["library_hash"],
                "pattern_count": library["counts"]["pattern_count"],
                "rating_counts": library["counts"]["rating_counts"],
                "persisted": persisted,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
