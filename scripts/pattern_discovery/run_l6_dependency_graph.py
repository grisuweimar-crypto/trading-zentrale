from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.pattern_discovery import (
    build_dependency_graph,
    write_dependency_graph,
)


def _json(path: str):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build Pattern Discovery Lab L6 dependency graph."
    )
    parser.add_argument(
        "--l5-snapshot",
        action="append",
        required=True,
        help="Path to an L5 freeze snapshot. Repeat for multiple Discovery runs.",
    )
    parser.add_argument("--event-context", required=True)
    parser.add_argument("--repo-root", default=".")
    args = parser.parse_args()

    snapshots = [_json(path) for path in args.l5_snapshot]
    event_context = _json(args.event_context)
    graph = build_dependency_graph(snapshots, event_context)
    output = write_dependency_graph(Path(args.repo_root), graph)
    print(
        json.dumps(
            {
                "graph_id": graph["graph_id"],
                "graph_hash": graph["graph_hash"],
                "node_count": graph["counts"]["node_count"],
                "evaluated_pair_count": graph["counts"]["evaluated_pair_count"],
                "high_or_critical_edge_count": graph["counts"]["high_or_critical_edge_count"],
                "output": str(output),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
