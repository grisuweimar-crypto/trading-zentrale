from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.pattern_discovery import (
    freeze_candidates,
    write_freeze_snapshot,
)


def _json(path: str):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Freeze Pattern Discovery Lab L5 candidates."
    )
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--l3-result", required=True)
    parser.add_argument("--l4-evidence", required=True)
    parser.add_argument("--qm-context", required=True)
    parser.add_argument("--freeze-timestamp", required=True)
    parser.add_argument("--actor-id", required=True)
    parser.add_argument("--actor-role", default="researcher")
    parser.add_argument("--repo-root", default=".")
    args = parser.parse_args()

    root = Path(args.repo_root)
    snapshot = freeze_candidates(
        _json(args.manifest),
        _json(args.l3_result),
        _json(args.l4_evidence),
        repo_root=root,
        freeze_timestamp=args.freeze_timestamp,
        qm_external_context=_json(args.qm_context),
        actor_id=args.actor_id,
        actor_role=args.actor_role,
    )
    output = write_freeze_snapshot(root, snapshot)
    print(
        json.dumps(
            {
                "run_id": snapshot["run_id"],
                "snapshot_hash": snapshot["snapshot_hash"],
                "frozen_pattern_count": snapshot["frozen_pattern_count"],
                "qm_handoff_status": (
                    "READY_NOT_APPLIED_BY_L5"
                    if snapshot["qm_c_handoff"][
                        "all_packages_ready_not_applied_by_l5"
                    ]
                    else "EMPTY_OR_INCOMPLETE"
                ),
                "output": str(output),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
