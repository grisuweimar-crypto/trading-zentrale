from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.pattern_discovery import (
    FeatureLibrary,
    run_discovery_search,
    write_search_result,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run one frozen Pattern Discovery Lab L3 search."
    )
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--observations", required=True)
    parser.add_argument("--repo-root", default=".")
    args = parser.parse_args()

    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    observations = json.loads(
        Path(args.observations).read_text(encoding="utf-8")
    )
    if not isinstance(observations, list):
        raise ValueError("observations JSON must contain a top-level array")

    root = Path(args.repo_root)
    result = run_discovery_search(
        manifest,
        observations,
        repo_root=root,
        feature_library=FeatureLibrary(
            root / "configs/pattern_discovery/feature_library_v1.json"
        ),
    )
    output = write_search_result(root, result)
    print(
        json.dumps(
            {
                "run_id": result["run_id"],
                "result_hash": result["result_hash"],
                "tested": result["candidate_counts"]["tested"],
                "eligible_for_l4": result["candidate_counts"][
                    "eligible_for_l4"
                ],
                "rejected_l3": result["candidate_counts"]["rejected_l3"],
                "shortlisted": result["candidate_counts"][
                    "discovery_shortlist"
                ],
                "output": str(output),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
