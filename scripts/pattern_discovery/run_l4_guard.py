from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.pattern_discovery import (
    FeatureLibrary,
    apply_statistical_guard,
    write_statistical_evidence,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Apply Pattern Discovery Lab L4 statistical guards."
    )
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--l3-result", required=True)
    parser.add_argument("--observations", required=True)
    parser.add_argument("--repo-root", default=".")
    args = parser.parse_args()

    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    l3_result = json.loads(Path(args.l3_result).read_text(encoding="utf-8"))
    observations = json.loads(
        Path(args.observations).read_text(encoding="utf-8")
    )
    if not isinstance(observations, list):
        raise ValueError("observations JSON must contain a top-level array")

    root = Path(args.repo_root)
    evidence = apply_statistical_guard(
        manifest,
        l3_result,
        observations,
        repo_root=root,
        feature_library=FeatureLibrary(
            root / "configs/pattern_discovery/feature_library_v1.json"
        ),
    )
    output = write_statistical_evidence(root, evidence)
    print(
        json.dumps(
            {
                "run_id": evidence["run_id"],
                "evidence_hash": evidence["evidence_hash"],
                "tested": evidence["counts"]["l3_tested_candidates"],
                "rejected_before_l4": evidence["counts"]["rejected_before_l4"],
                "rejected_l4": evidence["counts"]["rejected_l4"],
                "eligible_for_l5": evidence["counts"]["eligible_for_l5"],
                "output": str(output),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
