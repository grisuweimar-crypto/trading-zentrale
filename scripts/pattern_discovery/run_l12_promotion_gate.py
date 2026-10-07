from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.pattern_discovery import (
    PromotionRegistry,
    build_promotion_review,
    persist_promotion_review,
    promotion_registry_repo_path,
    record_promotion_decision,
)


def _json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _path(root: Path, value: str) -> Path:
    candidate = Path(value)
    return candidate if candidate.is_absolute() else root / candidate


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build a governed L12 promotion review and optionally record an "
            "explicit approve/reject/defer decision."
        )
    )
    parser.add_argument("--frozen-pattern", required=True)
    parser.add_argument("--rating-history", required=True)
    parser.add_argument("--l9-report", action="append", required=True)
    parser.add_argument("--dependency-graph", required=True)
    parser.add_argument("--policy", required=True)
    parser.add_argument("--opened-at", required=True)
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--decision", choices=["APPROVE", "REJECT", "DEFER"])
    parser.add_argument("--reviewer-id")
    parser.add_argument("--reviewer-role")
    parser.add_argument("--reviewed-at")
    parser.add_argument("--reason-code", action="append", default=[])
    args = parser.parse_args()

    root = Path(args.repo_root).resolve()
    frozen = _json(_path(root, args.frozen_pattern))
    rating = _json(_path(root, args.rating_history))
    reports = [_json(_path(root, value)) for value in args.l9_report]
    dependency = _json(_path(root, args.dependency_graph))
    policy = _json(_path(root, args.policy))

    review = build_promotion_review(
        frozen,
        rating,
        reports,
        dependency,
        requested_policy=policy,
        opened_at=args.opened_at,
    )
    persisted_review = persist_promotion_review(root, review)

    output = {
        "phase": "L12",
        "review_id": review["review_id"],
        "review_hash": review["review_hash"],
        "eligibility": review["promotion_eligibility"],
        "persisted_review": persisted_review,
        "decision": None,
        "registry": None,
    }

    if args.decision:
        missing = [
            name
            for name, value in (
                ("reviewer_id", args.reviewer_id),
                ("reviewer_role", args.reviewer_role),
                ("reviewed_at", args.reviewed_at),
            )
            if not value
        ]
        if missing:
            raise ValueError(
                "decision_requires:" + ",".join(missing)
            )
        decision = record_promotion_decision(
            review,
            decision=args.decision,
            reviewer_id=args.reviewer_id,
            reviewer_role=args.reviewer_role,
            reviewed_at=args.reviewed_at,
            reason_codes=args.reason_code,
        )
        registry = PromotionRegistry(
            root / promotion_registry_repo_path()
        )
        registry_result = registry.record_decision(decision)
        output["decision"] = {
            "decision_id": decision["decision_id"],
            "promotion_status": decision["promotion_status"],
            "decision_hash": decision["decision_hash"],
        }
        output["registry"] = registry_result

    print(json.dumps(output, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
