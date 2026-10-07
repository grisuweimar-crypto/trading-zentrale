from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.pattern_discovery import (
    PromotionRegistry,
    build_promotion_review,
    load_promotion_contract,
    persist_promotion_review,
    record_promotion_decision,
)


def _json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _path(root: Path, value: str) -> Path:
    candidate = Path(value)
    return candidate if candidate.is_absolute() else root / candidate


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build an L12 promotion review and optionally record its decision."
    )
    parser.add_argument("--frozen-pattern", required=True)
    parser.add_argument("--rating-history", required=True)
    parser.add_argument("--l9-report", action="append", default=[])
    parser.add_argument("--dependency-graph", required=True)
    parser.add_argument("--requested-policy", required=True)
    parser.add_argument("--opened-at", required=True)
    parser.add_argument("--decision", choices=["APPROVE", "REJECT", "DEFER"])
    parser.add_argument("--reviewer-id")
    parser.add_argument("--reviewer-role")
    parser.add_argument("--reviewed-at")
    parser.add_argument("--reason-code", action="append", default=[])
    parser.add_argument("--repo-root", default=".")
    args = parser.parse_args()

    root = Path(args.repo_root).resolve()
    review = build_promotion_review(
        _json(_path(root, args.frozen_pattern)),
        _json(_path(root, args.rating_history)),
        [_json(_path(root, value)) for value in args.l9_report],
        _json(_path(root, args.dependency_graph)),
        requested_policy=_json(_path(root, args.requested_policy)),
        opened_at=args.opened_at,
    )
    persisted = persist_promotion_review(root, review)
    output = {
        "phase": "L12",
        "review_id": review["review_id"],
        "eligibility": review["promotion_eligibility"],
        "persisted_review": persisted,
    }

    if args.decision:
        if not all((args.reviewer_id, args.reviewer_role, args.reviewed_at)):
            raise ValueError(
                "decision_requires_reviewer_id_reviewer_role_and_reviewed_at"
            )
        decision = record_promotion_decision(
            review,
            decision=args.decision,
            reviewer_id=args.reviewer_id,
            reviewer_role=args.reviewer_role,
            reviewed_at=args.reviewed_at,
            reason_codes=args.reason_code,
        )
        contract = load_promotion_contract()
        registry = PromotionRegistry(
            root / contract["storage"]["registry_path"],
            contract=contract,
        )
        output["decision"] = {
            "decision_id": decision["decision_id"],
            "promotion_status": decision["promotion_status"],
            "registry": registry.record_decision(decision),
        }

    print(json.dumps(output, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
