"""Build and validate the Phase-7I promotion-readiness report."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.decision_layer.promotion_validation import (
    run_promotion_review,
    validate_promotion_report,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path, default=Path("/tmp/decision_promotion_7i.json"))
    parser.add_argument(
        "--as-of",
        dest="reviewed_as_of",
        default=None,
        help="Review cutoff date/time. Defaults to artifacts/research/history_metadata.json as_of.",
    )
    parser.add_argument(
        "--trace-summary",
        type=Path,
        default=None,
        help="Optional private decision_shadow_trace_summary_v1 JSON. Raw positions must not be supplied.",
    )
    args = parser.parse_args()

    report = run_promotion_review(
        args.root,
        args.output,
        reviewed_as_of=args.reviewed_as_of,
        trace_summary_path=args.trace_summary,
    )
    validate_promotion_report(report)
    print(json.dumps({
        "reviewed_as_of": report["reviewed_as_of"],
        "state": report["readiness"]["state"],
        "shadow_collection_eligible": report["technical_readiness"]["shadow_collection_eligible"],
        "promotion_review_eligible": report["promotion"]["promotion_review_eligible"],
        "productive_promotion_approved": report["promotion"]["productive_promotion_approved"],
        "execution_allowed": report["execution_allowed"],
        "report_id": report["report_id"],
    }, indent=2))


if __name__ == "__main__":
    main()
