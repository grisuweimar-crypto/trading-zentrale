"""Daily research entry point and read-only publication quality gate."""
import argparse
import json
import os
from pathlib import Path

from scanner.reports.daily_research import begin_daily, generate_daily
from scanner.reports.research_validation import validate_publication
from scanner.reports.research_views import ValidationPolicy


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--begin-run", action="store_true")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--scanner-status", default="success", choices=("success", "failure", "cancelled", "skipped"))
    parser.add_argument("--expected-symbol-count", type=int)
    args = parser.parse_args()
    if args.validate_only:
        result = validate_publication(args.root)
    elif not args.receipt:
        parser.error("--receipt is required for daily generation and --begin-run")
    elif args.begin_run:
        run_id = (f"github-{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}"
                  if os.environ.get("GITHUB_RUN_ID") else None)
        result = begin_daily(args.root, args.receipt, run_id=run_id)
    else:
        result = generate_daily(args.root, args.receipt, scanner_status=args.scanner_status,
                                policy=ValidationPolicy(expected_symbol_count=args.expected_symbol_count))
        if output := os.environ.get("GITHUB_OUTPUT"):
            with open(output, "a", encoding="utf-8") as handle:
                handle.write(f"complete={str(result['latest_run_complete']).lower()}\n")
        if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
            with open(summary, "a", encoding="utf-8") as handle:
                handle.write("## Daily research publication\n\n```json\n" + json.dumps(result, indent=2) + "\n```\n")
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
