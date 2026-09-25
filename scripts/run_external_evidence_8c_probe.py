#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from pathlib import Path

from scanner.research.external_evidence.sec_edgar import (
    build_acceptance_index,
    companyfacts_rows,
    companyfacts_url,
    fetch_sec_json,
    filing_candidate_events,
    normalize_cik,
    submission_rows,
    submissions_url,
)
from scanner.research.external_evidence.sec_history import (
    assemble_full_submission_history,
    companyfacts_accession_coverage,
    historical_submission_file_specs,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FORMS = [
    "8-K",
    "8-K/A",
    "6-K",
    "6-K/A",
    "10-Q",
    "10-Q/A",
    "10-K",
    "10-K/A",
    "20-F",
    "20-F/A",
    "40-F",
    "40-F/A",
]


def load_item_mapping() -> dict[str, str]:
    payload = json.loads(
        (ROOT / "configs" / "external_event_taxonomy_8c_v1.json").read_text(encoding="utf-8")
    )
    return payload["8k_item_candidates"]


def self_test() -> dict[str, object]:
    return {
        "cik_normalization": normalize_cik("CIK320193"),
        "event_mapping_count": len(load_item_mapping()),
        "history_mode_available": True,
        "outcome_research": "NOT_RUN",
        "decision_integration": "NOT_RUN",
    }


def run_live(
    cik: str,
    *,
    user_agent: str,
    forms: list[str],
    include_history: bool = False,
) -> dict[str, object]:
    submissions = fetch_sec_json(submissions_url(cik), user_agent=user_agent)

    history_coverage: dict[str, object]
    if include_history:
        historical_payloads: dict[str, dict[str, object]] = {}
        for spec in historical_submission_file_specs(submissions):
            # Stay comfortably below the SEC fair-access ceiling of 10 requests/sec.
            time.sleep(0.12)
            historical_payloads[spec["name"]] = fetch_sec_json(
                spec["url"], user_agent=user_agent
            )
        history = assemble_full_submission_history(
            submissions,
            historical_payloads=historical_payloads,
            forms=forms,
            require_complete=True,
        )
        filing_rows = history["rows"]
        history_coverage = history["coverage"]
    else:
        filing_rows = submission_rows(submissions, forms=forms)
        history_coverage = {
            "history_complete": False,
            "research_ready": False,
            "reason_codes": ["RECENT_ONLY_DIAGNOSTIC"],
            "history_files_expected": len(historical_submission_file_specs(submissions)),
            "history_files_loaded": 0,
        }

    acceptance_index = build_acceptance_index(filing_rows)
    time.sleep(0.12)
    facts = fetch_sec_json(companyfacts_url(cik), user_agent=user_agent)
    fact_rows = companyfacts_rows(facts, acceptance_index=acceptance_index)
    fact_coverage = companyfacts_accession_coverage(fact_rows)

    mapping = load_item_mapping()
    events = [
        event
        for filing in filing_rows
        for event in filing_candidate_events(filing, item_mapping=mapping)
    ]

    exact_publication = sum(1 for row in filing_rows if row["pit_status"] == "SAFE")
    delayed_publication = sum(1 for row in filing_rows if row["pit_status"] == "DATE_ONLY_DELAYED")

    return {
        "cik": normalize_cik(cik),
        "include_history": include_history,
        "filing_count": len(filing_rows),
        "exact_publication_count": exact_publication,
        "date_only_delayed_count": delayed_publication,
        "forms": dict(Counter(row["form"] for row in filing_rows)),
        "history_coverage": history_coverage,
        "companyfacts_coverage": fact_coverage,
        "event_candidate_count": len(events),
        "event_candidates": dict(Counter(row["candidate_event_type"] for row in events)),
        "research_ready_for_feature_building": bool(
            history_coverage.get("research_ready") and fact_coverage.get("research_ready")
        ),
        "warning": (
            "No market outcomes are inspected. Recent-only mode is diagnostic and cannot support historical research."
        ),
        "outcome_research": "NOT_RUN",
        "decision_integration": "NOT_RUN",
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Phase 8C SEC/EDGAR PIT and coverage probe. Live mode never inspects market outcomes."
        )
    )
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--cik")
    parser.add_argument("--user-agent", help="Descriptive SEC User-Agent, e.g. project/contact")
    parser.add_argument(
        "--include-history",
        action="store_true",
        help="Fetch every SEC filings.files history block before measuring coverage",
    )
    parser.add_argument(
        "--forms",
        default=",".join(DEFAULT_FORMS),
        help="Comma-separated SEC forms to retain",
    )
    args = parser.parse_args()

    if args.self_test:
        print(json.dumps(self_test(), indent=2, sort_keys=True))
        return 0

    if not args.cik or not args.user_agent:
        parser.error("live mode requires --cik and --user-agent")

    forms = [item.strip().upper() for item in args.forms.split(",") if item.strip()]
    result = run_live(
        args.cik,
        user_agent=args.user_agent,
        forms=forms,
        include_history=args.include_history,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
