#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
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
        "outcome_research": "NOT_RUN",
        "decision_integration": "NOT_RUN",
    }


def run_live(cik: str, *, user_agent: str, forms: list[str]) -> dict[str, object]:
    submissions = fetch_sec_json(submissions_url(cik), user_agent=user_agent)
    filing_rows = submission_rows(submissions, forms=forms)
    acceptance_index = build_acceptance_index(filing_rows)

    facts = fetch_sec_json(companyfacts_url(cik), user_agent=user_agent)
    fact_rows = companyfacts_rows(facts, acceptance_index=acceptance_index)

    mapping = load_item_mapping()
    events = [
        event
        for filing in filing_rows
        for event in filing_candidate_events(filing, item_mapping=mapping)
    ]

    exact_publication = sum(1 for row in filing_rows if row["pit_status"] == "SAFE")
    delayed_publication = sum(1 for row in filing_rows if row["pit_status"] == "DATE_ONLY_DELAYED")
    unresolved_facts = sum(
        1 for row in fact_rows if "ACCESSION_NOT_IN_SUBMISSION_INDEX" in row["reason_codes"]
    )

    return {
        "cik": normalize_cik(cik),
        "filings_recent_scope_only": True,
        "filing_count": len(filing_rows),
        "exact_publication_count": exact_publication,
        "date_only_delayed_count": delayed_publication,
        "forms": dict(Counter(row["form"] for row in filing_rows)),
        "companyfact_row_count": len(fact_rows),
        "companyfact_accessions_missing_from_recent_submission_index": unresolved_facts,
        "event_candidate_count": len(events),
        "event_candidates": dict(Counter(row["candidate_event_type"] for row in events)),
        "warning": "Recent submissions alone are not a historical coverage proof; filings.files pagination is mandatory before backtesting.",
        "outcome_research": "NOT_RUN",
        "decision_integration": "NOT_RUN",
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Phase 8C SEC/EDGAR PIT probe. Live mode performs two SEC JSON requests and never inspects market outcomes."
    )
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--cik")
    parser.add_argument("--user-agent", help="Descriptive SEC User-Agent, e.g. project/contact")
    parser.add_argument(
        "--forms",
        default=",".join(DEFAULT_FORMS),
        help="Comma-separated SEC forms to retain from filings.recent",
    )
    args = parser.parse_args()

    if args.self_test:
        print(json.dumps(self_test(), indent=2, sort_keys=True))
        return 0

    if not args.cik or not args.user_agent:
        parser.error("live mode requires --cik and --user-agent")

    forms = [item.strip().upper() for item in args.forms.split(",") if item.strip()]
    result = run_live(args.cik, user_agent=args.user_agent, forms=forms)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
