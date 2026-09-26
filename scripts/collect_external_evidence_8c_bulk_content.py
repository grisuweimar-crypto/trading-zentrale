#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.external_evidence.bulk_content_completion import (
    acquire_and_extract_bulk_content,
)
from scanner.research.external_evidence.filing_documents import fetch_sec_document_bytes

ROOT = Path(__file__).resolve().parents[1]


class SecAccessBlocked(BaseException):
    """Abort the entire acquisition on an SEC 403/429 instead of retrying thousands of filings."""


def guarded_sec_fetch(url: str, *, user_agent: str, timeout: float) -> bytes:
    try:
        return fetch_sec_document_bytes(url, user_agent=user_agent, timeout=timeout)
    except Exception as exc:
        response = getattr(exc, "response", None)
        status = getattr(response, "status_code", None)
        if status in {403, 429}:
            raise SecAccessBlocked(
                f"SEC access blocked with HTTP {status} for {url}. "
                "Acquisition stopped immediately; no further filing requests were sent."
            ) from exc
        raise


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Complete Phase 8C-F/G on the authoritative local SEC bulk bundle by "
            "fetching only selected official SEC Archives 8-K/6-K full submission texts "
            "and running the frozen content-anchor parser."
        )
    )
    parser.add_argument(
        "--bundle-dir",
        default=str(ROOT / "artifacts" / "external_evidence" / "sec_bulk_snapshot"),
    )
    parser.add_argument(
        "--research-history",
        default=str(ROOT / "artifacts" / "research" / "history_recent.csv"),
    )
    parser.add_argument(
        "--parser-contract",
        default=str(ROOT / "configs" / "external_evidence_8c_content_parser_v1.json"),
    )
    parser.add_argument(
        "--output-dir",
        default=str(ROOT / "artifacts" / "external_evidence" / "8c_real_content"),
    )
    parser.add_argument(
        "--anchors-output",
        default=str(ROOT / "artifacts" / "research" / "external_evidence_8c_content_anchors.json"),
    )
    parser.add_argument(
        "--user-agent",
        required=True,
        help=(
            "SEC-declared User-Agent identifying the project/organization and a real "
            "contact email, e.g. 'Trading-Zentrale contact@example.com'."
        ),
    )
    parser.add_argument("--minimum-interval-seconds", type=float, default=0.22)
    parser.add_argument("--content-lookback-days", type=int, default=365)
    args = parser.parse_args()

    if "@" not in args.user_agent or " " not in args.user_agent.strip():
        parser.error(
            "--user-agent must identify the project/organization and include a real contact email, "
            "matching SEC programmatic-access guidance."
        )

    try:
        result = acquire_and_extract_bulk_content(
            bundle_dir=Path(args.bundle_dir),
            research_history_path=Path(args.research_history),
            parser_contract_path=Path(args.parser_contract),
            output_dir=Path(args.output_dir),
            anchors_output_path=Path(args.anchors_output),
            user_agent=args.user_agent,
            minimum_interval_seconds=args.minimum_interval_seconds,
            content_lookback_days=args.content_lookback_days,
            fetcher=guarded_sec_fetch,
        )
    except SecAccessBlocked as exc:
        print(
            json.dumps(
                {
                    "status": "BLOCKED_ON_SEC_ARCHIVES_ACCESS",
                    "error": str(exc),
                    "next_action": (
                        "Do not continue automated retries. Verify the declared User-Agent/contact "
                        "and, if SEC still returns Access Denied, follow SEC webmaster guidance."
                    ),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 2

    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
