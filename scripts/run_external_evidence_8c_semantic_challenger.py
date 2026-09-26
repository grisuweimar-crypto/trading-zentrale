#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from scanner.research.external_evidence.semantic_challenger import (
    extract_semantic_candidates,
    validate_semantic_candidates,
)

ROOT = Path(__file__).resolve().parents[1]


def load_contract() -> dict[str, Any]:
    return json.loads(
        (ROOT / "configs" / "external_evidence_8c_semantic_challenger_v1.json").read_text(
            encoding="utf-8"
        )
    )


def load_anchor_artifact(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != "external_evidence_8c_content_anchors_v1":
        raise ValueError("Unsupported 8C-G anchor artifact schema")
    if payload.get("market_outcomes_read") is not False:
        raise ValueError("8C-H refuses anchor artifacts contaminated by market outcomes")
    if payload.get("market_direction_assigned") is not False:
        raise ValueError("8C-H refuses anchor artifacts with assigned market direction")
    anchors = payload.get("anchors")
    if not isinstance(anchors, list):
        raise ValueError("Anchor artifact must contain anchors array")
    return payload


def run(*, anchors_path: Path, output_path: Path) -> dict[str, Any]:
    contract = load_contract()
    source = load_anchor_artifact(anchors_path)
    result = extract_semantic_candidates(source["anchors"], contract=contract)
    validate_semantic_candidates(
        result["candidates"],
        required_fields=contract["required_output_fields"],
    )
    family_counts: dict[str, int] = {}
    comparison_eligible_dividends = 0
    for row in result["candidates"]:
        family = str(row.get("family") or "UNKNOWN")
        family_counts[family] = family_counts.get(family, 0) + 1
        if family == "DIVIDEND" and row.get("comparison_eligible") is True:
            comparison_eligible_dividends += 1

    payload = {
        "schema_version": "external_evidence_8c_semantic_challenger_artifact_v1",
        "phase": "8C_H_semantic_challenger",
        "source_anchor_schema": source.get("schema_version"),
        "source_parser_version": source.get("parser_version"),
        "scanner_as_of": source.get("scanner_as_of"),
        "parser_version": contract["parser_version"],
        "status": "CHALLENGER_ONLY_NOT_PRODUCTION_EVIDENCE",
        "market_outcomes_read": False,
        "market_direction_assigned": False,
        "candidates": result["candidates"],
        "rejections": result["rejections"],
        "coverage": {
            "candidate_count": result["candidate_count"],
            "rejection_count": result["rejection_count"],
            "family_counts": dict(sorted(family_counts.items())),
            "comparison_eligible_dividend_count": comparison_eligible_dividends,
            "guidance_semantics": "DISABLED",
            "capital_raise_semantics": "DISABLED",
            "earnings_beat_miss": "BLOCKED_BY_8B_CONSENSUS_DEPENDENCY",
            "outcome_research": "NOT_RUN",
        },
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return payload


def self_test() -> dict[str, Any]:
    from hashlib import sha256

    contract = load_contract()
    excerpt = "The board authorized a new USD 5 billion share repurchase program."
    anchor = {
        "family": "BUYBACK",
        "excerpt": excerpt,
        "excerpt_sha256": sha256(excerpt.encode("utf-8")).hexdigest(),
        "semantic_status": "ANCHOR_ONLY",
        "symbol": "TEST",
        "cik": "0000000001",
        "accession_number": "0001-26-000001",
        "source_valid_from": "2026-09-25T12:00:00+00:00",
        "filename": "press.htm",
    }
    result = extract_semantic_candidates([anchor], contract=contract)
    validate_semantic_candidates(
        result["candidates"],
        required_fields=contract["required_output_fields"],
    )
    return {
        "candidate_count": result["candidate_count"],
        "event_type": result["candidates"][0]["event_type"],
        "market_direction": result["candidates"][0]["market_direction"],
        "market_outcomes_read": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run Phase 8C-H high-precision issuer semantic challenger over 8C-G anchors."
    )
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--anchors")
    parser.add_argument(
        "--output",
        default=str(
            ROOT
            / "artifacts"
            / "research"
            / "external_evidence_8c_semantic_challenger.json"
        ),
    )
    args = parser.parse_args()

    if args.self_test:
        print(json.dumps(self_test(), indent=2, sort_keys=True))
        return 0
    if not args.anchors:
        parser.error("--anchors is required")
    result = run(anchors_path=Path(args.anchors), output_path=Path(args.output))
    print(json.dumps(result["coverage"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
