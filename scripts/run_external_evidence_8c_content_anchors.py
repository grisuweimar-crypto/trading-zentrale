#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from scanner.research.external_evidence.content_parser import (
    anchor_coverage,
    extract_evidence_anchors,
    split_sec_documents,
    validate_anchor_contract,
)
from scanner.research.external_evidence.sec_snapshot import (
    load_snapshot_manifest,
    read_bytes_verified,
    validate_snapshot_bundle,
)

ROOT = Path(__file__).resolve().parents[1]


def load_contract() -> dict[str, Any]:
    return json.loads(
        (ROOT / "configs" / "external_evidence_8c_content_parser_v1.json").read_text(
            encoding="utf-8"
        )
    )


def run(*, snapshot_dir: Path, output_path: Path) -> dict[str, Any]:
    contract = load_contract()
    snapshot_summary = validate_snapshot_bundle(snapshot_dir)
    manifest = load_snapshot_manifest(snapshot_dir)
    anchors: list[dict[str, Any]] = []
    filings_processed = 0
    filing_errors: list[dict[str, Any]] = []

    policy = contract["document_policy"]
    for company in manifest["companies"]:
        if str(company.get("identity_status") or "") != "VERIFIED_BY_SEC_SUBMISSIONS":
            continue
        symbol = str(company.get("symbol") or "").strip().upper()
        cik = company.get("cik")
        for filing in company.get("content_filings") or []:
            try:
                raw = read_bytes_verified(snapshot_dir, filing["document_file"])
                documents = split_sec_documents(raw)
                filing_anchors = extract_evidence_anchors(
                    documents,
                    anchor_families=contract["evidence_anchor_families"],
                    parser_version=contract["parser_version"],
                    excluded_type_prefixes=policy["exclude_document_type_prefixes"],
                    minimum_visible_characters=policy["minimum_visible_characters"],
                )
                for anchor in filing_anchors:
                    anchor.update(
                        {
                            "symbol": symbol,
                            "cik": cik,
                            "accession_number": filing.get("accession_number"),
                            "form": filing.get("form"),
                            "source_valid_from": filing.get("valid_from"),
                            "source_published_at": filing.get("published_at"),
                            "publication_stage": filing.get("publication_stage"),
                            "source_document_file": filing.get("document_file", {}).get("path"),
                        }
                    )
                validate_anchor_contract(
                    filing_anchors,
                    required_fields=contract["anchor_output_fields"],
                )
                anchors.extend(filing_anchors)
                filings_processed += 1
            except Exception as exc:
                filing_errors.append(
                    {
                        "symbol": symbol,
                        "cik": cik,
                        "accession_number": filing.get("accession_number"),
                        "error_type": type(exc).__name__,
                        "error_message": str(exc)[:1000],
                    }
                )

    coverage = anchor_coverage(anchors)
    coverage["filings_processed"] = filings_processed
    coverage["filing_error_count"] = len(filing_errors)
    payload = {
        "schema_version": "external_evidence_8c_content_anchors_v1",
        "phase": "8C_G_content_parser_foundation",
        "snapshot_summary": snapshot_summary,
        "scanner_as_of": manifest.get("scanner_as_of"),
        "source_authority": manifest.get("source_authority"),
        "transport_mode": "OFFLINE_VERIFIED_SEC_SNAPSHOT",
        "parser_version": contract["parser_version"],
        "semantic_classification": "NOT_RUN",
        "market_outcomes_read": False,
        "market_direction_assigned": False,
        "anchors": anchors,
        "filing_errors": filing_errors,
        "coverage": coverage,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return payload


def self_test() -> dict[str, Any]:
    contract = load_contract()
    sample = b"""<SEC-DOCUMENT>\n<DOCUMENT>\n<TYPE>8-K\n<SEQUENCE>1\n<FILENAME>form8-k.htm\n<DESCRIPTION>CURRENT REPORT\n<TEXT><html><body>Company outlook remains unchanged.</body></html></TEXT>\n</DOCUMENT>\n<DOCUMENT>\n<TYPE>EX-99.1\n<SEQUENCE>2\n<FILENAME>press.htm\n<DESCRIPTION>PRESS RELEASE\n<TEXT><html><body>The board declared a quarterly dividend of $0.30 per share and approved a share repurchase program.</body></html></TEXT>\n</DOCUMENT>\n</SEC-DOCUMENT>"""
    docs = split_sec_documents(sample)
    policy = contract["document_policy"]
    anchors = extract_evidence_anchors(
        docs,
        anchor_families=contract["evidence_anchor_families"],
        parser_version=contract["parser_version"],
        excluded_type_prefixes=policy["exclude_document_type_prefixes"],
        minimum_visible_characters=policy["minimum_visible_characters"],
    )
    validate_anchor_contract(anchors, required_fields=contract["anchor_output_fields"])
    return {
        "document_count": len(docs),
        "families": sorted({row["family"] for row in anchors}),
        "anchor_count": len(anchors),
        "semantic_classification": "NOT_RUN",
        "market_outcomes_read": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract Phase 8C-G literal evidence anchors from verified SEC filing content."
    )
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--snapshot-dir")
    parser.add_argument(
        "--output",
        default=str(
            ROOT
            / "artifacts"
            / "research"
            / "external_evidence_8c_content_anchors.json"
        ),
    )
    args = parser.parse_args()

    if args.self_test:
        print(json.dumps(self_test(), indent=2, sort_keys=True))
        return 0
    if not args.snapshot_dir:
        parser.error("--snapshot-dir is required; 8C-G makes no live SEC calls")

    result = run(snapshot_dir=Path(args.snapshot_dir), output_path=Path(args.output))
    print(json.dumps(result["coverage"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
