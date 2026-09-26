#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from scanner.research.external_evidence.sec_history import (
    assemble_full_submission_history,
    historical_submission_file_specs,
)
from scanner.research.external_evidence.sec_snapshot import (
    load_snapshot_manifest,
    read_json_verified,
    validate_snapshot_bundle,
)
from scanner.research.external_evidence.structured_events import (
    build_event_candidates,
    event_coverage,
    validate_event_contract,
)

ROOT = Path(__file__).resolve().parents[1]


def load_contract() -> dict[str, Any]:
    return json.loads(
        (ROOT / "configs" / "external_evidence_8c_structured_events_v1.json").read_text(
            encoding="utf-8"
        )
    )


def run(*, snapshot_dir: Path, output_path: Path) -> dict[str, Any]:
    contract = load_contract()
    snapshot_summary = validate_snapshot_bundle(snapshot_dir)
    manifest = load_snapshot_manifest(snapshot_dir)
    events: list[dict[str, Any]] = []
    companies: list[dict[str, Any]] = []

    for company in manifest["companies"]:
        symbol = str(company.get("symbol") or "").strip().upper()
        identity_status = str(company.get("identity_status") or "UNKNOWN")
        if identity_status != "VERIFIED_BY_SEC_SUBMISSIONS":
            companies.append(
                {
                    "symbol": symbol,
                    "cik": company.get("cik"),
                    "identity_status": identity_status,
                    "processing_status": "SKIPPED_UNVERIFIED_IDENTITY",
                    "event_count": 0,
                    "reason_codes": list(company.get("reason_codes") or []),
                }
            )
            continue

        primary = read_json_verified(snapshot_dir, company["submissions_file"])
        history_specs_by_name = {
            Path(str(spec.get("path") or "")).name: spec
            for spec in (company.get("history_files") or [])
            if isinstance(spec, dict)
        }
        historical_payloads: dict[str, dict[str, Any]] = {}
        for required in historical_submission_file_specs(primary):
            spec = history_specs_by_name.get(required["name"])
            if spec is None:
                raise ValueError(
                    f"Snapshot missing referenced history file for {symbol}: {required['name']}"
                )
            historical_payloads[required["name"]] = read_json_verified(snapshot_dir, spec)

        history = assemble_full_submission_history(
            primary,
            historical_payloads=historical_payloads,
            forms=contract["allowed_forms"],
            require_complete=True,
        )
        company_events = build_event_candidates(
            history["rows"],
            item_mapping=contract["event_item_mapping"],
            allowed_forms=contract["allowed_forms"],
        )
        validate_event_contract(
            company_events,
            required_fields=contract["required_output_fields"],
        )
        for event in company_events:
            event["symbol"] = symbol
            event["sec_title"] = company.get("sec_title")
        events.extend(company_events)
        companies.append(
            {
                "symbol": symbol,
                "cik": company.get("cik"),
                "identity_status": identity_status,
                "processing_status": "SUCCESS",
                "event_count": len(company_events),
                "history_research_ready": history["coverage"].get("research_ready"),
                "reason_codes": list(history["coverage"].get("reason_codes") or []),
            }
        )

    payload = {
        "schema_version": "external_evidence_8c_structured_events_artifact_v1",
        "phase": "8C_D_structured_events",
        "snapshot_summary": snapshot_summary,
        "scanner_as_of": manifest.get("scanner_as_of"),
        "source_authority": manifest.get("source_authority"),
        "transport_mode": "OFFLINE_VERIFIED_SEC_SNAPSHOT",
        "semantic_layer": "FILING_METADATA_ONLY",
        "market_outcomes_read": False,
        "direction_assigned": False,
        "content_semantic_enrichment": "NOT_RUN",
        "companies": companies,
        "events": events,
        "coverage": event_coverage(events),
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return payload


def self_test() -> dict[str, Any]:
    contract = load_contract()
    rows = [
        {
            "source": "sec_edgar_submissions_8k_6k",
            "cik": "0000000001",
            "accession_number": "0001-26-000001",
            "form": "8-K",
            "items": ["2.02", "7.01"],
            "published_at": "2026-09-25T12:00:00+00:00",
            "valid_from": "2026-09-25T12:00:00+00:00",
            "revision_id": "0001-26-000001",
            "publication_stage": "CURRENT_REPORT",
            "status": "KNOWN",
            "reason_codes": [],
        }
    ]
    events = build_event_candidates(
        rows,
        item_mapping=contract["event_item_mapping"],
        allowed_forms=contract["allowed_forms"],
    )
    validate_event_contract(events, required_fields=contract["required_output_fields"])
    return {
        "event_types": [event["candidate_event_type"] for event in events],
        "all_directions_unknown": event_coverage(events)["all_directions_unknown"],
        "market_outcomes_read": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build Phase 8C-D non-directional structured events from a verified SEC snapshot."
    )
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--snapshot-dir")
    parser.add_argument(
        "--output",
        default=str(
            ROOT
            / "artifacts"
            / "research"
            / "external_evidence_8c_structured_events.json"
        ),
    )
    args = parser.parse_args()

    if args.self_test:
        print(json.dumps(self_test(), indent=2, sort_keys=True))
        return 0
    if not args.snapshot_dir:
        parser.error("--snapshot-dir is required; Phase 8C-D makes no live SEC calls")

    result = run(snapshot_dir=Path(args.snapshot_dir), output_path=Path(args.output))
    print(json.dumps(result["coverage"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
