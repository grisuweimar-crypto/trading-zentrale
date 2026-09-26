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
from scanner.research.external_evidence.semantic_validation import (
    anchor_id,
    deterministic_sample,
)

ROOT = Path(__file__).resolve().parents[1]
SUPPORTED_FAMILIES = {"DIVIDEND", "BUYBACK"}


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _annotation_stub(row: dict[str, Any]) -> dict[str, Any]:
    family = str(row.get("family") or "").upper()
    stub: dict[str, Any] = {
        "anchor_id": anchor_id(row),
        "family": family,
        "truth_label": None,
        "review_status": "PENDING",
        "market_outcomes_seen": False,
        "notes": "",
    }
    if family == "DIVIDEND":
        stub.update(
            {
                "truth_amount_per_share": None,
                "truth_currency": None,
                "truth_security_class": None,
                "truth_frequency": None,
            }
        )
    elif family == "BUYBACK":
        stub.update(
            {
                "truth_action": None,
                "truth_authorization_amount": None,
                "truth_currency": None,
            }
        )
    return stub


def _candidate_review_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "anchor_id": anchor_id(row),
        "audit_kind": "CANDIDATE_PRECISION_FIELD_ACCURACY",
        "family": row.get("family"),
        "symbol": row.get("symbol"),
        "cik": row.get("cik"),
        "accession_number": row.get("accession_number"),
        "source_valid_from": row.get("source_valid_from"),
        "source_document": row.get("source_document"),
        "evidence_excerpt": row.get("evidence_excerpt"),
        "observed": {
            "event_type": row.get("event_type"),
            "action": row.get("action"),
            "amount_per_share": row.get("amount_per_share"),
            "authorization_amount": row.get("authorization_amount"),
            "currency": row.get("currency"),
            "security_class": row.get("security_class"),
            "frequency": row.get("frequency"),
            "matched_rule": row.get("matched_rule"),
            "reason_codes": row.get("reason_codes"),
        },
        "annotation": _annotation_stub(row),
    }


def _anchor_review_row(row: dict[str, Any], emitted_ids: set[str]) -> dict[str, Any]:
    cid = anchor_id(row)
    return {
        "anchor_id": cid,
        "audit_kind": "INDEPENDENT_ANCHOR_FALSE_NEGATIVE_REVIEW",
        "family": row.get("family"),
        "symbol": row.get("symbol"),
        "cik": row.get("cik"),
        "accession_number": row.get("accession_number"),
        "source_valid_from": row.get("source_valid_from"),
        "filename": row.get("filename"),
        "document_type": row.get("document_type"),
        "matched_phrase": row.get("matched_phrase"),
        "excerpt": row.get("excerpt"),
        "challenger_emitted": cid in emitted_ids,
        "annotation": _annotation_stub(row),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run the frozen 8C-H challenger on real 8C-G anchors and prepare the "
            "preregistered deterministic 8C-I candidate and independent-anchor audit packets."
        )
    )
    parser.add_argument(
        "--anchors",
        default=str(ROOT / "artifacts" / "research" / "external_evidence_8c_content_anchors.json"),
    )
    parser.add_argument(
        "--challenger-contract",
        default=str(ROOT / "configs" / "external_evidence_8c_semantic_challenger_v1.json"),
    )
    parser.add_argument(
        "--validation-contract",
        default=str(ROOT / "configs" / "external_evidence_8c_semantic_validation_v1.json"),
    )
    parser.add_argument(
        "--challenger-output",
        default=str(ROOT / "artifacts" / "research" / "external_evidence_8c_semantic_challenger.json"),
    )
    parser.add_argument(
        "--audit-dir",
        default=str(ROOT / "artifacts" / "external_evidence" / "8c_i_real_audit"),
    )
    args = parser.parse_args()

    anchor_payload = _load_json(Path(args.anchors))
    if anchor_payload.get("schema_version") != "external_evidence_8c_content_anchors_v1":
        raise ValueError("Unsupported 8C-G anchor artifact schema")
    if anchor_payload.get("market_outcomes_read") is not False:
        raise ValueError("8C-I refuses anchors contaminated by market outcomes")
    if anchor_payload.get("market_direction_assigned") is not False:
        raise ValueError("8C-I refuses anchors with market direction assigned")
    all_anchors = anchor_payload.get("anchors")
    if not isinstance(all_anchors, list):
        raise ValueError("Anchor artifact requires anchors array")
    eligible_anchors = [
        dict(row)
        for row in all_anchors
        if str((row or {}).get("family") or "").upper() in SUPPORTED_FAMILIES
    ]

    challenger_contract = _load_json(Path(args.challenger_contract))
    validation_contract = _load_json(Path(args.validation_contract))
    challenger = extract_semantic_candidates(eligible_anchors, contract=challenger_contract)
    validate_semantic_candidates(
        challenger["candidates"],
        required_fields=challenger_contract["required_output_fields"],
    )

    family_counts: dict[str, int] = {}
    comparison_eligible_dividends = 0
    for row in challenger["candidates"]:
        family = str(row.get("family") or "UNKNOWN")
        family_counts[family] = family_counts.get(family, 0) + 1
        if family == "DIVIDEND" and row.get("comparison_eligible") is True:
            comparison_eligible_dividends += 1

    challenger_payload = {
        "schema_version": "external_evidence_8c_semantic_challenger_artifact_v1",
        "phase": "8C_H_semantic_challenger",
        "source_anchor_schema": anchor_payload.get("schema_version"),
        "source_parser_version": anchor_payload.get("parser_version"),
        "scanner_as_of": anchor_payload.get("scanner_as_of"),
        "parser_version": challenger_contract["parser_version"],
        "status": "CHALLENGER_ONLY_NOT_PRODUCTION_EVIDENCE",
        "market_outcomes_read": False,
        "market_direction_assigned": False,
        "candidates": challenger["candidates"],
        "rejections": challenger["rejections"],
        "coverage": {
            "candidate_count": challenger["candidate_count"],
            "rejection_count": challenger["rejection_count"],
            "family_counts": dict(sorted(family_counts.items())),
            "comparison_eligible_dividend_count": comparison_eligible_dividends,
            "guidance_semantics": "DISABLED",
            "capital_raise_semantics": "DISABLED",
            "earnings_beat_miss": "BLOCKED_BY_8B_CONSENSUS_DEPENDENCY",
            "outcome_research": "NOT_RUN",
        },
    }
    challenger_output = Path(args.challenger_output)
    _write_json(challenger_output, challenger_payload)

    seed = str(validation_contract["sampling"]["seed"])
    candidate_cfg = validation_contract["sampling"]["candidate_audit"]
    anchor_cfg = validation_contract["sampling"]["anchor_audit"]
    candidate_sample = deterministic_sample(
        challenger["candidates"],
        seed=seed,
        per_family_max=int(candidate_cfg["per_family_max"]),
    )
    anchor_sample = deterministic_sample(
        eligible_anchors,
        seed=f"{seed}|ANCHOR_AUDIT",
        per_family_max=int(anchor_cfg["per_family_max"]),
    )
    emitted_ids = {anchor_id(row) for row in challenger["candidates"]}

    audit_dir = Path(args.audit_dir)
    candidate_packet = {
        "schema_version": "external_evidence_8c_i_candidate_audit_packet_v1",
        "sampling_seed": seed,
        "market_outcomes_visible": False,
        "rows": [_candidate_review_row(row) for row in candidate_sample],
    }
    anchor_packet = {
        "schema_version": "external_evidence_8c_i_anchor_audit_packet_v1",
        "sampling_seed": f"{seed}|ANCHOR_AUDIT",
        "market_outcomes_visible": False,
        "rows": [_anchor_review_row(row, emitted_ids) for row in anchor_sample],
    }
    _write_json(audit_dir / "candidate_audit_packet.json", candidate_packet)
    _write_json(audit_dir / "anchor_audit_packet.json", anchor_packet)

    summary = {
        "schema_version": "external_evidence_8c_i_real_audit_handoff_v1",
        "market_outcomes_read": False,
        "challenger_candidate_count": challenger["candidate_count"],
        "challenger_rejection_count": challenger["rejection_count"],
        "challenger_family_counts": dict(sorted(family_counts.items())),
        "eligible_anchor_count": len(eligible_anchors),
        "candidate_audit_sample_count": len(candidate_sample),
        "anchor_audit_sample_count": len(anchor_sample),
        "candidate_audit_family_counts": {
            family: sum(1 for row in candidate_sample if row.get("family") == family)
            for family in sorted(SUPPORTED_FAMILIES)
        },
        "anchor_audit_family_counts": {
            family: sum(1 for row in anchor_sample if row.get("family") == family)
            for family in sorted(SUPPORTED_FAMILIES)
        },
        "challenger_output": str(challenger_output),
        "candidate_audit_packet": str(audit_dir / "candidate_audit_packet.json"),
        "anchor_audit_packet": str(audit_dir / "anchor_audit_packet.json"),
        "production_external_evidence_enabled": False,
        "phase7_integration_enabled": False,
    }
    _write_json(audit_dir / "audit_handoff_summary.json", summary)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
