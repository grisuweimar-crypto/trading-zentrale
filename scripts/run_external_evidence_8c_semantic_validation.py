#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from scanner.research.external_evidence.semantic_validation import (
    deterministic_sample,
    evaluate_all,
    evaluate_anchor_audit,
)

ROOT = Path(__file__).resolve().parents[1]
SUPPORTED_FAMILIES = {"DIVIDEND", "BUYBACK"}


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _rows(payload: Any, key: str | None = None) -> list[dict[str, Any]]:
    if key is not None and isinstance(payload, dict):
        payload = payload.get(key)
    if not isinstance(payload, list):
        raise ValueError("Expected a JSON array of rows")
    return [dict(row) for row in payload]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run preregistered Phase 8C-I semantic challenger validation."
    )
    parser.add_argument("--candidates", required=True)
    parser.add_argument("--annotations", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--anchors",
        help="Optional full 8C-G anchor artifact for the preregistered independent anchor audit.",
    )
    parser.add_argument(
        "--anchor-annotations",
        help="Annotations for the independently sampled anchor audit. Required with --anchors.",
    )
    parser.add_argument(
        "--contract",
        default=str(ROOT / "configs" / "external_evidence_8c_semantic_validation_v1.json"),
    )
    parser.add_argument("--candidate-key", default="candidates")
    parser.add_argument("--anchor-key", default="anchors")
    args = parser.parse_args()

    if bool(args.anchors) != bool(args.anchor_annotations):
        parser.error("--anchors and --anchor-annotations must be supplied together")

    contract = _load_json(Path(args.contract))
    candidate_payload = _load_json(Path(args.candidates))
    candidates = _rows(
        candidate_payload,
        args.candidate_key if isinstance(candidate_payload, dict) else None,
    )
    annotations = _rows(_load_json(Path(args.annotations)))

    sample_cfg = contract["sampling"]["candidate_audit"]
    sampled = deterministic_sample(
        candidates,
        seed=str(contract["sampling"]["seed"]),
        per_family_max=int(sample_cfg["per_family_max"]),
    )
    result = evaluate_all(
        candidate_rows=sampled,
        annotations=annotations,
        contract=contract,
    )
    result["candidate_sample_count"] = len(sampled)

    if args.anchors:
        anchor_payload = _load_json(Path(args.anchors))
        anchors = _rows(
            anchor_payload,
            args.anchor_key if isinstance(anchor_payload, dict) else None,
        )
        eligible_anchors = [
            row
            for row in anchors
            if str(row.get("family") or "").upper() in SUPPORTED_FAMILIES
        ]
        anchor_cfg = contract["sampling"]["anchor_audit"]
        anchor_sample = deterministic_sample(
            eligible_anchors,
            seed=f"{contract['sampling']['seed']}|ANCHOR_AUDIT",
            per_family_max=int(anchor_cfg["per_family_max"]),
        )
        anchor_annotations = _rows(_load_json(Path(args.anchor_annotations)))
        result["anchor_audit"] = evaluate_anchor_audit(
            anchor_rows=anchor_sample,
            annotations=anchor_annotations,
            candidate_rows=candidates,
        )
        result["anchor_audit_sample_count"] = len(anchor_sample)
    else:
        result["anchor_audit"] = {
            "status": "NOT_RUN",
            "reason": "ANCHOR_AUDIT_INPUTS_NOT_SUPPLIED",
            "promotion_effect": "NONE",
        }

    result["validation_status"] = "EVALUATED_WITHOUT_MARKET_OUTCOMES"

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    print(
        json.dumps(
            {
                "candidate_sample_count": len(sampled),
                "families": [
                    {
                        "family": row["family"],
                        "promotion_status": row["promotion_status"],
                    }
                    for row in result["families"]
                ],
                "anchor_audit_sample_count": result.get("anchor_audit_sample_count", 0),
                "anchor_audit_recall": [
                    {
                        "family": row["family"],
                        "descriptive_recall": row["descriptive_recall"],
                        "false_negative_count": row["false_negative_count"],
                    }
                    for row in (result.get("anchor_audit") or {}).get("families", [])
                ],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
