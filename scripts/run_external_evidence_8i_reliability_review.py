#!/usr/bin/env python3
"""Run the real metadata-only Phase 8I-E evaluation/readiness review."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from scanner.research.external_evidence.reliability_review_8i import build_real_review_status


def _load(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Start/re-run the real Phase 8I-E metadata-only review without opening "
            "peer_excess or any other forward outcome values."
        )
    )
    parser.add_argument(
        "--reliability-contract",
        default="configs/external_evidence_8i_reliability_extension_research_v1.json",
    )
    parser.add_argument(
        "--binding-contract",
        default="configs/external_evidence_8i_promotion_provenance_binding_v1.json",
    )
    parser.add_argument(
        "--split-manifest",
        default="artifacts/research/external_evidence_8g_split_manifest_v1.json",
    )
    parser.add_argument(
        "--holdout-ledger",
        default="artifacts/research/external_evidence_8g_holdout_consumption_v1.json",
    )
    parser.add_argument(
        "--source-identity-correction",
        default="configs/external_evidence_source_identity_correction_v1.json",
    )
    parser.add_argument(
        "--research-as-of",
        default=None,
        help="Timezone-aware ISO timestamp. Defaults to current UTC.",
    )
    parser.add_argument(
        "--output",
        default="artifacts/research/external_evidence_8i_evaluation_review_latest.json",
    )
    args = parser.parse_args()

    research_as_of = args.research_as_of or datetime.now(timezone.utc).isoformat()
    result = build_real_review_status(
        reliability_contract=_load(args.reliability_contract),
        binding_contract=_load(args.binding_contract),
        split_manifest=_load(args.split_manifest),
        holdout_ledger=_load(args.holdout_ledger),
        source_identity_correction=_load(args.source_identity_correction),
        research_as_of=research_as_of,
    )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({
        "state": result["state"],
        "evaluation_started": result["evaluation_started"],
        "review_started": result["review_started"],
        "blockers": result["blockers"],
        "real_outcomes_opened": result["real_outcomes_opened"],
        "output": str(output),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
