#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from scanner.research.external_evidence.semantic_validation import (
    deterministic_sample,
    evaluate_all,
)

ROOT = Path(__file__).resolve().parents[1]


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _rows(payload: Any, key: str | None = None) -> list[dict[str, Any]]:
    if key is not None and isinstance(payload, dict):
        payload = payload.get(key)
    if not isinstance(payload, list):
        raise ValueError("Expected a JSON array of rows")
    return [dict(row) for row in payload]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run preregistered Phase 8C-I semantic challenger validation.")
    parser.add_argument("--candidates", required=True)
    parser.add_argument("--annotations", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--contract",
        default=str(ROOT / "configs" / "external_evidence_8c_semantic_validation_v1.json"),
    )
    parser.add_argument("--candidate-key", default="candidates")
    args = parser.parse_args()

    contract = _load_json(Path(args.contract))
    candidate_payload = _load_json(Path(args.candidates))
    candidates = _rows(candidate_payload, args.candidate_key if isinstance(candidate_payload, dict) else None)
    annotations = _rows(_load_json(Path(args.annotations)))

    sample_cfg = contract["sampling"]["candidate_audit"]
    sampled = deterministic_sample(
        candidates,
        seed=str(contract["sampling"]["seed"]),
        per_family_max=int(sample_cfg["per_family_max"]),
    )
    result = evaluate_all(candidate_rows=sampled, annotations=annotations, contract=contract)
    result["candidate_sample_count"] = len(sampled)
    result["validation_status"] = "EVALUATED_WITHOUT_MARKET_OUTCOMES"

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({
        "candidate_sample_count": len(sampled),
        "families": [{"family": r["family"], "promotion_status": r["promotion_status"]} for r in result["families"]],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
