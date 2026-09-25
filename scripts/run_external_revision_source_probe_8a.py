#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "configs" / "external_revision_source_probe_v1.json"


def evaluate(config: dict) -> dict:
    candidates = config["candidates"]
    eligible = [c["source_id"] for c in candidates if c.get("retrospective_8B_eligible") is True]
    decision = "OPEN_8B" if eligible else "PROCEED_8C"
    return {
        "schema_version": config["schema_version"],
        "as_of": config["as_of"],
        "eligible_retrospective_8B_sources": eligible,
        "candidate_verdicts": {
            c["source_id"]: {
                "verdict": c["verdict"],
                "reason_codes": c["reason_codes"],
            }
            for c in candidates
        },
        "decision": decision,
        "declared_decision": config["decision"],
        "decision_matches_contract": decision == config["decision"],
        "phase8b_status": config["phase8b_status"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate Phase 8A revision-source eligibility for retrospective 8B research.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    config = json.loads(args.config.read_text(encoding="utf-8"))
    result = evaluate(config)
    text = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0 if result["decision_matches_contract"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
