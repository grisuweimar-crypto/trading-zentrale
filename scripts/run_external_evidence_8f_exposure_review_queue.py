from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.external_evidence.exposure_review_queue_8f import build_exposure_review_queue


DEFAULT_DOMAIN = Path("configs/external_evidence_8f_research_domain_v1.json")
DEFAULT_UNIVERSE = Path("data/inputs/universe_master.csv")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the Phase 8F outcome-blind exposure mapping review queue.")
    parser.add_argument("--domain-config", type=Path, default=DEFAULT_DOMAIN)
    parser.add_argument("--universe", type=Path, default=DEFAULT_UNIVERSE)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    domain = json.loads(args.domain_config.read_text(encoding="utf-8"))
    result = build_exposure_review_queue(
        domain_config=domain,
        universe_csv=args.universe.read_text(encoding="utf-8"),
    )
    text = json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
