from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.external_evidence.exposure_domain_8f import build_exposure_domain_audit


DEFAULT_DOMAIN_CONFIG = Path("configs/external_evidence_8f_research_domain_v1.json")
DEFAULT_EXPOSURE_MAP = Path("configs/external_evidence_8f_exposure_map_v1.json")


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the outcome-blind Phase 8F exposure-domain coverage audit.")
    parser.add_argument("--domain-config", type=Path, default=DEFAULT_DOMAIN_CONFIG)
    parser.add_argument("--exposure-map", type=Path, default=DEFAULT_EXPOSURE_MAP)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    result = build_exposure_domain_audit(
        domain_config=_load_json(args.domain_config),
        exposure_map=_load_json(args.exposure_map),
    )
    text = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
