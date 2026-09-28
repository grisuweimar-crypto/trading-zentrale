from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.external_evidence.exposure_mapping_candidates_8f import validate_mapping_candidates
from scanner.research.external_evidence.exposure_review_queue_8f import build_exposure_review_queue


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CANDIDATES = ROOT / "configs/external_evidence_8f_mapping_candidates_v1.json"
DEFAULT_MACRO = ROOT / "configs/external_evidence_8f_macro_exposure_v1.json"
DEFAULT_DOMAIN = ROOT / "configs/external_evidence_8f_research_domain_v1.json"
DEFAULT_UNIVERSE = ROOT / "data/inputs/universe_master.csv"


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate outcome-blind Phase 8F documentary mapping candidates.")
    parser.add_argument("--candidates", type=Path, default=DEFAULT_CANDIDATES)
    parser.add_argument("--macro-config", type=Path, default=DEFAULT_MACRO)
    parser.add_argument("--domain-config", type=Path, default=DEFAULT_DOMAIN)
    parser.add_argument("--universe", type=Path, default=DEFAULT_UNIVERSE)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    macro = _load_json(args.macro_config)
    domain = _load_json(args.domain_config)
    queue = build_exposure_review_queue(
        domain_config=domain,
        universe_csv=args.universe.read_text(encoding="utf-8"),
    )
    result = validate_mapping_candidates(
        _load_json(args.candidates),
        allowed_factor_ids={str(row["factor_id"]) for row in macro.get("factor_catalog") or []},
        allowed_subject_ids={str(row["subject_id"]) for row in queue["subjects"]},
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
