from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.external_evidence.exposure_mapping_review_8f import apply_human_mapping_review


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CANDIDATES = ROOT / "configs/external_evidence_8f_mapping_candidates_v1.json"
DEFAULT_REVIEW = ROOT / "configs/external_evidence_8f_mapping_review_decisions_v1.json"
DEFAULT_EXPOSURE = ROOT / "configs/external_evidence_8f_exposure_map_v1.json"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply explicit human review decisions to Phase 8F mapping candidates.")
    parser.add_argument("--candidates", type=Path, default=DEFAULT_CANDIDATES)
    parser.add_argument("--review", type=Path, default=DEFAULT_REVIEW)
    parser.add_argument("--exposure-map", type=Path, default=DEFAULT_EXPOSURE)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--write-exposure-map", action="store_true")
    args = parser.parse_args()

    result = apply_human_mapping_review(
        candidate_config=_load(args.candidates),
        review_config=_load(args.review),
        exposure_map=_load(args.exposure_map),
    )
    text = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")

    if args.write_exposure_map:
        args.exposure_map.write_text(
            json.dumps(result["exposure_map"], indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
