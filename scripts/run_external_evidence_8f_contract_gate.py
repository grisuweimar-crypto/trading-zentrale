from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.external_evidence.macro_exposure_8f import validate_phase8f_contract


DEFAULT_MACRO_CONFIG = Path("configs/external_evidence_8f_macro_exposure_v1.json")
DEFAULT_EXPOSURE_MAP = Path("configs/external_evidence_8f_exposure_map_v1.json")


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate the outcome-blind Phase 8F macro-vintage and exposure-map foundation."
    )
    parser.add_argument("--macro-config", type=Path, default=DEFAULT_MACRO_CONFIG)
    parser.add_argument("--exposure-map", type=Path, default=DEFAULT_EXPOSURE_MAP)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    result = validate_phase8f_contract(
        _load_json(args.macro_config),
        _load_json(args.exposure_map),
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
