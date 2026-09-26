from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.external_evidence.completion_8f import evaluate_phase8f_completion


DEFAULT_COMPLETION_CONFIG = Path("configs/external_evidence_8f_completion_v1.json")
DEFAULT_MACRO_CONFIG = Path("configs/external_evidence_8f_macro_exposure_v1.json")
DEFAULT_EXPOSURE_MAP = Path("configs/external_evidence_8f_exposure_map_v1.json")


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate the fail-closed Phase 8F completion/freeze gate.")
    parser.add_argument("--completion-config", type=Path, default=DEFAULT_COMPLETION_CONFIG)
    parser.add_argument("--macro-config", type=Path, default=DEFAULT_MACRO_CONFIG)
    parser.add_argument("--exposure-map", type=Path, default=DEFAULT_EXPOSURE_MAP)
    parser.add_argument("--macro-ledger", type=Path)
    parser.add_argument("--exposure-domain-audit", type=Path)
    parser.add_argument("--require-pass", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    result = evaluate_phase8f_completion(
        completion_config=_load_json(args.completion_config),
        macro_config=_load_json(args.macro_config),
        exposure_map=_load_json(args.exposure_map),
        macro_ledger=_load_json(args.macro_ledger) if args.macro_ledger else None,
        exposure_domain_audit=(
            _load_json(args.exposure_domain_audit) if args.exposure_domain_audit else None
        ),
    )

    text = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")

    if args.require_pass and result["status"] != "PASS_8F_COMPLETION":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
