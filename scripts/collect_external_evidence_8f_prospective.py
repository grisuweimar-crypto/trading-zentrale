from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.external_evidence.prospective_collector_8f import collect_phase8f_prospective


DEFAULT_MACRO_CONFIG = Path("configs/external_evidence_8f_macro_exposure_v1.json")
DEFAULT_LEDGER = Path("artifacts/research/external_evidence_8f_macro_ledger.json")
DEFAULT_RAW_DIR = Path("artifacts/research/external_evidence_8f_raw")
DEFAULT_MANIFEST = Path("artifacts/research/external_evidence_8f_collection_latest.json")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Collect an outcome-blind prospective Phase 8F macro snapshot from official sources."
    )
    parser.add_argument("--macro-config", type=Path, default=DEFAULT_MACRO_CONFIG)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    args = parser.parse_args()

    macro_config = json.loads(args.macro_config.read_text(encoding="utf-8"))
    result = collect_phase8f_prospective(
        macro_config=macro_config,
        existing_ledger_path=args.ledger,
        output_ledger_path=args.ledger,
        raw_snapshot_dir=args.raw_dir,
    )

    args.manifest.parent.mkdir(parents=True, exist_ok=True)
    args.manifest.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
