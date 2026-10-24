#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.external_evidence.sec_insider_b3_finalize import finalize_and_evaluate

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DIR = ROOT / "artifacts" / "external_evidence" / "8d_sec_insider"
DEFAULT_CONFIG = ROOT / "configs" / "external_evidence_8d_insider_validation_v1.json"
DEFAULT_BUNDLE = ROOT / "artifacts" / "external_evidence" / "sec_bulk_snapshot"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Finalize the frozen outcome-blind 8D-B3 audit by mechanically checking parser fields "
            "and PIT provenance against raw SEC evidence, then evaluate the preregistered gates."
        )
    )
    parser.add_argument("--package", default=str(DEFAULT_DIR / "validation_package_2026q2.json"))
    parser.add_argument("--raw-pack", default=str(DEFAULT_DIR / "validation_source_evidence_2026q2.json"))
    parser.add_argument(
        "--semantic-annotations",
        default=str(DEFAULT_DIR / "validation_annotations_blind_completed_2026q2.csv"),
    )
    parser.add_argument("--sec-bulk-bundle", default=str(DEFAULT_BUNDLE))
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument(
        "--final-annotations",
        default=str(DEFAULT_DIR / "validation_annotations_final_2026q2.csv"),
    )
    parser.add_argument(
        "--verification",
        default=str(DEFAULT_DIR / "validation_mechanical_checks_2026q2.json"),
    )
    parser.add_argument(
        "--result",
        default=str(DEFAULT_DIR / "validation_result_2026q2.json"),
    )
    args = parser.parse_args()

    verification, result = finalize_and_evaluate(
        package_path=Path(args.package),
        raw_pack_path=Path(args.raw_pack),
        semantic_annotations_path=Path(args.semantic_annotations),
        sec_bulk_bundle_dir=Path(args.sec_bulk_bundle),
        config_path=Path(args.config),
        output_annotations_path=Path(args.final_annotations),
        verification_path=Path(args.verification),
        result_path=Path(args.result),
    )
    print(
        json.dumps(
            {
                "mechanical_verification_status": verification["status"],
                "row_count": verification["row_count"],
                "check_counts": verification["check_counts"],
                "decision": result["decision"],
                "requirements": result["requirements"],
                "final_annotations": args.final_annotations,
                "verification": args.verification,
                "result": args.result,
                "market_outcomes_read": False,
                "phase7_integration_enabled": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
