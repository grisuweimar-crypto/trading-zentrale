#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.external_evidence.sec_insider_validation import (
    evaluate_insider_annotations,
    prepare_insider_validation_package,
    write_validation_package,
    write_validation_result,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "configs" / "external_evidence_8d_insider_validation_v1.json"
DEFAULT_EVIDENCE = (
    ROOT
    / "artifacts"
    / "external_evidence"
    / "8d_sec_insider"
    / "sec_insider_evidence.json"
)
DEFAULT_PACKAGE = (
    ROOT
    / "artifacts"
    / "external_evidence"
    / "8d_sec_insider"
    / "validation_package.json"
)
DEFAULT_ANNOTATIONS = (
    ROOT
    / "artifacts"
    / "external_evidence"
    / "8d_sec_insider"
    / "validation_annotations.csv"
)
DEFAULT_RESULT = (
    ROOT
    / "artifacts"
    / "external_evidence"
    / "8d_sec_insider"
    / "validation_result.json"
)


def _prepare(args: argparse.Namespace) -> int:
    package = prepare_insider_validation_package(
        evidence_path=Path(args.evidence),
        config_path=Path(args.config),
    )
    write_validation_package(
        package,
        json_path=Path(args.package),
        annotation_csv_path=Path(args.annotations),
    )
    print(
        json.dumps(
            {
                "schema_version": package["schema_version"],
                "status": package["status"],
                "source_quarter": package.get("source_quarter"),
                "population": package["population"],
                "sample": package["sample"],
                "package": args.package,
                "annotations": args.annotations,
                "market_outcomes_read": package["guards"]["market_outcomes_read"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _evaluate(args: argparse.Namespace) -> int:
    result = evaluate_insider_annotations(
        package_path=Path(args.package),
        annotation_csv_path=Path(args.annotations),
        config_path=Path(args.config),
    )
    write_validation_result(result, Path(args.result))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Prepare or evaluate the pre-registered outcome-blind Phase 8D-B3 "
            "SEC insider semantic/provenance audit."
        )
    )
    sub = parser.add_subparsers(dest="command", required=True)

    prepare = sub.add_parser("prepare", help="Create deterministic audit package and annotation CSV")
    prepare.add_argument("--evidence", default=str(DEFAULT_EVIDENCE))
    prepare.add_argument("--config", default=str(DEFAULT_CONFIG))
    prepare.add_argument("--package", default=str(DEFAULT_PACKAGE))
    prepare.add_argument("--annotations", default=str(DEFAULT_ANNOTATIONS))
    prepare.set_defaults(func=_prepare)

    evaluate = sub.add_parser("evaluate", help="Evaluate completed human annotations")
    evaluate.add_argument("--package", default=str(DEFAULT_PACKAGE))
    evaluate.add_argument("--annotations", default=str(DEFAULT_ANNOTATIONS))
    evaluate.add_argument("--config", default=str(DEFAULT_CONFIG))
    evaluate.add_argument("--result", default=str(DEFAULT_RESULT))
    evaluate.set_defaults(func=_evaluate)

    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
