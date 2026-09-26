#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.external_evidence.sec_insider_raw_audit import (
    build_raw_audit_pack,
    write_blind_annotation_template,
    write_raw_audit_pack,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DIR = ROOT / "artifacts" / "external_evidence" / "8d_sec_insider"


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a blind raw-SEC evidence pack for the frozen 8D-B3 audit sample.")
    parser.add_argument("--package", default=str(DEFAULT_DIR / "validation_package_2026q2.json"))
    parser.add_argument("--insider-zip", default=str(DEFAULT_DIR / "2026q2_form345.zip"))
    parser.add_argument("--raw-pack", default=str(DEFAULT_DIR / "validation_source_evidence_2026q2.json"))
    parser.add_argument("--blind-annotations", default=str(DEFAULT_DIR / "validation_annotations_blind_2026q2.csv"))
    args = parser.parse_args()

    payload = build_raw_audit_pack(package_path=Path(args.package), insider_zip_path=Path(args.insider_zip))
    write_raw_audit_pack(payload, Path(args.raw_pack))
    write_blind_annotation_template(package_path=Path(args.package), output_path=Path(args.blind_annotations))
    print(json.dumps({
        "schema_version": payload["schema_version"],
        "row_count": payload["row_count"],
        "raw_pack": args.raw_pack,
        "blind_annotations": args.blind_annotations,
        "market_outcomes_read": payload["guards"]["market_outcomes_read"],
        "parser_candidate_status_exposed_to_annotator": payload["guards"]["parser_candidate_status_exposed_to_annotator"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
