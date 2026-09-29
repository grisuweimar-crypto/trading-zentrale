#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import requests

from scanner.research.external_evidence.sec_insider_bulk import (
    import_sec_insider_quarter,
    write_sec_insider_evidence,
)
from scanner.research.external_evidence.sec_insider_validation import (
    prepare_insider_validation_package,
    write_validation_package,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "configs" / "external_evidence_8d_insider_validation_v1.json"
DEFAULT_BUNDLE = ROOT / "artifacts" / "external_evidence" / "sec_bulk_snapshot"
DEFAULT_OUT = ROOT / "artifacts" / "external_evidence" / "8d_sec_insider"


class RealB3RunnerError(ValueError):
    pass


def _load_config(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != "external_evidence_8d_insider_validation_v1":
        raise RealB3RunnerError("unsupported B3 validation config")
    corpus = payload.get("real_validation_corpus") or {}
    if corpus.get("quarter") != "2026Q2":
        raise RealB3RunnerError("frozen first real B3 corpus must be 2026Q2")
    if corpus.get("pool_with_earlier_quarters") is not False:
        raise RealB3RunnerError("B3 first validation may not pool earlier quarters")
    return payload


def _download(url: str, destination: Path, user_agent: str, timeout: float) -> None:
    if not user_agent.strip() or "@" not in user_agent:
        raise RealB3RunnerError(
            "SEC download requires a descriptive User-Agent containing a reachable email address"
        )
    response = requests.get(
        url,
        headers={
            "User-Agent": user_agent.strip(),
            "Accept": "application/zip,application/octet-stream,*/*",
            "Accept-Encoding": "gzip, deflate",
        },
        timeout=timeout,
    )
    if response.status_code != 200:
        raise RealB3RunnerError(
            f"SEC Q2 ZIP download failed HTTP {response.status_code}: {response.text[:500]}"
        )
    raw = bytes(response.content or b"")
    if len(raw) < 4 or raw[:2] != b"PK":
        raise RealB3RunnerError("SEC response is not a ZIP archive")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(raw)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run the frozen outcome-blind Phase 8D-B3 2026Q2 SEC insider validation preparation: "
            "obtain the official quarterly ZIP, run B1/B2, then create the deterministic B3 audit package."
        )
    )
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--sec-bulk-bundle", default=str(DEFAULT_BUNDLE))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUT))
    parser.add_argument(
        "--insider-zip",
        default=None,
        help="Existing official 2026Q2 SEC insider ZIP. If omitted, the runner downloads the frozen source URL.",
    )
    parser.add_argument(
        "--user-agent",
        default=None,
        help="Required only for automatic SEC download; example: 'Trading-Zentrale name@example.com'.",
    )
    parser.add_argument("--timeout", type=float, default=120.0)
    args = parser.parse_args()

    config_path = Path(args.config)
    config = _load_config(config_path)
    corpus = config["real_validation_corpus"]
    source_url = str(corpus["source_url"])
    quarter = str(corpus["quarter"])
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.insider_zip:
        insider_zip = Path(args.insider_zip)
        if not insider_zip.is_file():
            raise RealB3RunnerError(f"insider ZIP not found: {insider_zip}")
    else:
        insider_zip = output_dir / "2026q2_form345.zip"
        if not insider_zip.is_file():
            if not args.user_agent:
                raise RealB3RunnerError(
                    "--user-agent is required when --insider-zip is not supplied"
                )
            _download(source_url, insider_zip, args.user_agent, args.timeout)

    evidence_path = output_dir / "sec_insider_evidence_2026q2.json"
    package_path = output_dir / "validation_package_2026q2.json"
    annotations_path = output_dir / "validation_annotations_2026q2.csv"

    evidence = import_sec_insider_quarter(
        insider_zip_path=insider_zip,
        sec_bulk_bundle_dir=Path(args.sec_bulk_bundle),
        source_url=source_url,
        quarter_label=quarter,
    )
    write_sec_insider_evidence(evidence, evidence_path)

    package = prepare_insider_validation_package(
        evidence_path=evidence_path,
        config_path=config_path,
    )
    write_validation_package(
        package,
        json_path=package_path,
        annotation_csv_path=annotations_path,
    )

    print(
        json.dumps(
            {
                "quarter": quarter,
                "source_url": source_url,
                "evidence_counts": evidence["counts"],
                "candidate_status_counts": evidence["candidate_status_counts"],
                "validation_population": package["population"],
                "validation_sample": package["sample"],
                "evidence": str(evidence_path),
                "validation_package": str(package_path),
                "annotation_csv": str(annotations_path),
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
