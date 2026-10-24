from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

from scanner.research.external_evidence.sec_insider_b3_finalize import finalize_b3_annotations


def _write_json(path: Path, payload: dict) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(payload, sort_keys=True).encode("utf-8")
    path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


def _bundle(tmp_path: Path) -> Path:
    root = tmp_path / "bundle"
    sub = {
        "cik": 320193,
        "name": "Example Corp",
        "tickers": ["EXM"],
        "exchanges": ["Nasdaq"],
        "filings": {
            "recent": {
                "accessionNumber": ["0000320193-26-000111"],
                "form": ["4"],
                "filingDate": ["2026-04-01"],
                "acceptanceDateTime": ["2026-04-01T16:30:00-04:00"],
                "reportDate": ["2026-04-01"],
                "primaryDocument": ["ownership.xml"],
            },
            "files": [],
        },
    }
    rel = "raw/companies/0000320193/submissions.json"
    sha = _write_json(root / rel, sub)
    manifest = {
        "schema_version": "external_evidence_8c_sec_bulk_bundle_v1",
        "source_mode": "DIRECT_SEC_BULK_OPERATOR_ATTESTED",
        "source_authority": "U.S. SEC EDGAR",
        "operator_attested_direct_sec_download": True,
        "market_outcomes_read": False,
        "companies": [
            {
                "symbol": "EXM",
                "cik": "0000320193",
                "identity_status": "VERIFIED_BY_SEC_BULK_SUBMISSIONS",
                "submissions_file": {"path": rel, "sha256": sha},
                "history_files": [],
            }
        ],
    }
    _write_json(root / "bulk_manifest.json", manifest)
    return root


def _fixture(tmp_path: Path, *, valid_from: str) -> tuple[Path, Path, Path]:
    package = {
        "schema_version": "external_evidence_8d_insider_validation_package_v1",
        "rows": [
            {
                "validation_id": "v1",
                "issuer_cik": "0000320193",
                "accession_number": "0000320193-26-000111",
                "document_type": "4",
                "transaction_key": "1",
                "transaction_date": "2026-04-01",
                "transaction_code": "P",
                "acquired_disposed_code": "A",
                "transaction_shares": 100.0,
                "transaction_price_per_share": 12.5,
                "valid_from": valid_from,
                "pit_status": "SAFE",
                "reporting_owners": [
                    {
                        "reporting_owner_cik": "0000123456",
                        "reporting_owner_name": "Jane Example",
                        "reporting_owner_relationship": "Officer",
                        "reporting_owner_title": "CFO",
                        "reporting_owner_other_text": "",
                    }
                ],
            }
        ],
    }
    package_path = tmp_path / "package.json"
    _write_json(package_path, package)

    raw = {
        "schema_version": "external_evidence_8d_insider_raw_audit_v1",
        "validation_package_sha256": hashlib.sha256(package_path.read_bytes()).hexdigest(),
        "guards": {
            "market_outcomes_read": False,
            "parser_candidate_status_exposed_to_annotator": False,
        },
        "rows": [
            {
                "validation_id": "v1",
                "raw_submission": {
                    "ACCESSION_NUMBER": "0000320193-26-000111",
                    "ISSUERCIK": "320193",
                    "AFF10B5ONE": "0",
                    "DOCUMENT_TYPE": "4",
                },
                "raw_reporting_owners": [
                    {
                        "ACCESSION_NUMBER": "0000320193-26-000111",
                        "RPTOWNERCIK": "123456",
                        "RPTOWNERNAME": "Jane Example",
                        "RPTOWNER_RELATIONSHIP": "Officer",
                        "RPTOWNER_TITLE": "CFO",
                        "RPTOWNER_TXT": "",
                    }
                ],
                "raw_nonderiv_trans": {
                    "ACCESSION_NUMBER": "0000320193-26-000111",
                    "NONDERIV_TRANS_SK": "1",
                    "TRANS_DATE": "01-APR-2026",
                    "TRANS_CODE": "P",
                    "TRANS_ACQUIRED_DISP_CD": "A",
                    "TRANS_SHARES": "100",
                    "TRANS_PRICEPERSHARE": "12.50",
                },
            }
        ],
    }
    raw_path = tmp_path / "raw.json"
    _write_json(raw_path, raw)

    annotations_path = tmp_path / "annotations.csv"
    fields = [
        "validation_id", "truth_label", "review_status", "market_outcomes_seen", "notes",
        "issuer_cik_correct", "accession_number_correct", "transaction_code_correct",
        "acquired_disposed_code_correct", "transaction_date_correct", "transaction_shares_correct",
        "transaction_price_correct_when_present", "valid_from_correct", "reporting_owner_link_correct",
    ]
    with annotations_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerow(
            {
                "validation_id": "v1",
                "truth_label": "VALID_P_TRANSACTION",
                "review_status": "REVIEWED",
                "market_outcomes_seen": "FALSE",
                "notes": "blind raw-source label",
            }
        )
    return package_path, raw_path, annotations_path


def _read_final(path: Path) -> dict[str, str]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return next(csv.DictReader(handle))


def test_b3_finalize_mechanically_confirms_raw_fields_and_valid_from(tmp_path: Path) -> None:
    package, raw, annotations = _fixture(
        tmp_path, valid_from="2026-04-01T16:30:00-04:00"
    )
    output = tmp_path / "final.csv"
    result = finalize_b3_annotations(
        package_path=package,
        raw_pack_path=raw,
        semantic_annotations_path=annotations,
        sec_bulk_bundle_dir=_bundle(tmp_path),
        output_annotations_path=output,
    )
    row = _read_final(output)
    assert result["row_count"] == 1
    assert row["truth_label"] == "VALID_P_TRANSACTION"
    for field in (
        "issuer_cik_correct",
        "accession_number_correct",
        "transaction_code_correct",
        "acquired_disposed_code_correct",
        "transaction_date_correct",
        "transaction_shares_correct",
        "transaction_price_correct_when_present",
        "valid_from_correct",
        "reporting_owner_link_correct",
    ):
        assert row[field] == "TRUE"


def test_b3_finalize_detects_wrong_valid_from_without_changing_truth_label(tmp_path: Path) -> None:
    package, raw, annotations = _fixture(
        tmp_path, valid_from="2026-04-02T00:00:00-04:00"
    )
    output = tmp_path / "final.csv"
    finalize_b3_annotations(
        package_path=package,
        raw_pack_path=raw,
        semantic_annotations_path=annotations,
        sec_bulk_bundle_dir=_bundle(tmp_path),
        output_annotations_path=output,
    )
    row = _read_final(output)
    assert row["truth_label"] == "VALID_P_TRANSACTION"
    assert row["valid_from_correct"] == "FALSE"
