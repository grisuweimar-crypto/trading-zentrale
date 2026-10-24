from __future__ import annotations

import csv
import hashlib
import io
import json
import zipfile
from pathlib import Path

import pytest

from scanner.research.external_evidence.sec_insider_raw_audit import (
    SecInsiderRawAuditError,
    build_raw_audit_pack,
    write_blind_annotation_template,
)


def _tsv(rows: list[dict[str, str]], fields: list[str]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields, delimiter="\t", lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode()


def _zip(path: Path) -> str:
    submission = [{"ACCESSION_NUMBER": "0000000001-26-000001", "ISSUERCIK": "1", "DOCUMENT_TYPE": "4", "AFF10B5ONE": "0"}]
    owner = [{"ACCESSION_NUMBER": "0000000001-26-000001", "RPTOWNERCIK": "99", "RPTOWNERNAME": "Owner"}]
    tx = [{"ACCESSION_NUMBER": "0000000001-26-000001", "NONDERIV_TRANS_SK": "7", "TRANS_CODE": "P", "TRANS_ACQUIRED_DISP_CD": "A", "TRANS_DATE": "01-APR-2026", "TRANS_SHARES": "10", "TRANS_PRICEPERSHARE": "12.5"}]
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("SUBMISSION.tsv", _tsv(submission, list(submission[0])))
        zf.writestr("REPORTINGOWNER.tsv", _tsv(owner, list(owner[0])))
        zf.writestr("NONDERIV_TRANS.tsv", _tsv(tx, list(tx[0])))
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _package(path: Path, zip_sha: str) -> None:
    payload = {
        "schema_version": "external_evidence_8d_insider_validation_package_v1",
        "source_quarter": "2026Q2",
        "source_zip_sha256": zip_sha,
        "guards": {
            "market_outcomes_read": False,
            "market_direction_assigned": False,
            "threshold_selection_run": False,
            "phase7_integration_enabled": False,
            "production_external_evidence_enabled": False,
        },
        "rows": [{
            "validation_id": "v1",
            "accession_number": "0000000001-26-000001",
            "transaction_key": "7",
            "candidate_status": "SHOULD_NOT_APPEAR_IN_RAW_PACK",
            "reason_codes": ["SHOULD_NOT_APPEAR"],
        }],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_raw_pack_is_parser_blind_and_exact(tmp_path: Path) -> None:
    zip_path = tmp_path / "q2.zip"
    package_path = tmp_path / "package.json"
    _package(package_path, _zip(zip_path))
    result = build_raw_audit_pack(package_path=package_path, insider_zip_path=zip_path)
    assert result["row_count"] == 1
    assert result["guards"]["parser_candidate_status_exposed_to_annotator"] is False
    row = result["rows"][0]
    assert set(row) == {"validation_id", "raw_submission", "raw_reporting_owners", "raw_nonderiv_trans"}
    assert row["raw_nonderiv_trans"]["TRANS_CODE"] == "P"
    assert row["raw_submission"]["ISSUERCIK"] == "1"

    blind = tmp_path / "blind.csv"
    write_blind_annotation_template(package_path=package_path, output_path=blind)
    with blind.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["validation_id"] == "v1"
    assert "candidate_status" not in rows[0]
    assert rows[0]["market_outcomes_seen"] == "FALSE"


def test_raw_pack_rejects_wrong_zip_hash(tmp_path: Path) -> None:
    zip_path = tmp_path / "q2.zip"
    _zip(zip_path)
    package_path = tmp_path / "package.json"
    _package(package_path, "0" * 64)
    with pytest.raises(SecInsiderRawAuditError, match="SHA-256"):
        build_raw_audit_pack(package_path=package_path, insider_zip_path=zip_path)
