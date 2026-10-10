from __future__ import annotations

import csv
import hashlib
import io
import json
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping

PACKAGE_SCHEMA = "external_evidence_8d_insider_validation_package_v1"
RAW_AUDIT_SCHEMA = "external_evidence_8d_insider_raw_audit_v1"


class SecInsiderRawAuditError(ValueError):
    """Raised when a blind B3 raw-source audit pack cannot be constructed safely."""


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SecInsiderRawAuditError(f"cannot read JSON object: {path}") from exc
    if not isinstance(payload, dict):
        raise SecInsiderRawAuditError(f"JSON root must be an object: {path}")
    return payload


def _find_member(zf: zipfile.ZipFile, stem: str) -> str:
    target = stem.upper()
    matches = [
        name
        for name in zf.namelist()
        if not name.endswith("/") and Path(Path(name).name).stem.upper() == target
    ]
    if len(matches) != 1:
        raise SecInsiderRawAuditError(
            f"expected exactly one {stem} table in SEC insider ZIP, found {len(matches)}"
        )
    return matches[0]


def _read_tsv(zf: zipfile.ZipFile, member: str) -> tuple[list[dict[str, str]], str]:
    raw = zf.read(member)
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise SecInsiderRawAuditError(f"cannot decode SEC insider table {member}") from exc
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    if not reader.fieldnames:
        raise SecInsiderRawAuditError(f"SEC insider table has no header: {member}")
    return [dict(row) for row in reader], _sha256_bytes(raw)


def build_raw_audit_pack(*, package_path: Path, insider_zip_path: Path) -> dict[str, Any]:
    package = _load_json(package_path)
    if package.get("schema_version") != PACKAGE_SCHEMA:
        raise SecInsiderRawAuditError("unsupported B3 validation package schema")
    guards = package.get("guards") or {}
    for field in (
        "market_outcomes_read",
        "market_direction_assigned",
        "threshold_selection_run",
        "phase7_integration_enabled",
        "production_external_evidence_enabled",
    ):
        if guards.get(field) is not False:
            raise SecInsiderRawAuditError(f"validation package guard {field} must be false")
    if not insider_zip_path.is_file():
        raise SecInsiderRawAuditError(f"SEC insider ZIP not found: {insider_zip_path}")

    zip_sha = _sha256_file(insider_zip_path)
    expected_zip_sha = str(package.get("source_zip_sha256") or "").strip().lower()
    if expected_zip_sha and expected_zip_sha != zip_sha:
        raise SecInsiderRawAuditError("SEC insider ZIP SHA-256 does not match validation package")

    with zipfile.ZipFile(insider_zip_path, "r") as zf:
        submission_member = _find_member(zf, "SUBMISSION")
        owner_member = _find_member(zf, "REPORTINGOWNER")
        trans_member = _find_member(zf, "NONDERIV_TRANS")
        submissions, submission_sha = _read_tsv(zf, submission_member)
        owners, owner_sha = _read_tsv(zf, owner_member)
        transactions, trans_sha = _read_tsv(zf, trans_member)

    sub_by_accession: dict[str, list[dict[str, str]]] = defaultdict(list)
    owner_by_accession: dict[str, list[dict[str, str]]] = defaultdict(list)
    tx_by_identity: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)

    for row in submissions:
        accession = str(row.get("ACCESSION_NUMBER") or "").strip()
        if accession:
            sub_by_accession[accession].append(row)
    for row in owners:
        accession = str(row.get("ACCESSION_NUMBER") or "").strip()
        if accession:
            owner_by_accession[accession].append(row)
    for row in transactions:
        accession = str(row.get("ACCESSION_NUMBER") or "").strip()
        transaction_key = str(row.get("NONDERIV_TRANS_SK") or "").strip()
        if accession and transaction_key:
            tx_by_identity[(accession, transaction_key)].append(row)

    output_rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for sample in package.get("rows") or []:
        if not isinstance(sample, Mapping):
            raise SecInsiderRawAuditError("validation package rows must be objects")
        validation_id = str(sample.get("validation_id") or "").strip()
        accession = str(sample.get("accession_number") or "").strip()
        transaction_key = str(sample.get("transaction_key") or "").strip()
        if not validation_id or not accession or not transaction_key:
            raise SecInsiderRawAuditError("audit row missing validation_id/accession/transaction_key")
        if validation_id in seen_ids:
            raise SecInsiderRawAuditError(f"duplicate validation_id in package: {validation_id}")
        seen_ids.add(validation_id)

        submission_matches = sub_by_accession.get(accession, [])
        transaction_matches = tx_by_identity.get((accession, transaction_key), [])
        if len(submission_matches) != 1:
            raise SecInsiderRawAuditError(
                f"validation {validation_id}: expected one raw SUBMISSION row, found {len(submission_matches)}"
            )
        if len(transaction_matches) != 1:
            raise SecInsiderRawAuditError(
                f"validation {validation_id}: expected one raw NONDERIV_TRANS row, found {len(transaction_matches)}"
            )

        output_rows.append(
            {
                "validation_id": validation_id,
                "raw_submission": submission_matches[0],
                "raw_reporting_owners": owner_by_accession.get(accession, []),
                "raw_nonderiv_trans": transaction_matches[0],
            }
        )

    output_rows.sort(key=lambda row: row["validation_id"])
    if len(output_rows) != len(package.get("rows") or []):
        raise SecInsiderRawAuditError("raw audit pack row count does not match validation package")

    return {
        "schema_version": RAW_AUDIT_SCHEMA,
        "phase": "8D_B3_insider_validation",
        "status": "READY_FOR_BLIND_RAW_SOURCE_AUDIT",
        "source_quarter": package.get("source_quarter"),
        "validation_package_sha256": _sha256_file(package_path),
        "source_zip_sha256": zip_sha,
        "source_member_sha256": {
            "SUBMISSION": submission_sha,
            "REPORTINGOWNER": owner_sha,
            "NONDERIV_TRANS": trans_sha,
        },
        "row_count": len(output_rows),
        "rows": output_rows,
        "guards": {
            "parser_candidate_status_exposed_to_annotator": False,
            "parser_reason_codes_exposed_to_annotator": False,
            "market_outcomes_read": False,
            "market_direction_assigned": False,
            "phase7_integration_enabled": False,
        },
    }


def write_raw_audit_pack(payload: Mapping[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True), encoding="utf-8")


def write_blind_annotation_template(*, package_path: Path, output_path: Path) -> None:
    package = _load_json(package_path)
    if package.get("schema_version") != PACKAGE_SCHEMA:
        raise SecInsiderRawAuditError("unsupported B3 validation package schema")
    critical_fields = [
        "issuer_cik_correct",
        "accession_number_correct",
        "transaction_code_correct",
        "acquired_disposed_code_correct",
        "transaction_date_correct",
        "transaction_shares_correct",
        "transaction_price_correct_when_present",
        "valid_from_correct",
        "reporting_owner_link_correct",
    ]
    fields = [
        "validation_id",
        "truth_label",
        "review_status",
        "market_outcomes_seen",
        "notes",
        *critical_fields,
    ]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        seen: set[str] = set()
        for row in package.get("rows") or []:
            validation_id = str((row or {}).get("validation_id") or "").strip()
            if not validation_id or validation_id in seen:
                raise SecInsiderRawAuditError("invalid or duplicate validation_id in package")
            seen.add(validation_id)
            writer.writerow(
                {
                    "validation_id": validation_id,
                    "truth_label": "",
                    "review_status": "PENDING",
                    "market_outcomes_seen": "FALSE",
                    "notes": "",
                    **{field: "" for field in critical_fields},
                }
            )
