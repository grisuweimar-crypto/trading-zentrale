from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from .sec_edgar import compute_valid_from, normalize_cik
from .sec_insider_validation import evaluate_insider_annotations

PACKAGE_SCHEMA = "external_evidence_8d_insider_validation_package_v1"
RAW_AUDIT_SCHEMA = "external_evidence_8d_insider_raw_audit_v1"
BULK_SCHEMA = "external_evidence_8c_sec_bulk_bundle_v1"
DIRECT_BULK_MODE = "DIRECT_SEC_BULK_OPERATOR_ATTESTED"
FINALIZER_SCHEMA = "external_evidence_8d_insider_b3_mechanical_verification_v1"


class SecInsiderB3FinalizeError(ValueError):
    """Raised when B3 cannot be finalized against independent raw source evidence."""


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
        raise SecInsiderB3FinalizeError(f"cannot read JSON object: {path}") from exc
    if not isinstance(payload, dict):
        raise SecInsiderB3FinalizeError(f"JSON root must be an object: {path}")
    return payload


def _resolve(root: Path, relative: str) -> Path:
    base = root.resolve()
    candidate = (root / str(relative or "")).resolve()
    try:
        candidate.relative_to(base)
    except ValueError as exc:
        raise SecInsiderB3FinalizeError(f"bundle path escapes root: {relative!r}") from exc
    return candidate


def _read_verified_json(bundle_dir: Path, spec: Mapping[str, Any]) -> dict[str, Any]:
    relative = str(spec.get("path") or "").strip()
    expected = str(spec.get("sha256") or "").strip().lower()
    if not relative or len(expected) != 64:
        raise SecInsiderB3FinalizeError("bundle JSON spec requires path and SHA-256")
    path = _resolve(bundle_dir, relative)
    raw = path.read_bytes()
    if _sha256_bytes(raw) != expected:
        raise SecInsiderB3FinalizeError(f"SHA-256 mismatch for {relative}")
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, dict):
        raise SecInsiderB3FinalizeError(f"bundle JSON must be an object: {relative}")
    return payload


def _parallel_value(table: Mapping[str, Any], key: str, index: int) -> Any:
    values = table.get(key)
    if not isinstance(values, list) or index >= len(values):
        return None
    return values[index]


def _raw_submission_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    if "filings" in payload and isinstance(payload.get("filings"), Mapping):
        table = (payload.get("filings") or {}).get("recent") or {}
    else:
        table = payload
    accessions = table.get("accessionNumber") or []
    if not isinstance(accessions, list):
        raise SecInsiderB3FinalizeError("SEC submissions accessionNumber must be an array")
    rows: list[dict[str, Any]] = []
    for i, accession in enumerate(accessions):
        rows.append(
            {
                "accession_number": str(accession or "").strip(),
                "form": str(_parallel_value(table, "form", i) or "").strip().upper(),
                "filing_date": _parallel_value(table, "filingDate", i),
                "acceptance_datetime": _parallel_value(table, "acceptanceDateTime", i),
                "primary_document": _parallel_value(table, "primaryDocument", i),
            }
        )
    return rows


def _raw_acceptance_index(
    *, bundle_dir: Path, target_keys: set[tuple[str, str]]
) -> tuple[dict[tuple[str, str], dict[str, Any]], str]:
    manifest_path = bundle_dir / "bulk_manifest.json"
    manifest = _load_json(manifest_path)
    if manifest.get("schema_version") != BULK_SCHEMA:
        raise SecInsiderB3FinalizeError("unsupported SEC bulk manifest schema")
    if manifest.get("source_mode") != DIRECT_BULK_MODE:
        raise SecInsiderB3FinalizeError("B3 provenance requires direct official SEC bulk mode")
    if manifest.get("source_authority") != "U.S. SEC EDGAR":
        raise SecInsiderB3FinalizeError("source authority must be U.S. SEC EDGAR")
    if manifest.get("operator_attested_direct_sec_download") is not True:
        raise SecInsiderB3FinalizeError("direct SEC operator attestation is required")
    if manifest.get("market_outcomes_read") is not False:
        raise SecInsiderB3FinalizeError("SEC bundle must remain outcome-blind")

    wanted_by_cik: dict[str, set[str]] = {}
    for accession, cik in target_keys:
        wanted_by_cik.setdefault(cik, set()).add(accession)

    index: dict[tuple[str, str], dict[str, Any]] = {}
    for company in manifest.get("companies") or []:
        if not isinstance(company, Mapping):
            continue
        if company.get("identity_status") != "VERIFIED_BY_SEC_BULK_SUBMISSIONS":
            continue
        cik = normalize_cik(company.get("cik") or "")
        wanted = wanted_by_cik.get(cik)
        if not wanted:
            continue
        payloads = [_read_verified_json(bundle_dir, company.get("submissions_file") or {})]
        for spec in company.get("history_files") or []:
            payloads.append(_read_verified_json(bundle_dir, spec))
        for payload in payloads:
            for row in _raw_submission_rows(payload):
                accession = row["accession_number"]
                if accession not in wanted:
                    continue
                key = (accession, cik)
                existing = index.get(key)
                if existing is not None and existing != row:
                    raise SecInsiderB3FinalizeError(
                        f"conflicting raw SEC acceptance metadata for {accession}/{cik}"
                    )
                index[key] = row
    return index, _sha256_file(manifest_path)


def _sec_date(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%d-%b-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            pass
    return None


def _decimal(value: Any) -> Decimal | None:
    text = str(value or "").strip().replace(",", "")
    if not text:
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def _bool_text(value: bool | None) -> str:
    if value is True:
        return "TRUE"
    if value is False:
        return "FALSE"
    return "UNCERTAIN"


def _canonical_raw_owners(raw: list[Mapping[str, Any]]) -> list[tuple[str, str, str, str, str]]:
    rows: list[tuple[str, str, str, str, str]] = []
    for owner in raw:
        cik_raw = str(owner.get("RPTOWNERCIK") or "").strip()
        cik = normalize_cik(cik_raw) if cik_raw else ""
        rows.append(
            (
                cik,
                str(owner.get("RPTOWNERNAME") or ""),
                str(owner.get("RPTOWNER_RELATIONSHIP") or ""),
                str(owner.get("RPTOWNER_TITLE") or ""),
                str(owner.get("RPTOWNER_TXT") or ""),
            )
        )
    return sorted(rows)


def _canonical_package_owners(raw: list[Mapping[str, Any]]) -> list[tuple[str, str, str, str, str]]:
    rows: list[tuple[str, str, str, str, str]] = []
    for owner in raw:
        cik_raw = str(owner.get("reporting_owner_cik") or "").strip()
        cik = normalize_cik(cik_raw) if cik_raw else ""
        rows.append(
            (
                cik,
                str(owner.get("reporting_owner_name") or ""),
                str(owner.get("reporting_owner_relationship") or ""),
                str(owner.get("reporting_owner_title") or ""),
                str(owner.get("reporting_owner_other_text") or ""),
            )
        )
    return sorted(rows)


def finalize_b3_annotations(
    *,
    package_path: Path,
    raw_pack_path: Path,
    semantic_annotations_path: Path,
    sec_bulk_bundle_dir: Path,
    output_annotations_path: Path,
) -> dict[str, Any]:
    package = _load_json(package_path)
    raw_pack = _load_json(raw_pack_path)
    if package.get("schema_version") != PACKAGE_SCHEMA:
        raise SecInsiderB3FinalizeError("unsupported validation package schema")
    if raw_pack.get("schema_version") != RAW_AUDIT_SCHEMA:
        raise SecInsiderB3FinalizeError("unsupported raw audit schema")
    if str(raw_pack.get("validation_package_sha256") or "").lower() != _sha256_file(package_path):
        raise SecInsiderB3FinalizeError("raw audit pack does not match validation package")
    guards = raw_pack.get("guards") or {}
    if guards.get("market_outcomes_read") is not False:
        raise SecInsiderB3FinalizeError("raw audit pack is outcome-contaminated")
    if guards.get("parser_candidate_status_exposed_to_annotator") is not False:
        raise SecInsiderB3FinalizeError("raw audit exposed parser candidate status")

    package_rows = {
        str(row.get("validation_id") or ""): dict(row)
        for row in package.get("rows") or []
        if isinstance(row, Mapping)
    }
    raw_rows = {
        str(row.get("validation_id") or ""): dict(row)
        for row in raw_pack.get("rows") or []
        if isinstance(row, Mapping)
    }
    if set(package_rows) != set(raw_rows):
        raise SecInsiderB3FinalizeError("package and raw-audit validation IDs differ")

    with semantic_annotations_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        annotations = [dict(row) for row in reader]
    if not fieldnames or "validation_id" not in fieldnames:
        raise SecInsiderB3FinalizeError("semantic annotation CSV has no validation_id")
    by_id: dict[str, dict[str, str]] = {}
    for row in annotations:
        vid = str(row.get("validation_id") or "").strip()
        if not vid or vid not in raw_rows or vid in by_id:
            raise SecInsiderB3FinalizeError(f"invalid/duplicate annotation validation_id: {vid!r}")
        if str(row.get("market_outcomes_seen") or "").strip().upper() != "FALSE":
            raise SecInsiderB3FinalizeError(f"annotation {vid} is not outcome-blind")
        by_id[vid] = row
    if set(by_id) != set(raw_rows):
        raise SecInsiderB3FinalizeError("semantic annotation CSV does not cover the frozen sample")

    target_keys: set[tuple[str, str]] = set()
    for raw in raw_rows.values():
        sub = raw.get("raw_submission") or {}
        accession = str(sub.get("ACCESSION_NUMBER") or "").strip()
        cik = normalize_cik(sub.get("ISSUERCIK") or "")
        target_keys.add((accession, cik))
    acceptance_index, manifest_sha = _raw_acceptance_index(
        bundle_dir=sec_bulk_bundle_dir, target_keys=target_keys
    )

    check_fields = [
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
    for field in check_fields:
        if field not in fieldnames:
            fieldnames.append(field)

    check_counts = {field: {"TRUE": 0, "FALSE": 0, "UNCERTAIN": 0} for field in check_fields}
    finalized: list[dict[str, str]] = []
    for vid in sorted(by_id):
        annotation = by_id[vid]
        source = package_rows[vid]
        raw = raw_rows[vid]
        sub = raw.get("raw_submission") or {}
        tx = raw.get("raw_nonderiv_trans") or {}
        owners = raw.get("raw_reporting_owners") or []

        raw_cik = normalize_cik(sub.get("ISSUERCIK") or "")
        raw_accession = str(sub.get("ACCESSION_NUMBER") or "").strip()
        tx_accession = str(tx.get("ACCESSION_NUMBER") or "").strip()
        owner_accessions = {str(owner.get("ACCESSION_NUMBER") or "").strip() for owner in owners}

        issuer_ok = normalize_cik(source.get("issuer_cik") or "") == raw_cik
        accession_ok = (
            str(source.get("accession_number") or "").strip() == raw_accession == tx_accession
            and (not owner_accessions or owner_accessions == {raw_accession})
        )
        code_ok = str(source.get("transaction_code") or "").strip().upper() == str(tx.get("TRANS_CODE") or "").strip().upper()
        acq_ok = str(source.get("acquired_disposed_code") or "").strip().upper() == str(tx.get("TRANS_ACQUIRED_DISP_CD") or "").strip().upper()
        date_ok = _sec_date(source.get("transaction_date")) == _sec_date(tx.get("TRANS_DATE"))
        shares_ok = _decimal(source.get("transaction_shares")) == _decimal(tx.get("TRANS_SHARES"))
        raw_price = _decimal(tx.get("TRANS_PRICEPERSHARE"))
        package_price = _decimal(source.get("transaction_price_per_share"))
        price_ok = package_price == raw_price
        owners_ok = _canonical_package_owners(source.get("reporting_owners") or []) == _canonical_raw_owners(owners)

        acceptance_raw = acceptance_index.get((raw_accession, raw_cik))
        if acceptance_raw is None:
            valid_ok: bool | None = None
        else:
            pit = compute_valid_from(
                filing_date=acceptance_raw.get("filing_date"),
                acceptance_datetime=acceptance_raw.get("acceptance_datetime"),
            )
            valid_ok = (
                str(source.get("valid_from") or "") == str(pit.get("valid_from") or "")
                and str(source.get("pit_status") or "") == str(pit.get("pit_status") or "")
                and str(source.get("document_type") or "").upper() == str(acceptance_raw.get("form") or "").upper()
            )

        checks: dict[str, bool | None] = {
            "issuer_cik_correct": issuer_ok,
            "accession_number_correct": accession_ok,
            "transaction_code_correct": code_ok,
            "acquired_disposed_code_correct": acq_ok,
            "transaction_date_correct": date_ok,
            "transaction_shares_correct": shares_ok,
            "transaction_price_correct_when_present": price_ok,
            "valid_from_correct": valid_ok,
            "reporting_owner_link_correct": owners_ok,
        }
        output = dict(annotation)
        for field, value in checks.items():
            text = _bool_text(value)
            output[field] = text
            check_counts[field][text] += 1
        finalized.append(output)

    output_annotations_path.parent.mkdir(parents=True, exist_ok=True)
    with output_annotations_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(finalized)

    return {
        "schema_version": FINALIZER_SCHEMA,
        "phase": "8D_B3_insider_validation",
        "status": "MECHANICAL_FIELD_AND_PIT_CHECKS_COMPLETE",
        "row_count": len(finalized),
        "package_sha256": _sha256_file(package_path),
        "raw_audit_sha256": _sha256_file(raw_pack_path),
        "semantic_annotations_sha256": _sha256_file(semantic_annotations_path),
        "sec_bulk_manifest_sha256": manifest_sha,
        "check_counts": check_counts,
        "guards": {
            "market_outcomes_read": False,
            "market_direction_assigned": False,
            "parser_candidate_status_used_for_truth_labels": False,
            "phase7_integration_enabled": False,
        },
    }


def write_verification(payload: Mapping[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True), encoding="utf-8")


def finalize_and_evaluate(
    *,
    package_path: Path,
    raw_pack_path: Path,
    semantic_annotations_path: Path,
    sec_bulk_bundle_dir: Path,
    config_path: Path,
    output_annotations_path: Path,
    verification_path: Path,
    result_path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    verification = finalize_b3_annotations(
        package_path=package_path,
        raw_pack_path=raw_pack_path,
        semantic_annotations_path=semantic_annotations_path,
        sec_bulk_bundle_dir=sec_bulk_bundle_dir,
        output_annotations_path=output_annotations_path,
    )
    write_verification(verification, verification_path)
    result = evaluate_insider_annotations(
        package_path=package_path,
        annotation_csv_path=output_annotations_path,
        config_path=config_path,
    )
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    return verification, result
