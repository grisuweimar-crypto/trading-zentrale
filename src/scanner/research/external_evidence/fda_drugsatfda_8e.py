from __future__ import annotations

import csv
import hashlib
import io
import json
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping


SCHEMA_VERSION = "external_evidence_8e_fda_drugsatfda_approval_v1"


class FDAApproval8EError(ValueError):
    pass


def _aware_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise FDAApproval8EError(f"invalid ingested_at: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise FDAApproval8EError("ingested_at must be timezone-aware")
    return parsed


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canonical_hash(payload: Mapping[str, Any]) -> str:
    raw = json.dumps(dict(payload), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return _sha256_bytes(raw)


def _decode_table(data: bytes) -> str:
    for encoding in ("utf-8-sig", "cp1252"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise FDAApproval8EError("unable to decode Drugs@FDA table as UTF-8 or Windows-1252")


def _read_tsv(data: bytes) -> tuple[list[str], list[dict[str, str]]]:
    text = _decode_table(data)
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    if not reader.fieldnames:
        raise FDAApproval8EError("Drugs@FDA table has no header")
    fields = [str(value or "").strip() for value in reader.fieldnames]
    rows: list[dict[str, str]] = []
    for raw in reader:
        rows.append({str(key or "").strip(): str(value or "").strip() for key, value in raw.items()})
    return fields, rows


def _find_tables(
    archive: zipfile.ZipFile,
    *,
    required_tables: Mapping[str, Iterable[str]],
) -> tuple[dict[str, list[dict[str, str]]], dict[str, dict[str, str]]]:
    required = {name: set(columns) for name, columns in required_tables.items()}
    matched: dict[str, list[dict[str, str]]] = {}
    provenance: dict[str, dict[str, str]] = {}

    for member in archive.infolist():
        if member.is_dir() or not member.filename.lower().endswith((".txt", ".tsv")):
            continue
        data = archive.read(member)
        try:
            fields, rows = _read_tsv(data)
        except FDAApproval8EError:
            continue
        field_set = set(fields)
        for logical_name, needed in required.items():
            if logical_name in matched:
                continue
            if needed <= field_set:
                matched[logical_name] = rows
                provenance[logical_name] = {
                    "member": member.filename,
                    "sha256": _sha256_bytes(data),
                }

    missing = sorted(set(required) - set(matched))
    if missing:
        raise FDAApproval8EError(
            "missing required Drugs@FDA tables by column signature: " + ", ".join(missing)
        )
    return matched, provenance


def _key(row: Mapping[str, str]) -> tuple[str, str, str]:
    return (
        str(row.get("ApplNo") or "").strip(),
        str(row.get("SubmissionType") or "").strip(),
        str(row.get("SubmissionNo") or "").strip(),
    )


def build_fda_approval_evidence(
    *,
    zip_path: Path,
    ingested_at: str | datetime,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    if config.get("schema_version") != "external_evidence_8e_fda_approval_v1":
        raise FDAApproval8EError("unsupported FDA approval config")
    hard = config.get("hard_boundaries") or {}
    if hard.get("market_outcomes_may_be_read") is not False:
        raise FDAApproval8EError("FDA adapter must remain outcome-blind")
    if hard.get("phase7_integration_enabled") is not False:
        raise FDAApproval8EError("FDA adapter may not enable Phase-7 integration")

    observed_at = _aware_datetime(ingested_at)
    zip_bytes = zip_path.read_bytes()
    zip_sha = _sha256_bytes(zip_bytes)
    required_tables = config.get("required_bulk_tables_by_columns") or {}
    if not isinstance(required_tables, Mapping) or not required_tables:
        raise FDAApproval8EError("FDA config missing required bulk table signatures")

    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as archive:
        tables, member_provenance = _find_tables(
            archive,
            required_tables={key: list(value) for key, value in required_tables.items()},
        )

    applications = {
        str(row.get("ApplNo") or "").strip(): row
        for row in tables["applications"]
        if str(row.get("ApplNo") or "").strip()
    }
    submissions = {_key(row): row for row in tables["submissions"] if all(_key(row))}
    action_types = {
        str(row.get("ActionTypes_LookupID") or "").strip(): str(
            row.get("ActionTypes_LookupDescription") or ""
        ).strip()
        for row in tables["action_types"]
        if str(row.get("ActionTypes_LookupID") or "").strip()
    }

    allowed_descriptions = {
        str(value).strip().casefold()
        for value in config.get("approval_action_descriptions_exact") or []
        if str(value).strip()
    }
    if not allowed_descriptions:
        raise FDAApproval8EError("FDA config has no exact approval action descriptions")

    evidence_rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for join_row in tables["submission_action_join"]:
        submission_key = _key(join_row)
        if not all(submission_key):
            continue
        action_id = str(join_row.get("ActionTypes_LookupID") or "").strip()
        action_description = action_types.get(action_id)
        if not action_description or action_description.casefold() not in allowed_descriptions:
            continue
        submission = submissions.get(submission_key)
        if submission is None:
            raise FDAApproval8EError(
                "submission/action join references missing submission: " + ":".join(submission_key)
            )

        appl_no, submission_type, submission_no = submission_key
        application = applications.get(appl_no, {})
        source_event_id = f"{appl_no}:{submission_type}:{submission_no}"
        dedup_key = (*submission_key, action_description.casefold())
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        source_record = {
            "ApplNo": appl_no,
            "SubmissionType": submission_type,
            "SubmissionNo": submission_no,
            "SubmissionStatus": str(submission.get("SubmissionStatus") or "").strip(),
            "SubmissionStatusDate": str(submission.get("SubmissionStatusDate") or "").strip(),
            "ActionTypes_LookupID": action_id,
            "ActionTypes_LookupDescription": action_description,
            "SponsorName": str(application.get("SponsorName") or "").strip(),
        }
        row_hash = _canonical_hash(source_record)
        evidence_rows.append(
            {
                "canonical_event_key": f"FDA:REGULATORY_APPROVAL:{source_event_id}",
                "event_type": "REGULATORY_APPROVAL",
                "subject_id": f"FDA_APPLICATION:{appl_no}",
                "source_id": "fda_drugsatfda_bulk",
                "source_class": "PUBLIC_AUTHORITY_PRIMARY",
                "source_event_id": source_event_id,
                "event_state": "APPROVED",
                "authority_scope": "FDA_HUMAN_DRUG_APPLICATION_APPROVAL",
                "published_at": None,
                "valid_from": observed_at.isoformat(),
                "ingested_at": observed_at.isoformat(),
                "content_sha256": row_hash,
                "strict_pit_eligible": True,
                "discovery_only": False,
                "public_release_proof_status": "INGESTION_ONLY",
                "historical_publication_time_independently_proven": False,
                "fda_action_date": source_record["SubmissionStatusDate"] or None,
                "submission_status": source_record["SubmissionStatus"] or None,
                "submission_type": submission_type,
                "submission_number": submission_no,
                "application_number": appl_no,
                "sponsor_name": source_record["SponsorName"] or None,
                "action_type_description": action_description,
                "source_record": source_record,
            }
        )

    evidence_rows.sort(
        key=lambda row: (
            row["application_number"],
            row["submission_type"],
            row["submission_number"],
        )
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "phase": "8E_B2_fda_regulatory_approval",
        "status": "AUTHORITATIVE_APPROVAL_STATE_PIT_FROM_INGESTION_ONLY",
        "source_id": "fda_drugsatfda_bulk",
        "source_zip": str(zip_path),
        "source_zip_sha256": zip_sha,
        "ingested_at": observed_at.isoformat(),
        "source_members": member_provenance,
        "row_count": len(evidence_rows),
        "rows": evidence_rows,
        "guards": {
            "market_outcomes_read": False,
            "market_direction_assigned": False,
            "approval_predefined_bullish": False,
            "submission_status_date_used_as_published_at": False,
            "submission_status_date_used_as_valid_from": False,
            "historical_first_public_release_reconstructed": False,
            "sponsor_name_to_ticker_mapping_enabled": False,
            "phase7_integration_enabled": False,
            "production_external_evidence_enabled": False,
        },
    }


def write_fda_approval_evidence(payload: Mapping[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True), encoding="utf-8")
