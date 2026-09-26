from __future__ import annotations

import csv
import hashlib
import json
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from scanner.research.external_evidence.sec_edgar import normalize_cik
from scanner.research.external_evidence.sec_snapshot import write_bytes_with_digest, write_json_with_digest

BULK_BUNDLE_SCHEMA = "external_evidence_8c_sec_bulk_bundle_v1"
DIRECT_BULK_MODE = "DIRECT_SEC_BULK_OPERATOR_ATTESTED"
MIRROR_MODE = "NON_AUTHORITATIVE_TRANSPORT_FOR_CHALLENGER_ONLY"
_ALLOWED_MODES = {DIRECT_BULK_MODE, MIRROR_MODE}
_PRIMARY_RE = re.compile(r"^CIK(\d{10})\.json$")


class SecBulkImportError(ValueError):
    """Raised when an offline SEC bulk archive cannot satisfy the 8C transport contract."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _scanner_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows or "symbol" not in rows[0]:
        raise SecBulkImportError("scanner CSV must contain symbol")
    unique: dict[str, dict[str, str]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        if symbol and symbol not in unique:
            normalized = dict(row)
            normalized["symbol"] = symbol
            unique[symbol] = normalized
    return list(unique.values())


def _zip_member_by_basename(archive: zipfile.ZipFile) -> dict[str, str]:
    result: dict[str, str] = {}
    duplicates: set[str] = set()
    for member in archive.namelist():
        if member.endswith("/"):
            continue
        base = Path(member).name
        if base in result and result[base] != member:
            duplicates.add(base)
        else:
            result[base] = member
    for base in duplicates:
        result.pop(base, None)
    return result


def _load_json_member(archive: zipfile.ZipFile, member: str) -> dict[str, Any]:
    try:
        payload = json.loads(archive.read(member).decode("utf-8"))
    except Exception as exc:  # pragma: no cover - exact zip/json errors vary
        raise SecBulkImportError(f"cannot decode JSON member {member!r}") from exc
    if not isinstance(payload, dict):
        raise SecBulkImportError(f"JSON member {member!r} must contain an object")
    return payload


def build_bulk_submissions_index(
    archive: zipfile.ZipFile,
) -> tuple[dict[str, list[dict[str, str]]], dict[str, tuple[str, dict[str, Any]]], dict[str, str]]:
    """Index primary CIK submissions JSON without relying on company_tickers.json.

    The SEC bulk submissions archive contains one current primary submissions JSON per
    CIK plus historical continuation JSON files. Current ticker metadata in each
    primary payload is used only to build exact candidates; every selected candidate
    is re-checked against the same primary payload before export.
    """
    ticker_candidates: dict[str, list[dict[str, str]]] = {}
    primary_by_cik: dict[str, tuple[str, dict[str, Any]]] = {}
    by_basename = _zip_member_by_basename(archive)

    for base, member in by_basename.items():
        match = _PRIMARY_RE.fullmatch(base)
        if not match:
            continue
        cik = match.group(1)
        payload = _load_json_member(archive, member)
        payload_cik = normalize_cik(payload.get("cik", cik))
        if payload_cik != cik:
            raise SecBulkImportError(f"CIK mismatch inside {member!r}: {payload_cik} != {cik}")
        primary_by_cik[cik] = (member, payload)
        for raw_ticker in payload.get("tickers") or []:
            ticker = str(raw_ticker or "").strip().upper()
            if not ticker:
                continue
            ticker_candidates.setdefault(ticker, []).append(
                {"cik": cik, "name": str(payload.get("name") or ""), "member": member}
            )

    if not primary_by_cik:
        raise SecBulkImportError("submissions ZIP contains no primary CIK##########.json members")
    return ticker_candidates, primary_by_cik, by_basename


def _content_index(content_dir: Path | None) -> dict[str, Path]:
    if content_dir is None:
        return {}
    if not content_dir.is_dir():
        raise SecBulkImportError(f"filing content directory does not exist: {content_dir}")
    index: dict[str, Path] = {}
    duplicates: set[str] = set()
    for path in content_dir.rglob("*"):
        if not path.is_file():
            continue
        if path.name in index and index[path.name] != path:
            duplicates.add(path.name)
        else:
            index[path.name] = path
    for name in duplicates:
        index.pop(name, None)
    return index


def import_sec_bulk_bundle(
    *,
    scanner_path: Path,
    submissions_zip: Path,
    companyfacts_zip: Path,
    output_dir: Path,
    source_mode: str,
    submissions_source_url: str,
    companyfacts_source_url: str,
    acquired_at: str | None = None,
    filing_content_dir: Path | None = None,
) -> dict[str, Any]:
    if source_mode not in _ALLOWED_MODES:
        raise SecBulkImportError(f"unsupported source_mode: {source_mode}")
    if not submissions_zip.is_file() or not companyfacts_zip.is_file():
        raise SecBulkImportError("both submissions_zip and companyfacts_zip are required")
    if source_mode == DIRECT_BULK_MODE:
        if not submissions_source_url.startswith("https://www.sec.gov/") and not submissions_source_url.startswith("https://data.sec.gov/"):
            raise SecBulkImportError("direct SEC bulk mode requires an official SEC submissions source URL")
        if not companyfacts_source_url.startswith("https://www.sec.gov/") and not companyfacts_source_url.startswith("https://data.sec.gov/"):
            raise SecBulkImportError("direct SEC bulk mode requires an official SEC companyfacts source URL")

    rows = _scanner_rows(scanner_path)
    acquired = acquired_at or datetime.now(timezone.utc).isoformat()
    content_files = _content_index(filing_content_dir)
    submissions_sha = sha256_file(submissions_zip)
    companyfacts_sha = sha256_file(companyfacts_zip)

    companies: list[dict[str, Any]] = []
    output_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(submissions_zip) as submissions_archive, zipfile.ZipFile(companyfacts_zip) as facts_archive:
        ticker_candidates, primary_by_cik, submission_members = build_bulk_submissions_index(submissions_archive)
        facts_members = _zip_member_by_basename(facts_archive)

        for row in rows:
            symbol = row["symbol"]
            candidates = ticker_candidates.get(symbol, [])
            entry: dict[str, Any] = {
                "symbol": symbol,
                "scanner_name": row.get("name"),
                "scanner_as_of": row.get("as_of"),
                "identity_status": "UNKNOWN",
                "reason_codes": [],
                "history_files": [],
                "content_filings": [],
            }
            if not candidates:
                entry["reason_codes"].append("NO_EXACT_BULK_SUBMISSIONS_TICKER_MATCH")
                companies.append(entry)
                continue
            unique_ciks = sorted({candidate["cik"] for candidate in candidates})
            if len(unique_ciks) != 1:
                entry["identity_status"] = "AMBIGUOUS"
                entry["reason_codes"].append("TICKER_MAPS_TO_MULTIPLE_CIKS_IN_BULK_SUBMISSIONS")
                entry["candidate_ciks"] = unique_ciks
                companies.append(entry)
                continue

            cik = unique_ciks[0]
            member, primary = primary_by_cik[cik]
            current_tickers = {str(value or "").strip().upper() for value in (primary.get("tickers") or [])}
            if symbol not in current_tickers or normalize_cik(primary.get("cik", cik)) != cik:
                entry["identity_status"] = "REJECTED_STALE_OR_MISMATCHED_TICKER"
                entry["reason_codes"].append("BULK_SUBMISSIONS_IDENTITY_MISMATCH")
                companies.append(entry)
                continue

            company_dir = f"raw/companies/{cik}"
            submissions_rel = f"{company_dir}/submissions.json"
            submissions_digest = write_json_with_digest(output_dir / submissions_rel, primary)
            entry.update(
                {
                    "cik": cik,
                    "sec_title": primary.get("name"),
                    "identity_status": "VERIFIED_BY_SEC_BULK_SUBMISSIONS" if source_mode == DIRECT_BULK_MODE else "VERIFIED_BY_NONAUTHORITATIVE_BULK_COPY",
                    "submissions_file": {
                        "path": submissions_rel,
                        "sha256": submissions_digest,
                        "archive_member": member,
                        "source_url": submissions_source_url,
                    },
                }
            )

            for spec in ((primary.get("filings") or {}).get("files") or []):
                name = str((spec or {}).get("name") or "").strip()
                member_name = submission_members.get(name)
                if not name or member_name is None:
                    entry["reason_codes"].append("MISSING_REFERENCED_SUBMISSION_HISTORY_MEMBER")
                    continue
                history = _load_json_member(submissions_archive, member_name)
                history_rel = f"{company_dir}/history/{name}"
                history_digest = write_json_with_digest(output_dir / history_rel, history)
                entry["history_files"].append(
                    {
                        "path": history_rel,
                        "sha256": history_digest,
                        "archive_member": member_name,
                        "source_url": submissions_source_url,
                    }
                )

            facts_base = f"CIK{cik}.json"
            facts_member = facts_members.get(facts_base)
            if facts_member is None:
                entry["reason_codes"].append("COMPANYFACTS_MEMBER_MISSING")
            else:
                facts = _load_json_member(facts_archive, facts_member)
                facts_cik = normalize_cik(facts.get("cik", cik))
                if facts_cik != cik:
                    entry["reason_codes"].append("COMPANYFACTS_CIK_MISMATCH")
                else:
                    facts_rel = f"{company_dir}/companyfacts.json"
                    facts_digest = write_json_with_digest(output_dir / facts_rel, facts)
                    entry["companyfacts_file"] = {
                        "path": facts_rel,
                        "sha256": facts_digest,
                        "archive_member": facts_member,
                        "source_url": companyfacts_source_url,
                    }

            recent = ((primary.get("filings") or {}).get("recent") or {})
            accessions = recent.get("accessionNumber") or []
            forms = recent.get("form") or []
            for idx, accession in enumerate(accessions):
                form = str(forms[idx] if idx < len(forms) else "").upper()
                if form not in {"8-K", "8-K/A", "6-K", "6-K/A"}:
                    continue
                accession_text = str(accession or "").strip()
                candidates_for_content = [
                    f"{accession_text}.txt",
                    f"{accession_text.replace('-', '')}.txt",
                ]
                local = next((content_files[name] for name in candidates_for_content if name in content_files), None)
                if local is None:
                    continue
                rel = f"{company_dir}/filing_content/{accession_text.replace('-', '')}/{accession_text}.txt"
                digest = write_bytes_with_digest(output_dir / rel, local.read_bytes())
                entry["content_filings"].append(
                    {
                        "accession_number": accession_text,
                        "form": form,
                        "document_file": {
                            "path": rel,
                            "sha256": digest,
                            "source_path": str(local),
                        },
                    }
                )
            companies.append(entry)

    direct = source_mode == DIRECT_BULK_MODE
    verified = [company for company in companies if company.get("identity_status") == "VERIFIED_BY_SEC_BULK_SUBMISSIONS"]
    facts_verified = [company for company in verified if company.get("companyfacts_file")]
    content_count = sum(len(company.get("content_filings") or []) for company in verified)
    manifest = {
        "schema_version": BULK_BUNDLE_SCHEMA,
        "phase": "8C-J",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "scanner_source": str(scanner_path),
        "source_mode": source_mode,
        "source_authority": "U.S. SEC EDGAR" if direct else "NON_AUTHORITATIVE_TRANSPORT",
        "operator_attested_direct_sec_download": direct,
        "transport_provenance": {
            "acquired_at": acquired,
            "submissions_archive": {
                "path": str(submissions_zip),
                "sha256": submissions_sha,
                "source_url": submissions_source_url,
            },
            "companyfacts_archive": {
                "path": str(companyfacts_zip),
                "sha256": companyfacts_sha,
                "source_url": companyfacts_source_url,
            },
        },
        "companies": companies,
        "coverage": {
            "scanner_symbol_count": len(rows),
            "sec_bulk_identity_verified_count": len(verified),
            "companyfacts_verified_count": len(facts_verified),
            "content_document_count": content_count,
        },
        "eligibility": {
            "fundamentals_real_data_validation": direct and bool(facts_verified),
            "structured_event_semantic_validation": direct and content_count > 0,
            "final_phase8c_completion": false,
            "phase7_integration": false,
            "market_outcome_research": false,
        },
        "market_outcomes_read": false,
        "direction_assigned": false,
    }
    write_json_with_digest(output_dir / "bulk_manifest.json", manifest)
    return manifest
