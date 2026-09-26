from __future__ import annotations

import csv
import hashlib
import io
import json
import zipfile
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from .sec_edgar import normalize_cik
from .sec_history import assemble_full_submission_history

SCHEMA_VERSION = "external_evidence_8d_sec_insider_bulk_v1"
BULK_SCHEMA = "external_evidence_8c_sec_bulk_bundle_v1"
DIRECT_BULK_MODE = "DIRECT_SEC_BULK_OPERATOR_ATTESTED"
_ALLOWED_DOCUMENT_TYPES = {"4", "4/A"}
_TARGET_CODES = {"P", "S"}


class SecInsiderBulkError(ValueError):
    """Raised when SEC insider evidence cannot satisfy the frozen 8D contract."""


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


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
        raise SecInsiderBulkError(f"cannot read JSON object: {path}") from exc
    if not isinstance(payload, dict):
        raise SecInsiderBulkError(f"JSON root must be an object: {path}")
    return payload


def _resolve(root: Path, relative: str) -> Path:
    base = root.resolve()
    candidate = (root / str(relative or "")).resolve()
    try:
        candidate.relative_to(base)
    except ValueError as exc:
        raise SecInsiderBulkError(f"bundle path escapes root: {relative!r}") from exc
    return candidate


def _read_verified_json(bundle_dir: Path, spec: Mapping[str, Any]) -> dict[str, Any]:
    relative = str(spec.get("path") or "").strip()
    expected = str(spec.get("sha256") or "").strip().lower()
    if not relative or len(expected) != 64:
        raise SecInsiderBulkError("bundle JSON spec requires path and SHA-256")
    path = _resolve(bundle_dir, relative)
    if not path.is_file():
        raise SecInsiderBulkError(f"bundle file missing: {relative}")
    raw = path.read_bytes()
    if _sha256_bytes(raw) != expected:
        raise SecInsiderBulkError(f"SHA-256 mismatch for {relative}")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise SecInsiderBulkError(f"cannot decode bundle JSON: {relative}") from exc
    if not isinstance(payload, dict):
        raise SecInsiderBulkError(f"bundle JSON must contain an object: {relative}")
    return payload


def _load_sec_bundle(bundle_dir: Path) -> tuple[dict[str, Any], str]:
    path = bundle_dir / "bulk_manifest.json"
    manifest = _load_json(path)
    if manifest.get("schema_version") != BULK_SCHEMA:
        raise SecInsiderBulkError("unsupported SEC bulk manifest schema")
    if manifest.get("source_mode") != DIRECT_BULK_MODE:
        raise SecInsiderBulkError("insider PIT reconstruction requires direct SEC bulk mode")
    if manifest.get("source_authority") != "U.S. SEC EDGAR":
        raise SecInsiderBulkError("source authority must be U.S. SEC EDGAR")
    if manifest.get("operator_attested_direct_sec_download") is not True:
        raise SecInsiderBulkError("direct SEC bulk operator attestation is required")
    if manifest.get("market_outcomes_read") is not False:
        raise SecInsiderBulkError("SEC bulk bundle must remain outcome-blind")
    return manifest, _sha256_file(path)


def _acceptance_index(
    bundle_dir: Path, manifest: Mapping[str, Any]
) -> tuple[
    dict[tuple[str, str], dict[str, Any]],
    set[str],
    dict[str, set[str]],
]:
    """Index Form 4 publication metadata by accession + SEC entity context.

    A Section-16 filing can legitimately appear in submissions histories for more than
    one participating SEC entity. Therefore accession number alone is insufficient to
    infer the issuer. The official Insider Transactions SUBMISSION.ISSUERCIK is used
    later to choose the matching issuer context. Conflicts within the same
    (accession, CIK) context still fail closed.
    """
    by_accession_and_cik: dict[tuple[str, str], dict[str, Any]] = {}
    context_ciks_by_accession: dict[str, set[str]] = defaultdict(set)
    verified_ciks: set[str] = set()
    for company in manifest.get("companies") or []:
        if not isinstance(company, Mapping):
            continue
        if company.get("identity_status") != "VERIFIED_BY_SEC_BULK_SUBMISSIONS":
            continue
        symbol = str(company.get("symbol") or "").strip().upper()
        cik = normalize_cik(company.get("cik") or "")
        primary = _read_verified_json(bundle_dir, company.get("submissions_file") or {})
        if normalize_cik(primary.get("cik") or "") != cik:
            raise SecInsiderBulkError(f"submissions CIK mismatch for {symbol}")
        tickers = {
            str(value or "").strip().upper() for value in (primary.get("tickers") or [])
        }
        if symbol not in tickers:
            raise SecInsiderBulkError(f"submissions ticker mismatch for {symbol}")

        historical: dict[str, dict[str, Any]] = {}
        for spec in company.get("history_files") or []:
            historical[Path(str(spec.get("path") or "")).name] = _read_verified_json(
                bundle_dir, spec
            )
        assembled = assemble_full_submission_history(
            primary,
            historical_payloads=historical,
            forms=("4", "4/A"),
            require_complete=True,
        )
        verified_ciks.add(cik)
        for row in assembled["rows"]:
            accession = str(row.get("accession_number") or "").strip()
            if not accession:
                continue
            payload = {
                "symbol": symbol,
                "sec_context_cik": cik,
                "form": str(row.get("form") or "").upper(),
                "filed_date": row.get("filed_date"),
                "published_at": row.get("published_at"),
                "valid_from": row.get("valid_from"),
                "pit_status": row.get("pit_status"),
                "primary_document": row.get("primary_document"),
                "reason_codes": list(row.get("reason_codes") or []),
            }
            key = (accession, cik)
            existing = by_accession_and_cik.get(key)
            if existing is not None and existing != payload:
                raise SecInsiderBulkError(
                    "conflicting SEC bundle metadata within accession/CIK context "
                    f"{accession} / {cik}"
                )
            by_accession_and_cik[key] = payload
            context_ciks_by_accession[accession].add(cik)
    if not by_accession_and_cik:
        raise SecInsiderBulkError("verified SEC bundle contains no Form 4/4-A accessions")
    return by_accession_and_cik, verified_ciks, dict(context_ciks_by_accession)


def _find_member(zf: zipfile.ZipFile, stem: str) -> str:
    target = stem.upper()
    matches = [
        name
        for name in zf.namelist()
        if not name.endswith("/") and Path(Path(name).name).stem.upper() == target
    ]
    if len(matches) != 1:
        raise SecInsiderBulkError(
            f"expected exactly one {stem} table in SEC insider ZIP, found {len(matches)}"
        )
    return matches[0]


def _read_tsv(
    zf: zipfile.ZipFile, member: str
) -> tuple[list[dict[str, str]], str]:
    raw = zf.read(member)
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise SecInsiderBulkError(f"cannot decode SEC insider table {member}") from exc
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    if not reader.fieldnames:
        raise SecInsiderBulkError(f"SEC insider table has no header: {member}")
    rows = [dict(row) for row in reader]
    return rows, _sha256_bytes(raw)


def _sec_date(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%d-%b-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            pass
    raise SecInsiderBulkError(f"invalid SEC insider date: {value!r}")


def _number(value: Any) -> float | None:
    text = str(value or "").strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError as exc:
        raise SecInsiderBulkError(f"invalid numeric value: {value!r}") from exc


def _bool01(value: Any) -> bool | None:
    text = str(value or "").strip().upper()
    if text in {"1", "Y", "TRUE"}:
        return True
    if text in {"0", "N", "FALSE"}:
        return False
    return None


def import_sec_insider_quarter(
    *,
    insider_zip_path: Path,
    sec_bulk_bundle_dir: Path,
    source_url: str,
    quarter_label: str,
) -> dict[str, Any]:
    """Import one official SEC quarterly Insider Transactions ZIP outcome-blind.

    Quarterly tables provide immutable as-filed accession content. Historical availability
    is reconstructed only by exact accession + issuer-CIK join to the operator-attested SEC
    submissions bundle. Current ticker is descriptive metadata, never historical identity.
    """
    if not insider_zip_path.is_file():
        raise SecInsiderBulkError(f"SEC insider ZIP not found: {insider_zip_path}")
    if not str(source_url).startswith("https://www.sec.gov/"):
        raise SecInsiderBulkError("source_url must be an official sec.gov URL")
    label = str(quarter_label or "").strip().upper()
    if not label:
        raise SecInsiderBulkError("quarter_label is required")

    manifest, bundle_manifest_sha = _load_sec_bundle(sec_bulk_bundle_dir)
    acceptance, verified_ciks, accession_context_ciks = _acceptance_index(
        sec_bulk_bundle_dir, manifest
    )
    zip_sha = _sha256_file(insider_zip_path)

    with zipfile.ZipFile(insider_zip_path, "r") as zf:
        submission_member = _find_member(zf, "SUBMISSION")
        owner_member = _find_member(zf, "REPORTINGOWNER")
        trans_member = _find_member(zf, "NONDERIV_TRANS")
        submissions, submission_sha = _read_tsv(zf, submission_member)
        owners, owner_sha = _read_tsv(zf, owner_member)
        transactions, trans_sha = _read_tsv(zf, trans_member)

    owner_by_accession: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in owners:
        accession = str(raw.get("ACCESSION_NUMBER") or "").strip()
        if not accession:
            continue
        owner_by_accession[accession].append(
            {
                "reporting_owner_cik": (
                    normalize_cik(raw.get("RPTOWNERCIK") or "")
                    if str(raw.get("RPTOWNERCIK") or "").strip()
                    else None
                ),
                "reporting_owner_name": raw.get("RPTOWNERNAME"),
                "reporting_owner_relationship": raw.get("RPTOWNER_RELATIONSHIP"),
                "reporting_owner_title": raw.get("RPTOWNER_TITLE"),
                "reporting_owner_other_text": raw.get("RPTOWNER_TXT"),
            }
        )

    submission_by_accession: dict[str, dict[str, str]] = {}
    duplicate_submissions: list[str] = []
    for raw in submissions:
        accession = str(raw.get("ACCESSION_NUMBER") or "").strip()
        if not accession:
            continue
        if accession in submission_by_accession:
            duplicate_submissions.append(accession)
        submission_by_accession[accession] = raw
    if duplicate_submissions:
        raise SecInsiderBulkError(
            "duplicate SUBMISSION accessions in insider ZIP: "
            + str(sorted(set(duplicate_submissions))[:5])
        )

    trans_by_accession: dict[str, list[dict[str, str]]] = defaultdict(list)
    seen_transaction_keys: set[tuple[str, str]] = set()
    for raw in transactions:
        accession = str(raw.get("ACCESSION_NUMBER") or "").strip()
        trans_key = str(raw.get("NONDERIV_TRANS_SK") or "").strip()
        if not accession:
            continue
        identity = (accession, trans_key)
        if trans_key and identity in seen_transaction_keys:
            raise SecInsiderBulkError(f"duplicate NONDERIV_TRANS identity {identity}")
        if trans_key:
            seen_transaction_keys.add(identity)
        trans_by_accession[accession].append(raw)

    evidence_rows: list[dict[str, Any]] = []
    status_counts: Counter[str] = Counter()
    excluded_code_counts: Counter[str] = Counter()
    unmatched_accessions = 0
    issuer_context_mismatches = 0

    for accession, sub in submission_by_accession.items():
        document_type = str(sub.get("DOCUMENT_TYPE") or "").strip().upper()
        if document_type not in _ALLOWED_DOCUMENT_TYPES:
            continue
        issuer_cik_raw = str(sub.get("ISSUERCIK") or "").strip()
        if not issuer_cik_raw:
            status_counts["UNKNOWN_MISSING_ISSUER_CIK"] += 1
            continue
        issuer_cik = normalize_cik(issuer_cik_raw)
        if issuer_cik not in verified_ciks:
            continue

        filing_meta = acceptance.get((accession, issuer_cik))
        if filing_meta is None:
            if accession in accession_context_ciks:
                issuer_context_mismatches += 1
                status_counts[
                    "UNKNOWN_ACCESSION_PRESENT_ONLY_IN_OTHER_SEC_CONTEXTS"
                ] += 1
            else:
                unmatched_accessions += 1
                status_counts["UNKNOWN_ACCESSION_NOT_IN_SEC_BUNDLE"] += 1
            continue
        if filing_meta["sec_context_cik"] != issuer_cik:
            raise SecInsiderBulkError(
                "internal accession/issuer-CIK acceptance index mismatch"
            )
        if filing_meta["form"] != document_type:
            status_counts["CONFLICTING_FORM_TYPE"] += 1
            continue

        aff10b5_raw = sub.get("AFF10B5ONE")
        aff10b5 = _bool01(aff10b5_raw)
        reporting_owners = owner_by_accession.get(accession, [])

        for tx in trans_by_accession.get(accession, []):
            code = str(tx.get("TRANS_CODE") or "").strip().upper()
            if code not in _TARGET_CODES:
                if code:
                    excluded_code_counts[code] += 1
                continue

            trans_form_type = str(tx.get("TRANS_FORM_TYPE") or "").strip().upper()
            acq_disp = str(tx.get("TRANS_ACQUIRED_DISP_CD") or "").strip().upper()
            expected_acq_disp = "A" if code == "P" else "D"
            equity_swap = _bool01(tx.get("EQUITY_SWAP_INVOLVED"))
            shares = _number(tx.get("TRANS_SHARES"))
            price = _number(tx.get("TRANS_PRICEPERSHARE"))
            value = (shares * price) if shares is not None and price is not None else None
            reason_codes: list[str] = []

            if acq_disp != expected_acq_disp:
                candidate_status = "CONFLICTING_ACQUIRED_DISPOSED_CODE"
                reason_codes.append("P_S_CODE_INCONSISTENT_WITH_ACQUIRED_DISPOSED")
            elif trans_form_type != "4":
                candidate_status = "EXCLUDED_NON_FORM4_TRANSACTION"
                reason_codes.append("TRANS_FORM_TYPE_NOT_4")
            elif equity_swap is True:
                candidate_status = "EXCLUDED_EQUITY_SWAP"
                reason_codes.append("EQUITY_SWAP_INVOLVED")
            elif aff10b5 is True:
                candidate_status = "EXCLUDED_10B5_1_PLAN"
                reason_codes.append("AFF10B5ONE_TRUE")
            elif aff10b5 is None:
                candidate_status = "P_S_DISCRETIONARY_UNRESOLVED_10B5_1"
                reason_codes.append("AFF10B5ONE_UNKNOWN")
            elif shares is None:
                candidate_status = "P_S_PARTIAL_MISSING_SHARES"
                reason_codes.append("TRANSACTION_SHARES_UNKNOWN")
            else:
                candidate_status = "P_S_HIGH_PRECISION_DISCRETIONARY_CANDIDATE"

            if len(reporting_owners) != 1:
                reason_codes.append("REPORTING_OWNER_NOT_UNIQUE")
            if price is None:
                reason_codes.append("TRANSACTION_PRICE_UNKNOWN")

            strict_pit = (
                filing_meta.get("pit_status") in {"SAFE", "DATE_ONLY_DELAYED"}
                and bool(filing_meta.get("valid_from"))
            )
            transaction_date = _sec_date(tx.get("TRANS_DATE"))
            evidence_rows.append(
                {
                    "source_authority": "U.S. SEC EDGAR",
                    "source_dataset": "Insider Transactions Data Sets",
                    "source_quarter": label,
                    "accession_number": accession,
                    "document_type": document_type,
                    "issuer_cik": issuer_cik,
                    "issuer_name": sub.get("ISSUERNAME"),
                    "issuer_trading_symbol_reported": sub.get("ISSUERTRADINGSYMBOL"),
                    "scanner_symbol": filing_meta.get("symbol"),
                    "transaction_key": tx.get("NONDERIV_TRANS_SK"),
                    "transaction_date": transaction_date,
                    "event_time": transaction_date,
                    "transaction_form_type": trans_form_type,
                    "transaction_code": code,
                    "acquired_disposed_code": acq_disp,
                    "transaction_shares": shares,
                    "transaction_price_per_share": price,
                    "transaction_value_when_price_known": value,
                    "security_title": tx.get("SECURITY_TITLE"),
                    "post_transaction_shares": _number(
                        tx.get("SHRS_OWND_FOLWNG_TRANS")
                    ),
                    "direct_or_indirect_ownership": tx.get(
                        "DIRECT_INDIRECT_OWNERSHIP"
                    ),
                    "nature_of_ownership": tx.get("NATURE_OF_OWNERSHIP"),
                    "equity_swap_involved": equity_swap,
                    "transaction_timeliness": tx.get("TRANS_TIMELINESS"),
                    "aff10b5one": aff10b5,
                    "aff10b5one_raw": aff10b5_raw,
                    "reporting_owners": reporting_owners,
                    "published_at": filing_meta.get("published_at"),
                    "valid_from": filing_meta.get("valid_from"),
                    "pit_status": filing_meta.get("pit_status"),
                    "strict_pit_eligible": strict_pit,
                    "amendment": document_type == "4/A",
                    "candidate_status": candidate_status,
                    "discretionary_semantics_eligible": (
                        candidate_status
                        == "P_S_HIGH_PRECISION_DISCRETIONARY_CANDIDATE"
                    ),
                    "reason_codes": reason_codes,
                    "market_direction": "UNASSIGNED",
                }
            )
            status_counts[candidate_status] += 1

    evidence_rows.sort(
        key=lambda row: (
            str(row.get("valid_from") or ""),
            str(row.get("accession_number") or ""),
            str(row.get("transaction_key") or ""),
        )
    )
    discretionary_count = sum(
        1 for row in evidence_rows if row["discretionary_semantics_eligible"] is True
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "phase": "8D_B1_B2_SEC_insider_bulk",
        "source_authority": "U.S. SEC EDGAR",
        "source_dataset": "Insider Transactions Data Sets",
        "source_url": source_url,
        "source_quarter": label,
        "source_zip": str(insider_zip_path),
        "source_zip_sha256": zip_sha,
        "source_member_sha256": {
            "SUBMISSION": submission_sha,
            "REPORTINGOWNER": owner_sha,
            "NONDERIV_TRANS": trans_sha,
        },
        "sec_bulk_manifest_sha256": bundle_manifest_sha,
        "source_semantics": (
            "AS_FILED_FLAT_EXTRACTION_JOINED_TO_EXACT_ACCESSION_PLUS_ISSUER_CIK_"
            "ACCEPTANCE_METADATA"
        ),
        "counts": {
            "submission_rows": len(submissions),
            "reporting_owner_rows": len(owners),
            "nonderivative_transaction_rows": len(transactions),
            "verified_sec_cik_count": len(verified_ciks),
            "p_s_evidence_row_count": len(evidence_rows),
            "high_precision_discretionary_candidate_count": discretionary_count,
            "unmatched_verified_accession_count": unmatched_accessions,
            "issuer_context_mismatch_count": issuer_context_mismatches,
            "issuer_cik_conflict_count": issuer_context_mismatches,
        },
        "candidate_status_counts": dict(sorted(status_counts.items())),
        "excluded_transaction_code_counts": dict(sorted(excluded_code_counts.items())),
        "rows": evidence_rows,
        "guards": {
            "exact_accession_join_required": True,
            "exact_issuer_cik_join_required": True,
            "acceptance_index_key_includes_issuer_cik": True,
            "cross_entity_accession_contexts_allowed": True,
            "current_ticker_used_as_historical_identity": False,
            "forms_restricted_to_4_and_4_a": True,
            "nonderivative_transactions_only": True,
            "transaction_codes_restricted_to_p_and_s": True,
            "transaction_form_type_4_required_for_discretionary_candidate": True,
            "equity_swaps_excluded_from_discretionary_candidate": True,
            "ten_b5_1_true_excluded_from_discretionary_candidate": True,
            "ten_b5_1_unknown_not_assumed_discretionary": True,
            "amendments_silently_overwrite_original": False,
            "missing_evidence_defaults_to_neutral": False,
            "market_outcomes_read": False,
            "market_direction_assigned": False,
            "threshold_selection_run": False,
            "phase7_integration_enabled": False,
            "production_external_evidence_enabled": False,
            "historical_selection_research_enabled": False,
        },
    }


def write_sec_insider_evidence(payload: Mapping[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(dict(payload), indent=2, sort_keys=True), encoding="utf-8"
    )
