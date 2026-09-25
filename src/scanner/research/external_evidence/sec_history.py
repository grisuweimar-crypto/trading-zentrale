from __future__ import annotations

from typing import Any, Iterable, Mapping

from .sec_edgar import SEC_DATA_BASE, SecEdgarContractError, normalize_cik, submission_rows


def historical_submission_file_url(name: str) -> str:
    value = str(name or "").strip()
    if not value or "/" in value or "\\" in value or ".." in value:
        raise SecEdgarContractError(f"Unsafe SEC history file name: {name!r}")
    return f"{SEC_DATA_BASE}/submissions/{value}"


def historical_submission_file_specs(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    filings = payload.get("filings") or {}
    files = filings.get("files") or []
    if not isinstance(files, list):
        raise SecEdgarContractError("filings.files must be an array")

    specs: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in files:
        if not isinstance(raw, Mapping):
            raise SecEdgarContractError("Each filings.files entry must be an object")
        name = str(raw.get("name") or "").strip()
        if not name:
            raise SecEdgarContractError("Historical submission file is missing name")
        historical_submission_file_url(name)
        if name in seen:
            raise SecEdgarContractError(f"Duplicate historical submission file: {name}")
        seen.add(name)
        specs.append(
            {
                "name": name,
                "url": historical_submission_file_url(name),
                "filing_count": raw.get("filingCount"),
                "filing_from": raw.get("filingFrom"),
                "filing_to": raw.get("filingTo"),
            }
        )
    return specs


def historical_submission_rows(
    payload: Mapping[str, Any],
    *,
    cik: str | int,
    company_name: str | None = None,
    tickers: Iterable[str] | None = None,
    exchanges: Iterable[str] | None = None,
    forms: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    """Normalize one SEC historical submissions file.

    SEC historical files use the same parallel-array fields as `filings.recent`, but
    the arrays are stored at the JSON root. We wrap them into the canonical recent
    shape and reuse the Phase 8C-A normalizer.
    """
    if "filings" in payload and isinstance(payload.get("filings"), Mapping):
        recent = (payload.get("filings") or {}).get("recent") or {}
    else:
        recent = payload
    wrapped = {
        "cik": normalize_cik(cik),
        "name": company_name,
        "tickers": list(tickers or []),
        "exchanges": list(exchanges or []),
        "filings": {"recent": recent},
    }
    return submission_rows(wrapped, forms=forms)


def _duplicate_signature(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("form"),
        row.get("filed_date"),
        row.get("report_date"),
        row.get("published_at"),
        row.get("valid_from"),
        tuple(row.get("items") or []),
        row.get("primary_document"),
    )


def assemble_full_submission_history(
    primary_payload: Mapping[str, Any],
    *,
    historical_payloads: Mapping[str, Mapping[str, Any]],
    forms: Iterable[str] | None = None,
    require_complete: bool = True,
) -> dict[str, Any]:
    """Assemble recent + every referenced historical submissions file.

    The function refuses silent `latest row wins` behavior for duplicate accession
    numbers. If the same accession appears with conflicting metadata, the history is
    not deterministic and a contract error is raised.
    """
    cik = normalize_cik(primary_payload.get("cik", ""))
    specs = historical_submission_file_specs(primary_payload)
    expected_names = [spec["name"] for spec in specs]
    missing = [name for name in expected_names if name not in historical_payloads]
    if require_complete and missing:
        raise SecEdgarContractError(
            "Incomplete SEC submission history; missing files: " + ", ".join(missing)
        )

    rows = submission_rows(primary_payload, forms=forms)
    company_name = primary_payload.get("name")
    tickers = list(primary_payload.get("tickers") or [])
    exchanges = list(primary_payload.get("exchanges") or [])

    loaded_names: list[str] = []
    for spec in specs:
        name = spec["name"]
        historical = historical_payloads.get(name)
        if historical is None:
            continue
        loaded_names.append(name)
        rows.extend(
            historical_submission_rows(
                historical,
                cik=cik,
                company_name=company_name,
                tickers=tickers,
                exchanges=exchanges,
                forms=forms,
            )
        )

    by_accession: dict[str, dict[str, Any]] = {}
    without_accession: list[dict[str, Any]] = []
    for row in rows:
        accession = str(row.get("accession_number") or "").strip()
        if not accession:
            without_accession.append(row)
            continue
        existing = by_accession.get(accession)
        if existing is None:
            by_accession[accession] = row
            continue
        if _duplicate_signature(existing) != _duplicate_signature(row):
            raise SecEdgarContractError(
                f"Conflicting metadata for duplicate accession {accession}"
            )

    unique_rows = list(by_accession.values()) + without_accession
    filing_dates = [
        str(row["filed_date"])
        for row in unique_rows
        if row.get("filed_date")
    ]
    exact = sum(1 for row in unique_rows if row.get("pit_status") == "SAFE")
    delayed = sum(1 for row in unique_rows if row.get("pit_status") == "DATE_ONLY_DELAYED")
    unknown = sum(1 for row in unique_rows if row.get("pit_status") == "UNKNOWN")

    coverage = {
        "cik": cik,
        "history_files_expected": len(expected_names),
        "history_files_loaded": len(loaded_names),
        "missing_history_files": missing,
        "history_complete": not missing,
        "filing_count": len(unique_rows),
        "unique_accession_count": len(by_accession),
        "rows_without_accession": len(without_accession),
        "earliest_filing_date": min(filing_dates) if filing_dates else None,
        "latest_filing_date": max(filing_dates) if filing_dates else None,
        "exact_publication_timestamp_count": exact,
        "date_only_delayed_count": delayed,
        "unknown_publication_time_count": unknown,
        "forms_covered": sorted({str(row.get("form") or "") for row in unique_rows if row.get("form")}),
        "research_ready": (not missing) and unknown == 0 and len(without_accession) == 0,
        "reason_codes": [],
    }
    if missing:
        coverage["reason_codes"].append("INCOMPLETE_HISTORY")
    if unknown:
        coverage["reason_codes"].append("UNKNOWN_PUBLICATION_TIME")
    if without_accession:
        coverage["reason_codes"].append("ROWS_WITHOUT_ACCESSION")

    return {
        "rows": unique_rows,
        "coverage": coverage,
        "history_file_specs": specs,
    }


def companyfacts_accession_coverage(
    fact_rows: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    rows = list(fact_rows)
    total = len(rows)
    resolved = sum(
        1
        for row in rows
        if row.get("accession_number")
        and "ACCESSION_NOT_IN_SUBMISSION_INDEX" not in (row.get("reason_codes") or [])
        and row.get("valid_from")
    )
    missing_accession = sum(1 for row in rows if not row.get("accession_number"))
    unresolved = total - resolved
    return {
        "fact_row_count": total,
        "resolved_accession_count": resolved,
        "unresolved_accession_count": unresolved,
        "missing_accession_count": missing_accession,
        "accession_resolution_rate": (resolved / total) if total else None,
        "research_ready": total > 0 and unresolved == 0,
        "reason_codes": [] if unresolved == 0 else ["UNRESOLVED_COMPANYFACTS_ACCESSIONS"],
    }
