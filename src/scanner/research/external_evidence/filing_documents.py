from __future__ import annotations

import csv
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping

import requests

from .sec_edgar import accession_without_dashes, normalize_cik

SEC_ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data"


class FilingDocumentContractError(ValueError):
    """Raised when filing document acquisition would violate the 8C-F contract."""


def research_window_from_history(path: Path, *, lookback_days: int = 0) -> dict[str, str | int]:
    if lookback_days < 0:
        raise FilingDocumentContractError("lookback_days must be >= 0")
    dates: list[date] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or "date" not in reader.fieldnames:
            raise FilingDocumentContractError("history CSV requires date column")
        for row in reader:
            raw = str(row.get("date") or "").strip()
            if not raw:
                continue
            try:
                dates.append(date.fromisoformat(raw[:10]))
            except ValueError as exc:
                raise FilingDocumentContractError(f"Invalid history date: {raw!r}") from exc
    if not dates:
        raise FilingDocumentContractError("history CSV contains no valid dates")
    observed_start = min(dates)
    observed_end = max(dates)
    acquisition_start = observed_start - timedelta(days=lookback_days)
    return {
        "observed_start": observed_start.isoformat(),
        "observed_end": observed_end.isoformat(),
        "acquisition_start": acquisition_start.isoformat(),
        "acquisition_end": observed_end.isoformat(),
        "lookback_days": lookback_days,
    }


def archive_company_id(cik: str | int) -> str:
    return str(int(normalize_cik(cik)))


def full_submission_text_url(cik: str | int, accession_number: str) -> str:
    accession = str(accession_number or "").strip()
    if not accession:
        raise FilingDocumentContractError("accession_number is required")
    accession_dir = accession_without_dashes(accession)
    return f"{SEC_ARCHIVES_BASE}/{archive_company_id(cik)}/{accession_dir}/{accession}.txt"


def _valid_from_date(row: Mapping[str, Any]) -> date | None:
    value = row.get("valid_from")
    if value:
        text = str(value).strip().replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(text).date()
        except ValueError:
            pass
    filed = str(row.get("filed_date") or "").strip()
    if filed:
        try:
            return date.fromisoformat(filed[:10])
        except ValueError:
            return None
    return None


def select_content_filings(
    filing_rows: Iterable[Mapping[str, Any]],
    *,
    acquisition_start: str,
    acquisition_end: str,
    forms: Iterable[str] = ("8-K", "8-K/A", "6-K", "6-K/A"),
) -> list[dict[str, Any]]:
    start = date.fromisoformat(acquisition_start)
    end = date.fromisoformat(acquisition_end)
    if start > end:
        raise FilingDocumentContractError("acquisition_start is after acquisition_end")
    allowed = {str(form).upper() for form in forms}
    selected: list[dict[str, Any]] = []
    for raw in filing_rows:
        form = str(raw.get("form") or "").upper()
        if form not in allowed:
            continue
        accession = str(raw.get("accession_number") or "").strip()
        if not accession:
            continue
        valid_date = _valid_from_date(raw)
        if valid_date is None or valid_date < start or valid_date > end:
            continue
        row = dict(raw)
        row["content_source_url"] = full_submission_text_url(raw.get("cik"), accession)
        row["content_valid_date"] = valid_date.isoformat()
        selected.append(row)
    selected.sort(key=lambda row: (str(row.get("valid_from") or ""), str(row.get("accession_number") or "")))
    return selected


def fetch_sec_document_bytes(url: str, *, user_agent: str, timeout: float = 60.0) -> bytes:
    if not str(user_agent or "").strip():
        raise FilingDocumentContractError("A descriptive SEC User-Agent is required")
    response = requests.get(
        url,
        headers={
            "User-Agent": user_agent.strip(),
            "Accept-Encoding": "gzip, deflate",
            "Accept": "text/plain,text/html,*/*",
        },
        timeout=timeout,
    )
    response.raise_for_status()
    if not response.content:
        raise FilingDocumentContractError(f"Empty SEC filing document: {url}")
    return response.content
