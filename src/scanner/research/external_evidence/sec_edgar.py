from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

import requests

SEC_DATA_BASE = "https://data.sec.gov"
SEC_EASTERN = ZoneInfo("America/New_York")


class SecEdgarContractError(ValueError):
    """Raised when SEC input cannot satisfy the deterministic 8C contract."""


def normalize_cik(cik: str | int) -> str:
    """Return the SEC API CIK representation: exactly ten decimal digits."""
    text = str(cik).strip()
    if text.upper().startswith("CIK"):
        text = text[3:]
    if not text.isdigit():
        raise SecEdgarContractError(f"CIK must be numeric, got {cik!r}")
    if len(text) > 10:
        raise SecEdgarContractError(f"CIK exceeds 10 digits: {cik!r}")
    return text.zfill(10)


def accession_without_dashes(accession_number: str) -> str:
    value = str(accession_number or "").strip()
    if not value:
        raise SecEdgarContractError("accession_number is required")
    return value.replace("-", "")


def submissions_url(cik: str | int) -> str:
    return f"{SEC_DATA_BASE}/submissions/CIK{normalize_cik(cik)}.json"


def companyfacts_url(cik: str | int) -> str:
    return f"{SEC_DATA_BASE}/api/xbrl/companyfacts/CIK{normalize_cik(cik)}.json"


def fetch_sec_json(url: str, *, user_agent: str, timeout: float = 30.0) -> dict[str, Any]:
    """Fetch one SEC JSON resource.

    The caller is responsible for aggregate rate limiting. SEC fair-access guidance
    currently limits automated clients to no more than 10 requests/second.
    """
    if not str(user_agent or "").strip():
        raise SecEdgarContractError("A descriptive SEC User-Agent is required")
    response = requests.get(
        url,
        headers={
            "User-Agent": user_agent.strip(),
            "Accept-Encoding": "gzip, deflate",
            "Accept": "application/json",
        },
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise SecEdgarContractError("SEC JSON response must be an object")
    return payload


def _parse_aware_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def compute_valid_from(
    *, filing_date: Any = None, acceptance_datetime: Any = None
) -> dict[str, Any]:
    """Apply the conservative 8C publication-time rule.

    Exact, timezone-aware EDGAR acceptance time is preferred. If only a filing date
    is known, the record is delayed until midnight US/Eastern on the next calendar
    day; it is never allowed to become valid intraday on the filing date.
    """
    accepted = _parse_aware_datetime(acceptance_datetime)
    if accepted is not None:
        stamp = accepted.isoformat()
        return {
            "published_at": stamp,
            "valid_from": stamp,
            "pit_status": "SAFE",
            "reason_codes": [],
        }

    filed = _parse_date(filing_date)
    if filed is not None:
        next_day = filed + timedelta(days=1)
        delayed = datetime.combine(next_day, time.min, tzinfo=SEC_EASTERN)
        return {
            "published_at": None,
            "valid_from": delayed.isoformat(),
            "pit_status": "DATE_ONLY_DELAYED",
            "reason_codes": ["DATE_ONLY_DELAYED_VALID_FROM"],
        }

    return {
        "published_at": None,
        "valid_from": None,
        "pit_status": "UNKNOWN",
        "reason_codes": ["MISSING_PUBLICATION_TIME"],
    }


def classify_publication_stage(form: Any) -> str:
    form_text = str(form or "").strip().upper()
    if not form_text:
        return "UNKNOWN"
    if form_text.endswith("/A"):
        return "AMENDMENT"
    if form_text in {"8-K", "6-K"}:
        return "CURRENT_REPORT"
    if form_text in {"10-Q", "10-K", "20-F", "40-F"}:
        return "PERIODIC_REPORT"
    return "UNKNOWN"


def _parallel_value(recent: Mapping[str, Any], key: str, index: int) -> Any:
    values = recent.get(key)
    if not isinstance(values, list) or index >= len(values):
        return None
    return values[index]


def _normalize_items(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        raw = value
    else:
        raw = str(value).replace(";", ",").split(",")
    return [str(item).strip().replace("Item ", "") for item in raw if str(item).strip()]


def submission_rows(
    payload: Mapping[str, Any], *, forms: Iterable[str] | None = None
) -> list[dict[str, Any]]:
    """Normalize SEC `filings.recent` parallel arrays into deterministic rows.

    This function deliberately handles only the `recent` block. Historical coverage
    claims must additionally ingest every file referenced by `filings.files`.
    """
    cik = normalize_cik(payload.get("cik", ""))
    recent = ((payload.get("filings") or {}).get("recent") or {})
    if not isinstance(recent, Mapping):
        raise SecEdgarContractError("filings.recent must be an object of parallel arrays")

    accessions = recent.get("accessionNumber") or []
    if not isinstance(accessions, list):
        raise SecEdgarContractError("filings.recent.accessionNumber must be an array")

    form_filter = {str(x).upper() for x in forms} if forms is not None else None
    rows: list[dict[str, Any]] = []
    for i, accession in enumerate(accessions):
        form = str(_parallel_value(recent, "form", i) or "")
        if form_filter is not None and form.upper() not in form_filter:
            continue
        filing_date = _parallel_value(recent, "filingDate", i)
        acceptance = _parallel_value(recent, "acceptanceDateTime", i)
        pit = compute_valid_from(
            filing_date=filing_date,
            acceptance_datetime=acceptance,
        )
        items = _normalize_items(_parallel_value(recent, "items", i))
        row = {
            "source": "sec_edgar_submissions_8k_6k",
            "cik": cik,
            "company_name": payload.get("name"),
            "tickers": list(payload.get("tickers") or []),
            "exchanges": list(payload.get("exchanges") or []),
            "accession_number": accession,
            "form": form,
            "filed_date": filing_date,
            "report_date": _parallel_value(recent, "reportDate", i),
            "acceptance_datetime_raw": acceptance,
            "published_at": pit["published_at"],
            "valid_from": pit["valid_from"],
            "pit_status": pit["pit_status"],
            "publication_stage": classify_publication_stage(form),
            "revision_id": accession,
            "items": items,
            "primary_document": _parallel_value(recent, "primaryDocument", i),
            "primary_doc_description": _parallel_value(recent, "primaryDocDescription", i),
            "is_xbrl": _parallel_value(recent, "isXBRL", i),
            "is_inline_xbrl": _parallel_value(recent, "isInlineXBRL", i),
            "status": "KNOWN" if accession else "UNKNOWN",
            "reason_codes": list(pit["reason_codes"]),
        }
        if not accession:
            row["reason_codes"].append("MISSING_ACCESSION")
        rows.append(row)
    return rows


def build_acceptance_index(rows: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    """Index normalized filing publication metadata by accession number."""
    index: dict[str, dict[str, Any]] = {}
    for row in rows:
        accession = str(row.get("accession_number") or "").strip()
        if not accession:
            continue
        index[accession] = {
            "filed_date": row.get("filed_date"),
            "published_at": row.get("published_at"),
            "valid_from": row.get("valid_from"),
            "pit_status": row.get("pit_status"),
            "publication_stage": row.get("publication_stage"),
            "reason_codes": list(row.get("reason_codes") or []),
        }
    return index


def companyfacts_rows(
    payload: Mapping[str, Any],
    *,
    acceptance_index: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Flatten SEC Company Facts while preserving every accession/version.

    No attempt is made here to choose a 'best' concept, period or latest value. That
    would destroy the point-in-time evidence needed by Phase 8C.
    """
    cik = normalize_cik(payload.get("cik", ""))
    output: list[dict[str, Any]] = []
    facts = payload.get("facts") or {}
    if not isinstance(facts, Mapping):
        raise SecEdgarContractError("companyfacts.facts must be an object")

    for taxonomy, concepts in facts.items():
        if not isinstance(concepts, Mapping):
            continue
        for concept, concept_payload in concepts.items():
            if not isinstance(concept_payload, Mapping):
                continue
            units = concept_payload.get("units") or {}
            if not isinstance(units, Mapping):
                continue
            for unit, unit_facts in units.items():
                if not isinstance(unit_facts, list):
                    continue
                for raw in unit_facts:
                    if not isinstance(raw, Mapping):
                        continue
                    accession = str(raw.get("accn") or "").strip()
                    publication = acceptance_index.get(accession) or {}
                    reason_codes = list(publication.get("reason_codes") or [])
                    if not accession:
                        reason_codes.append("MISSING_ACCESSION")
                    if accession and accession not in acceptance_index:
                        reason_codes.append("ACCESSION_NOT_IN_SUBMISSION_INDEX")
                    output.append(
                        {
                            "source": "sec_edgar_companyfacts",
                            "cik": cik,
                            "entity_name": payload.get("entityName"),
                            "accession_number": accession or None,
                            "revision_id": accession or None,
                            "taxonomy": taxonomy,
                            "concept": concept,
                            "label": concept_payload.get("label"),
                            "description": concept_payload.get("description"),
                            "unit": unit,
                            "value": raw.get("val"),
                            "start": raw.get("start"),
                            "end": raw.get("end"),
                            "filed_date": raw.get("filed") or publication.get("filed_date"),
                            "form": raw.get("form"),
                            "fiscal_year": raw.get("fy"),
                            "fiscal_period": raw.get("fp"),
                            "frame": raw.get("frame"),
                            "published_at": publication.get("published_at"),
                            "valid_from": publication.get("valid_from"),
                            "pit_status": publication.get("pit_status", "UNKNOWN"),
                            "publication_stage": publication.get("publication_stage")
                            or classify_publication_stage(raw.get("form")),
                            "status": "KNOWN" if raw.get("val") is not None else "UNKNOWN",
                            "reason_codes": reason_codes,
                        }
                    )
    return output


def filing_candidate_events(
    row: Mapping[str, Any], *, item_mapping: Mapping[str, str]
) -> list[dict[str, Any]]:
    """Emit non-directional event candidates from one normalized filing row."""
    form = str(row.get("form") or "").upper()
    items = list(row.get("items") or [])
    candidates: list[tuple[str | None, str, str]] = []

    if form in {"8-K", "8-K/A"}:
        for item in items:
            event_type = item_mapping.get(str(item))
            if event_type:
                candidates.append((str(item), event_type, "FILING_ITEM_ONLY"))
        if not candidates:
            candidates.append((None, "UNCLASSIFIED_CURRENT_REPORT", "UNKNOWN"))
    elif form in {"6-K", "6-K/A"}:
        candidates.append((None, "FOREIGN_CURRENT_REPORT_UNCLASSIFIED", "UNKNOWN"))
    else:
        return []

    return [
        {
            "source": row.get("source"),
            "cik": row.get("cik"),
            "accession_number": row.get("accession_number"),
            "form": row.get("form"),
            "item_code": item,
            "candidate_event_type": event_type,
            "semantic_state": semantic_state,
            "direction": "UNKNOWN",
            "published_at": row.get("published_at"),
            "valid_from": row.get("valid_from"),
            "revision_id": row.get("revision_id"),
            "status": row.get("status", "UNKNOWN"),
            "reason_codes": list(row.get("reason_codes") or []),
        }
        for item, event_type, semantic_state in candidates
    ]
