from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

import requests

from .finra_short_interest import (
    PROSPECTIVE_MODE,
    FinraShortInterestError,
    build_finra_snapshot,
    publication_valid_from,
    write_finra_snapshot,
)


FINRA_DATASET_URL = "https://api.finra.org/data/group/otcmarket/name/consolidatedShortInterest"
ACQUISITION_SCHEMA = "external_evidence_8d_finra_short_interest_acquisition_v1"
_FIELDS = [
    "symbolCode",
    "settlementDate",
    "currentShortPositionQuantity",
    "previousShortPositionQuantity",
    "changePreviousNumber",
    "changePercent",
    "averageDailyVolumeQuantity",
    "daysToCoverQuantity",
    "revisionFlag",
    "stockSplitFlag",
    "marketClassCode",
    "issueName",
]


class FinraShortInterestAcquisitionError(ValueError):
    """Raised when a FINRA publication snapshot cannot be acquired safely."""


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(value: str) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise FinraShortInterestAcquisitionError(f"invalid ISO timestamp: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise FinraShortInterestAcquisitionError("timestamp must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _header_int(headers: Mapping[str, Any], name: str) -> int | None:
    lookup = {str(key).lower(): value for key, value in headers.items()}
    raw = lookup.get(name.lower())
    if raw in {None, ""}:
        return None
    try:
        return int(str(raw))
    except ValueError:
        return None


def _request_page(
    *,
    settlement_date: str,
    offset: int,
    limit: int,
    timeout: float,
    post_fn: Callable[..., Any],
) -> tuple[list[dict[str, Any]], dict[str, str], bytes]:
    payload = {
        "fields": _FIELDS,
        "dateRangeFilters": [
            {
                "fieldName": "settlementDate",
                "startDate": settlement_date,
                "endDate": settlement_date,
            }
        ],
        "limit": limit,
        "offset": offset,
    }
    response = post_fn(
        FINRA_DATASET_URL,
        json=payload,
        headers={"Accept": "application/json"},
        timeout=timeout,
    )
    status = int(getattr(response, "status_code", 0) or 0)
    if status != 200:
        text = str(getattr(response, "text", ""))[:1000]
        raise FinraShortInterestAcquisitionError(
            f"FINRA request failed HTTP {status}: {text}"
        )
    raw = bytes(getattr(response, "content", b"") or b"")
    if not raw:
        raise FinraShortInterestAcquisitionError("FINRA returned an empty response body")
    try:
        data = response.json()
    except Exception:
        try:
            data = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise FinraShortInterestAcquisitionError("FINRA response is not valid JSON") from exc
    if not isinstance(data, list):
        raise FinraShortInterestAcquisitionError("FINRA response root must be an array")
    rows = [dict(row) for row in data if isinstance(row, Mapping)]
    headers = {str(key): str(value) for key, value in dict(getattr(response, "headers", {}) or {}).items()}
    return rows, headers, raw


def collect_finra_publication_snapshot(
    *,
    settlement_date: str,
    publication_date: str,
    output_dir: Path,
    collected_at: str | None = None,
    limit: int = 5000,
    timeout: float = 60.0,
    minimum_interval_seconds: float = 0.25,
    max_pages: int = 200,
    post_fn: Callable[..., Any] = requests.post,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Collect one FINRA publication snapshot for one settlement date.

    This collector is intentionally prospective-only. Historical backfill must use the
    separate importer and remain latest-available-vintage evidence.
    """
    if limit <= 0 or limit > 5000:
        raise FinraShortInterestAcquisitionError("limit must be between 1 and 5000")
    if max_pages <= 0:
        raise FinraShortInterestAcquisitionError("max_pages must be positive")

    try:
        datetime.fromisoformat(f"{settlement_date}T00:00:00")
    except ValueError as exc:
        raise FinraShortInterestAcquisitionError(
            f"invalid settlement_date: {settlement_date!r}"
        ) from exc

    collected_dt = _parse_iso(collected_at) if collected_at else _utc_now()
    published_dt = _parse_iso(publication_valid_from(publication_date))
    if collected_dt < published_dt:
        raise FinraShortInterestAcquisitionError(
            "prospective FINRA collection attempted before 16:40 ET publication availability"
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    pages_dir = output_dir / "pages"
    pages_dir.mkdir(parents=True, exist_ok=True)

    all_rows: list[dict[str, Any]] = []
    page_manifest: list[dict[str, Any]] = []
    offset = 0
    record_total: int | None = None
    previous_request_finished: float | None = None

    for page_number in range(1, max_pages + 1):
        if previous_request_finished is not None:
            remaining = minimum_interval_seconds - (time.monotonic() - previous_request_finished)
            if remaining > 0:
                sleep_fn(remaining)

        rows, headers, raw = _request_page(
            settlement_date=settlement_date,
            offset=offset,
            limit=limit,
            timeout=timeout,
            post_fn=post_fn,
        )
        previous_request_finished = time.monotonic()
        page_path = pages_dir / f"page_{page_number:04d}_offset_{offset}.json"
        page_path.write_bytes(raw)
        page_hash = _sha256(raw)

        header_total = _header_int(headers, "Record-Total")
        if header_total is not None:
            if record_total is None:
                record_total = header_total
            elif record_total != header_total:
                raise FinraShortInterestAcquisitionError(
                    "FINRA Record-Total changed during one snapshot acquisition"
                )

        page_manifest.append(
            {
                "page_number": page_number,
                "offset": offset,
                "requested_limit": limit,
                "row_count": len(rows),
                "sha256": page_hash,
                "path": str(page_path.relative_to(output_dir)),
                "record_total_header": header_total,
                "request_id": (
                    headers.get("FINRA-api-request-id")
                    or headers.get("Finra-Api-Request-Id")
                    or headers.get("finra-api-request-id")
                ),
            }
        )
        all_rows.extend(rows)

        if not rows:
            break
        offset += len(rows)
        if record_total is not None and offset >= record_total:
            break
        if len(rows) < limit and record_total is None:
            break
        if offset > 500000:
            raise FinraShortInterestAcquisitionError(
                "FINRA synchronous offset ceiling reached; refine the acquisition filter"
            )
    else:
        raise FinraShortInterestAcquisitionError("max_pages reached before collection completed")

    if not all_rows:
        raise FinraShortInterestAcquisitionError(
            f"FINRA returned no rows for settlement date {settlement_date}"
        )
    if record_total is not None and len(all_rows) != record_total:
        raise FinraShortInterestAcquisitionError(
            f"FINRA pagination incomplete: collected {len(all_rows)} of {record_total} rows"
        )

    wrong_dates = sorted(
        {
            str(row.get("settlementDate") or "")
            for row in all_rows
            if str(row.get("settlementDate") or "") != settlement_date
        }
    )
    if wrong_dates:
        raise FinraShortInterestAcquisitionError(
            f"FINRA filtered response contained unexpected settlement dates: {wrong_dates[:5]}"
        )

    raw_rows_path = output_dir / "raw_rows.json"
    canonical_raw = (json.dumps(all_rows, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    raw_rows_path.write_bytes(canonical_raw)

    normalized_path = output_dir / "snapshot.json"
    snapshot = build_finra_snapshot(
        source_path=raw_rows_path,
        publication_date=publication_date,
        ingested_at=collected_dt.isoformat(),
        source_mode=PROSPECTIVE_MODE,
        source_url=FINRA_DATASET_URL,
    )
    write_finra_snapshot(snapshot, normalized_path)

    manifest = {
        "schema_version": ACQUISITION_SCHEMA,
        "phase": "8D_A1_FINRA_publication_snapshot",
        "source_authority": "FINRA",
        "source_dataset": "consolidatedShortInterest",
        "source_url": FINRA_DATASET_URL,
        "settlement_date": settlement_date,
        "publication_date": publication_date,
        "published_at": publication_valid_from(publication_date),
        "collected_at": collected_dt.isoformat(),
        "source_mode": PROSPECTIVE_MODE,
        "strict_pit_eligible": True,
        "raw_row_count": len(all_rows),
        "record_total_header": record_total,
        "raw_rows_path": str(raw_rows_path.relative_to(output_dir)),
        "raw_rows_sha256": _sha256(canonical_raw),
        "normalized_snapshot_path": str(normalized_path.relative_to(output_dir)),
        "pages": page_manifest,
        "guards": {
            "filtered_to_one_settlement_date": True,
            "publication_time_gate_applied": True,
            "market_outcomes_read": False,
            "market_direction_assigned": False,
            "phase7_integration_enabled": False,
            "production_external_evidence_enabled": False,
        },
    }
    manifest_path = output_dir / "acquisition_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return manifest
