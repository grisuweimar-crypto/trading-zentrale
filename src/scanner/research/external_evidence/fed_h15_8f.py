from __future__ import annotations

import csv
import hashlib
import io
from datetime import date, datetime
from typing import Any


class FedH158FError(ValueError):
    pass


SOURCE_ID = "federal_reserve_board_h15"
SERIES_SPECS: dict[str, dict[str, str]] = {
    "RIFSPFF_N.D": {
        "factor_id": "rates_policy",
        "units": "percent_per_year",
        "label": "Federal funds effective rate",
    },
    "RIFLGFCY02_N.B": {
        "factor_id": "yield_curve",
        "units": "percent_per_year",
        "label": "2-year Treasury constant maturity",
    },
    "RIFLGFCY10_N.B": {
        "factor_id": "yield_curve",
        "units": "percent_per_year",
        "label": "10-year Treasury constant maturity",
    },
}

_MISSING = {"", "ND", "N.A.", "NA", "N/A", "."}


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _date(value: str) -> str:
    try:
        return date.fromisoformat(value.strip()).isoformat()
    except ValueError as exc:
        raise FedH158FError(f"invalid H15 daily observation date: {value!r}") from exc


def build_fed_h15_prospective_macro_observations(
    csv_text: str,
    *,
    ingested_at: datetime,
    selected_series_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    if ingested_at.tzinfo is None or ingested_at.utcoffset() is None:
        raise FedH158FError("ingested_at must be timezone-aware")
    if not isinstance(csv_text, str) or not csv_text.strip():
        raise FedH158FError("csv_text must be non-empty")

    requested = set(SERIES_SPECS) if selected_series_ids is None else set(selected_series_ids)
    unknown = requested - set(SERIES_SPECS)
    if unknown:
        raise FedH158FError(f"unregistered H15 series requested: {sorted(unknown)}")

    rows = list(csv.reader(io.StringIO(csv_text)))
    header_index = next(
        (index for index, row in enumerate(rows) if row and row[0].strip().lower() == "series"),
        None,
    )
    if header_index is None:
        raise FedH158FError("H15 CSV missing Series header row")

    header = rows[header_index]
    if len(header) < 3 or header[1].strip().lower().replace("\u00a0", " ") not in {
        "series description",
        "seriesdescription",
    }:
        raise FedH158FError("H15 CSV missing Series Description column")

    date_columns: list[tuple[int, str]] = []
    for index, value in enumerate(header[2:], start=2):
        text = value.strip()
        if not text:
            continue
        date_columns.append((index, _date(text)))
    if not date_columns:
        raise FedH158FError("H15 CSV contains no daily date columns")

    source_sha = _sha256(csv_text)
    snapshot_date = ingested_at.date().isoformat()
    snapshot_token = ingested_at.isoformat()
    observations: list[dict[str, Any]] = []
    seen_series: set[str] = set()

    for row in rows[header_index + 1 :]:
        if not row:
            continue
        series_id = row[0].strip()
        if series_id not in requested:
            continue
        seen_series.add(series_id)
        spec = SERIES_SPECS[series_id]

        for column_index, observation_date in date_columns:
            raw_value = row[column_index].strip() if column_index < len(row) else ""
            normalized = raw_value.upper()
            if normalized in _MISSING:
                status = "UNKNOWN"
                value = None
            else:
                try:
                    value = float(raw_value)
                except ValueError as exc:
                    raise FedH158FError(
                        f"non-numeric H15 value for {series_id} on {observation_date}: {raw_value!r}"
                    ) from exc
                status = "KNOWN"

            observations.append(
                {
                    "series_id": series_id,
                    "factor_id": spec["factor_id"],
                    "observation_date": observation_date,
                    "value": value,
                    "units": spec["units"],
                    "realtime_start": snapshot_date,
                    "realtime_end": "9999-12-31",
                    "revision_id": f"{series_id}:{observation_date}:snapshot:{snapshot_token}",
                    "source_id": SOURCE_ID,
                    "source_record_sha256": source_sha,
                    "license_status": "USABLE_PUBLIC_DOMAIN_UNLESS_MARKED_OTHERWISE",
                    "status": status,
                    "ingested_at": ingested_at.isoformat(),
                    "valid_from": ingested_at.isoformat(),
                    "historical_vintage_independently_proven": False,
                    "historical_publication_time_independently_proven": False,
                    "published_at": None,
                    "availability_proof_type": "ACTUAL_PROSPECTIVE_INGESTION",
                }
            )

    missing_series = requested - seen_series
    if missing_series:
        raise FedH158FError(f"requested H15 series absent from CSV: {sorted(missing_series)}")

    return sorted(observations, key=lambda row: (row["series_id"], row["observation_date"]))
