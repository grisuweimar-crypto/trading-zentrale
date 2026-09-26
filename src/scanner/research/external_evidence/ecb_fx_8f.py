from __future__ import annotations

import csv
import hashlib
import io
from datetime import date, datetime
from typing import Any


class ECBFX8FError(ValueError):
    pass


SOURCE_ID = "ecb_data_portal"
SERIES_ID = "EXR.D.USD.EUR.SP00.A"
FACTOR_ID = "fx"
UNITS = "USD_per_EUR"

_MISSING = {"", "NA", "N/A", "ND", "."}


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _parse_date(value: str) -> str:
    try:
        return date.fromisoformat(value.strip()).isoformat()
    except ValueError as exc:
        raise ECBFX8FError(f"ECB TIME_PERIOD must be YYYY-MM-DD for daily FX data, got {value!r}") from exc


def build_ecb_fx_prospective_macro_observations(
    csv_text: str,
    *,
    ingested_at: datetime,
) -> list[dict[str, Any]]:
    if ingested_at.tzinfo is None or ingested_at.utcoffset() is None:
        raise ECBFX8FError("ingested_at must be timezone-aware")
    if not isinstance(csv_text, str) or not csv_text.strip():
        raise ECBFX8FError("csv_text must be non-empty")

    reader = csv.DictReader(io.StringIO(csv_text))
    fieldnames = {str(name or "").strip().upper() for name in reader.fieldnames or []}
    required = {"TIME_PERIOD", "OBS_VALUE"}
    if not required.issubset(fieldnames):
        raise ECBFX8FError(
            "ECB CSV must contain TIME_PERIOD and OBS_VALUE columns"
        )

    source_sha = _sha256(csv_text)
    snapshot_date = ingested_at.date().isoformat()
    snapshot_token = ingested_at.isoformat()
    observations: list[dict[str, Any]] = []

    for raw in reader:
        row = {str(key or "").strip().upper(): value for key, value in raw.items()}
        if "KEY" in row and str(row.get("KEY") or "").strip() not in {"", SERIES_ID}:
            continue
        if "CURRENCY" in row and str(row.get("CURRENCY") or "").strip() not in {"", "USD"}:
            continue
        if "CURRENCY_DENOM" in row and str(row.get("CURRENCY_DENOM") or "").strip() not in {"", "EUR"}:
            continue
        if "FREQ" in row and str(row.get("FREQ") or "").strip() not in {"", "D"}:
            continue

        observation_date = _parse_date(str(row.get("TIME_PERIOD") or ""))
        raw_value = str(row.get("OBS_VALUE") or "").strip()
        if raw_value.upper() in _MISSING:
            status = "UNKNOWN"
            value = None
        else:
            try:
                value = float(raw_value)
            except ValueError as exc:
                raise ECBFX8FError(
                    f"non-numeric ECB FX value for {observation_date}: {raw_value!r}"
                ) from exc
            status = "KNOWN"

        observations.append(
            {
                "series_id": SERIES_ID,
                "factor_id": FACTOR_ID,
                "observation_date": observation_date,
                "value": value,
                "units": UNITS,
                "realtime_start": snapshot_date,
                "realtime_end": "9999-12-31",
                "revision_id": f"{SERIES_ID}:{observation_date}:snapshot:{snapshot_token}",
                "source_id": SOURCE_ID,
                "source_record_sha256": source_sha,
                "license_status": "USABLE_FREE_REUSE_WITH_ATTRIBUTION_FOR_ESCB_STATISTICS",
                "status": status,
                "ingested_at": ingested_at.isoformat(),
                "valid_from": ingested_at.isoformat(),
                "historical_vintage_independently_proven": False,
                "historical_publication_time_independently_proven": False,
                "published_at": None,
                "availability_proof_type": "ACTUAL_PROSPECTIVE_INGESTION",
            }
        )

    if not observations:
        raise ECBFX8FError(f"ECB CSV contains no observations for {SERIES_ID}")

    return sorted(observations, key=lambda row: row["observation_date"])
