from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from typing import Any, Mapping


class EIAEnergy8FError(ValueError):
    pass


SOURCE_ID = "eia_open_data_energy"
SERIES_SPECS: dict[str, dict[str, str]] = {
    "PET.RWTC.D": {
        "factor_id": "oil",
        "units": "dollars_per_barrel",
        "label": "Cushing, OK WTI Spot Price FOB",
    },
    "PET.RBRTE.D": {
        "factor_id": "oil",
        "units": "dollars_per_barrel",
        "label": "Europe Brent Spot Price FOB",
    },
    "NG.RNGWHHD.D": {
        "factor_id": "gas",
        "units": "dollars_per_million_btu",
        "label": "Henry Hub Natural Gas Spot Price",
    },
}


def _canonical_sha256(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _parse_period(value: Any) -> str:
    text = str(value or "").strip()
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise EIAEnergy8FError(f"EIA daily observation period must be YYYY-MM-DD, got {value!r}") from exc


def build_eia_prospective_macro_observations(
    payload: Mapping[str, Any],
    *,
    series_id: str,
    ingested_at: datetime,
) -> list[dict[str, Any]]:
    if series_id not in SERIES_SPECS:
        raise EIAEnergy8FError(f"series_id is not predeclared for Phase 8F: {series_id}")
    if ingested_at.tzinfo is None or ingested_at.utcoffset() is None:
        raise EIAEnergy8FError("ingested_at must be timezone-aware")

    response = payload.get("response")
    if not isinstance(response, Mapping):
        raise EIAEnergy8FError("EIA payload missing response object")
    rows = response.get("data")
    if not isinstance(rows, list):
        raise EIAEnergy8FError("EIA payload response.data must be a list")

    spec = SERIES_SPECS[series_id]
    source_sha = _canonical_sha256(payload)
    snapshot_date = ingested_at.date().isoformat()
    snapshot_token = ingested_at.isoformat()
    observations: list[dict[str, Any]] = []

    for raw in rows:
        if not isinstance(raw, Mapping):
            raise EIAEnergy8FError("EIA response.data row must be an object")
        observation_date = _parse_period(raw.get("period"))
        raw_value = raw.get("value")
        status = "KNOWN"
        value: float | None
        if raw_value in (None, "", "NA", "N/A"):
            status = "UNKNOWN"
            value = None
        else:
            try:
                value = float(raw_value)
            except (TypeError, ValueError) as exc:
                raise EIAEnergy8FError(f"non-numeric EIA value for {observation_date}: {raw_value!r}") from exc

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
                "license_status": "USABLE_API_TERMS",
                "status": status,
                "ingested_at": ingested_at.isoformat(),
                "valid_from": ingested_at.isoformat(),
                "historical_vintage_independently_proven": False,
                "historical_publication_time_independently_proven": False,
                "published_at": None,
                "availability_proof_type": "ACTUAL_PROSPECTIVE_INGESTION",
            }
        )

    return sorted(observations, key=lambda row: row["observation_date"])
