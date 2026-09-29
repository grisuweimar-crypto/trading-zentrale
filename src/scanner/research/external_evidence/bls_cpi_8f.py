from __future__ import annotations

import calendar
import hashlib
import re
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo


class BLSCPI8FError(ValueError):
    pass


SOURCE_ID = "bls_cpi_archived_releases"
SERIES_MOM_SA = "BLS_CPI_U_ALL_ITEMS_MOM_SA_RELEASE_PCT"
SERIES_YOY_NSA = "BLS_CPI_U_ALL_ITEMS_YOY_NSA_RELEASE_PCT"

_RELEASE_TIME_RE = re.compile(
    r"embargoed\s+until\s+"
    r"(?P<hour>\d{1,2}):(?P<minute>\d{2})\s*"
    r"(?P<ampm>a\.m\.|p\.m\.)\s*\(ET\)\s*"
    r"(?P<weekday>[A-Za-z]+),?\s+"
    r"(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),\s+(?P<year>\d{4})",
    re.IGNORECASE | re.MULTILINE,
)
_RELEASE_ID_RE = re.compile(r"\bUSDL[-\s](?P<release_id>\d{2}-\d{4})\b", re.IGNORECASE)
_REFERENCE_PERIOD_RE = re.compile(
    r"CONSUMER\s+PRICE\s+INDEX\s*[-–]\s*(?P<month>[A-Z]+)\s+(?P<year>\d{4})",
    re.IGNORECASE,
)
_MOM_RE = re.compile(
    r"CPI-U\)\s+(?P<verb>increased|decreased|was unchanged)"
    r"(?:\s+(?P<value>\d+(?:\.\d+)?)\s+percent)?\s+"
    r"on\s+a\s+seasonally\s+adjusted\s+basis",
    re.IGNORECASE,
)
_YOY_RE = re.compile(
    r"Over\s+the\s+last\s+12\s+months,\s+the\s+all\s+items\s+index\s+"
    r"(?P<verb>increased|decreased|was unchanged)"
    r"(?:\s+(?P<value>\d+(?:\.\d+)?)\s+percent)?\s+"
    r"before\s+seasonal\s+adjustment",
    re.IGNORECASE,
)


def _signed_percent(match: re.Match[str], *, field: str) -> float:
    verb = match.group("verb").lower()
    if verb == "was unchanged":
        return 0.0
    raw = match.group("value")
    if raw is None:
        raise BLSCPI8FError(f"missing numeric value for {field}")
    value = float(raw)
    return -value if verb == "decreased" else value


def _month_number(name: str) -> int:
    normalized = name.strip().lower()
    lookup = {month.lower(): index for index, month in enumerate(calendar.month_name) if month}
    try:
        return lookup[normalized]
    except KeyError as exc:
        raise BLSCPI8FError(f"unsupported month name: {name!r}") from exc


def parse_bls_cpi_release(release_text: str) -> dict[str, Any]:
    if not isinstance(release_text, str) or not release_text.strip():
        raise BLSCPI8FError("release_text must be non-empty text")

    time_match = _RELEASE_TIME_RE.search(release_text)
    release_id_match = _RELEASE_ID_RE.search(release_text)
    period_match = _REFERENCE_PERIOD_RE.search(release_text)
    mom_match = _MOM_RE.search(release_text)
    yoy_match = _YOY_RE.search(release_text)

    missing = [
        name
        for name, match in (
            ("release timestamp", time_match),
            ("USDL release id", release_id_match),
            ("reference period", period_match),
            ("CPI-U monthly SA change", mom_match),
            ("CPI-U 12-month NSA change", yoy_match),
        )
        if match is None
    ]
    if missing:
        raise BLSCPI8FError("BLS CPI release missing required fields: " + ", ".join(missing))

    assert time_match is not None
    assert release_id_match is not None
    assert period_match is not None
    assert mom_match is not None
    assert yoy_match is not None

    hour = int(time_match.group("hour"))
    minute = int(time_match.group("minute"))
    ampm = time_match.group("ampm").lower()
    if hour < 1 or hour > 12 or minute < 0 or minute > 59:
        raise BLSCPI8FError("invalid release clock time")
    if ampm == "p.m." and hour != 12:
        hour += 12
    elif ampm == "a.m." and hour == 12:
        hour = 0

    release_month = _month_number(time_match.group("month"))
    release_at = datetime(
        int(time_match.group("year")),
        release_month,
        int(time_match.group("day")),
        hour,
        minute,
        tzinfo=ZoneInfo("America/New_York"),
    )

    reference_month = _month_number(period_match.group("month"))
    reference_year = int(period_match.group("year"))
    observation_date = f"{reference_year:04d}-{reference_month:02d}-01"
    source_sha256 = hashlib.sha256(release_text.encode("utf-8")).hexdigest()

    return {
        "release_id": f"USDL-{release_id_match.group('release_id')}",
        "published_at": release_at.isoformat(),
        "reference_period": f"{reference_year:04d}-{reference_month:02d}",
        "observation_date": observation_date,
        "mom_sa_percent": _signed_percent(mom_match, field="mom_sa_percent"),
        "yoy_nsa_percent": _signed_percent(yoy_match, field="yoy_nsa_percent"),
        "source_record_sha256": source_sha256,
    }


def build_bls_cpi_macro_observations(
    release_text: str,
    *,
    ingested_at: datetime,
) -> list[dict[str, Any]]:
    if ingested_at.tzinfo is None or ingested_at.utcoffset() is None:
        raise BLSCPI8FError("ingested_at must be timezone-aware")

    parsed = parse_bls_cpi_release(release_text)
    published_at = datetime.fromisoformat(parsed["published_at"])
    if ingested_at < published_at:
        raise BLSCPI8FError("ingested_at cannot precede the archived release publication time")

    release_date = published_at.date().isoformat()
    common = {
        "factor_id": "inflation",
        "observation_date": parsed["observation_date"],
        "units": "percent_change",
        "realtime_start": release_date,
        "realtime_end": "9999-12-31",
        "source_id": SOURCE_ID,
        "source_record_sha256": parsed["source_record_sha256"],
        "license_status": "USABLE_PUBLIC_DOMAIN",
        "status": "KNOWN",
        "ingested_at": ingested_at.isoformat(),
        "valid_from": parsed["published_at"],
        "historical_vintage_independently_proven": True,
        "historical_publication_time_independently_proven": True,
        "published_at": parsed["published_at"],
        "availability_proof_type": "ARCHIVED_RELEASE_EXACT_TIMESTAMP",
    }

    return [
        {
            **common,
            "series_id": SERIES_MOM_SA,
            "value": parsed["mom_sa_percent"],
            "revision_id": f"{parsed['release_id']}:{SERIES_MOM_SA}",
        },
        {
            **common,
            "series_id": SERIES_YOY_NSA,
            "value": parsed["yoy_nsa_percent"],
            "revision_id": f"{parsed['release_id']}:{SERIES_YOY_NSA}",
        },
    ]
