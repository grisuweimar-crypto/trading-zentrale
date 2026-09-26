from __future__ import annotations

import hashlib
import re
from datetime import datetime
from html.parser import HTMLParser
from typing import Any


class FedH15Release8FError(ValueError):
    pass


SOURCE_ID = "federal_reserve_board_h15"
SERIES_EFFR = "RIFSPFF_N.D"
SERIES_2Y = "RIFLGFCY02_N.B"
SERIES_10Y = "RIFLGFCY10_N.B"


class _TableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[str]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "tr":
            self._row = []
        elif tag.lower() in {"th", "td"} and self._row is not None:
            self._cell = []

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"th", "td"} and self._cell is not None and self._row is not None:
            text = " ".join("".join(self._cell).replace("\xa0", " ").split())
            self._row.append(text)
            self._cell = None
        elif tag == "tr" and self._row is not None:
            if self._row:
                self.rows.append(self._row)
            self._row = None
            self._cell = None


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _parse_header_dates(cells: list[str]) -> list[str] | None:
    if not cells or "instruments" not in cells[0].lower():
        return None
    dates: list[str] = []
    for value in cells[1:]:
        text = " ".join(value.split())
        parsed = None
        for fmt in ("%Y %b %d", "%Y %B %d", "%b %d %Y", "%B %d %Y"):
            try:
                parsed = datetime.strptime(text, fmt).date()
                break
            except ValueError:
                pass
        if parsed is None:
            return None
        dates.append(parsed.isoformat())
    return dates or None


def _value(text: str) -> tuple[str, float | None]:
    normalized = " ".join(text.replace("\xa0", " ").split()).strip().lower()
    if normalized in {"", "n.a.", "n.a", "na", "n/a", "nd"}:
        return "UNKNOWN", None
    cleaned = re.sub(r"[^0-9+\-.]", "", normalized)
    try:
        return "KNOWN", float(cleaned)
    except ValueError as exc:
        raise FedH15Release8FError(f"invalid H15 release value: {text!r}") from exc


def build_fed_h15_release_prospective_observations(
    html_text: str,
    *,
    ingested_at: datetime,
) -> list[dict[str, Any]]:
    if ingested_at.tzinfo is None or ingested_at.utcoffset() is None:
        raise FedH15Release8FError("ingested_at must be timezone-aware")
    if not isinstance(html_text, str) or not html_text.strip():
        raise FedH15Release8FError("html_text must be non-empty")

    parser = _TableParser()
    parser.feed(html_text)

    dates: list[str] | None = None
    effr_values: list[str] | None = None
    two_year_values: list[str] | None = None
    ten_year_values: list[str] | None = None
    treasury_seen = False
    nominal_treasury = False

    for cells in parser.rows:
        maybe_dates = _parse_header_dates(cells)
        if maybe_dates is not None:
            dates = maybe_dates
            continue
        if not cells:
            continue

        label = cells[0].lower()
        if "treasury constant maturities" in label:
            treasury_seen = True
            nominal_treasury = False
            continue
        if treasury_seen and label.startswith("nominal"):
            nominal_treasury = True
            continue
        if treasury_seen and "inflation indexed" in label:
            nominal_treasury = False

        if label.startswith("federal funds (effective)"):
            effr_values = cells[1:]
        elif nominal_treasury and label.startswith("2-year"):
            two_year_values = cells[1:]
        elif nominal_treasury and label.startswith("10-year"):
            ten_year_values = cells[1:]

    if dates is None:
        raise FedH15Release8FError("H15 release page missing Instruments/date header")

    selected = {
        SERIES_EFFR: ("rates_policy", effr_values),
        SERIES_2Y: ("yield_curve", two_year_values),
        SERIES_10Y: ("yield_curve", ten_year_values),
    }
    missing = [series_id for series_id, (_, values) in selected.items() if values is None]
    if missing:
        raise FedH15Release8FError(f"H15 release page missing required series rows: {missing}")

    source_sha = _sha256(html_text)
    snapshot_date = ingested_at.date().isoformat()
    token = ingested_at.isoformat()
    observations: list[dict[str, Any]] = []

    for series_id, (factor_id, values) in selected.items():
        assert values is not None
        if len(values) != len(dates):
            raise FedH15Release8FError(
                f"H15 release row length mismatch for {series_id}: {len(values)} != {len(dates)}"
            )
        for observation_date, raw_value in zip(dates, values):
            status, value = _value(raw_value)
            observations.append(
                {
                    "series_id": series_id,
                    "factor_id": factor_id,
                    "observation_date": observation_date,
                    "value": value,
                    "units": "percent_per_year",
                    "realtime_start": snapshot_date,
                    "realtime_end": "9999-12-31",
                    "revision_id": f"{series_id}:{observation_date}:release_snapshot:{token}",
                    "source_id": SOURCE_ID,
                    "source_record_sha256": source_sha,
                    "license_status": "USABLE_PUBLIC_DOMAIN_UNLESS_MARKED_OTHERWISE",
                    "status": status,
                    "ingested_at": token,
                    "valid_from": token,
                    "historical_vintage_independently_proven": False,
                    "historical_publication_time_independently_proven": False,
                    "published_at": None,
                    "availability_proof_type": "ACTUAL_PROSPECTIVE_INGESTION",
                }
            )

    return sorted(observations, key=lambda row: (row["series_id"], row["observation_date"]))
