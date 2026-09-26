from __future__ import annotations

import csv
import hashlib
import json
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo


SCHEMA_VERSION = "external_evidence_8d_finra_short_interest_snapshot_v1"
PROSPECTIVE_MODE = "PROSPECTIVE_PUBLICATION_SNAPSHOT"
HISTORICAL_MODE = "HISTORICAL_BACKFILL_LATEST_AVAILABLE_VINTAGE"
_ALLOWED_MODES = {PROSPECTIVE_MODE, HISTORICAL_MODE}
_PUBLICATION_CLOCK_ET = time(16, 40)
_REQUIRED_FIELDS = {
    "symbolCode",
    "settlementDate",
    "currentShortPositionQuantity",
}


class FinraShortInterestError(ValueError):
    """Raised when FINRA short-interest evidence violates the Phase 8D contract."""


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso_datetime(value: str) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise FinraShortInterestError(f"invalid ISO datetime: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise FinraShortInterestError("ingested_at must include a timezone offset")
    return parsed.astimezone(timezone.utc)


def publication_valid_from(publication_date: str) -> str:
    try:
        day = date.fromisoformat(str(publication_date).strip())
    except ValueError as exc:
        raise FinraShortInterestError(
            f"invalid FINRA publication date: {publication_date!r}"
        ) from exc
    local = datetime.combine(day, _PUBLICATION_CLOCK_ET, tzinfo=ZoneInfo("America/New_York"))
    return _iso_utc(local)


def _number(value: Any, *, field: str, integer: bool = False) -> int | float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise FinraShortInterestError(f"{field} may not be boolean")
    try:
        numeric = float(str(value).replace(",", ""))
    except (TypeError, ValueError) as exc:
        raise FinraShortInterestError(f"invalid numeric {field}: {value!r}") from exc
    if integer:
        if not numeric.is_integer():
            raise FinraShortInterestError(f"{field} must be integer-like: {value!r}")
        return int(numeric)
    return numeric


def _load_rows(path: Path) -> tuple[list[dict[str, Any]], bytes, str]:
    raw = path.read_bytes()
    suffix = path.suffix.lower()
    if suffix == ".json":
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise FinraShortInterestError(f"cannot decode FINRA JSON: {path}") from exc
        if isinstance(payload, dict):
            payload = payload.get("data") or payload.get("rows")
        if not isinstance(payload, list):
            raise FinraShortInterestError("FINRA JSON must be an array or contain data/rows array")
        rows = [dict(row) for row in payload if isinstance(row, Mapping)]
        return rows, raw, "JSON"
    if suffix == ".csv":
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise FinraShortInterestError(f"cannot decode FINRA CSV: {path}") from exc
        rows = [dict(row) for row in csv.DictReader(text.splitlines())]
        return rows, raw, "CSV"
    raise FinraShortInterestError("FINRA input must be .json or .csv")


def normalize_finra_row(row: Mapping[str, Any]) -> dict[str, Any]:
    missing = sorted(field for field in _REQUIRED_FIELDS if row.get(field) in {None, ""})
    if missing:
        raise FinraShortInterestError(
            "FINRA row missing required fields: " + ", ".join(missing)
        )

    symbol = str(row.get("symbolCode") or "").strip().upper()
    if not symbol:
        raise FinraShortInterestError("symbolCode is blank")
    try:
        settlement = date.fromisoformat(str(row.get("settlementDate") or "").strip())
    except ValueError as exc:
        raise FinraShortInterestError(
            f"invalid settlementDate for {symbol}: {row.get('settlementDate')!r}"
        ) from exc

    current = _number(
        row.get("currentShortPositionQuantity"),
        field="currentShortPositionQuantity",
        integer=True,
    )
    previous = _number(
        row.get("previousShortPositionQuantity"),
        field="previousShortPositionQuantity",
        integer=True,
    )
    change_quantity = _number(
        row.get("changePreviousNumber"),
        field="changePreviousNumber",
        integer=True,
    )
    change_percent = _number(row.get("changePercent"), field="changePercent")
    average_daily_volume = _number(
        row.get("averageDailyVolumeQuantity"),
        field="averageDailyVolumeQuantity",
        integer=True,
    )
    days_to_cover = _number(row.get("daysToCoverQuantity"), field="daysToCoverQuantity")

    reason_codes: list[str] = []
    if previous is not None and change_quantity is not None:
        computed_change = int(current) - int(previous)
        if computed_change != int(change_quantity):
            reason_codes.append("REPORTED_CHANGE_QUANTITY_INCONSISTENT_WITH_POSITIONS")
    if previous not in {None, 0} and change_percent is not None:
        computed_pct = ((float(current) - float(previous)) / float(previous)) * 100.0
        if abs(computed_pct - float(change_percent)) > 0.11:
            reason_codes.append("REPORTED_CHANGE_PERCENT_INCONSISTENT_WITH_POSITIONS")

    feature_status = "KNOWN"
    if any(
        value is None
        for value in (change_quantity, change_percent, average_daily_volume, days_to_cover)
    ):
        feature_status = "PARTIAL"

    return {
        "symbol": symbol,
        "settlement_date": settlement.isoformat(),
        "market_class_code": row.get("marketClassCode"),
        "revision_flag": row.get("revisionFlag"),
        "stock_split_flag": row.get("stockSplitFlag"),
        "issue_name": row.get("issueName"),
        "short_position_quantity": current,
        "previous_short_position_quantity": previous,
        "short_position_change_quantity": change_quantity,
        "short_position_change_percent": change_percent,
        "average_daily_volume_quantity": average_daily_volume,
        "days_to_cover": days_to_cover,
        "feature_status": feature_status,
        "reason_codes": reason_codes,
        "market_direction": "UNASSIGNED",
    }


def build_finra_snapshot(
    *,
    source_path: Path,
    publication_date: str,
    ingested_at: str,
    source_mode: str,
    source_url: str = "https://api.finra.org/data/group/otcmarket/name/consolidatedShortInterest",
) -> dict[str, Any]:
    if source_mode not in _ALLOWED_MODES:
        raise FinraShortInterestError(f"unsupported source_mode: {source_mode}")
    if not str(source_url).startswith("https://"):
        raise FinraShortInterestError("source_url must use HTTPS")

    rows, raw, source_format = _load_rows(source_path)
    if not rows:
        raise FinraShortInterestError("FINRA snapshot contains no rows")

    valid_from = publication_valid_from(publication_date)
    ingested = _parse_iso_datetime(ingested_at)
    valid_dt = _parse_iso_datetime(valid_from)
    if ingested < valid_dt:
        raise FinraShortInterestError(
            "ingested_at precedes FINRA publication availability time"
        )

    normalized: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for index, row in enumerate(rows):
        try:
            item = normalize_finra_row(row)
            identity = (
                item["symbol"],
                item["settlement_date"],
                str(item.get("market_class_code") or ""),
            )
            if identity in seen:
                raise FinraShortInterestError(
                    f"duplicate symbol/settlement/market identity: {identity}"
                )
            seen.add(identity)
            item.update(
                {
                    "event_time": item["settlement_date"],
                    "published_at": valid_from,
                    "valid_from": valid_from,
                    "ingested_at": _iso_utc(ingested),
                    "source_authority": "FINRA",
                    "source_dataset": "consolidatedShortInterest",
                    "source_mode": source_mode,
                    "strict_pit_eligible": source_mode == PROSPECTIVE_MODE,
                    "vintage_status": (
                        "STRICT_PIT_PROSPECTIVE_SNAPSHOT"
                        if source_mode == PROSPECTIVE_MODE
                        else "LATEST_AVAILABLE_VINTAGE_NOT_ORIGINAL_PUBLICATION_VINTAGE"
                    ),
                }
            )
            normalized.append(item)
        except Exception as exc:
            rejected.append(
                {
                    "row_index": index,
                    "symbolCode": row.get("symbolCode"),
                    "settlementDate": row.get("settlementDate"),
                    "error_type": type(exc).__name__,
                    "error_message": str(exc)[:500],
                }
            )

    if not normalized:
        raise FinraShortInterestError("no FINRA rows survived normalization")

    strict_pit = source_mode == PROSPECTIVE_MODE
    return {
        "schema_version": SCHEMA_VERSION,
        "phase": "8D_FINRA_short_interest",
        "source_authority": "FINRA",
        "source_dataset": "consolidatedShortInterest",
        "source_url": source_url,
        "source_format": source_format,
        "source_file": str(source_path),
        "source_sha256": _sha256_bytes(raw),
        "source_mode": source_mode,
        "publication_date": publication_date,
        "published_at": valid_from,
        "ingested_at": _iso_utc(ingested),
        "strict_pit_eligible": strict_pit,
        "vintage_status": (
            "STRICT_PIT_PROSPECTIVE_SNAPSHOT"
            if strict_pit
            else "LATEST_AVAILABLE_VINTAGE_NOT_ORIGINAL_PUBLICATION_VINTAGE"
        ),
        "row_count": len(normalized),
        "rejected_row_count": len(rejected),
        "rows": normalized,
        "rejections": rejected,
        "guards": {
            "settlement_date_is_publication_time": False,
            "short_sale_volume_substituted_for_short_interest": False,
            "short_interest_percent_float_enabled": False,
            "market_outcomes_read": False,
            "market_direction_assigned": False,
            "phase7_integration_enabled": False,
            "production_external_evidence_enabled": False,
        },
    }


def write_finra_snapshot(payload: Mapping[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(dict(payload), indent=2, sort_keys=True),
        encoding="utf-8",
    )
