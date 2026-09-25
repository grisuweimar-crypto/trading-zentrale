from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from math import isfinite
from typing import Any, Iterable, Mapping


USABLE_PIT_STATES = {"SAFE", "DATE_ONLY_DELAYED"}


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _parse_datetime(value: Any) -> datetime | None:
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
    return parsed if parsed.tzinfo is not None else None


def _finite_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if isfinite(result) else None


def is_pit_usable(row: Mapping[str, Any]) -> bool:
    return bool(
        row.get("accession_number")
        and row.get("valid_from")
        and row.get("pit_status") in USABLE_PIT_STATES
        and _parse_datetime(row.get("valid_from")) is not None
    )


def metric_for_row(
    row: Mapping[str, Any], metric_families: Mapping[str, Mapping[str, Any]]
) -> str | None:
    """Map only explicitly configured standard concepts into metric families.

    Custom taxonomy tags are intentionally not inferred here.
    """
    if str(row.get("taxonomy") or "").lower() != "us-gaap":
        return None
    concept = str(row.get("concept") or "")
    matches = [
        metric
        for metric, spec in metric_families.items()
        if concept in set(spec.get("concepts") or [])
    ]
    if len(matches) != 1:
        return None
    return matches[0]


def fact_context_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    """Context identity excluding publication/version fields."""
    return (
        row.get("cik"),
        row.get("taxonomy"),
        row.get("concept"),
        row.get("unit"),
        row.get("start"),
        row.get("end"),
    )


def first_release_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        if is_pit_usable(row):
            grouped[fact_context_key(row)].append(row)

    selected: list[dict[str, Any]] = []
    for candidates in grouped.values():
        ordered = sorted(
            candidates,
            key=lambda row: (
                _parse_datetime(row.get("valid_from")) or datetime.max.astimezone(),
                str(row.get("accession_number") or ""),
            ),
        )
        chosen = dict(ordered[0])
        chosen["comparison_basis"] = "FIRST_RELEASE"
        selected.append(chosen)
    return selected


def asof_latest_rows(
    rows: Iterable[Mapping[str, Any]], *, as_of: str
) -> list[dict[str, Any]]:
    cutoff = _parse_datetime(as_of)
    if cutoff is None:
        raise ValueError("as_of must be a timezone-aware ISO datetime")

    grouped: dict[tuple[Any, ...], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        if not is_pit_usable(row):
            continue
        valid_from = _parse_datetime(row.get("valid_from"))
        if valid_from is None or valid_from > cutoff:
            continue
        grouped[fact_context_key(row)].append(row)

    selected: list[dict[str, Any]] = []
    for candidates in grouped.values():
        chosen = dict(
            max(
                candidates,
                key=lambda row: (
                    _parse_datetime(row.get("valid_from")),
                    str(row.get("accession_number") or ""),
                ),
            )
        )
        chosen["comparison_basis"] = "ASOF_LATEST_VERSION"
        chosen["as_of"] = cutoff.isoformat()
        selected.append(chosen)
    return selected


def _duration_days(row: Mapping[str, Any]) -> int | None:
    start = _parse_date(row.get("start"))
    end = _parse_date(row.get("end"))
    if start is None or end is None or end < start:
        return None
    return (end - start).days


def comparable_yoy_pair(
    current: Mapping[str, Any],
    previous: Mapping[str, Any],
    *,
    max_period_length_difference_days: int = 7,
    min_end_gap_days: int = 330,
    max_end_gap_days: int = 400,
) -> bool:
    if current.get("cik") != previous.get("cik"):
        return False
    if current.get("taxonomy") != previous.get("taxonomy"):
        return False
    if current.get("concept") != previous.get("concept"):
        return False
    if current.get("unit") != previous.get("unit"):
        return False
    current_end = _parse_date(current.get("end"))
    previous_end = _parse_date(previous.get("end"))
    if current_end is None or previous_end is None or current_end <= previous_end:
        return False
    end_gap = (current_end - previous_end).days
    if not (min_end_gap_days <= end_gap <= max_end_gap_days):
        return False
    current_duration = _duration_days(current)
    previous_duration = _duration_days(previous)
    if current_duration is None or previous_duration is None:
        return False
    if abs(current_duration - previous_duration) > max_period_length_difference_days:
        return False
    current_fp = current.get("fiscal_period")
    previous_fp = previous.get("fiscal_period")
    if current_fp and previous_fp and current_fp != previous_fp:
        return False
    return True


def build_yoy_changes(
    rows: Iterable[Mapping[str, Any]],
    *,
    metric: str,
    metric_families: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    metric_rows = [
        dict(row)
        for row in rows
        if metric_for_row(row, metric_families) == metric and is_pit_usable(row)
    ]
    output: list[dict[str, Any]] = []
    for current in metric_rows:
        candidates = [
            previous
            for previous in metric_rows
            if comparable_yoy_pair(current, previous)
        ]
        if not candidates:
            continue
        current_end = _parse_date(current.get("end"))
        previous = min(
            candidates,
            key=lambda row: abs(
                ((current_end - _parse_date(row.get("end"))).days) - 365
            ),
        )
        current_value = _finite_number(current.get("value"))
        previous_value = _finite_number(previous.get("value"))
        if current_value is None or previous_value is None:
            continue
        pct_change = None if previous_value == 0 else (current_value / previous_value) - 1.0
        output.append(
            {
                "feature": f"{metric}_yoy_change",
                "metric": metric,
                "cik": current.get("cik"),
                "current_accession": current.get("accession_number"),
                "previous_accession": previous.get("accession_number"),
                "current_end": current.get("end"),
                "previous_end": previous.get("end"),
                "unit": current.get("unit"),
                "current_value": current_value,
                "previous_value": previous_value,
                "absolute_change": current_value - previous_value,
                "pct_change": pct_change,
                "valid_from": current.get("valid_from"),
                "direction": "UNASSIGNED",
                "status": "KNOWN" if pct_change is not None else "PARTIAL",
                "reason_codes": [] if pct_change is not None else ["ZERO_PREVIOUS_VALUE"],
            }
        )
    return output


def _same_duration_component_context(a: Mapping[str, Any], b: Mapping[str, Any]) -> bool:
    return bool(
        a.get("cik") == b.get("cik")
        and a.get("accession_number") == b.get("accession_number")
        and a.get("unit") == b.get("unit")
        and a.get("start") == b.get("start")
        and a.get("end") == b.get("end")
        and is_pit_usable(a)
        and is_pit_usable(b)
    )


def build_operating_margin(
    rows: Iterable[Mapping[str, Any]],
    *,
    metric_families: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    all_rows = [dict(row) for row in rows]
    revenues = [r for r in all_rows if metric_for_row(r, metric_families) == "revenue"]
    operating = [r for r in all_rows if metric_for_row(r, metric_families) == "operating_income"]
    output: list[dict[str, Any]] = []
    for numerator in operating:
        matches = [r for r in revenues if _same_duration_component_context(numerator, r)]
        if len(matches) != 1:
            continue
        denominator = matches[0]
        op_value = _finite_number(numerator.get("value"))
        revenue_value = _finite_number(denominator.get("value"))
        if op_value is None or revenue_value in (None, 0.0):
            continue
        output.append(
            {
                "feature": "operating_margin",
                "cik": numerator.get("cik"),
                "accession_number": numerator.get("accession_number"),
                "start": numerator.get("start"),
                "end": numerator.get("end"),
                "unit": numerator.get("unit"),
                "value": op_value / revenue_value,
                "valid_from": max(str(numerator.get("valid_from")), str(denominator.get("valid_from"))),
                "direction": "UNASSIGNED",
                "status": "KNOWN",
                "reason_codes": [],
            }
        )
    return output


def build_free_cash_flow(
    rows: Iterable[Mapping[str, Any]],
    *,
    metric_families: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    all_rows = [dict(row) for row in rows]
    ocf = [r for r in all_rows if metric_for_row(r, metric_families) == "operating_cash_flow"]
    capex = [r for r in all_rows if metric_for_row(r, metric_families) == "capex"]
    output: list[dict[str, Any]] = []
    for left in ocf:
        matches = [r for r in capex if _same_duration_component_context(left, r)]
        if len(matches) != 1:
            continue
        right = matches[0]
        left_value = _finite_number(left.get("value"))
        right_value = _finite_number(right.get("value"))
        if left_value is None or right_value is None:
            continue
        output.append(
            {
                "feature": "free_cash_flow",
                "cik": left.get("cik"),
                "accession_number": left.get("accession_number"),
                "start": left.get("start"),
                "end": left.get("end"),
                "unit": left.get("unit"),
                "value": left_value - right_value,
                "valid_from": max(str(left.get("valid_from")), str(right.get("valid_from"))),
                "direction": "UNASSIGNED",
                "status": "KNOWN",
                "reason_codes": [],
            }
        )
    return output
