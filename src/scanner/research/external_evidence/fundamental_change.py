from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
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


def _latest_valid_from(rows: Iterable[Mapping[str, Any]]) -> str | None:
    parsed = [
        (stamp, row.get("valid_from"))
        for row in rows
        if (stamp := _parse_datetime(row.get("valid_from"))) is not None
    ]
    if not parsed:
        return None
    return str(max(parsed, key=lambda item: item[0])[1])


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

    Custom taxonomy tags are intentionally not inferred here. If a metric family
    declares allowed units, a row outside that set is not silently mapped.
    """
    if str(row.get("taxonomy") or "").lower() != "us-gaap":
        return None
    concept = str(row.get("concept") or "")
    unit = str(row.get("unit") or "")
    matches = []
    for metric, spec in metric_families.items():
        if concept not in set(spec.get("concepts") or []):
            continue
        allowed_units = set(spec.get("allowed_units") or [])
        if allowed_units and unit not in allowed_units:
            continue
        matches.append(metric)
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
                _parse_datetime(row.get("valid_from")) or datetime.max.replace(tzinfo=timezone.utc),
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
                "taxonomy": current.get("taxonomy"),
                "concept": current.get("concept"),
                "fiscal_period": current.get("fiscal_period"),
                "current_accession": current.get("accession_number"),
                "previous_accession": previous.get("accession_number"),
                "current_start": current.get("start"),
                "previous_start": previous.get("start"),
                "current_end": current.get("end"),
                "previous_end": previous.get("end"),
                "unit": current.get("unit"),
                "current_value": current_value,
                "previous_value": previous_value,
                "absolute_change": current_value - previous_value,
                "pct_change": pct_change,
                "valid_from": current.get("valid_from"),
                "comparison_basis": current.get("comparison_basis"),
                "direction": "UNASSIGNED",
                "status": "KNOWN" if pct_change is not None else "PARTIAL",
                "reason_codes": [] if pct_change is not None else ["ZERO_PREVIOUS_VALUE"],
            }
        )
    return output


def build_growth_acceleration(
    yoy_rows: Iterable[Mapping[str, Any]],
    *,
    feature_name: str = "revenue_growth_acceleration",
) -> list[dict[str, Any]]:
    """Difference between two consecutive YoY growth observations.

    Consecutive means the current observation's comparison period is exactly the
    prior YoY observation's current period. No interpolation is performed.
    """
    rows = [dict(row) for row in yoy_rows if _finite_number(row.get("pct_change")) is not None]
    output: list[dict[str, Any]] = []
    for current in rows:
        candidates = [
            previous
            for previous in rows
            if previous is not current
            and previous.get("cik") == current.get("cik")
            and previous.get("metric") == current.get("metric")
            and previous.get("concept") == current.get("concept")
            and previous.get("unit") == current.get("unit")
            and previous.get("fiscal_period") == current.get("fiscal_period")
            and previous.get("current_end") == current.get("previous_end")
        ]
        if len(candidates) != 1:
            continue
        prior = candidates[0]
        current_growth = _finite_number(current.get("pct_change"))
        prior_growth = _finite_number(prior.get("pct_change"))
        if current_growth is None or prior_growth is None:
            continue
        output.append(
            {
                "feature": feature_name,
                "metric": current.get("metric"),
                "cik": current.get("cik"),
                "concept": current.get("concept"),
                "fiscal_period": current.get("fiscal_period"),
                "unit": current.get("unit"),
                "current_end": current.get("current_end"),
                "prior_growth_end": prior.get("current_end"),
                "current_growth": current_growth,
                "prior_growth": prior_growth,
                "value": current_growth - prior_growth,
                "valid_from": current.get("valid_from"),
                "comparison_basis": current.get("comparison_basis"),
                "direction": "UNASSIGNED",
                "status": "KNOWN",
                "reason_codes": [],
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
                "fiscal_period": numerator.get("fiscal_period"),
                "unit": numerator.get("unit"),
                "value": op_value / revenue_value,
                "valid_from": _latest_valid_from([numerator, denominator]),
                "comparison_basis": numerator.get("comparison_basis"),
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
                "fiscal_period": left.get("fiscal_period"),
                "unit": left.get("unit"),
                "value": left_value - right_value,
                "valid_from": _latest_valid_from([left, right]),
                "comparison_basis": left.get("comparison_basis"),
                "direction": "UNASSIGNED",
                "status": "KNOWN",
                "reason_codes": [],
            }
        )
    return output


def _derived_comparable_yoy_pair(
    current: Mapping[str, Any],
    previous: Mapping[str, Any],
    *,
    kind: str,
    max_period_length_difference_days: int = 7,
    min_end_gap_days: int = 330,
    max_end_gap_days: int = 400,
) -> bool:
    if current.get("cik") != previous.get("cik") or current.get("unit") != previous.get("unit"):
        return False
    current_end = _parse_date(current.get("end"))
    previous_end = _parse_date(previous.get("end"))
    if current_end is None or previous_end is None or current_end <= previous_end:
        return False
    gap = (current_end - previous_end).days
    if not (min_end_gap_days <= gap <= max_end_gap_days):
        return False
    current_fp = current.get("fiscal_period")
    previous_fp = previous.get("fiscal_period")
    if current_fp and previous_fp and current_fp != previous_fp:
        return False
    if kind == "duration":
        current_duration = _duration_days(current)
        previous_duration = _duration_days(previous)
        if current_duration is None or previous_duration is None:
            return False
        if abs(current_duration - previous_duration) > max_period_length_difference_days:
            return False
    elif kind != "instant":
        raise ValueError("kind must be 'duration' or 'instant'")
    return True


def build_derived_yoy_changes(
    rows: Iterable[Mapping[str, Any]],
    *,
    source_feature: str,
    output_feature: str,
    kind: str = "duration",
) -> list[dict[str, Any]]:
    """Build absolute YoY changes for already-derived, non-directional features."""
    candidates = [
        dict(row)
        for row in rows
        if row.get("feature") == source_feature
        and row.get("status") == "KNOWN"
        and _finite_number(row.get("value")) is not None
    ]
    output: list[dict[str, Any]] = []
    for current in candidates:
        previous_candidates = [
            previous
            for previous in candidates
            if _derived_comparable_yoy_pair(current, previous, kind=kind)
        ]
        if not previous_candidates:
            continue
        current_end = _parse_date(current.get("end"))
        previous = min(
            previous_candidates,
            key=lambda row: abs(((current_end - _parse_date(row.get("end"))).days) - 365),
        )
        current_value = _finite_number(current.get("value"))
        previous_value = _finite_number(previous.get("value"))
        if current_value is None or previous_value is None:
            continue
        output.append(
            {
                "feature": output_feature,
                "source_feature": source_feature,
                "cik": current.get("cik"),
                "current_accession": current.get("accession_number"),
                "previous_accession": previous.get("accession_number"),
                "current_end": current.get("end"),
                "previous_end": previous.get("end"),
                "fiscal_period": current.get("fiscal_period"),
                "unit": current.get("unit"),
                "current_value": current_value,
                "previous_value": previous_value,
                "absolute_change": current_value - previous_value,
                "value": current_value - previous_value,
                "valid_from": current.get("valid_from"),
                "comparison_basis": current.get("comparison_basis"),
                "direction": "UNASSIGNED",
                "status": "KNOWN",
                "reason_codes": [],
            }
        )
    return output


def resolve_debt_values(
    rows: Iterable[Mapping[str, Any]],
    *,
    metric_families: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Resolve debt without double counting total and component concepts.

    Accepted contexts are deliberately narrow:
    - exactly one LongTermDebt fact and no current/noncurrent component facts, or
    - exactly one current plus exactly one noncurrent fact and no LongTermDebt fact.

    If total and components coexist, or duplicates occur, the context is retained as
    unresolved instead of guessing which representation is authoritative.
    """
    debt_rows = [
        dict(row)
        for row in rows
        if metric_for_row(row, metric_families) == "debt" and is_pit_usable(row)
    ]
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in debt_rows:
        grouped[
            (
                row.get("cik"),
                row.get("accession_number"),
                row.get("unit"),
                row.get("end"),
            )
        ].append(row)

    output: list[dict[str, Any]] = []
    for (cik, accession, unit, end), group in grouped.items():
        totals = [row for row in group if row.get("concept") == "LongTermDebt"]
        current = [row for row in group if row.get("concept") == "LongTermDebtCurrent"]
        noncurrent = [row for row in group if row.get("concept") == "LongTermDebtNoncurrent"]
        status = "UNKNOWN"
        reason_codes: list[str] = []
        mode = None
        value = None
        components: list[dict[str, Any]] = []

        if len(totals) == 1 and not current and not noncurrent:
            candidate = _finite_number(totals[0].get("value"))
            if candidate is not None:
                value = candidate
                components = totals
                mode = "TOTAL_ONLY"
                status = "KNOWN"
            else:
                reason_codes.append("NON_NUMERIC_DEBT_TOTAL")
        elif not totals and len(current) == 1 and len(noncurrent) == 1:
            current_value = _finite_number(current[0].get("value"))
            noncurrent_value = _finite_number(noncurrent[0].get("value"))
            if current_value is not None and noncurrent_value is not None:
                value = current_value + noncurrent_value
                components = [current[0], noncurrent[0]]
                mode = "CURRENT_PLUS_NONCURRENT"
                status = "KNOWN"
            else:
                reason_codes.append("NON_NUMERIC_DEBT_COMPONENT")
        elif totals and (current or noncurrent):
            status = "CONFLICTING_SOURCES"
            reason_codes.append("TOTAL_AND_COMPONENT_DEBT_COEXIST")
        elif len(totals) > 1 or len(current) > 1 or len(noncurrent) > 1:
            status = "CONFLICTING_SOURCES"
            reason_codes.append("DUPLICATE_DEBT_CONCEPT_IN_CONTEXT")
        else:
            reason_codes.append("INCOMPLETE_DEBT_COMPONENT_SET")

        output.append(
            {
                "feature": "resolved_debt",
                "cik": cik,
                "accession_number": accession,
                "end": end,
                "fiscal_period": group[0].get("fiscal_period") if group else None,
                "unit": unit,
                "value": value,
                "resolution_mode": mode,
                "component_concepts": sorted(str(row.get("concept")) for row in components),
                "valid_from": _latest_valid_from(group),
                "comparison_basis": group[0].get("comparison_basis") if group else None,
                "direction": "UNASSIGNED",
                "status": status,
                "reason_codes": reason_codes,
            }
        )
    return output


def build_debt_to_equity(
    debt_rows: Iterable[Mapping[str, Any]],
    fact_rows: Iterable[Mapping[str, Any]],
    *,
    metric_families: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    facts = [dict(row) for row in fact_rows]
    equity_rows = [
        row
        for row in facts
        if metric_for_row(row, metric_families) == "equity" and is_pit_usable(row)
    ]
    output: list[dict[str, Any]] = []
    for debt in debt_rows:
        base = {
            "feature": "debt_to_equity",
            "cik": debt.get("cik"),
            "accession_number": debt.get("accession_number"),
            "end": debt.get("end"),
            "fiscal_period": debt.get("fiscal_period"),
            "unit": debt.get("unit"),
            "valid_from": debt.get("valid_from"),
            "comparison_basis": debt.get("comparison_basis"),
            "direction": "UNASSIGNED",
        }
        if debt.get("status") != "KNOWN":
            output.append(
                {
                    **base,
                    "value": None,
                    "status": debt.get("status", "UNKNOWN"),
                    "reason_codes": list(debt.get("reason_codes") or []),
                }
            )
            continue

        matches = [
            row
            for row in equity_rows
            if row.get("cik") == debt.get("cik")
            and row.get("accession_number") == debt.get("accession_number")
            and row.get("unit") == debt.get("unit")
            and row.get("end") == debt.get("end")
        ]
        if len(matches) != 1:
            output.append(
                {
                    **base,
                    "value": None,
                    "status": "UNKNOWN" if not matches else "CONFLICTING_SOURCES",
                    "reason_codes": ["MISSING_UNIQUE_EQUITY_CONTEXT"],
                }
            )
            continue
        equity = matches[0]
        debt_value = _finite_number(debt.get("value"))
        equity_value = _finite_number(equity.get("value"))
        if debt_value is None or equity_value is None:
            output.append(
                {
                    **base,
                    "value": None,
                    "status": "UNKNOWN",
                    "reason_codes": ["NON_NUMERIC_DEBT_OR_EQUITY"],
                }
            )
            continue
        if equity_value == 0:
            output.append(
                {
                    **base,
                    "value": None,
                    "status": "PARTIAL",
                    "reason_codes": ["ZERO_EQUITY"],
                }
            )
            continue
        output.append(
            {
                **base,
                "value": debt_value / equity_value,
                "debt_value": debt_value,
                "equity_value": equity_value,
                "valid_from": _latest_valid_from([debt, equity]),
                "status": "KNOWN",
                "reason_codes": [],
            }
        )
    return output
