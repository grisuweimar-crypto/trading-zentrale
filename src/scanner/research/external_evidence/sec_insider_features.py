from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping

FEATURE_SCHEMA = "external_evidence_8d_insider_features_v1"
EVIDENCE_SCHEMA = "external_evidence_8d_sec_insider_bulk_v1"
VALIDATION_SCHEMA = "external_evidence_8d_insider_validation_result_v1"


class SecInsiderFeatureError(ValueError):
    pass


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SecInsiderFeatureError(f"cannot read JSON object: {path}") from exc
    if not isinstance(payload, dict):
        raise SecInsiderFeatureError(f"JSON root must be an object: {path}")
    return payload


def _aware_datetime(value: Any) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise SecInsiderFeatureError(f"invalid datetime: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SecInsiderFeatureError(f"datetime must be timezone-aware: {value!r}")
    return parsed


def _event_date(value: Any) -> date:
    text = str(value or "").strip()
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise SecInsiderFeatureError(f"invalid event date: {value!r}") from exc


def _quarter_bounds(label: str) -> tuple[date, date]:
    text = str(label or "").strip().upper()
    if len(text) != 6 or text[4] != "Q" or not text[:4].isdigit() or text[5] not in "1234":
        raise SecInsiderFeatureError(f"invalid source_quarter: {label!r}")
    year = int(text[:4])
    quarter = int(text[5])
    month = 1 + (quarter - 1) * 3
    start = date(year, month, 1)
    if quarter == 4:
        next_start = date(year + 1, 1, 1)
    else:
        next_start = date(year, month + 3, 1)
    return start, next_start - timedelta(days=1)


def _interval_covered(start: date, end: date, intervals: Iterable[tuple[date, date]]) -> bool:
    if start > end:
        return False
    ordered = sorted(intervals)
    cursor = start
    for left, right in ordered:
        if right < cursor:
            continue
        if left > cursor:
            return False
        cursor = max(cursor, right + timedelta(days=1))
        if cursor > end:
            return True
    return cursor > end


def _owner_cik(row: Mapping[str, Any]) -> str | None:
    owners = row.get("reporting_owners") or []
    if not isinstance(owners, list) or len(owners) != 1:
        return None
    owner = owners[0] if isinstance(owners[0], Mapping) else {}
    cik = str(owner.get("reporting_owner_cik") or "").strip()
    return cik or None


def _eligible_row(row: Mapping[str, Any]) -> bool:
    return (
        row.get("strict_pit_eligible") is True
        and str(row.get("document_type") or "").upper() == "4"
        and row.get("amendment") is False
        and str(row.get("candidate_status") or "")
        == "P_S_HIGH_PRECISION_DISCRETIONARY_CANDIDATE"
        and str(row.get("transaction_code") or "").upper() in {"P", "S"}
        and row.get("aff10b5one") is False
        and row.get("equity_swap_involved") is not True
    )


def _aggregate_window(
    *,
    issuer_rows: list[Mapping[str, Any]],
    issuer_cik: str,
    as_of: datetime,
    days: int,
    coverage_intervals: list[tuple[date, date]],
) -> dict[str, Any]:
    window_start = as_of.date() - timedelta(days=days - 1)
    window_end = as_of.date()
    complete = _interval_covered(window_start, window_end, coverage_intervals)
    if not complete:
        return {
            "issuer_cik": issuer_cik,
            "as_of": as_of.isoformat(),
            "window_days": days,
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "coverage_status": "INSUFFICIENT_SOURCE_WINDOW",
            "purchase_count": None,
            "sale_count": None,
            "distinct_buyer_count": None,
            "distinct_seller_count": None,
            "purchase_shares": None,
            "sale_shares": None,
            "purchase_value_when_price_known": None,
            "sale_value_when_price_known": None,
            "owner_identity_status": "NOT_EVALUATED",
            "numeric_feature_status": "CHALLENGER_NUMERIC",
        }

    selected: list[Mapping[str, Any]] = []
    for row in issuer_rows:
        if not _eligible_row(row):
            continue
        valid_from = _aware_datetime(row.get("valid_from"))
        if valid_from > as_of:
            continue
        event = _event_date(row.get("transaction_date"))
        if window_start <= event <= window_end:
            selected.append(row)

    purchase = [row for row in selected if str(row.get("transaction_code") or "").upper() == "P"]
    sale = [row for row in selected if str(row.get("transaction_code") or "").upper() == "S"]

    owner_complete = all(_owner_cik(row) is not None for row in selected)
    buyers = {_owner_cik(row) for row in purchase if _owner_cik(row) is not None}
    sellers = {_owner_cik(row) for row in sale if _owner_cik(row) is not None}

    def _sum_known(rows: list[Mapping[str, Any]], field: str) -> float | None:
        values = [row.get(field) for row in rows]
        known = [float(value) for value in values if value is not None]
        if not rows:
            return 0.0
        if len(known) != len(rows):
            return None
        return float(sum(known))

    return {
        "issuer_cik": issuer_cik,
        "as_of": as_of.isoformat(),
        "window_days": days,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "coverage_status": "KNOWN" if selected else "KNOWN_ZERO",
        "purchase_count": len(purchase),
        "sale_count": len(sale),
        "distinct_buyer_count": len(buyers) if owner_complete else None,
        "distinct_seller_count": len(sellers) if owner_complete else None,
        "purchase_shares": _sum_known(purchase, "transaction_shares"),
        "sale_shares": _sum_known(sale, "transaction_shares"),
        "purchase_value_when_price_known": _sum_known(
            purchase, "transaction_value_when_price_known"
        ),
        "sale_value_when_price_known": _sum_known(
            sale, "transaction_value_when_price_known"
        ),
        "owner_identity_status": "KNOWN" if owner_complete else "PARTIAL_OWNER_IDENTITY",
        "numeric_feature_status": "CHALLENGER_NUMERIC",
    }


def build_insider_features(
    *,
    evidence_payloads: list[Mapping[str, Any]],
    validation_result: Mapping[str, Any],
    asof_grid: Iterable[Mapping[str, Any]],
    primary_days: int = 30,
    robustness_days: Iterable[int] = (90,),
) -> dict[str, Any]:
    if validation_result.get("schema_version") != VALIDATION_SCHEMA:
        raise SecInsiderFeatureError("unsupported validation result schema")
    if validation_result.get("decision") != "PASS_SOURCE_SEMANTICS_VALIDATION":
        raise SecInsiderFeatureError("B4 requires PASS_SOURCE_SEMANTICS_VALIDATION")
    guards = validation_result.get("guards") or {}
    if guards.get("market_outcomes_read_by_pipeline") is not False:
        raise SecInsiderFeatureError("validation result must remain outcome-blind")

    rows_by_issuer: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    coverage_intervals: list[tuple[date, date]] = []
    quarters: list[str] = []
    for payload in evidence_payloads:
        if payload.get("schema_version") != EVIDENCE_SCHEMA:
            raise SecInsiderFeatureError("unsupported insider evidence schema")
        payload_guards = payload.get("guards") or {}
        if payload_guards.get("market_outcomes_read") is not False:
            raise SecInsiderFeatureError("insider evidence must remain outcome-blind")
        quarter = str(payload.get("source_quarter") or "").upper()
        coverage_intervals.append(_quarter_bounds(quarter))
        quarters.append(quarter)
        for row in payload.get("rows") or []:
            if not isinstance(row, Mapping):
                continue
            issuer_cik = str(row.get("issuer_cik") or "").strip()
            if issuer_cik:
                rows_by_issuer[issuer_cik].append(row)

    windows = [int(primary_days)] + [int(value) for value in robustness_days]
    if any(value <= 0 for value in windows) or len(set(windows)) != len(windows):
        raise SecInsiderFeatureError("feature windows must be unique positive integers")

    output_rows: list[dict[str, Any]] = []
    for raw in asof_grid:
        issuer_cik = str(raw.get("issuer_cik") or "").strip()
        if not issuer_cik:
            raise SecInsiderFeatureError("as-of grid row missing issuer_cik")
        as_of = _aware_datetime(raw.get("as_of"))
        for days in windows:
            row = _aggregate_window(
                issuer_rows=rows_by_issuer.get(issuer_cik, []),
                issuer_cik=issuer_cik,
                as_of=as_of,
                days=days,
                coverage_intervals=coverage_intervals,
            )
            row["window_role"] = "PRIMARY" if days == primary_days else "ROBUSTNESS_ONLY"
            output_rows.append(row)

    output_rows.sort(key=lambda row: (row["as_of"], row["issuer_cik"], row["window_days"]))
    return {
        "schema_version": FEATURE_SCHEMA,
        "phase": "8D_B4_insider_feature_aggregation",
        "status": "OUTCOME_BLIND_DESCRIPTIVE_FEATURES",
        "source_quarters": sorted(set(quarters)),
        "primary_window_days": primary_days,
        "robustness_window_days": sorted(set(int(value) for value in robustness_days)),
        "rows": output_rows,
        "guards": {
            "market_outcomes_read": False,
            "market_direction_assigned": False,
            "threshold_selection_run": False,
            "phase7_integration_enabled": False,
            "production_external_evidence_enabled": False,
            "amendments_included_in_v1_aggregation": False,
            "numeric_features_promoted": False,
        },
    }


def load_insider_features_inputs(
    *, evidence_paths: Iterable[Path], validation_result_path: Path
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    return [_load_json(path) for path in evidence_paths], _load_json(validation_result_path)


def write_insider_features(payload: Mapping[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True), encoding="utf-8")
