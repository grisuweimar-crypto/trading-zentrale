"""QM-B provider coverage and forward-outcome availability audit.

Research-only. This module deliberately separates:
1) a provider/fetch observation,
2) stored price-session coverage, and
3) a mature forward outcome for a historical scanner event.

None of these facts is silently promoted into historical provider coverage or a
strict instrument-level outcome ledger. Missing/unknown outcomes never enter a
research denominator.
"""
from __future__ import annotations

from bisect import bisect_left
from collections import Counter, defaultdict
import csv
from datetime import date, datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from scanner.data.price_history import validated_rows
from scanner.reports.research_views import observed


SCHEMA_VERSION = "qm_b_provider_outcome_availability_audit_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_b_provider_outcome_availability_v1.json"


class ProviderOutcomeAuditError(ValueError):
    """Raised when frozen inputs or audit semantics are invalid."""


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _parse_date(value: Any, *, field: str) -> date:
    text = _clean(value)
    if not text:
        raise ProviderOutcomeAuditError(f"date_required:{field}")
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise ProviderOutcomeAuditError(f"date_invalid:{field}:{value}") from exc


def _parse_utc(value: Any, *, field: str, allow_none: bool = False) -> datetime | None:
    text = _clean(value)
    if not text:
        if allow_none:
            return None
        raise ProviderOutcomeAuditError(f"timestamp_required:{field}")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ProviderOutcomeAuditError(f"timestamp_invalid:{field}:{value}") from exc
    if parsed.tzinfo is None:
        raise ProviderOutcomeAuditError(f"timestamp_timezone_required:{field}")
    return parsed.astimezone(timezone.utc)


def _sha256_path(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_csv(path: Path) -> list[dict[str, str]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return list(csv.DictReader(handle))
    except OSError as exc:
        raise ProviderOutcomeAuditError(f"csv_unreadable:{path}") from exc


def load_provider_outcome_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProviderOutcomeAuditError(f"contract_unreadable:{target}") from exc
    if payload.get("schema_version") != "qm_b_provider_outcome_availability_v1":
        raise ProviderOutcomeAuditError("contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise ProviderOutcomeAuditError("contract_scope_invalid")
    pit = payload.get("pit_rules") or {}
    if pit.get("historical_retrojection_permitted") is not False:
        raise ProviderOutcomeAuditError("retrojection_must_be_forbidden")
    if pit.get("missing_outcome_may_not_enter_denominator") is not True:
        raise ProviderOutcomeAuditError("missing_outcome_fail_closed_required")
    if pit.get("unknown_outcome_may_not_enter_denominator") is not True:
        raise ProviderOutcomeAuditError("unknown_outcome_fail_closed_required")
    provider = payload.get("provider_coverage") or {}
    outcome = payload.get("outcome_availability") or {}
    if provider.get("strict_ledger_promotion_enabled") is not False:
        raise ProviderOutcomeAuditError("provider_promotion_must_be_disabled")
    if outcome.get("strict_ledger_promotion_enabled") is not False:
        raise ProviderOutcomeAuditError("outcome_promotion_must_be_disabled")
    if outcome.get("neutral_imputation_allowed") is not False:
        raise ProviderOutcomeAuditError("neutral_imputation_must_be_forbidden")
    horizons = outcome.get("horizons_trading_sessions")
    if not isinstance(horizons, list) or not horizons or any(type(h) is not int or h <= 0 for h in horizons):
        raise ProviderOutcomeAuditError("outcome_horizons_invalid")
    return payload


def _validate_frozen_inputs(root: Path, contract: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, str]]:
    inputs = contract.get("inputs") or {}
    actual: dict[str, str] = {}
    for name in ("history_analysis", "price_backfill", "latest_scanner"):
        spec = inputs.get(name) or {}
        path = root / _clean(spec.get("path"))
        expected = _clean(spec.get("sha256"))
        if not path.exists() or not expected:
            raise ProviderOutcomeAuditError(f"frozen_input_spec_invalid:{name}")
        digest = _sha256_path(path)
        actual[name] = digest
        if digest != expected:
            raise ProviderOutcomeAuditError(f"frozen_input_hash_mismatch:{name}:{digest}")

    metadata_spec = inputs.get("metadata") or {}
    metadata_path = root / _clean(metadata_spec.get("path"))
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProviderOutcomeAuditError(f"metadata_unreadable:{metadata_path}") from exc
    expected_as_of = _clean(metadata_spec.get("expected_as_of"))
    if _clean(metadata.get("as_of")) != expected_as_of:
        raise ProviderOutcomeAuditError("metadata_as_of_mismatch")
    for name, digest in actual.items():
        metadata_digest = _clean((metadata.get(name) or {}).get("sha256"))
        if metadata_digest != digest:
            raise ProviderOutcomeAuditError(f"metadata_hash_mismatch:{name}")
    return metadata, actual


def classify_current_provider_reachability(detail: Mapping[str, Any], *, snapshot_generated_at: datetime) -> dict[str, Any]:
    status = _clean(detail.get("status"))
    provider_symbol = _clean(detail.get("provider_symbol"))
    attempt = _parse_utc(detail.get("last_attempt_at"), field="last_attempt_at", allow_none=True)
    has_price_coverage = status in {"price_data_ok", "price_data_partial"} and int(detail.get("sessions") or 0) > 0

    if attempt is not None and provider_symbol and has_price_coverage:
        if attempt <= snapshot_generated_at:
            classification = "PIT_OBSERVED_AT_OR_BEFORE_SNAPSHOT"
            snapshot_pit = True
        else:
            classification = "POST_SNAPSHOT_OBSERVED"
            snapshot_pit = False
    elif attempt is not None:
        classification = "ATTEMPT_WITHOUT_PRICE_COVERAGE"
        snapshot_pit = False
    else:
        classification = "NO_ATTEMPT_EVIDENCE"
        snapshot_pit = False

    return {
        "classification": classification,
        "snapshot_pit": snapshot_pit,
        "price_status": status,
        "provider_symbol_present": bool(provider_symbol),
        "last_attempt_at": attempt.isoformat() if attempt else None,
        "sessions": int(detail.get("sessions") or 0),
    }


def audit_current_provider_metadata(metadata: Mapping[str, Any]) -> dict[str, Any]:
    generated = _parse_utc(metadata.get("views_generated_at") or metadata.get("generated_at"), field="views_generated_at")
    assert generated is not None
    coverage = metadata.get("price_coverage")
    if not isinstance(coverage, Mapping):
        raise ProviderOutcomeAuditError("price_coverage_missing")
    symbols = coverage.get("symbols")
    if not isinstance(symbols, Mapping):
        raise ProviderOutcomeAuditError("price_coverage_symbols_missing")

    rows: dict[str, dict[str, Any]] = {}
    counts: Counter[str] = Counter()
    for symbol, detail in sorted(symbols.items()):
        if not isinstance(detail, Mapping):
            raise ProviderOutcomeAuditError(f"price_coverage_detail_invalid:{symbol}")
        result = classify_current_provider_reachability(detail, snapshot_generated_at=generated)
        rows[str(symbol)] = result
        counts[result["classification"]] += 1

    return {
        "snapshot_generated_at": generated.isoformat(),
        "current_symbol_count": len(rows),
        "classification_counts": dict(sorted(counts.items())),
        "snapshot_pit_reachability_count": sum(1 for row in rows.values() if row["snapshot_pit"]),
        "strict_provider_coverage_promoted_count": 0,
        "historical_provider_coverage_claimed": False,
        "rows": rows,
    }


def _scanner_events(rows: Sequence[Mapping[str, Any]]) -> list[tuple[date, str]]:
    seen: set[tuple[str, date]] = set()
    result: list[tuple[date, str]] = []
    for row in rows:
        if not observed(row):
            continue
        symbol = _clean(row.get("symbol"))
        raw_day = _clean(row.get("date"))
        if not symbol or not raw_day:
            continue
        try:
            day = date.fromisoformat(raw_day)
        except ValueError:
            continue
        key = (symbol, day)
        if key in seen:
            continue
        seen.add(key)
        result.append((day, symbol))
    result.sort(key=lambda item: (item[0], item[1]))
    return result


def _price_index(rows: Sequence[Mapping[str, Any]], *, as_of: date) -> tuple[dict[str, list[date]], dict[tuple[str, date], float], dict[str, Any]]:
    valid, issues = validated_rows(rows)
    dates: dict[str, list[date]] = defaultdict(list)
    closes: dict[tuple[str, date], float] = {}
    source_counts: Counter[str] = Counter()
    retrieved_known = 0
    retrieved_unknown = 0
    kept = 0
    for row in valid:
        day = _parse_date(row.get("date"), field="price.date")
        if day > as_of:
            continue
        symbol = _clean(row.get("symbol"))
        dates[symbol].append(day)
        closes[(symbol, day)] = float(row["close"])
        source_counts[_clean(row.get("source")) or "<blank>"] += 1
        if _clean(row.get("retrieved_at")):
            retrieved_known += 1
        else:
            retrieved_unknown += 1
        kept += 1
    for symbol in dates:
        dates[symbol] = sorted(set(dates[symbol]))
    diagnostics = {
        "valid_rows_as_of": kept,
        "symbol_count": len(dates),
        "source_counts": dict(sorted(source_counts.items())),
        "retrieved_at_known_rows": retrieved_known,
        "retrieved_at_unknown_rows": retrieved_unknown,
        "validation_issue_symbol_count": len(issues),
        "validation_issues": issues,
    }
    return dict(dates), closes, diagnostics


def classify_forward_outcome(
    *,
    symbol: str,
    event_date: date,
    horizon: int,
    price_dates: Mapping[str, Sequence[date]],
    closes: Mapping[tuple[str, date], float],
    audit_as_of: date,
) -> dict[str, Any]:
    days = list(price_dates.get(symbol, ()))
    index = bisect_left(days, event_date)
    if index >= len(days) or days[index] != event_date or (symbol, event_date) not in closes:
        return {
            "availability_status": "MISSING",
            "available_from": None,
            "denominator_eligible": False,
            "forward_return": None,
            "reason_codes": ["EXACT_EVENT_DATE_PRICE_MISSING"],
        }
    target = index + horizon
    if target >= len(days) or days[target] > audit_as_of:
        return {
            "availability_status": "UNKNOWN",
            "available_from": None,
            "denominator_eligible": False,
            "forward_return": None,
            "reason_codes": ["HORIZON_TARGET_NOT_PRESENT_IN_FROZEN_PRICE_SESSIONS", "NOT_YET_AVAILABLE_NOT_INFERRED_WITHOUT_CALENDAR_EVIDENCE"],
        }
    target_day = days[target]
    start_close = closes[(symbol, event_date)]
    target_close = closes[(symbol, target_day)]
    value = target_close / start_close - 1.0
    return {
        "availability_status": "AVAILABLE",
        "available_from": target_day.isoformat(),
        "denominator_eligible": True,
        "forward_return": value,
        "reason_codes": ["EXACT_START_AND_HORIZON_TARGET_PRESENT_IN_FROZEN_PRICE_SESSIONS"],
    }


def _label_hash(horizon: int) -> str:
    definition = {
        "event": "first stored observed scanner row per symbol/date",
        "start": "exact event-date valid stored close",
        "target": f"{horizon}-th later valid stored price session",
        "return": "target_close/start_close-1",
        "neutral_imputation": False,
    }
    raw = json.dumps(definition, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return "sha256:" + sha256(raw.encode("utf-8")).hexdigest()


def audit_outcome_availability(
    history_rows: Sequence[Mapping[str, Any]],
    price_rows: Sequence[Mapping[str, Any]],
    *,
    audit_as_of: date,
    horizons: Sequence[int],
) -> dict[str, Any]:
    events = _scanner_events(history_rows)
    price_dates, closes, price_diagnostics = _price_index(price_rows, as_of=audit_as_of)
    horizon_reports: dict[str, dict[str, Any]] = {}
    total_available = 0
    total_missing = 0
    total_unknown = 0

    for horizon in horizons:
        counts: Counter[str] = Counter()
        available_from_values: list[str] = []
        for event_date, symbol in events:
            result = classify_forward_outcome(
                symbol=symbol,
                event_date=event_date,
                horizon=horizon,
                price_dates=price_dates,
                closes=closes,
                audit_as_of=audit_as_of,
            )
            counts[result["availability_status"]] += 1
            if result["available_from"]:
                available_from_values.append(str(result["available_from"]))
        available = counts["AVAILABLE"]
        missing = counts["MISSING"]
        unknown = counts["UNKNOWN"]
        total_available += available
        total_missing += missing
        total_unknown += unknown
        horizon_reports[str(horizon)] = {
            "outcome_id": f"forward_return_{horizon}t",
            "label_definition_hash": _label_hash(horizon),
            "event_count": len(events),
            "availability_counts": {"AVAILABLE": available, "MISSING": missing, "UNKNOWN": unknown},
            "denominator_eligible_count": available,
            "denominator_excluded_count": missing + unknown,
            "eligible_fraction": (available / len(events)) if events else None,
            "first_available_from": min(available_from_values) if available_from_values else None,
            "last_available_from": max(available_from_values) if available_from_values else None,
        }

    return {
        "audit_as_of": audit_as_of.isoformat(),
        "scanner_event_count": len(events),
        "unique_observed_symbol_count": len({symbol for _, symbol in events}),
        "horizons": horizon_reports,
        "availability_totals_across_horizons": {
            "AVAILABLE": total_available,
            "MISSING": total_missing,
            "UNKNOWN": total_unknown,
        },
        "strict_outcome_ledger_promoted_count": 0,
        "prior_analysis_time_availability_claimed": False,
        "neutral_imputation_performed": False,
        "price_diagnostics": price_diagnostics,
    }


def run_provider_outcome_audit(root: str | Path, *, contract_path: str | Path | None = None) -> dict[str, Any]:
    root_path = Path(root).resolve()
    contract = load_provider_outcome_contract(contract_path)
    metadata, hashes = _validate_frozen_inputs(root_path, contract)
    inputs = contract["inputs"]
    history_rows = _read_csv(root_path / inputs["history_analysis"]["path"])
    price_rows = _read_csv(root_path / inputs["price_backfill"]["path"])
    latest_rows = _read_csv(root_path / inputs["latest_scanner"]["path"])
    expected_latest = int((metadata.get("latest_scanner") or {}).get("row_count") or 0)
    if len(latest_rows) != expected_latest:
        raise ProviderOutcomeAuditError("latest_scanner_row_count_mismatch")

    provider = audit_current_provider_metadata(metadata)
    audit_as_of = _parse_date((metadata.get("price_coverage") or {}).get("as_of") or metadata.get("as_of"), field="audit_as_of")
    horizons = [int(value) for value in contract["outcome_availability"]["horizons_trading_sessions"]]
    outcomes = audit_outcome_availability(history_rows, price_rows, audit_as_of=audit_as_of, horizons=horizons)

    return {
        "schema_version": SCHEMA_VERSION,
        "module": "QM-B",
        "research_only": True,
        "productive_integration_enabled": False,
        "frozen_input_commit": contract["frozen_input_commit"],
        "frozen_input_sha256": hashes,
        "metadata_as_of": metadata.get("as_of"),
        "provider_coverage": provider,
        "outcome_availability": outcomes,
        "strict_provider_coverage_ledger_promoted": False,
        "strict_outcome_availability_ledger_promoted": False,
        "historical_retrojection_permitted": False,
        "missing_or_unknown_outcomes_enter_denominator": False,
        "audit_gate_status": "PASS",
        "remaining_research_status": "RESEARCH_REQUIRED",
        "remaining_reason_codes": [
            "STRICT_HISTORICAL_PROVIDER_COVERAGE_NOT_ESTABLISHED",
            "OUTCOME_ROWS_NOT_YET_BOUND_TO_STRICT_STABLE_INSTRUMENT_LEDGER",
            "PRIOR_ANALYSIS_TIME_OUTCOME_AVAILABILITY_NOT_RECONSTRUCTED_FROM_CURRENT_SNAPSHOT",
        ],
    }
