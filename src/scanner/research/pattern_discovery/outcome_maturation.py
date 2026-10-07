"""Phase L8 outcome maturation for Pattern Discovery Lab v2.

L8 consumes immutable, hash-valid L7 prospective capture reports. It never
mutates an L7 claim. A claim can receive a matured outcome only when the exact
start market-session date is present and the complete forward horizon exists in
validated observed price sessions.

Raw close establishes whether a market session exists. Research returns and
path metrics require adj_close and never fall back to raw close. All values stay
in the observed original currency; no FX conversion is performed.

L8 stops before confirmation. It does not compute hit rates, probabilities,
sequential looks, ratings, promotion state, Decision Layer evidence, portfolio
actions or execution instructions.
"""
from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timezone
from hashlib import sha256
import json
import math
import os
from pathlib import Path
import re
from statistics import median
from typing import Any, Mapping, Sequence

from scanner.data.price_history import number, validated_rows

from .boundary import PatternDiscoveryBoundary
from .prospective_capture import verify_capture_report, verify_prospective_claim


SCHEMA_VERSION = "pattern_discovery_l8_outcome_maturation_v1"
OUTCOME_SCHEMA_VERSION = "pattern_discovery_l8_matured_outcome_v1"
EVENT_SCHEMA_VERSION = "pattern_discovery_l8_maturation_event_v1"
CHECK_REPORT_SCHEMA_VERSION = "pattern_discovery_l8_maturation_check_v1"
DEFAULT_CONTRACT_PATH = (
    Path(__file__).resolve().parents[4]
    / "configs"
    / "pattern_discovery"
    / "l8_outcome_maturation_v1.json"
)


class OutcomeMaturationError(ValueError):
    """Raised when an L8 outcome-maturation invariant is violated."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _hash(value: Any) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise OutcomeMaturationError(f"value_required:{field}")
    return result


def _safe_token(value: Any, field: str) -> str:
    result = _text(value, field)
    if not re.fullmatch(r"[A-Za-z0-9._:-]+", result):
        raise OutcomeMaturationError(f"safe_token_required:{field}")
    return result


def _sha256_text(value: Any, field: str) -> str:
    result = _text(value, field).lower()
    if not re.fullmatch(r"[0-9a-f]{64}", result):
        raise OutcomeMaturationError(f"sha256_required:{field}")
    return result


def _as_datetime(value: Any, field: str) -> datetime:
    text = _text(value, field)
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    if len(normalized) == 10:
        normalized += "T00:00:00+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise OutcomeMaturationError(f"invalid_timestamp:{field}") from exc
    if parsed.tzinfo is None:
        raise OutcomeMaturationError(f"timezone_required:{field}")
    return parsed.astimezone(timezone.utc)


def _timestamp(value: Any, field: str) -> str:
    return _as_datetime(value, field).isoformat().replace("+00:00", "Z")


def _as_date(value: Any, field: str) -> date:
    text = _text(value, field)
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise OutcomeMaturationError(f"invalid_date:{field}") from exc


def _currency(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return None if text in {"", "NONE", "NULL", "NAN"} else text


def _finite(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise OutcomeMaturationError(f"finite_number_required:{field}")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise OutcomeMaturationError(f"finite_number_required:{field}") from exc
    if not math.isfinite(result):
        raise OutcomeMaturationError(f"finite_number_required:{field}")
    return result


def load_outcome_maturation_contract(
    path: str | Path | None = None,
) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OutcomeMaturationError(
            f"outcome_maturation_contract_unreadable:{target}"
        ) from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != SCHEMA_VERSION:
        raise OutcomeMaturationError("outcome_maturation_contract_schema_invalid")
    if payload.get("research_only") is not True:
        raise OutcomeMaturationError("outcome_maturation_must_be_research_only")
    if payload.get("productive_integration_enabled") is not False:
        raise OutcomeMaturationError(
            "outcome_maturation_productive_integration_forbidden"
        )
    if payload.get("execution_allowed") is not False:
        raise OutcomeMaturationError("outcome_maturation_execution_forbidden")
    principles = payload.get("principles") or {}
    if principles.get("adjusted_close_is_required_for_returns") is not True:
        raise OutcomeMaturationError("adjusted_close_guard_required")
    if principles.get("premature_outcomes_are_forbidden") is not True:
        raise OutcomeMaturationError("premature_outcome_guard_required")
    if principles.get("l9_confirmation_is_not_performed_here") is not True:
        raise OutcomeMaturationError("l8_l9_boundary_guard_required")
    return payload


def outcome_maturation_contract_hash(
    contract: Mapping[str, Any] | None = None,
) -> str:
    value = (
        dict(contract)
        if contract is not None
        else load_outcome_maturation_contract()
    )
    return _hash(value)


def _normalize_peer_snapshot(
    rows: Sequence[Mapping[str, Any]],
    *,
    capture_report: Mapping[str, Any],
    peer_snapshot_file_sha256: str,
    contract: Mapping[str, Any],
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    if isinstance(rows, (str, bytes, bytearray)) or not isinstance(rows, Sequence):
        raise OutcomeMaturationError("peer_snapshot_rows_sequence_required")
    required = tuple(contract["inputs"]["peer_snapshot_required_fields"])
    expected_snapshot_id = _safe_token(
        capture_report["snapshot_binding"]["snapshot_id"],
        "capture_report.snapshot_id",
    )
    expected_file_hash = _sha256_text(
        capture_report["snapshot_binding"]["snapshot_file_sha256"],
        "capture_report.snapshot_file_sha256",
    )
    supplied_file_hash = _sha256_text(
        peer_snapshot_file_sha256,
        "peer_snapshot_file_sha256",
    )
    if supplied_file_hash != expected_file_hash:
        raise OutcomeMaturationError("peer_snapshot_file_hash_mismatch_l7")

    by_symbol: dict[str, dict[str, Any]] = {}
    for index, raw in enumerate(rows):
        if not isinstance(raw, Mapping):
            raise OutcomeMaturationError(f"peer_snapshot_row_must_be_object:{index}")
        missing = [field for field in required if field not in raw]
        if missing:
            raise OutcomeMaturationError(
                f"peer_snapshot_fields_missing:{index}:" + ",".join(missing)
            )
        symbol = _text(raw.get("symbol"), f"peer_snapshot[{index}].symbol")
        if symbol in by_symbol:
            raise OutcomeMaturationError(f"duplicate_peer_snapshot_symbol:{symbol}")
        snapshot_id = _safe_token(
            raw.get("snapshot_id"),
            f"peer_snapshot[{index}].snapshot_id",
        )
        if snapshot_id != expected_snapshot_id:
            raise OutcomeMaturationError(
                f"peer_snapshot_id_mismatch:{symbol}:{snapshot_id}"
            )
        by_symbol[symbol] = {
            "symbol": symbol,
            "currency": _currency(raw.get("currency")),
            "snapshot_id": snapshot_id,
        }

    normalized = [by_symbol[symbol] for symbol in sorted(by_symbol)]
    binding = {
        "snapshot_id": expected_snapshot_id,
        "snapshot_file_sha256": supplied_file_hash,
        "row_count": len(normalized),
        "symbol_count": len(normalized),
        "membership_hash": _hash(normalized),
    }
    binding["peer_snapshot_binding_hash"] = _hash(binding)
    return by_symbol, binding


def _normalize_prices(
    rows: Sequence[Mapping[str, Any]],
    *,
    price_as_of: str,
    checked_at: str,
    price_file_sha256: str,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    if isinstance(rows, (str, bytes, bytearray)) or not isinstance(rows, Sequence):
        raise OutcomeMaturationError("price_rows_sequence_required")
    as_of = _as_date(price_as_of, "price_as_of")
    checked = _as_datetime(checked_at, "checked_at")
    if as_of > checked.date():
        raise OutcomeMaturationError("price_as_of_after_checked_at")

    valid, issues = validated_rows(rows)
    grouped: dict[str, list[dict[str, Any]]] = {}
    kept = 0
    for raw in valid:
        day = _as_date(raw.get("date"), "price.date")
        if day > as_of:
            continue
        symbol = _text(raw.get("symbol"), "price.symbol")
        close = number(raw.get("close"))
        if close is None or close <= 0:
            raise OutcomeMaturationError("validated_price_close_invalid")
        adj = number(raw.get("adj_close"))
        normalized = {
            "symbol": symbol,
            "date": day.isoformat(),
            "currency": _currency(raw.get("currency")),
            "close": float(close),
            "adj_close": float(adj) if adj is not None and adj > 0 else None,
            "source": str(raw.get("source") or "").strip() or None,
            "retrieved_at": str(raw.get("retrieved_at") or "").strip() or None,
            "observation_type": (
                str(raw.get("observation_type") or "").strip() or None
            ),
        }
        grouped.setdefault(symbol, []).append(normalized)
        kept += 1

    for symbol, series in grouped.items():
        series.sort(key=lambda row: row["date"])
        dates = [str(row["date"]) for row in series]
        if len(dates) != len(set(dates)):
            raise OutcomeMaturationError(
                f"normalized_duplicate_price_session:{symbol}"
            )

    binding = {
        "price_file_sha256": _sha256_text(
            price_file_sha256,
            "price_file_sha256",
        ),
        "price_as_of": as_of.isoformat(),
        "valid_session_count_as_of": kept,
        "symbol_count_as_of": len(grouped),
        "validation_issue_symbol_count": len(issues),
        "validation_issues": issues,
        "normalized_price_hash": _hash(
            [
                row
                for symbol in sorted(grouped)
                for row in grouped[symbol]
            ]
        ),
    }
    binding["price_binding_hash"] = _hash(binding)
    return grouped, binding


def _parse_target(
    claim: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    forecast = claim.get("forecast")
    if not isinstance(forecast, Mapping):
        raise OutcomeMaturationError("claim_forecast_missing")
    target_id = _text(forecast.get("target_id"), "claim.forecast.target_id")
    claim_horizon = int(forecast.get("horizon_sessions"))
    allowed = {
        int(value)
        for value in contract["price_semantics"]["allowed_horizons_sessions"]
    }
    if claim_horizon not in allowed:
        raise OutcomeMaturationError(
            f"claim_horizon_not_supported:{claim_horizon}"
        )
    expected_direction = _text(
        forecast.get("expected_direction"),
        "claim.forecast.expected_direction",
    )
    if expected_direction not in {"POSITIVE", "NEGATIVE"}:
        raise OutcomeMaturationError(
            f"claim_expected_direction_invalid:{expected_direction}"
        )

    for pattern_type, regex in contract["outcome"][
        "supported_target_patterns"
    ].items():
        match = re.fullmatch(str(regex), target_id)
        if not match:
            continue
        target_horizon = int(match.group(1))
        direction = "POSITIVE" if match.group(2).lower() == "gt" else "NEGATIVE"
        if target_horizon != claim_horizon:
            raise OutcomeMaturationError("target_horizon_mismatch_claim")
        if direction != expected_direction:
            raise OutcomeMaturationError("target_direction_mismatch_claim")
        return {
            "pattern_type": str(pattern_type),
            "target_id": target_id,
            "horizon_sessions": claim_horizon,
            "expected_direction": expected_direction,
        }
    raise OutcomeMaturationError(f"target_not_supported_by_l8:{target_id}")


def _claim_start_date(claim: Mapping[str, Any]) -> str:
    start = claim.get("start_market_session")
    if not isinstance(start, Mapping):
        raise OutcomeMaturationError("claim_start_market_session_missing")
    return _as_datetime(
        start.get("start_at"),
        "claim.start_market_session.start_at",
    ).date().isoformat()


def _path_hash(path: Sequence[Mapping[str, Any]]) -> str:
    return _hash(
        [
            {
                "symbol": row["symbol"],
                "date": row["date"],
                "currency": row["currency"],
                "adj_close": row["adj_close"],
            }
            for row in path
        ]
    )


def _max_drawdown(prices: Sequence[float]) -> float:
    peak = prices[0]
    worst = 0.0
    for value in prices:
        peak = max(peak, value)
        drawdown = value / peak - 1.0
        worst = min(worst, drawdown)
    return float(worst)


def _adverse_excursion(
    prices: Sequence[float],
    *,
    expected_direction: str,
) -> float:
    start = prices[0]
    sign = 1.0 if expected_direction == "POSITIVE" else -1.0
    aligned = [sign * (value / start - 1.0) for value in prices]
    return float(min(aligned))


def _complete_path(
    series: Sequence[Mapping[str, Any]],
    *,
    start_index: int,
    horizon: int,
    expected_direction: str,
) -> dict[str, Any]:
    target_index = start_index + horizon
    if target_index >= len(series):
        return {
            "status": "IMMATURE_HORIZON",
            "reason_codes": [
                "HORIZON_TARGET_NOT_PRESENT_IN_OBSERVED_PRICE_SESSIONS"
            ],
        }
    path = [dict(row) for row in series[start_index : target_index + 1]]
    currencies = {_currency(row.get("currency")) for row in path}
    if None in currencies:
        return {
            "status": "CURRENCY_UNAVAILABLE",
            "reason_codes": ["ORIGINAL_CURRENCY_MISSING_ON_SUBJECT_PATH"],
        }
    if len(currencies) != 1:
        return {
            "status": "CURRENCY_MISMATCH",
            "reason_codes": ["ORIGINAL_CURRENCY_CHANGED_WITHIN_SUBJECT_PATH"],
        }
    adjusted = [row.get("adj_close") for row in path]
    if any(value is None for value in adjusted):
        return {
            "status": "MISSING_ADJUSTED_PRICE",
            "reason_codes": [
                "ADJUSTED_PRICE_MISSING_ON_REQUIRED_SUBJECT_SESSION"
            ],
        }
    values = [
        _finite(value, "path.adj_close")
        for value in adjusted
    ]
    if any(value <= 0 for value in values):
        return {
            "status": "MISSING_ADJUSTED_PRICE",
            "reason_codes": ["ADJUSTED_PRICE_NOT_POSITIVE_ON_SUBJECT_PATH"],
        }

    start_value = values[0]
    target_value = values[-1]
    return {
        "status": "COMPLETE",
        "reason_codes": ["EXACT_FULL_HORIZON_PRESENT"],
        "currency": next(iter(currencies)),
        "path": path,
        "path_hash": _path_hash(path),
        "start_session_date": path[0]["date"],
        "target_session_date": path[-1]["date"],
        "session_dates": [row["date"] for row in path],
        "return": float(target_value / start_value - 1.0),
        "adverse_excursion": _adverse_excursion(
            values,
            expected_direction=expected_direction,
        ),
        "path_max_drawdown": _max_drawdown(values),
    }


def _subject_path(
    claim: Mapping[str, Any],
    price_groups: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    horizon: int,
    expected_direction: str,
) -> dict[str, Any]:
    symbol = _text(claim.get("match", {}).get("symbol"), "claim.match.symbol")
    start_date = _claim_start_date(claim)
    series = list(price_groups.get(symbol, ()))
    if not series:
        return {
            "status": "MISSING_START_SESSION",
            "reason_codes": ["NO_VALID_PRICE_SESSIONS_FOR_SUBJECT"],
        }
    positions = {
        str(row["date"]): index
        for index, row in enumerate(series)
    }
    if start_date not in positions:
        return {
            "status": "MISSING_START_SESSION",
            "reason_codes": [
                "EXACT_L7_START_SESSION_DATE_PRICE_BAR_MISSING"
            ],
        }
    return _complete_path(
        series,
        start_index=positions[start_date],
        horizon=horizon,
        expected_direction=expected_direction,
    )


def _peer_path(
    symbol: str,
    price_groups: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    subject_start_date: str,
    horizon: int,
) -> dict[str, Any]:
    series = list(price_groups.get(symbol, ()))
    if not series:
        return {
            "status": "MISSING_START_SESSION",
            "reason_codes": ["NO_VALID_PRICE_SESSIONS_FOR_PEER"],
        }
    start_index: int | None = None
    for index, row in enumerate(series):
        if str(row["date"]) >= subject_start_date:
            start_index = index
            break
    if start_index is None:
        return {
            "status": "IMMATURE_HORIZON",
            "reason_codes": ["NO_PEER_SESSION_ON_OR_AFTER_SUBJECT_START"],
        }
    return _complete_path(
        series,
        start_index=start_index,
        horizon=horizon,
        expected_direction="POSITIVE",
    )


def _reference_outcome(
    *,
    subject_symbol: str,
    subject_currency: str,
    subject_start_date: str,
    horizon: int,
    peer_snapshot: Mapping[str, Mapping[str, Any]],
    price_groups: Mapping[str, Sequence[Mapping[str, Any]]],
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    if subject_symbol not in peer_snapshot:
        return {
            "status": "UNAVAILABLE",
            "method_id": contract["reference_semantics"]["method_id"],
            "reason_codes": ["SUBJECT_NOT_PRESENT_IN_BOUND_PEER_SNAPSHOT"],
            "peer_return": None,
            "peer_count": 0,
            "same_currency_peer_count": 0,
            "global_peer_count": 0,
            "eligible_peer_count": 0,
            "excluded_peer_count": len(peer_snapshot),
            "peer_details_hash": _hash([]),
        }

    subject_snapshot_currency = _currency(
        peer_snapshot[subject_symbol].get("currency")
    )
    if (
        subject_snapshot_currency is not None
        and subject_snapshot_currency != subject_currency
    ):
        return {
            "status": "UNAVAILABLE",
            "method_id": contract["reference_semantics"]["method_id"],
            "reason_codes": ["SUBJECT_SNAPSHOT_PRICE_CURRENCY_MISMATCH"],
            "peer_return": None,
            "peer_count": 0,
            "same_currency_peer_count": 0,
            "global_peer_count": 0,
            "eligible_peer_count": 0,
            "excluded_peer_count": max(0, len(peer_snapshot) - 1),
            "peer_details_hash": _hash([]),
        }

    peer_details: list[dict[str, Any]] = []
    excluded: Counter[str] = Counter()
    for symbol in sorted(peer_snapshot):
        if symbol == subject_symbol:
            continue
        peer = _peer_path(
            symbol,
            price_groups,
            subject_start_date=subject_start_date,
            horizon=horizon,
        )
        if peer["status"] != "COMPLETE":
            excluded[str(peer["status"])] += 1
            continue
        snapshot_currency = _currency(peer_snapshot[symbol].get("currency"))
        price_currency = _currency(peer.get("currency"))
        if snapshot_currency is not None and snapshot_currency != price_currency:
            excluded["CURRENCY_MISMATCH"] += 1
            continue
        peer_details.append(
            {
                "symbol": symbol,
                "currency": price_currency,
                "start_session_date": peer["start_session_date"],
                "target_session_date": peer["target_session_date"],
                "return": peer["return"],
                "path_hash": peer["path_hash"],
            }
        )

    same_currency = [
        row
        for row in peer_details
        if row["currency"] == subject_currency
    ]
    chosen = same_currency if same_currency else peer_details
    if len(chosen) < int(contract["reference_semantics"]["minimum_peer_count"]):
        return {
            "status": "UNAVAILABLE",
            "method_id": contract["reference_semantics"]["method_id"],
            "reason_codes": ["NO_COMPLETE_LEAVE_ONE_OUT_PEER_RETURN"],
            "peer_return": None,
            "peer_count": 0,
            "same_currency_peer_count": len(same_currency),
            "global_peer_count": len(peer_details),
            "eligible_peer_count": len(peer_details),
            "excluded_peer_count": sum(excluded.values()),
            "excluded_peer_status_counts": dict(sorted(excluded.items())),
            "peer_details_hash": _hash(peer_details),
        }

    method_scope = (
        "SAME_CURRENCY"
        if same_currency
        else "GLOBAL_FALLBACK"
    )
    peer_return = float(median(float(row["return"]) for row in chosen))
    return {
        "status": "AVAILABLE",
        "method_id": contract["reference_semantics"]["method_id"],
        "scope": method_scope,
        "reason_codes": [
            (
                "LEAVE_ONE_OUT_SAME_CURRENCY_MEDIAN"
                if same_currency
                else "LEAVE_ONE_OUT_GLOBAL_MEDIAN_FALLBACK"
            )
        ],
        "peer_return": peer_return,
        "peer_count": len(chosen),
        "same_currency_peer_count": len(same_currency),
        "global_peer_count": len(peer_details),
        "eligible_peer_count": len(peer_details),
        "excluded_peer_count": sum(excluded.values()),
        "excluded_peer_status_counts": dict(sorted(excluded.items())),
        "peer_details_hash": _hash(peer_details),
        "chosen_peer_set_hash": _hash(chosen),
    }


def _record_id(
    claim_id: str,
    *,
    contract: Mapping[str, Any],
) -> str:
    digest_chars = int(contract["outcome"]["id_digest_chars"])
    return (
        f"{contract['outcome']['record_id_prefix']}-"
        f"{_hash({'claim_id': claim_id})[:digest_chars].upper()}"
    )


def _build_matured_record(
    claim: Mapping[str, Any],
    *,
    target: Mapping[str, Any],
    subject: Mapping[str, Any],
    reference: Mapping[str, Any],
    peer_snapshot_binding: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    claim_id = _text(claim.get("claim_id"), "claim_id")
    pattern_type = str(target["pattern_type"])
    subject_return = float(subject["return"])
    peer_return = (
        float(reference["peer_return"])
        if reference.get("status") == "AVAILABLE"
        else None
    )
    peer_excess = (
        subject_return - peer_return
        if peer_return is not None
        else None
    )

    if pattern_type == "DIRECTIONAL":
        target_value = subject_return
        target_value_kind = "RETURN"
    elif pattern_type == "RELATIVE_ALPHA":
        if peer_excess is None:
            raise OutcomeMaturationError(
                "relative_alpha_requires_reference_outcome"
            )
        target_value = peer_excess
        target_value_kind = "PEER_EXCESS"
    else:
        raise OutcomeMaturationError(
            f"unsupported_pattern_type:{pattern_type}"
        )

    start_session = claim["start_market_session"]
    record: dict[str, Any] = {
        "schema_version": OUTCOME_SCHEMA_VERSION,
        "state": contract["outcome"]["matured_state"],
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "record_id": _record_id(claim_id, contract=contract),
        "claim": {
            "claim_id": claim_id,
            "event_id": claim["event_id"],
            "claim_hash": claim["claim_hash"],
            "pattern_id": claim["pattern"]["pattern_id"],
            "pattern_version": claim["pattern"]["pattern_version"],
            "pattern_spec_hash": claim["pattern"]["pattern_spec_hash"],
            "symbol": claim["match"]["symbol"],
            "capture_snapshot_id": claim["match"]["snapshot_id"],
            "capture_snapshot_binding_hash": claim["match"][
                "snapshot_binding_hash"
            ],
        },
        "target": {
            "pattern_type": pattern_type,
            "target_id": target["target_id"],
            "expected_direction": target["expected_direction"],
            "horizon_sessions": int(target["horizon_sessions"]),
            "baseline": claim["forecast"]["baseline"],
            "reference_definition": claim["forecast"][
                "reference_definition"
            ],
        },
        "horizon_provenance": {
            "start_market_session_id": start_session["session_id"],
            "calendar_id": start_session["calendar_id"],
            "session_source": start_session["source"],
            "start_at": start_session["start_at"],
            "start_session_date": subject["start_session_date"],
            "target_session_date": subject["target_session_date"],
            "horizon_sessions": int(target["horizon_sessions"]),
            "observed_session_count_including_start": len(
                subject["session_dates"]
            ),
            "session_dates": list(subject["session_dates"]),
        },
        "price_provenance": {
            "price_kind": "ADJUSTED_CLOSE",
            "currency": subject["currency"],
            "currency_conversion_performed": False,
            "subject_path_hash": subject["path_hash"],
        },
        "reference": {
            "status": reference["status"],
            "method_id": reference["method_id"],
            "scope": reference.get("scope"),
            "peer_snapshot_id": peer_snapshot_binding["snapshot_id"],
            "peer_snapshot_binding_hash": peer_snapshot_binding[
                "peer_snapshot_binding_hash"
            ],
            "peer_return": peer_return,
            "peer_count": int(reference.get("peer_count") or 0),
            "same_currency_peer_count": int(
                reference.get("same_currency_peer_count") or 0
            ),
            "global_peer_count": int(
                reference.get("global_peer_count") or 0
            ),
            "eligible_peer_count": int(
                reference.get("eligible_peer_count") or 0
            ),
            "excluded_peer_count": int(
                reference.get("excluded_peer_count") or 0
            ),
            "excluded_peer_status_counts": dict(
                reference.get("excluded_peer_status_counts") or {}
            ),
            "peer_details_hash": reference.get("peer_details_hash"),
            "chosen_peer_set_hash": reference.get("chosen_peer_set_hash"),
            "reason_codes": list(reference.get("reason_codes") or []),
        },
        "outcome": {
            "return": subject_return,
            "peer_excess": peer_excess,
            "adverse_excursion": float(subject["adverse_excursion"]),
            "path_max_drawdown": float(subject["path_max_drawdown"]),
            "target_value": float(target_value),
            "target_value_kind": target_value_kind,
        },
        "boundaries": {
            "direction_hit_computed": False,
            "probability_computed": False,
            "confirmation_evaluation_performed": False,
            "sequential_monitoring_performed": False,
            "rating_assigned": False,
            "promotion_performed": False,
            "decision_layer_integration_performed": False,
        },
    }
    PatternDiscoveryBoundary().assert_research_payload(record)
    record["outcome_hash"] = _hash(record)
    return record


def verify_matured_outcome(
    record: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = (
        dict(contract)
        if contract is not None
        else load_outcome_maturation_contract()
    )
    if not isinstance(record, Mapping):
        raise OutcomeMaturationError("matured_outcome_must_be_object")
    if record.get("schema_version") != OUTCOME_SCHEMA_VERSION:
        raise OutcomeMaturationError("matured_outcome_schema_invalid")
    if record.get("state") != spec["outcome"]["matured_state"]:
        raise OutcomeMaturationError("matured_outcome_state_invalid")
    if record.get("research_only") is not True:
        raise OutcomeMaturationError("matured_outcome_research_only_guard_missing")
    if record.get("productive_integration_enabled") is not False:
        raise OutcomeMaturationError(
            "matured_outcome_productive_integration_forbidden"
        )
    if record.get("execution_allowed") is not False:
        raise OutcomeMaturationError("matured_outcome_execution_forbidden")

    stored = _sha256_text(record.get("outcome_hash"), "outcome_hash")
    body = dict(record)
    body.pop("outcome_hash", None)
    if _hash(body) != stored:
        raise OutcomeMaturationError("matured_outcome_hash_mismatch")

    claim = record.get("claim")
    if not isinstance(claim, Mapping):
        raise OutcomeMaturationError("matured_outcome_claim_binding_missing")
    claim_id = _text(claim.get("claim_id"), "record.claim.claim_id")
    expected_record_id = _record_id(claim_id, contract=spec)
    if record.get("record_id") != expected_record_id:
        raise OutcomeMaturationError("matured_outcome_record_id_mismatch")

    horizon = record.get("horizon_provenance")
    target = record.get("target")
    if not isinstance(horizon, Mapping) or not isinstance(target, Mapping):
        raise OutcomeMaturationError("matured_outcome_horizon_or_target_missing")
    sessions = horizon.get("session_dates")
    if not isinstance(sessions, Sequence) or isinstance(
        sessions, (str, bytes, bytearray)
    ):
        raise OutcomeMaturationError("matured_outcome_session_dates_invalid")
    expected_count = int(target.get("horizon_sessions")) + 1
    if len(sessions) != expected_count:
        raise OutcomeMaturationError("matured_outcome_horizon_session_count_invalid")
    if horizon.get("start_session_date") != sessions[0]:
        raise OutcomeMaturationError("matured_outcome_start_session_mismatch")
    if horizon.get("target_session_date") != sessions[-1]:
        raise OutcomeMaturationError("matured_outcome_target_session_mismatch")

    price = record.get("price_provenance")
    if not isinstance(price, Mapping):
        raise OutcomeMaturationError("matured_outcome_price_provenance_missing")
    if price.get("price_kind") != "ADJUSTED_CLOSE":
        raise OutcomeMaturationError("matured_outcome_adjusted_price_required")
    if price.get("currency_conversion_performed") is not False:
        raise OutcomeMaturationError("matured_outcome_fx_conversion_forbidden")
    _text(price.get("currency"), "record.price_provenance.currency")
    _sha256_text(price.get("subject_path_hash"), "subject_path_hash")

    outcome = record.get("outcome")
    if not isinstance(outcome, Mapping):
        raise OutcomeMaturationError("matured_outcome_metrics_missing")
    for field in (
        "return",
        "adverse_excursion",
        "path_max_drawdown",
        "target_value",
    ):
        _finite(outcome.get(field), f"record.outcome.{field}")
    if target.get("pattern_type") == "RELATIVE_ALPHA":
        _finite(outcome.get("peer_excess"), "record.outcome.peer_excess")

    boundaries = record.get("boundaries")
    if not isinstance(boundaries, Mapping):
        raise OutcomeMaturationError("matured_outcome_boundaries_missing")
    for field in (
        "direction_hit_computed",
        "probability_computed",
        "confirmation_evaluation_performed",
        "sequential_monitoring_performed",
        "rating_assigned",
        "promotion_performed",
        "decision_layer_integration_performed",
    ):
        if boundaries.get(field) is not False:
            raise OutcomeMaturationError(
                f"matured_outcome_boundary_invalid:{field}"
            )
    PatternDiscoveryBoundary().assert_research_payload(record)
    return {
        "valid": True,
        "record_id": record.get("record_id"),
        "claim_id": claim_id,
        "outcome_hash": stored,
    }


def evaluate_claim_maturation(
    claim: Mapping[str, Any],
    *,
    peer_snapshot: Mapping[str, Mapping[str, Any]],
    peer_snapshot_binding: Mapping[str, Any],
    price_groups: Mapping[str, Sequence[Mapping[str, Any]]],
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = (
        dict(contract)
        if contract is not None
        else load_outcome_maturation_contract()
    )
    verify_prospective_claim(claim)
    target = _parse_target(claim, contract=spec)
    subject = _subject_path(
        claim,
        price_groups,
        horizon=int(target["horizon_sessions"]),
        expected_direction=str(target["expected_direction"]),
    )
    if subject["status"] != "COMPLETE":
        return {
            "claim_id": claim["claim_id"],
            "event_id": claim["event_id"],
            "pattern_id": claim["pattern"]["pattern_id"],
            "pattern_version": claim["pattern"]["pattern_version"],
            "symbol": claim["match"]["symbol"],
            "target_id": target["target_id"],
            "horizon_sessions": int(target["horizon_sessions"]),
            "status": subject["status"],
            "reason_codes": list(subject["reason_codes"]),
            "matured_outcome": None,
        }

    reference = _reference_outcome(
        subject_symbol=str(claim["match"]["symbol"]),
        subject_currency=str(subject["currency"]),
        subject_start_date=str(subject["start_session_date"]),
        horizon=int(target["horizon_sessions"]),
        peer_snapshot=peer_snapshot,
        price_groups=price_groups,
        contract=spec,
    )
    if (
        target["pattern_type"] == "RELATIVE_ALPHA"
        and reference["status"] != "AVAILABLE"
    ):
        return {
            "claim_id": claim["claim_id"],
            "event_id": claim["event_id"],
            "pattern_id": claim["pattern"]["pattern_id"],
            "pattern_version": claim["pattern"]["pattern_version"],
            "symbol": claim["match"]["symbol"],
            "target_id": target["target_id"],
            "horizon_sessions": int(target["horizon_sessions"]),
            "status": "REFERENCE_UNAVAILABLE",
            "reason_codes": list(reference.get("reason_codes") or []),
            "subject_path_hash": subject["path_hash"],
            "matured_outcome": None,
        }

    record = _build_matured_record(
        claim,
        target=target,
        subject=subject,
        reference=reference,
        peer_snapshot_binding=peer_snapshot_binding,
        contract=spec,
    )
    verify_matured_outcome(record, contract=spec)
    return {
        "claim_id": claim["claim_id"],
        "event_id": claim["event_id"],
        "pattern_id": claim["pattern"]["pattern_id"],
        "pattern_version": claim["pattern"]["pattern_version"],
        "symbol": claim["match"]["symbol"],
        "target_id": target["target_id"],
        "horizon_sessions": int(target["horizon_sessions"]),
        "status": "MATURED",
        "reason_codes": ["FULL_HORIZON_MATURED_WITH_ADJUSTED_PRICES"],
        "subject_path_hash": subject["path_hash"],
        "matured_outcome": record,
    }


def build_outcome_maturation_check(
    capture_report: Mapping[str, Any],
    peer_snapshot_rows: Sequence[Mapping[str, Any]],
    price_rows: Sequence[Mapping[str, Any]],
    *,
    checked_at: str,
    price_as_of: str,
    peer_snapshot_file_sha256: str,
    price_file_sha256: str,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Evaluate one hash-valid L7 capture without mutating claims or registries."""
    spec = (
        dict(contract)
        if contract is not None
        else load_outcome_maturation_contract()
    )
    verify_capture_report(capture_report)
    capture_hash = _sha256_text(
        capture_report.get("capture_hash"),
        "capture_hash",
    )
    checked = _timestamp(checked_at, "checked_at")
    peer_snapshot, peer_binding = _normalize_peer_snapshot(
        peer_snapshot_rows,
        capture_report=capture_report,
        peer_snapshot_file_sha256=peer_snapshot_file_sha256,
        contract=spec,
    )
    price_groups, price_binding = _normalize_prices(
        price_rows,
        price_as_of=price_as_of,
        checked_at=checked,
        price_file_sha256=price_file_sha256,
    )

    evaluations = [
        evaluate_claim_maturation(
            claim,
            peer_snapshot=peer_snapshot,
            peer_snapshot_binding=peer_binding,
            price_groups=price_groups,
            contract=spec,
        )
        for claim in capture_report["claims"]
    ]
    evaluations.sort(
        key=lambda item: (
            int(item["horizon_sessions"]),
            str(item["claim_id"]),
        )
    )
    outcomes = [
        item["matured_outcome"]
        for item in evaluations
        if item["status"] == "MATURED"
    ]
    statuses = Counter(str(item["status"]) for item in evaluations)

    identity = {
        "l8_contract_hash": outcome_maturation_contract_hash(spec),
        "capture_hash": capture_hash,
        "peer_snapshot_binding_hash": peer_binding[
            "peer_snapshot_binding_hash"
        ],
        "price_binding_hash": price_binding["price_binding_hash"],
        "checked_at": checked,
    }
    check_id = f"PMCHK-{_hash(identity)[:24].upper()}"
    report: dict[str, Any] = {
        "schema_version": CHECK_REPORT_SCHEMA_VERSION,
        "module": "pattern_discovery_lab",
        "phase": "L8",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "check_id": check_id,
        "checked_at": checked,
        "l8_contract_hash": identity["l8_contract_hash"],
        "l7_capture_id": capture_report["capture_id"],
        "l7_capture_hash": capture_hash,
        "peer_snapshot_binding": peer_binding,
        "price_binding": price_binding,
        "counts": {
            "claim_count": len(evaluations),
            "matured_count": len(outcomes),
            "non_matured_count": len(evaluations) - len(outcomes),
            "status_counts": dict(sorted(statuses.items())),
        },
        "evaluations": evaluations,
        "matured_outcomes": outcomes,
        "boundaries": {
            "l7_claim_mutation_performed": False,
            "confirmation_evaluation_performed": False,
            "sequential_monitoring_performed": False,
            "rating_assigned": False,
            "promotion_performed": False,
            "decision_layer_integration_performed": False,
        },
    }
    PatternDiscoveryBoundary().assert_research_payload(report)
    report["check_hash"] = _hash(report)
    return report


def verify_maturation_check(
    report: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = (
        dict(contract)
        if contract is not None
        else load_outcome_maturation_contract()
    )
    if not isinstance(report, Mapping):
        raise OutcomeMaturationError("maturation_check_must_be_object")
    if report.get("schema_version") != CHECK_REPORT_SCHEMA_VERSION:
        raise OutcomeMaturationError("maturation_check_schema_invalid")
    if report.get("research_only") is not True:
        raise OutcomeMaturationError("maturation_check_research_only_guard_missing")
    if report.get("productive_integration_enabled") is not False:
        raise OutcomeMaturationError(
            "maturation_check_productive_integration_forbidden"
        )
    if report.get("execution_allowed") is not False:
        raise OutcomeMaturationError("maturation_check_execution_forbidden")
    if report.get("l8_contract_hash") != outcome_maturation_contract_hash(spec):
        raise OutcomeMaturationError("maturation_check_contract_hash_mismatch")

    stored = _sha256_text(report.get("check_hash"), "check_hash")
    body = dict(report)
    body.pop("check_hash", None)
    if _hash(body) != stored:
        raise OutcomeMaturationError("maturation_check_hash_mismatch")

    evaluations = report.get("evaluations")
    outcomes = report.get("matured_outcomes")
    if not isinstance(evaluations, Sequence) or isinstance(
        evaluations, (str, bytes, bytearray)
    ):
        raise OutcomeMaturationError("maturation_check_evaluations_invalid")
    if not isinstance(outcomes, Sequence) or isinstance(
        outcomes, (str, bytes, bytearray)
    ):
        raise OutcomeMaturationError("maturation_check_outcomes_invalid")
    for record in outcomes:
        verify_matured_outcome(record, contract=spec)
    if report.get("counts", {}).get("claim_count") != len(evaluations):
        raise OutcomeMaturationError("maturation_check_claim_count_mismatch")
    if report.get("counts", {}).get("matured_count") != len(outcomes):
        raise OutcomeMaturationError("maturation_check_matured_count_mismatch")
    boundaries = report.get("boundaries")
    if not isinstance(boundaries, Mapping):
        raise OutcomeMaturationError("maturation_check_boundaries_missing")
    for field in (
        "l7_claim_mutation_performed",
        "confirmation_evaluation_performed",
        "sequential_monitoring_performed",
        "rating_assigned",
        "promotion_performed",
        "decision_layer_integration_performed",
    ):
        if boundaries.get(field) is not False:
            raise OutcomeMaturationError(
                f"maturation_check_boundary_invalid:{field}"
            )
    PatternDiscoveryBoundary().assert_research_payload(report)
    return {
        "valid": True,
        "check_id": report.get("check_id"),
        "check_hash": stored,
        "claim_count": len(evaluations),
        "matured_count": len(outcomes),
    }


class MaturedOutcomeRegistry:
    """Append-only hash chain with exactly one immutable outcome per L7 claim."""

    def __init__(
        self,
        path: str | Path,
        *,
        contract: Mapping[str, Any] | None = None,
    ) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.contract = (
            dict(contract)
            if contract is not None
            else load_outcome_maturation_contract()
        )

    def _read_events(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        events: list[dict[str, Any]] = []
        for line_number, line in enumerate(
            self.path.read_text(encoding="utf-8").splitlines(),
            start=1,
        ):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise OutcomeMaturationError(
                    f"maturation_registry_invalid_json:{line_number}"
                ) from exc
            if not isinstance(event, dict):
                raise OutcomeMaturationError(
                    f"maturation_registry_event_not_object:{line_number}"
                )
            events.append(event)
        return events

    def _verify_hash_chain(
        self,
        events: Sequence[Mapping[str, Any]],
    ) -> None:
        previous: str | None = None
        for expected_sequence, event in enumerate(events, start=1):
            if event.get("schema_version") != EVENT_SCHEMA_VERSION:
                raise OutcomeMaturationError(
                    f"maturation_registry_schema_invalid:{expected_sequence}"
                )
            if event.get("sequence") != expected_sequence:
                raise OutcomeMaturationError(
                    f"maturation_registry_sequence_invalid:{expected_sequence}"
                )
            if event.get("previous_event_hash") != previous:
                raise OutcomeMaturationError(
                    f"maturation_registry_previous_hash_invalid:{expected_sequence}"
                )
            stored = _sha256_text(
                event.get("entry_hash"),
                f"maturation_registry.entry_hash.{expected_sequence}",
            )
            body = dict(event)
            body.pop("entry_hash", None)
            if _hash(body) != stored:
                raise OutcomeMaturationError(
                    f"maturation_registry_entry_hash_invalid:{expected_sequence}"
                )
            previous = stored

    def _replay(
        self,
        events: Sequence[Mapping[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        records: dict[str, dict[str, Any]] = {}
        for event in events:
            if event.get("event_type") != "OUTCOME_MATURED":
                raise OutcomeMaturationError(
                    f"maturation_registry_event_type_unknown:{event.get('event_type')}"
                )
            record = event.get("record")
            if not isinstance(record, Mapping):
                raise OutcomeMaturationError("maturation_registry_record_missing")
            verify_matured_outcome(record, contract=self.contract)
            claim_id = _text(
                record.get("claim", {}).get("claim_id"),
                "record.claim.claim_id",
            )
            if claim_id in records:
                raise OutcomeMaturationError(
                    f"maturation_duplicate_claim_in_registry:{claim_id}"
                )
            records[claim_id] = dict(record)
        return records

    def _load(
        self,
    ) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        events = self._read_events()
        self._verify_hash_chain(events)
        return events, self._replay(events)

    def get_outcome(self, claim_id: str) -> dict[str, Any]:
        _, records = self._load()
        key = _text(claim_id, "claim_id")
        if key not in records:
            raise OutcomeMaturationError(f"matured_outcome_not_found:{key}")
        return dict(records[key])

    def register_outcomes(
        self,
        records: Sequence[Mapping[str, Any]],
        *,
        recorded_at: str,
        actor_id: str,
        actor_role: str,
    ) -> dict[str, Any]:
        if isinstance(records, (str, bytes, bytearray)) or not isinstance(
            records, Sequence
        ):
            raise OutcomeMaturationError("matured_outcomes_sequence_required")
        timestamp = _timestamp(recorded_at, "recorded_at")
        actor = _text(actor_id, "actor_id")
        role = _text(actor_role, "actor_role")
        normalized = [dict(record) for record in records]
        for record in normalized:
            verify_matured_outcome(record, contract=self.contract)
        claim_ids = [
            str(record["claim"]["claim_id"])
            for record in normalized
        ]
        if len(claim_ids) != len(set(claim_ids)):
            raise OutcomeMaturationError("duplicate_claim_id_in_maturation_batch")

        self.path.parent.mkdir(parents=True, exist_ok=True)
        lock_fd: int | None = None
        try:
            try:
                lock_fd = os.open(
                    self.lock_path,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o644,
                )
            except FileExistsError as exc:
                raise OutcomeMaturationError(
                    "maturation_registry_lock_already_held"
                ) from exc

            events, existing = self._load()
            new_records: list[dict[str, Any]] = []
            idempotent_count = 0
            for record in normalized:
                claim_id = str(record["claim"]["claim_id"])
                if claim_id not in existing:
                    new_records.append(record)
                    continue
                if existing[claim_id] == record:
                    idempotent_count += 1
                    continue
                raise OutcomeMaturationError(
                    f"matured_outcome_claim_collision:{claim_id}"
                )

            staged = [dict(event) for event in events]
            previous = staged[-1]["entry_hash"] if staged else None
            for record in new_records:
                event: dict[str, Any] = {
                    "schema_version": EVENT_SCHEMA_VERSION,
                    "sequence": len(staged) + 1,
                    "event_id": (
                        "PME-"
                        + _hash(
                            {
                                "claim_id": record["claim"]["claim_id"],
                                "outcome_hash": record["outcome_hash"],
                            }
                        )[:24].upper()
                    ),
                    "event_type": "OUTCOME_MATURED",
                    "recorded_at": timestamp,
                    "actor_id": actor,
                    "actor_role": role,
                    "record": record,
                    "previous_event_hash": previous,
                }
                event["entry_hash"] = _hash(event)
                staged.append(event)
                previous = event["entry_hash"]

            self._verify_hash_chain(staged)
            self._replay(staged)

            if new_records:
                payload = "".join(
                    _canonical_json(event) + "\n"
                    for event in staged[len(events) :]
                ).encode("utf-8")
                fd = os.open(
                    self.path,
                    os.O_CREAT | os.O_APPEND | os.O_WRONLY,
                    0o644,
                )
                try:
                    view = memoryview(payload)
                    while view:
                        written = os.write(fd, view)
                        if written <= 0:
                            raise OutcomeMaturationError(
                                "maturation_registry_short_write"
                            )
                        view = view[written:]
                    os.fsync(fd)
                finally:
                    os.close(fd)
            status = self.verify_integrity()
            return {
                "valid": True,
                "requested_outcome_count": len(normalized),
                "appended_outcome_count": len(new_records),
                "idempotent_outcome_count": idempotent_count,
                **status,
            }
        finally:
            if lock_fd is not None:
                os.close(lock_fd)
                try:
                    self.lock_path.unlink()
                except FileNotFoundError:
                    pass

    def verify_integrity(self) -> dict[str, Any]:
        events, records = self._load()
        return {
            "registry_event_count": len(events),
            "registry_outcome_count": len(records),
            "registry_head_hash": (
                events[-1]["entry_hash"] if events else None
            ),
        }


def maturation_registry_repo_path(
    *,
    contract: Mapping[str, Any] | None = None,
) -> str:
    spec = (
        dict(contract)
        if contract is not None
        else load_outcome_maturation_contract()
    )
    return str(spec["storage"]["registry_path"])


def maturation_check_repo_path(
    capture_id: str,
    check_id: str,
    *,
    contract: Mapping[str, Any] | None = None,
) -> str:
    spec = (
        dict(contract)
        if contract is not None
        else load_outcome_maturation_contract()
    )
    return str(spec["storage"]["check_report_path_template"]).format(
        capture_id=_safe_token(capture_id, "capture_id"),
        check_id=_safe_token(check_id, "check_id"),
    )


def persist_outcome_maturation(
    repo_root: str | Path,
    report: Mapping[str, Any],
    *,
    actor_id: str,
    actor_role: str,
    boundary: PatternDiscoveryBoundary | None = None,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Persist matured outcomes append-only and write one immutable check report."""
    spec = (
        dict(contract)
        if contract is not None
        else load_outcome_maturation_contract()
    )
    verify_maturation_check(report, contract=spec)
    guard = boundary or PatternDiscoveryBoundary()
    root = Path(repo_root).resolve()

    registry_repo_path = maturation_registry_repo_path(contract=spec)
    report_repo_path = maturation_check_repo_path(
        str(report["l7_capture_id"]),
        str(report["check_id"]),
        contract=spec,
    )
    guard.assert_write_path_allowed(registry_repo_path)
    guard.assert_write_path_allowed(report_repo_path)

    registry_path = (root / registry_repo_path).resolve()
    report_path = (root / report_repo_path).resolve()
    try:
        registry_path.relative_to(root)
        report_path.relative_to(root)
    except ValueError as exc:
        raise OutcomeMaturationError("maturation_path_outside_repo") from exc

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_already_exists = report_path.exists()
    if report_already_exists:
        try:
            existing_report = json.loads(
                report_path.read_text(encoding="utf-8")
            )
        except (OSError, json.JSONDecodeError) as exc:
            raise OutcomeMaturationError(
                "existing_maturation_check_unreadable"
            ) from exc
        verify_maturation_check(existing_report, contract=spec)
        if existing_report != dict(report):
            raise OutcomeMaturationError(
                f"maturation_check_identity_collision:{report_repo_path}"
            )

    registry = MaturedOutcomeRegistry(registry_path, contract=spec)
    registry_status = registry.register_outcomes(
        report["matured_outcomes"],
        recorded_at=str(report["checked_at"]),
        actor_id=actor_id,
        actor_role=actor_role,
    )

    if not report_already_exists:
        with report_path.open(
            "x",
            encoding="utf-8",
            newline="\n",
        ) as handle:
            json.dump(
                dict(report),
                handle,
                indent=2,
                sort_keys=True,
                ensure_ascii=True,
                allow_nan=False,
            )
            handle.write("\n")

    return {
        "valid": True,
        "check_id": report["check_id"],
        "check_hash": report["check_hash"],
        "claim_count": report["counts"]["claim_count"],
        "matured_count": report["counts"]["matured_count"],
        "registry": registry_status,
        "registry_path": str(registry_path),
        "check_report_path": str(report_path),
    }
