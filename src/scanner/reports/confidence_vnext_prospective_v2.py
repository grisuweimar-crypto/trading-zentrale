from __future__ import annotations

"""Phase 5B prospective evidence contract.

This module starts a new prospective v2 stream. It never rewrites or backfills
Phase-4E shadow-v1. V2 integrity-binds claim-time Statistical Context and the
exact market-session path used to mature each forward outcome.

No adaptive weights, scalar Confidence score, production thresholds, portfolio
logic, or trading actions are created here.
"""

from hashlib import sha256
from pathlib import Path
import json
import re

import numpy as np
import pandas as pd

from scanner.reports.confidence_vnext_prospective import evidence_fingerprints
from scanner.reports.confidence_vnext_research import _atom_prerequisites, _number, _present
from scanner.reports.selection_timing import (
    HORIZONS,
    _norm_currency,
    _peer_median,
    _peer_medians,
    _price_rows,
    _scanner_rows,
)
from scanner.reports.timing_patterns import Phase1BConfig, feature_rows


SCHEMA_VERSION = "phase5b_shadow_v2"
FIXED_EVENT_SPACING_SESSIONS = 5
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

CLAIM_COLUMNS_V2 = (
    "claim_id", "schema_version", "source_commit", "as_of", "generated_at", "run_id",
    "snapshot_id", "symbol", "currency", "horizon_sessions", "start_market_date",
    "start_adjusted_close", "outcome_eligibility", "outcome_unavailable_reason",
    "cooldown_forbidden_start_dates", "cooldown_context_complete", "evidence_version",
    "evidence_fingerprint", "phase4_report_sha256", "phase2_sha256", "phase3_sha256",
    "risk_scale_sha256", "phase2_source_as_of", "phase3_source_as_of", "selection_band",
    "selection_statistical_state", "selection_statistical_direction",
    "selection_statistical_context", "timing_statistical_context", "timing_matched_states",
    "timing_unevaluable_state_counts", "risk_statistical_context", "risk_evidence_states",
    "statistical_context_sha256", "timing_model_state", "timing_model_direction",
    "risk_model_state", "agreement_state", "agreement_conflicts", "return_claim_direction",
    "dq_selection_state", "dq_timing_state", "dq_risk_state", "volatility_application_status",
)

OUTCOME_COLUMNS_V2 = (
    "outcome_id", "claim_id", "schema_version", "source_commit", "as_of", "evaluated_at",
    "symbol", "currency", "horizon_sessions", "start_market_date", "end_market_date",
    "elapsed_market_sessions", "path_session_count", "session_dates_sha256", "path_sha256",
    "start_adjusted_close", "end_adjusted_close", "return", "adverse_excursion",
    "path_max_drawdown",
)

DERIVED_COLUMNS_V2 = (
    "claim_id", "snapshot_id", "as_of", "symbol", "currency", "horizon_sessions",
    "start_market_date", "return", "adverse_excursion", "path_max_drawdown",
    "peer_median_return", "peer_excess", "return_claim_direction", "signed_peer_excess",
    "direction_hit", "selection_statistical_state", "timing_matched_states",
    "risk_evidence_states", "agreement_state", "timing_model_state", "risk_model_state",
    "dq_selection_state", "dq_timing_state", "dq_risk_state",
    "cooldown_forbidden_start_dates", "cooldown_context_complete",
)

_RECORD_INT_FIELDS = {"horizon_sessions", "elapsed_market_sessions", "path_session_count"}
_RECORD_FLOAT_FIELDS = {
    "start_adjusted_close", "end_adjusted_close", "return",
    "adverse_excursion", "path_max_drawdown",
}
_RECORD_BOOL_FIELDS = {"cooldown_context_complete"}


def _is_missing_scalar(value: object) -> bool:
    if value is None or value is pd.NA:
        return True
    if isinstance(value, (float, np.floating)) and np.isnan(float(value)):
        return True
    return isinstance(value, str) and value.strip().lower() in {"", "nan", "none", "null", "<na>"}


def _canonical_record_scalar(key: str, value: object) -> object:
    """Normalize CSV-sensitive scalar types before hashing v2 flat records."""

    if key in _RECORD_BOOL_FIELDS:
        if _is_missing_scalar(value):
            return ""
        if isinstance(value, (bool, np.bool_)):
            return bool(value)
        text = str(value).strip().lower()
        if text in {"true", "1", "yes"}:
            return True
        if text in {"false", "0", "no"}:
            return False
        raise ValueError(f"{key} must be boolean")

    if key in _RECORD_INT_FIELDS:
        if _is_missing_scalar(value):
            return ""
        try:
            number = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{key} must be integer") from exc
        if not np.isfinite(number) or not number.is_integer():
            raise ValueError(f"{key} must be integer")
        return int(number)

    if key in _RECORD_FLOAT_FIELDS:
        if _is_missing_scalar(value):
            return ""
        try:
            number = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{key} must be numeric") from exc
        if not np.isfinite(number):
            raise ValueError(f"{key} must be finite")
        return number

    return "" if _is_missing_scalar(value) else str(value)


def _json_ready(value: object) -> object:
    if isinstance(value, dict):
        if value.get("schema_version") == SCHEMA_VERSION:
            return {str(key): _canonical_record_scalar(str(key), item) for key, item in value.items()}
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if value is pd.NA or value is None:
        return None
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        number = float(value)
        if not np.isfinite(number):
            raise ValueError("non-finite values cannot be hashed")
        return number
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def _canonical_json(value: object) -> str:
    return json.dumps(
        _json_ready(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _sha_payload(payload: object) -> str:
    return sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _required_text(value: object, label: str) -> str:
    text = "" if value is None else str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "<na>"}:
        raise ValueError(f"{label} is required")
    return text


def _required_utc(value: object, label: str) -> pd.Timestamp:
    text = _required_text(value, label)
    try:
        parsed = pd.to_datetime(text, errors="raise", utc=True)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a parseable timestamp") from exc
    if pd.isna(parsed):
        raise ValueError(f"{label} must be a parseable timestamp")
    return pd.Timestamp(parsed)


def _read_csv(path: str | Path, columns: tuple[str, ...]) -> pd.DataFrame:
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame(columns=columns)
    frame = pd.read_csv(p, dtype=str, keep_default_na=False)
    if list(frame.columns) != list(columns):
        raise ValueError(f"unexpected Phase 5B v2 schema in {p}")
    return frame


def _write_csv(frame: pd.DataFrame, path: str | Path, columns: tuple[str, ...]) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.loc[:, columns].to_csv(output, index=False, lineterminator="\n")


def _latest_stock_context(
    history: pd.DataFrame,
    latest: pd.DataFrame,
    latest_date: pd.Timestamp,
) -> pd.DataFrame:
    scanner = _scanner_rows(latest)
    current = scanner.loc[
        pd.to_datetime(scanner["date"], errors="coerce").dt.normalize().eq(latest_date.normalize())
        & ~scanner["is_crypto"]
    ].copy()
    if current.empty:
        raise ValueError("no current stock scanner rows")

    features, _ = feature_rows(history, Phase1BConfig(stable_start="2026-04-15"))
    if features.empty:
        return current

    feature_current = features.loc[
        pd.to_datetime(features["date"], errors="coerce").dt.normalize().eq(latest_date.normalize())
    ].copy()
    feature_columns = [
        column for column in feature_current.columns
        if column not in current.columns or column in {"date", "symbol"}
    ]
    return current.merge(
        feature_current[feature_columns],
        on=["date", "symbol"],
        how="left",
        validate="one_to_one",
        suffixes=("", "_feature"),
    )


def _claim_start_context(
    prices: pd.DataFrame,
    symbol: str,
    as_of: str,
) -> tuple[str, float | None, str, str, str, bool]:
    price = _price_rows(prices)
    group = price.loc[price["symbol"].astype(str).eq(symbol)].copy()
    if group.empty:
        return "", None, "unevaluable", "claim_time_price_history_missing", "[]", False

    group["_date"] = pd.to_datetime(group["date"], errors="coerce")
    group["_adj"] = pd.to_numeric(group["adj_close"], errors="coerce")
    group = group.loc[group["_date"].notna()].sort_values("_date", kind="mergesort").reset_index(drop=True)
    obs_date = pd.Timestamp(as_of).normalize()
    eligible = group.loc[group["_date"].dt.normalize().le(obs_date)]
    if eligible.empty:
        return "", None, "unevaluable", "claim_time_start_session_missing", "[]", False

    pos = int(eligible.index[-1])
    row = group.iloc[pos]
    start_date = pd.Timestamp(row["_date"]).normalize()
    if (obs_date - start_date).days > 7:
        return "", None, "unevaluable", "claim_time_start_session_stale", "[]", False

    try:
        start_value = float(row["_adj"])
    except (TypeError, ValueError):
        return "", None, "unevaluable", "claim_time_adjusted_close_invalid", "[]", False
    if not np.isfinite(start_value) or start_value <= 0:
        return "", None, "unevaluable", "claim_time_adjusted_close_invalid", "[]", False

    recent = [
        pd.Timestamp(value).normalize().date().isoformat()
        for value in group.iloc[
            max(0, pos - (FIXED_EVENT_SPACING_SESSIONS - 1)) : pos + 1
        ]["_date"].tolist()
    ]
    cooldown_context_complete = len(recent) == FIXED_EVENT_SPACING_SESSIONS
    return (
        start_date.date().isoformat(),
        start_value,
        "eligible",
        "",
        _canonical_json(recent),
        cooldown_context_complete,
    )


def _compact_evidence(evidence: dict[str, object]) -> dict[str, object]:
    keep = (
        "state", "direction", "reason", "pattern", "conditions", "feature", "N", "days",
        "symbols", "top_symbol_share", "support_regions", "mean_peer_excess",
        "probability_advantage_vs_baseline", "alpha_interval_95",
        "probability_advantage_interval_95", "low_cutoff", "high_cutoff",
        "adverse_gap_high_minus_low", "path_drawdown_gap_high_minus_low",
        "adverse_gap_interval_95", "path_drawdown_gap_interval_95",
    )
    return {key: evidence.get(key) for key in keep if key in evidence}


def _timing_context(
    feature_row: pd.Series,
    registry: dict[str, dict[str, object]],
) -> tuple[list[dict[str, object]], dict[str, int]]:
    matched: list[dict[str, object]] = []
    unevaluable_counts: dict[str, int] = {}
    for pattern_name, raw_evidence in sorted(registry.items()):
        evidence = dict(raw_evidence or {})
        state = str(evidence.get("state") or "unavailable")
        conditions = [str(value) for value in (evidence.get("conditions") or [])]
        prerequisites = sorted({
            item
            for condition in conditions
            for item in _atom_prerequisites(condition)
        })
        missing = [
            item for item in prerequisites
            if item not in feature_row.index or not _present(feature_row.get(item))
        ]
        if missing:
            unevaluable_counts[state] = int(unevaluable_counts.get(state, 0) + 1)
            continue
        if conditions and all(bool(feature_row.get(condition, False)) for condition in conditions):
            item = _compact_evidence(evidence)
            item["pattern"] = pattern_name
            matched.append(item)
    return matched, dict(sorted(unevaluable_counts.items()))


def _risk_context(
    feature_row: pd.Series,
    registry: dict[str, dict[str, object]],
) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for feature, raw_evidence in sorted(registry.items()):
        evidence = dict(raw_evidence or {})
        value = _number(feature_row.get(feature))
        low = _number(evidence.get("low_cutoff"))
        high = _number(evidence.get("high_cutoff"))
        if value is None:
            level = "unavailable"
        elif low is None or high is None:
            level = "uncalibrated"
        elif value >= high:
            level = "high"
        elif value <= low:
            level = "low"
        else:
            level = "middle"
        item = _compact_evidence(evidence)
        item.update({"feature": feature, "claim_value": value, "claim_level": level})
        result.append(item)
    return result


def _return_claim_direction(row: dict[str, object]) -> str:
    directions: list[str] = []
    selection = row.get("selection") or {}
    timing = row.get("timing") or {}
    if (
        isinstance(selection, dict)
        and selection.get("state") == "robust"
        and selection.get("direction") in {"positive", "negative"}
    ):
        directions.append(str(selection["direction"]))
    if (
        isinstance(timing, dict)
        and timing.get("state") == "robust_claim"
        and timing.get("direction") in {"positive", "negative"}
    ):
        directions.append(str(timing["direction"]))
    unique = set(directions)
    return next(iter(unique)) if len(unique) == 1 else ""


def build_claim_rows_v2(
    phase4_report: dict,
    history: pd.DataFrame,
    latest: pd.DataFrame,
    metadata: dict,
    fingerprints: dict[str, str],
    claim_prices: pd.DataFrame,
    *,
    source_commit: str,
) -> pd.DataFrame:
    if metadata.get("latest_run_complete") is not True:
        raise ValueError("v2 claims require a complete scanner publication")
    daily = metadata.get("daily_run") or {}
    if daily.get("scanner_status") != "success":
        raise ValueError("v2 claims require scanner_status=success")

    run_id = _required_text(daily.get("run_id"), "run_id")
    snapshot_id = _required_text(metadata.get("snapshot_id"), "snapshot_id")
    generated_at = _required_text(metadata.get("generated_at"), "generated_at")
    as_of = _required_text(metadata.get("as_of"), "metadata as_of")
    source_commit = _required_text(source_commit, "source_commit")

    current = phase4_report.get("current") or {}
    if _required_text(current.get("as_of"), "Phase 4 current as_of") != as_of:
        raise ValueError("Phase 4 current as_of does not match metadata as_of")
    if current.get("asset_scope") != "stocks_only":
        raise ValueError("Phase 5B v2 accepts stock-only Phase 4 evidence")

    registry = current.get("statistical_registry") or {}
    horizon_registry = registry.get("horizons") or {}
    if not horizon_registry:
        raise ValueError("Phase 4 statistical registry is missing")

    latest_date = pd.Timestamp(as_of).normalize()
    feature_frame = _latest_stock_context(history, latest, latest_date)
    feature_by_symbol = {
        str(row["symbol"]): pd.Series(row)
        for row in feature_frame.to_dict("records")
    }
    currency_by_symbol = {
        str(row["symbol"]): (_norm_currency(row.get("currency")) or "")
        for row in feature_frame.to_dict("records")
    }
    starts = {
        symbol: _claim_start_context(claim_prices, symbol, as_of)
        for symbol in feature_by_symbol
    }

    pit = current.get("pit_source_checks") or {}
    phase2_source_as_of = _required_text(
        (pit.get("phase2") or {}).get("source_as_of"),
        "phase2 source_as_of",
    )
    phase3_source_as_of = _required_text(
        (pit.get("phase3") or {}).get("source_as_of"),
        "phase3 source_as_of",
    )
    volatility_status = _required_text(
        ((current.get("risk_metric_applicability") or {}).get("volatility") or {}).get("status"),
        "volatility application status",
    )
    evidence_version = _required_text(
        (phase4_report.get("config") or {}).get("evidence_version"),
        "evidence_version",
    )

    rows: list[dict[str, object]] = []
    for row in current.get("rows") or []:
        symbol = _required_text(row.get("symbol"), "symbol")
        if symbol not in feature_by_symbol:
            raise ValueError(f"Phase 4 row missing from current feature context: {symbol}")

        horizon = int(row.get("horizon_sessions"))
        if horizon not in HORIZONS:
            raise ValueError(f"unsupported horizon: {horizon}")
        h_registry = horizon_registry.get(str(horizon)) or {}
        feature_row = feature_by_symbol[symbol]

        selection = dict(row.get("selection") or {})
        timing_model = dict(row.get("timing") or {})
        risk_model = dict(row.get("risk") or {})
        agreement = dict(row.get("model_agreement") or {})
        dq = dict(row.get("data_quality") or {})

        timing_context, timing_unevaluable = _timing_context(
            feature_row,
            h_registry.get("timing") or {},
        )
        risk_context = _risk_context(
            feature_row,
            h_registry.get("risk") or {},
        )
        selection_context = _compact_evidence(selection)
        statistical_context = {
            "selection": selection_context,
            "timing_matched": timing_context,
            "timing_unevaluable_state_counts": timing_unevaluable,
            "risk": risk_context,
            "probability_role": h_registry.get("probability_role"),
            "regime_role": h_registry.get("regime_role"),
        }
        timing_states = sorted({
            str(item.get("state") or "unavailable")
            for item in timing_context
        })
        risk_states = sorted(
            f"{item.get('feature')}:{item.get('state') or 'unavailable'}:{item.get('claim_level') or 'unavailable'}"
            for item in risk_context
        )
        (
            start_date,
            start_value,
            eligibility,
            unavailable_reason,
            cooldown_dates,
            cooldown_complete,
        ) = starts[symbol]

        payload: dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "source_commit": source_commit,
            "as_of": as_of,
            "generated_at": generated_at,
            "run_id": run_id,
            "snapshot_id": snapshot_id,
            "symbol": symbol,
            "currency": currency_by_symbol[symbol],
            "horizon_sessions": horizon,
            "start_market_date": start_date,
            "start_adjusted_close": start_value,
            "outcome_eligibility": eligibility,
            "outcome_unavailable_reason": unavailable_reason,
            "cooldown_forbidden_start_dates": cooldown_dates,
            "cooldown_context_complete": bool(cooldown_complete),
            "evidence_version": evidence_version,
            **fingerprints,
            "phase2_source_as_of": phase2_source_as_of,
            "phase3_source_as_of": phase3_source_as_of,
            "selection_band": str(row.get("selection_band") or ""),
            "selection_statistical_state": str(selection.get("state") or "unavailable"),
            "selection_statistical_direction": str(selection.get("direction") or ""),
            "selection_statistical_context": _canonical_json(selection_context),
            "timing_statistical_context": _canonical_json(timing_context),
            "timing_matched_states": "|".join(timing_states),
            "timing_unevaluable_state_counts": _canonical_json(timing_unevaluable),
            "risk_statistical_context": _canonical_json(risk_context),
            "risk_evidence_states": "|".join(risk_states),
            "statistical_context_sha256": _sha_payload(statistical_context),
            "timing_model_state": str(timing_model.get("state") or "unknown"),
            "timing_model_direction": str(timing_model.get("direction") or ""),
            "risk_model_state": str(risk_model.get("state") or "unknown"),
            "agreement_state": str(agreement.get("state") or "insufficient_evidence"),
            "agreement_conflicts": "|".join(
                sorted(str(value) for value in (agreement.get("conflicts") or []))
            ),
            "return_claim_direction": _return_claim_direction(row),
            "dq_selection_state": str(
                ((dq.get("selection") or {}).get("state")) or "proxy_insufficient"
            ),
            "dq_timing_state": str(
                ((dq.get("timing") or {}).get("state")) or "proxy_insufficient"
            ),
            "dq_risk_state": str(
                ((dq.get("risk") or {}).get("state")) or "proxy_insufficient"
            ),
            "volatility_application_status": volatility_status,
        }
        payload["claim_id"] = _sha_payload(payload)
        rows.append(payload)

    if not rows:
        return pd.DataFrame(columns=CLAIM_COLUMNS_V2)
    return (
        pd.DataFrame(rows, columns=CLAIM_COLUMNS_V2)
        .sort_values(["symbol", "horizon_sessions"], kind="mergesort")
        .reset_index(drop=True)
    )


def append_claims_v2(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        return new.copy().reset_index(drop=True)
    if new.empty:
        return existing.copy().reset_index(drop=True)

    old = existing.copy()
    fresh = new.copy()
    old_key = (
        old["snapshot_id"].astype(str) + "|"
        + old["symbol"].astype(str) + "|"
        + old["horizon_sessions"].astype(str)
    )
    fresh_key = (
        fresh["snapshot_id"].astype(str) + "|"
        + fresh["symbol"].astype(str) + "|"
        + fresh["horizon_sessions"].astype(str)
    )
    old_by_key = dict(zip(old_key, old["claim_id"].astype(str)))
    keep: list[int] = []
    for idx, key in zip(fresh.index, fresh_key):
        previous = old_by_key.get(str(key))
        current = str(fresh.at[idx, "claim_id"])
        if previous is None:
            keep.append(idx)
        elif previous != current:
            raise ValueError(f"immutable v2 claim conflict for {key}")
    return pd.concat([old, fresh.loc[keep]], ignore_index=True).loc[:, CLAIM_COLUMNS_V2]


def _price_groups(prices: pd.DataFrame) -> dict[str, pd.DataFrame]:
    price = _price_rows(prices)
    return {
        str(symbol): group.sort_values("date", kind="mergesort").reset_index(drop=True)
        for symbol, group in price.groupby("symbol", sort=False)
    }


def _mature_one_v2(
    claim: dict[str, object],
    group: pd.DataFrame,
    evaluated_at: str,
) -> dict[str, object] | None:
    if str(claim.get("outcome_eligibility") or "") != "eligible":
        return None

    evaluated = _required_utc(evaluated_at, "evaluated_at")
    start_date = pd.Timestamp(
        _required_text(claim.get("start_market_date"), "start_market_date")
    ).normalize()

    dates = pd.to_datetime(group["date"], errors="coerce")
    matches = np.flatnonzero(dates.dt.normalize().eq(start_date).to_numpy())
    if len(matches) != 1:
        return None

    pos = int(matches[0])
    horizon = int(claim["horizon_sessions"])
    target = pos + horizon
    if horizon not in HORIZONS or target >= len(group):
        return None

    target_date = pd.Timestamp(dates.iloc[target]).normalize()
    target_utc = (
        target_date.tz_localize("UTC")
        if target_date.tzinfo is None
        else target_date.tz_convert("UTC")
    )
    if target_utc >= evaluated.normalize():
        return None

    path_frame = group.iloc[pos : target + 1].copy()
    path_dates = pd.to_datetime(path_frame["date"], errors="coerce")
    path = pd.to_numeric(path_frame["adj_close"], errors="coerce").to_numpy(dtype=float)
    if (
        len(path) != horizon + 1
        or path_dates.isna().any()
        or not np.isfinite(path).all()
        or (path <= 0).any()
    ):
        return None

    date_strings = [
        pd.Timestamp(value).normalize().date().isoformat()
        for value in path_dates
    ]
    if len(set(date_strings)) != len(date_strings):
        return None

    path_payload = [
        {"date": day, "adjusted_close": repr(float(value))}
        for day, value in zip(date_strings, path)
    ]
    entry_returns = path / float(path[0]) - 1.0
    peaks = np.maximum.accumulate(path)
    drawdowns = path / peaks - 1.0

    payload: dict[str, object] = {
        "claim_id": str(claim["claim_id"]),
        "schema_version": SCHEMA_VERSION,
        "source_commit": str(claim["source_commit"]),
        "as_of": str(claim["as_of"]),
        "evaluated_at": evaluated.isoformat(),
        "symbol": str(claim["symbol"]),
        "currency": str(claim.get("currency") or ""),
        "horizon_sessions": horizon,
        "start_market_date": date_strings[0],
        "end_market_date": date_strings[-1],
        "elapsed_market_sessions": int(len(path) - 1),
        "path_session_count": int(len(path)),
        "session_dates_sha256": _sha_payload(date_strings),
        "path_sha256": _sha_payload(path_payload),
        "start_adjusted_close": float(path[0]),
        "end_adjusted_close": float(path[-1]),
        "return": float(path[-1] / path[0] - 1.0),
        "adverse_excursion": max(0.0, -float(np.min(entry_returns))),
        "path_max_drawdown": max(0.0, -float(np.min(drawdowns))),
    }
    if (
        payload["elapsed_market_sessions"] != horizon
        or payload["path_session_count"] != horizon + 1
    ):
        raise AssertionError("v2 outcome session provenance mismatch")
    payload["outcome_id"] = _sha_payload(payload)
    return payload


def compute_mature_outcomes_v2(
    claims: pd.DataFrame,
    prices: pd.DataFrame,
    existing_outcomes: pd.DataFrame,
    evaluated_at: str,
) -> pd.DataFrame:
    _required_utc(evaluated_at, "evaluated_at")
    existing_ids = (
        set(existing_outcomes["claim_id"].astype(str))
        if not existing_outcomes.empty
        else set()
    )
    groups = _price_groups(prices)
    rows: list[dict[str, object]] = []
    for claim in claims.to_dict("records"):
        if str(claim["claim_id"]) in existing_ids:
            continue
        group = groups.get(str(claim["symbol"]))
        if group is None or group.empty:
            continue
        matured = _mature_one_v2(claim, group, evaluated_at)
        if matured is not None:
            rows.append(matured)
    if not rows:
        return pd.DataFrame(columns=OUTCOME_COLUMNS_V2)
    return pd.DataFrame(rows, columns=OUTCOME_COLUMNS_V2)


def append_outcomes_v2(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        return new.copy().reset_index(drop=True)
    if new.empty:
        return existing.copy().reset_index(drop=True)

    previous = dict(
        zip(existing["claim_id"].astype(str), existing["outcome_id"].astype(str))
    )
    keep: list[int] = []
    for idx, row in new.iterrows():
        claim_id = str(row["claim_id"])
        outcome_id = str(row["outcome_id"])
        if claim_id not in previous:
            keep.append(idx)
        elif previous[claim_id] != outcome_id:
            raise ValueError(f"immutable v2 outcome conflict for {claim_id}")
    return pd.concat([existing, new.loc[keep]], ignore_index=True).loc[:, OUTCOME_COLUMNS_V2]


def _valid_sha256(value: object) -> bool:
    return bool(_SHA256_RE.fullmatch(str(value).strip().lower()))


def validate_v2_archives(claims: pd.DataFrame, outcomes: pd.DataFrame) -> None:
    if list(claims.columns) != list(CLAIM_COLUMNS_V2):
        raise ValueError("unexpected v2 claim schema")
    if list(outcomes.columns) != list(OUTCOME_COLUMNS_V2):
        raise ValueError("unexpected v2 outcome schema")

    if not claims.empty:
        if claims["claim_id"].astype(str).duplicated().any():
            raise ValueError("duplicate v2 claim_id")

        for source in claims.to_dict("records"):
            row = dict(source)
            supplied = str(row.pop("claim_id"))
            if row.get("schema_version") != SCHEMA_VERSION or supplied != _sha_payload(row):
                raise ValueError("v2 claim integrity failure")

            horizon = int(_canonical_record_scalar("horizon_sessions", row["horizon_sessions"]))
            if horizon not in HORIZONS:
                raise ValueError("v2 claim horizon failure")
            if not _valid_sha256(supplied):
                raise ValueError("v2 claim id format failure")
            if not _valid_sha256(row.get("statistical_context_sha256")):
                raise ValueError("v2 statistical context hash format failure")

            eligibility = str(row.get("outcome_eligibility") or "")
            if eligibility not in {"eligible", "unevaluable"}:
                raise ValueError("v2 claim eligibility failure")
            as_of = _required_utc(row["as_of"], "claim as_of").normalize()
            generated = _required_utc(row["generated_at"], "claim generated_at")
            if generated.normalize() != as_of:
                raise ValueError("v2 claim chronology failure")
            if eligibility == "eligible":
                start = pd.to_datetime(row.get("start_market_date"), errors="coerce")
                start_value = pd.to_numeric(
                    pd.Series([row.get("start_adjusted_close")]), errors="coerce"
                ).iloc[0]
                if pd.isna(start) or pd.isna(start_value) or not np.isfinite(float(start_value)) or float(start_value) <= 0:
                    raise ValueError("v2 eligible claim start failure")
                if pd.Timestamp(start).normalize() > as_of.tz_localize(None):
                    raise ValueError("v2 eligible claim chronology failure")
            elif not str(row.get("outcome_unavailable_reason") or "").strip():
                raise ValueError("v2 unevaluable claim requires reason")

    claim_by_id = {
        str(row["claim_id"]): row
        for row in claims.to_dict("records")
    }

    if not outcomes.empty:
        if (
            outcomes["claim_id"].astype(str).duplicated().any()
            or outcomes["outcome_id"].astype(str).duplicated().any()
        ):
            raise ValueError("duplicate v2 outcome")

        for source in outcomes.to_dict("records"):
            row = dict(source)
            outcome_id = str(row.pop("outcome_id"))
            if row.get("schema_version") != SCHEMA_VERSION or outcome_id != _sha_payload(row):
                raise ValueError("v2 outcome integrity failure")
            if not _valid_sha256(outcome_id):
                raise ValueError("v2 outcome id format failure")

            claim_id = str(row["claim_id"])
            claim = claim_by_id.get(claim_id)
            if claim is None:
                raise ValueError("v2 outcome without matching claim")

            horizon = int(_canonical_record_scalar("horizon_sessions", row["horizon_sessions"]))
            elapsed = int(_canonical_record_scalar("elapsed_market_sessions", row["elapsed_market_sessions"]))
            path_count = int(_canonical_record_scalar("path_session_count", row["path_session_count"]))
            if horizon not in HORIZONS or elapsed != horizon or path_count != horizon + 1:
                raise ValueError("v2 outcome horizon provenance failure")

            for field in ("source_commit", "as_of", "symbol", "currency", "start_market_date"):
                if str(row.get(field) or "") != str(claim.get(field) or ""):
                    raise ValueError(f"v2 outcome/claim provenance mismatch: {field}")
            if int(float(row["horizon_sessions"])) != int(float(claim["horizon_sessions"])):
                raise ValueError("v2 outcome/claim provenance mismatch: horizon_sessions")

            start = pd.to_datetime(row["start_market_date"], errors="coerce")
            end = pd.to_datetime(row["end_market_date"], errors="coerce")
            evaluated = _required_utc(row["evaluated_at"], "evaluated_at")
            if pd.isna(start) or pd.isna(end) or pd.Timestamp(end).normalize() <= pd.Timestamp(start).normalize():
                raise ValueError("v2 outcome chronology failure")
            end_utc = pd.Timestamp(end).normalize().tz_localize("UTC")
            if evaluated.normalize() <= end_utc:
                raise ValueError("v2 outcome evaluated before completed target day")

            numeric = {
                field: float(_canonical_record_scalar(field, row[field]))
                for field in (
                    "start_adjusted_close", "end_adjusted_close", "return",
                    "adverse_excursion", "path_max_drawdown",
                )
            }
            if numeric["start_adjusted_close"] <= 0 or numeric["end_adjusted_close"] <= 0:
                raise ValueError("v2 outcome price failure")
            expected_return = numeric["end_adjusted_close"] / numeric["start_adjusted_close"] - 1.0
            if not np.isclose(numeric["return"], expected_return, rtol=1e-9, atol=1e-12):
                raise ValueError("v2 outcome return consistency failure")
            adverse = numeric["adverse_excursion"]
            drawdown = numeric["path_max_drawdown"]
            if not (0.0 <= adverse <= 1.0 and 0.0 <= drawdown <= 1.0):
                raise ValueError("v2 outcome path-risk range failure")
            tolerance = 1e-12
            if adverse + tolerance < max(0.0, -numeric["return"]):
                raise ValueError("v2 outcome adverse excursion consistency failure")
            if drawdown + tolerance < adverse:
                raise ValueError("v2 outcome drawdown consistency failure")
            if not _valid_sha256(row.get("session_dates_sha256")) or not _valid_sha256(row.get("path_sha256")):
                raise ValueError("v2 outcome path hash format failure")


def derive_peer_labels_v2(
    claims: pd.DataFrame,
    outcomes: pd.DataFrame,
) -> pd.DataFrame:
    if claims.empty or outcomes.empty:
        return pd.DataFrame(columns=DERIVED_COLUMNS_V2)

    validate_v2_archives(claims, outcomes)
    claim_columns = [
        "claim_id", "snapshot_id", "return_claim_direction",
        "selection_statistical_state", "timing_matched_states",
        "risk_evidence_states", "agreement_state", "timing_model_state",
        "risk_model_state", "dq_selection_state", "dq_timing_state",
        "dq_risk_state", "cooldown_forbidden_start_dates",
        "cooldown_context_complete",
    ]
    work = outcomes.merge(
        claims[claim_columns],
        on="claim_id",
        how="inner",
        validate="one_to_one",
    )
    work["return"] = pd.to_numeric(work["return"], errors="coerce")
    work["peer_median_return"] = np.nan
    work["peer_excess"] = np.nan
    work["signed_peer_excess"] = np.nan
    work["direction_hit"] = np.nan

    horizon_values = pd.to_numeric(work["horizon_sessions"], errors="coerce")
    for snapshot_id in work["snapshot_id"].astype(str).unique():
        snapshot_mask = work["snapshot_id"].astype(str).eq(snapshot_id)
        for horizon in HORIZONS:
            subset = work.loc[snapshot_mask & horizon_values.eq(horizon)].copy()
            if subset.empty:
                continue
            subset["obs_date"] = pd.to_datetime(
                subset["as_of"], errors="coerce"
            ).dt.normalize()
            baselines, fallback = _peer_medians(subset, "return")
            peer = subset.apply(
                lambda row: _peer_median(row, baselines, fallback),
                axis=1,
            )
            work.loc[subset.index, "peer_median_return"] = peer.values
            work.loc[subset.index, "peer_excess"] = (
                subset["return"].to_numpy(dtype=float)
                - peer.to_numpy(dtype=float)
            )

    for idx, row in work.iterrows():
        direction = str(row.get("return_claim_direction") or "")
        peer_excess = row.get("peer_excess")
        if direction in {"positive", "negative"} and pd.notna(peer_excess):
            signed = float(peer_excess) if direction == "positive" else -float(peer_excess)
            work.at[idx, "signed_peer_excess"] = signed
            work.at[idx, "direction_hit"] = int(signed > 0)

    return work.loc[:, DERIVED_COLUMNS_V2]


def run_v2(
    phase4_report_path: str | Path,
    history_path: str | Path,
    latest_path: str | Path,
    metadata_path: str | Path,
    claim_prices_path: str | Path,
    prices_path: str | Path,
    phase2_path: str | Path,
    phase3_path: str | Path,
    claims_path: str | Path,
    outcomes_path: str | Path,
    *,
    source_commit: str,
    risk_scale_path: str | Path | None = None,
) -> dict[str, object]:
    phase4_report = json.loads(Path(phase4_report_path).read_text(encoding="utf-8"))
    metadata = json.loads(Path(metadata_path).read_text(encoding="utf-8"))
    history = pd.read_csv(history_path, low_memory=False)
    latest = pd.read_csv(latest_path, low_memory=False)
    claim_prices = pd.read_csv(claim_prices_path, low_memory=False)
    prices = pd.read_csv(prices_path, low_memory=False)
    fingerprints = evidence_fingerprints(
        phase4_report_path,
        phase2_path,
        phase3_path,
        risk_scale_path,
    )

    existing_claims = _read_csv(claims_path, CLAIM_COLUMNS_V2)
    existing_outcomes = _read_csv(outcomes_path, OUTCOME_COLUMNS_V2)
    validate_v2_archives(existing_claims, existing_outcomes)

    new_claims = build_claim_rows_v2(
        phase4_report,
        history,
        latest,
        metadata,
        fingerprints,
        claim_prices,
        source_commit=source_commit,
    )
    claims = append_claims_v2(existing_claims, new_claims)
    validate_v2_archives(claims, existing_outcomes)
    _write_csv(claims, claims_path, CLAIM_COLUMNS_V2)

    claims = _read_csv(claims_path, CLAIM_COLUMNS_V2)
    validate_v2_archives(claims, existing_outcomes)

    evaluated_at = _required_text(metadata.get("generated_at"), "generated_at")
    new_outcomes = compute_mature_outcomes_v2(
        claims,
        prices,
        existing_outcomes,
        evaluated_at,
    )
    outcomes = append_outcomes_v2(existing_outcomes, new_outcomes)
    validate_v2_archives(claims, outcomes)
    _write_csv(outcomes, outcomes_path, OUTCOME_COLUMNS_V2)

    outcomes = _read_csv(outcomes_path, OUTCOME_COLUMNS_V2)
    validate_v2_archives(claims, outcomes)

    return {
        "phase": "5B_prospective_shadow_v2",
        "schema_version": SCHEMA_VERSION,
        "status": "collecting_prospective_evidence",
        "claims": int(len(claims)),
        "raw_mature_outcomes": int(len(outcomes)),
        "new_claims": int(len(claims) - len(existing_claims)),
        "new_mature_outcomes": int(len(outcomes) - len(existing_outcomes)),
        "source_commit": source_commit,
        "semantics": {
            "prospective_only": True,
            "v1_backfill_performed": False,
            "claim_time_statistical_context_integrity_bound": True,
            "exact_outcome_market_session_count_integrity_bound": True,
            "csv_round_trip_integrity_verified": True,
            "production_confidence_changed": False,
            "adaptive_weights_created": False,
            "scalar_confidence_mapping_created": False,
        },
    }
