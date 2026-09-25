from __future__ import annotations

"""Module 6F: external market / sector context research layer.

The module is deliberately separate from scanner history. It validates a
versioned context registry, point-in-time asset→context assignments, and
observed OHLC(V) rows. Only externally defined context with sufficient/limited
quality may become real market evidence. Scanner peers remain internal context.

No function emits a trade decision or order instruction.
"""

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd

CONTEXT_TYPES = (
    "market", "sector", "industry", "theme", "commodity", "peer_basket",
    "scanner_peer_context",
)
PROXY_KINDS = (
    "official_or_broad_index",
    "broad_liquid_sector_or_theme_etf",
    "externally_defined_point_in_time_peer_basket",
    "scanner_peer_context_only",
)
CONTEXT_QUALITIES = ("sufficient", "limited", "unreliable", "unavailable")
REAL_EVIDENCE_QUALITIES = frozenset({"sufficient", "limited"})
PRICE_BASES = ("raw", "adjusted", "provider_adjusted", "not_eligible_for_elliott")

REGISTRY_REQUIRED = (
    "context_id", "context_type", "name", "symbol", "proxy_kind", "source",
    "context_quality", "price_basis", "valid_from", "valid_to",
)
ASSIGNMENT_REQUIRED = (
    "symbol", "context_id", "relationship", "source", "assignment_quality",
    "pit_verified", "valid_from", "valid_to",
)
HISTORY_RAW_REQUIRED = ("date", "context_id", "open", "high", "low", "close")
HISTORY_COLUMNS = (
    "date", "context_id", "context_type", "name", "symbol", "open", "high",
    "low", "close", "adj_close", "volume", "currency", "source", "retrieved_at",
    "context_quality", "proxy_kind", "price_basis", "valid_from", "valid_to",
    "usable_as_real_market_evidence", "notes",
)


class MarketContextInputError(ValueError):
    """Raised when 6F input violates provenance, PIT, or OHLC rules."""


@dataclass(frozen=True)
class ContextSnapshotConfig:
    """Optional descriptive freshness annotation, never a predictive gate."""

    stale_after_calendar_days: int | None = None

    def __post_init__(self) -> None:
        if self.stale_after_calendar_days is not None and self.stale_after_calendar_days < 0:
            raise ValueError("stale_after_calendar_days must be >= 0 or None")


def _required(frame: pd.DataFrame, names: Iterable[str], label: str) -> None:
    missing = [name for name in names if name not in frame.columns]
    if missing:
        raise MarketContextInputError(f"{label}_missing_columns:{','.join(missing)}")


def _missing(value: object) -> bool:
    if value is None:
        return True
    try:
        result = pd.isna(value)
        return bool(result) if np.isscalar(result) else False
    except (TypeError, ValueError):
        return False


def _text(value: object) -> str:
    return "" if _missing(value) else str(value).strip()


def _date_series(series: pd.Series, field: str, *, allow_null: bool) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce").dt.normalize()
    if not allow_null and parsed.isna().any():
        raise MarketContextInputError(f"invalid_or_missing_{field}")
    supplied = series.map(lambda value: not _missing(value) and _text(value) != "")
    if (supplied & parsed.isna()).any():
        raise MarketContextInputError(f"invalid_{field}")
    return parsed


def _bool_value(value: object, field: str) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    text = _text(value).lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    raise MarketContextInputError(f"invalid_boolean_{field}")


def _active(frame: pd.DataFrame, day: pd.Timestamp) -> pd.DataFrame:
    return frame.loc[
        (frame["valid_from"] <= day)
        & (frame["valid_to"].isna() | (frame["valid_to"] >= day))
    ]


def _assert_intervals_do_not_overlap(frame: pd.DataFrame, keys: list[str], label: str) -> None:
    if frame.empty:
        return
    sentinel = pd.Timestamp.max.normalize()
    ordered = frame.sort_values(keys + ["valid_from"], kind="mergesort")
    for _, group in ordered.groupby(keys, dropna=False, sort=False):
        previous_end: pd.Timestamp | None = None
        for row in group.itertuples(index=False):
            start = pd.Timestamp(row.valid_from).normalize()
            end = pd.Timestamp(row.valid_to).normalize() if pd.notna(row.valid_to) else sentinel
            if previous_end is not None and start <= previous_end:
                raise MarketContextInputError(f"overlapping_{label}_validity")
            previous_end = end


def _real_registry_evidence(row: pd.Series) -> bool:
    return bool(
        row["context_quality"] in REAL_EVIDENCE_QUALITIES
        and row["proxy_kind"] != "scanner_peer_context_only"
        and row["context_type"] != "scanner_peer_context"
    )


def normalize_context_registry(registry: pd.DataFrame) -> pd.DataFrame:
    """Validate and normalize the versioned external context registry."""

    frame = registry.copy()
    _required(frame, REGISTRY_REQUIRED, "context_registry")
    if frame.empty:
        return frame.assign(usable_as_real_market_evidence=pd.Series(dtype=bool))

    text_cols = (
        "context_id", "context_type", "name", "symbol", "proxy_kind", "source",
        "context_quality", "price_basis",
    )
    for col in text_cols:
        frame[col] = frame[col].map(_text)
        if frame[col].eq("").any():
            raise MarketContextInputError(f"blank_registry_{col}")

    if (~frame["context_type"].isin(CONTEXT_TYPES)).any():
        raise MarketContextInputError("unsupported_context_type")
    if (~frame["proxy_kind"].isin(PROXY_KINDS)).any():
        raise MarketContextInputError("unsupported_proxy_kind")
    if (~frame["context_quality"].isin(CONTEXT_QUALITIES)).any():
        raise MarketContextInputError("unsupported_context_quality")
    if (~frame["price_basis"].isin(PRICE_BASES)).any():
        raise MarketContextInputError("unsupported_price_basis")

    frame["valid_from"] = _date_series(frame["valid_from"], "registry_valid_from", allow_null=False)
    frame["valid_to"] = _date_series(frame["valid_to"], "registry_valid_to", allow_null=True)
    if (frame["valid_to"].notna() & (frame["valid_to"] < frame["valid_from"])).any():
        raise MarketContextInputError("registry_valid_to_before_valid_from")
    _assert_intervals_do_not_overlap(frame, ["context_id"], "context_registry")

    frame["usable_as_real_market_evidence"] = frame.apply(_real_registry_evidence, axis=1).astype(bool)
    return frame.sort_values(["context_id", "valid_from"], kind="mergesort").reset_index(drop=True)


def normalize_context_assignments(
    assignments: pd.DataFrame,
    registry: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Validate PIT symbol→context assignments without inventing classifications."""

    frame = assignments.copy()
    _required(frame, ASSIGNMENT_REQUIRED, "context_assignments")
    if frame.empty:
        return frame.assign(usable_for_historical_context=pd.Series(dtype=bool))

    for col in ("symbol", "context_id", "relationship", "source", "assignment_quality"):
        frame[col] = frame[col].map(_text)
        if frame[col].eq("").any():
            raise MarketContextInputError(f"blank_assignment_{col}")
    if (~frame["relationship"].isin(CONTEXT_TYPES)).any():
        raise MarketContextInputError("unsupported_assignment_relationship")
    if (~frame["assignment_quality"].isin(CONTEXT_QUALITIES)).any():
        raise MarketContextInputError("unsupported_assignment_quality")

    frame["pit_verified"] = frame["pit_verified"].map(lambda value: _bool_value(value, "pit_verified"))
    frame["valid_from"] = _date_series(frame["valid_from"], "assignment_valid_from", allow_null=False)
    frame["valid_to"] = _date_series(frame["valid_to"], "assignment_valid_to", allow_null=True)
    if (frame["valid_to"].notna() & (frame["valid_to"] < frame["valid_from"])).any():
        raise MarketContextInputError("assignment_valid_to_before_valid_from")
    _assert_intervals_do_not_overlap(
        frame, ["symbol", "context_id", "relationship"], "context_assignment"
    )

    frame["usable_for_historical_context"] = (
        frame["pit_verified"] & frame["assignment_quality"].isin(REAL_EVIDENCE_QUALITIES)
    )
    if registry is not None:
        reg = normalize_context_registry(registry)
        unknown = sorted(set(frame["context_id"]) - set(reg["context_id"]))
        if unknown:
            raise MarketContextInputError(f"assignment_unknown_context_id:{','.join(unknown)}")

    return frame.sort_values(
        ["symbol", "relationship", "context_id", "valid_from"], kind="mergesort"
    ).reset_index(drop=True)


def _registry_row(registry: pd.DataFrame, context_id: str, day: pd.Timestamp) -> pd.Series:
    candidates = _active(registry.loc[registry["context_id"].eq(context_id)], day)
    if candidates.empty:
        raise MarketContextInputError(f"context_not_valid_on_date:{context_id}:{day.date()}")
    if len(candidates) != 1:
        raise MarketContextInputError(f"ambiguous_context_registry_version:{context_id}:{day.date()}")
    return candidates.iloc[0]


def _numeric(frame: pd.DataFrame, column: str, *, required: bool) -> pd.Series:
    if column not in frame.columns:
        if required:
            raise MarketContextInputError(f"missing_history_{column}")
        return pd.Series(np.nan, index=frame.index, dtype=float)
    values = pd.to_numeric(frame[column], errors="coerce")
    if required and values.isna().any():
        raise MarketContextInputError(f"invalid_history_{column}")
    return values


def build_market_context_history(
    raw_rows: pd.DataFrame,
    registry: pd.DataFrame,
    *,
    as_of: str | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Build registry-backed context history without fabricating sessions."""

    raw = raw_rows.copy()
    _required(raw, HISTORY_RAW_REQUIRED, "market_context_history")
    reg = normalize_context_registry(registry)
    if raw.empty:
        return pd.DataFrame(columns=HISTORY_COLUMNS)

    raw["date"] = _date_series(raw["date"], "context_date", allow_null=False)
    if as_of is not None:
        raw = raw.loc[raw["date"] <= pd.Timestamp(as_of).normalize()].copy()
    if raw.empty:
        return pd.DataFrame(columns=HISTORY_COLUMNS)

    raw["context_id"] = raw["context_id"].map(_text)
    if raw["context_id"].eq("").any():
        raise MarketContextInputError("blank_history_context_id")
    for col in ("open", "high", "low", "close"):
        raw[col] = _numeric(raw, col, required=True)
        if (~np.isfinite(raw[col])).any() or (raw[col] <= 0).any():
            raise MarketContextInputError(f"nonfinite_or_nonpositive_history_{col}")
    raw["adj_close"] = _numeric(raw, "adj_close", required=False)
    raw["volume"] = _numeric(raw, "volume", required=False)
    adj_bad = raw["adj_close"].notna() & ((~np.isfinite(raw["adj_close"])) | (raw["adj_close"] <= 0))
    vol_bad = raw["volume"].notna() & ((~np.isfinite(raw["volume"])) | (raw["volume"] < 0))
    if adj_bad.any():
        raise MarketContextInputError("nonfinite_or_nonpositive_history_adj_close")
    if vol_bad.any():
        raise MarketContextInputError("nonfinite_or_negative_history_volume")

    bad_high = raw["high"] < raw[["open", "low", "close"]].max(axis=1)
    bad_low = raw["low"] > raw[["open", "high", "close"]].min(axis=1)
    if bad_high.any() or bad_low.any():
        raise MarketContextInputError("invalid_history_ohlc_geometry")

    for _, group in raw.groupby(["context_id", "date"], sort=False):
        if len(group) <= 1:
            continue
        for col in ("open", "high", "low", "close", "adj_close", "volume"):
            if len(group[col].dropna().unique()) > 1:
                raise MarketContextInputError("conflicting_duplicate_context_history_row")
    raw = raw.drop_duplicates(["context_id", "date"], keep="last")

    records: list[dict[str, object]] = []
    for row in raw.itertuples(index=False):
        day = pd.Timestamp(row.date).normalize()
        context_id = str(row.context_id)
        meta = _registry_row(reg, context_id, day)
        for optional_meta in ("symbol", "source"):
            if optional_meta in raw.columns:
                supplied = _text(getattr(row, optional_meta, ""))
                if supplied and supplied != str(meta[optional_meta]):
                    raise MarketContextInputError(f"history_{optional_meta}_conflicts_with_registry")

        price_basis = str(meta["price_basis"])
        adj_close = getattr(row, "adj_close", np.nan)
        if price_basis == "adjusted" and pd.isna(adj_close):
            raise MarketContextInputError(f"adjusted_context_missing_adj_close:{context_id}:{day.date()}")
        records.append(
            {
                "date": day,
                "context_id": context_id,
                "context_type": str(meta["context_type"]),
                "name": str(meta["name"]),
                "symbol": str(meta["symbol"]),
                "open": float(row.open),
                "high": float(row.high),
                "low": float(row.low),
                "close": float(row.close),
                "adj_close": float(adj_close) if pd.notna(adj_close) else np.nan,
                "volume": float(row.volume) if pd.notna(row.volume) else np.nan,
                "currency": _text(getattr(row, "currency", "")) or None,
                "source": str(meta["source"]),
                "retrieved_at": _text(getattr(row, "retrieved_at", "")) or _text(meta.get("retrieved_at", "")) or None,
                "context_quality": str(meta["context_quality"]),
                "proxy_kind": str(meta["proxy_kind"]),
                "price_basis": price_basis,
                "valid_from": pd.Timestamp(meta["valid_from"]).normalize(),
                "valid_to": pd.Timestamp(meta["valid_to"]).normalize() if pd.notna(meta["valid_to"]) else pd.NaT,
                "usable_as_real_market_evidence": bool(meta["usable_as_real_market_evidence"]),
                "notes": _text(meta.get("notes", "")) or None,
            }
        )
    return pd.DataFrame(records, columns=HISTORY_COLUMNS).sort_values(
        ["context_id", "date"], kind="mergesort"
    ).reset_index(drop=True)


def resolve_context_assignments(
    assignments: pd.DataFrame,
    registry: pd.DataFrame,
    *,
    symbol: str,
    as_of: str | pd.Timestamp,
    include_unusable: bool = False,
) -> pd.DataFrame:
    """Resolve context assignments valid for one asset at one historical date."""

    day = pd.Timestamp(as_of).normalize()
    reg = normalize_context_registry(registry)
    assn = normalize_context_assignments(assignments, reg)
    current = _active(assn.loc[assn["symbol"].eq(str(symbol))], day).copy()
    if current.empty:
        return current.assign(usable_as_real_market_evidence=pd.Series(dtype=bool))

    records: list[dict[str, object]] = []
    for _, row in current.iterrows():
        meta = _registry_row(reg, str(row["context_id"]), day)
        record = row.to_dict()
        record.update(
            {
                "context_type": meta["context_type"],
                "context_name": meta["name"],
                "context_symbol": meta["symbol"],
                "proxy_kind": meta["proxy_kind"],
                "context_quality": meta["context_quality"],
                "price_basis": meta["price_basis"],
                "context_valid_from": meta["valid_from"],
                "context_valid_to": meta["valid_to"],
                "usable_as_real_market_evidence": bool(
                    row["usable_for_historical_context"]
                    and meta["usable_as_real_market_evidence"]
                ),
            }
        )
        records.append(record)
    out = pd.DataFrame(records)
    if not include_unusable:
        out = out.loc[out["usable_as_real_market_evidence"]].copy()
    return out.sort_values(["relationship", "context_id"], kind="mergesort").reset_index(drop=True)


def build_context_snapshot(
    history: pd.DataFrame,
    assignments: pd.DataFrame,
    registry: pd.DataFrame,
    *,
    symbol: str,
    as_of: str | pd.Timestamp,
    config: ContextSnapshotConfig = ContextSnapshotConfig(),
    include_unusable_assignments: bool = False,
) -> pd.DataFrame:
    """Return the last causally visible row from the active context version."""

    day = pd.Timestamp(as_of).normalize()
    resolved = resolve_context_assignments(
        assignments, registry, symbol=symbol, as_of=day,
        include_unusable=include_unusable_assignments,
    )
    if resolved.empty:
        return pd.DataFrame()

    hist = history.copy()
    _required(hist, HISTORY_COLUMNS, "validated_context_history")
    hist["date"] = pd.to_datetime(hist["date"], errors="coerce").dt.normalize()
    hist = hist.loc[hist["date"] <= day].copy()

    records: list[dict[str, object]] = []
    for _, assignment in resolved.iterrows():
        context_id = str(assignment["context_id"])
        assignment_start = pd.Timestamp(assignment["valid_from"]).normalize()
        context_start = pd.Timestamp(assignment["context_valid_from"]).normalize()
        lower = max(assignment_start, context_start)
        upper_candidates = [day]
        if pd.notna(assignment["valid_to"]):
            upper_candidates.append(pd.Timestamp(assignment["valid_to"]).normalize())
        if pd.notna(assignment["context_valid_to"]):
            upper_candidates.append(pd.Timestamp(assignment["context_valid_to"]).normalize())
        upper = min(upper_candidates)

        rows = hist.loc[
            hist["context_id"].eq(context_id)
            & (hist["date"] >= lower)
            & (hist["date"] <= upper)
        ].sort_values("date", kind="mergesort")
        base = {
            "asset_symbol": str(symbol),
            "as_of": day,
            "context_id": context_id,
            "relationship": assignment["relationship"],
            "assignment_quality": assignment["assignment_quality"],
            "assignment_pit_verified": bool(assignment["pit_verified"]),
            "assignment_source": assignment["source"],
            "usable_as_real_market_evidence": bool(assignment["usable_as_real_market_evidence"]),
        }
        if rows.empty:
            base.update({"status": "missing_context_history", "context_date": pd.NaT, "age_calendar_days": None})
            records.append(base)
            continue

        latest = rows.iloc[-1]
        row_usable = bool(latest["usable_as_real_market_evidence"])
        base["usable_as_real_market_evidence"] = bool(base["usable_as_real_market_evidence"] and row_usable)
        age = int((day - pd.Timestamp(latest["date"]).normalize()).days)
        stale = config.stale_after_calendar_days is not None and age > config.stale_after_calendar_days
        base.update(
            {
                "status": "observed",
                "context_date": latest["date"],
                "age_calendar_days": age,
                "stale_by_config": stale,
                "context_type": latest["context_type"],
                "context_name": latest["name"],
                "context_symbol": latest["symbol"],
                "context_quality": latest["context_quality"],
                "proxy_kind": latest["proxy_kind"],
                "price_basis": latest["price_basis"],
                "close": latest["close"],
                "adj_close": latest["adj_close"],
                "source": latest["source"],
            }
        )
        records.append(base)
    return pd.DataFrame(records).sort_values(["relationship", "context_id"], kind="mergesort").reset_index(drop=True)


def prepare_context_ohlcv_for_elliott(
    history: pd.DataFrame,
    *,
    context_id: str,
    as_of: str | pd.Timestamp | None = None,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Prepare one eligible real external context series for the 6A pivot layer."""

    frame = history.loc[history["context_id"].astype(str).eq(str(context_id))].copy()
    if frame.empty:
        raise MarketContextInputError(f"context_history_missing:{context_id}")
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
    if frame["date"].isna().any():
        raise MarketContextInputError("invalid_validated_context_date")
    if as_of is not None:
        frame = frame.loc[frame["date"] <= pd.Timestamp(as_of).normalize()].copy()
    if frame.empty:
        raise MarketContextInputError(f"context_not_yet_visible:{context_id}")

    if (~frame["usable_as_real_market_evidence"].astype(bool)).any():
        raise MarketContextInputError(f"context_contains_unusable_real_market_evidence:{context_id}")
    if not set(frame["context_quality"].astype(str)) <= REAL_EVIDENCE_QUALITIES:
        raise MarketContextInputError(f"context_quality_not_eligible_for_elliott:{context_id}")
    if (
        "scanner_peer_context_only" in set(frame["proxy_kind"].astype(str))
        or "scanner_peer_context" in set(frame["context_type"].astype(str))
    ):
        raise MarketContextInputError("scanner_peer_context_cannot_generate_real_market_wave")

    bases = set(frame["price_basis"].astype(str))
    if len(bases) != 1:
        raise MarketContextInputError("mixed_context_price_basis")
    price_basis = next(iter(bases))
    if price_basis == "not_eligible_for_elliott":
        raise MarketContextInputError("context_price_basis_not_eligible_for_elliott")

    for col in ("open", "high", "low", "close"):
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame["adj_close"] = pd.to_numeric(frame.get("adj_close"), errors="coerce")
    frame["volume"] = pd.to_numeric(frame.get("volume"), errors="coerce")

    if price_basis == "adjusted":
        if frame["adj_close"].isna().any():
            raise MarketContextInputError("partial_adjusted_context_coverage")
        factor = frame["adj_close"] / frame["close"]
        for col in ("open", "high", "low"):
            frame[col] = frame[col] * factor
        frame["close"] = frame["adj_close"]
        six_a_basis = "adjusted"
    else:
        frame["adj_close"] = np.nan
        six_a_basis = "raw"

    out = pd.DataFrame(
        {
            "date": frame["date"],
            "symbol": str(context_id),
            "currency": frame.get("currency"),
            "open": frame["open"],
            "high": frame["high"],
            "low": frame["low"],
            "close": frame["close"],
            "adj_close": frame["adj_close"],
            "volume": frame["volume"],
            "retrieved_at": frame.get("retrieved_at"),
        }
    ).sort_values("date", kind="mergesort").reset_index(drop=True)
    metadata = {
        "context_id": str(context_id),
        "source_symbol": str(frame.iloc[-1]["symbol"]),
        "context_type": str(frame.iloc[-1]["context_type"]),
        "context_quality": str(frame.iloc[-1]["context_quality"]),
        "proxy_kind": str(frame.iloc[-1]["proxy_kind"]),
        "declared_price_basis": price_basis,
        "six_a_price_basis": six_a_basis,
        "research_only": True,
    }
    return out, metadata


def summarize_market_context(
    history: pd.DataFrame,
    registry: pd.DataFrame,
    assignments: pd.DataFrame,
) -> dict[str, object]:
    """Coverage/provenance summary only; no predictive-performance claim."""

    reg = normalize_context_registry(registry)
    assn = normalize_context_assignments(assignments, reg)
    hist = history.copy()
    if not hist.empty:
        _required(hist, HISTORY_COLUMNS, "validated_context_history")
        hist["date"] = pd.to_datetime(hist["date"], errors="coerce").dt.normalize()
    return {
        "schema_version": "elliott_vnext_market_context_summary_v1",
        "registry_rows": int(len(reg)),
        "registry_context_ids": int(reg["context_id"].nunique()) if len(reg) else 0,
        "real_external_registry_rows": int(reg["usable_as_real_market_evidence"].sum()) if len(reg) else 0,
        "assignment_rows": int(len(assn)),
        "pit_usable_assignment_rows": int(assn["usable_for_historical_context"].sum()) if len(assn) else 0,
        "history_rows": int(len(hist)),
        "history_context_ids": int(hist["context_id"].nunique()) if len(hist) else 0,
        "history_date_min": str(hist["date"].min().date()) if len(hist) else None,
        "history_date_max": str(hist["date"].max().date()) if len(hist) else None,
        "quality_counts": (
            {str(k): int(v) for k, v in hist["context_quality"].value_counts(dropna=False).items()}
            if len(hist) else {}
        ),
        "predictive_value_evaluated": False,
        "incremental_value_evaluated": False,
        "research_only": True,
        "trade_decision_emitted": False,
        "order_instruction_emitted": False,
    }
