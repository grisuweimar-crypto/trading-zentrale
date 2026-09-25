"""Causal OHLCV preparation and pivot detection for Elliott vNext Module 6A.

This module is deliberately research-only.  It does not count Elliott waves,
calculate Fibonacci targets, score scanner candidates, or emit trade actions.

The central point-in-time rule is explicit: a pivot belongs to ``pivot_time``
but is not usable until ``confirmed_time``.  Weekly bars follow the same rule;
a weekly aggregate becomes available only when the first observed session of
the next ISO week proves that the previous week is complete.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from scanner.data.price_history import validated_rows


_DAILY_COLUMNS = [
    "date",
    "symbol",
    "currency",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "timeframe",
    "price_basis",
    "bar_confirmed_time",
    "session_count",
]


@dataclass(frozen=True)
class PivotSpec:
    """Frozen candidate parameters for one research wave degree.

    The defaults below are *research candidates*, not empirically validated
    Elliott degrees.  Module 6A/6G must test stability before any later layer
    may treat them as useful.
    """

    degree: str
    timeframe: str
    left_bars: int
    right_bars: int
    atr_window: int
    min_excursion_atr: float
    validated: bool = False

    def __post_init__(self) -> None:
        if self.timeframe not in {"daily", "weekly"}:
            raise ValueError("timeframe must be daily or weekly")
        if self.left_bars < 1 or self.right_bars < 1:
            raise ValueError("left_bars and right_bars must be >= 1")
        if self.atr_window < 1:
            raise ValueError("atr_window must be >= 1")
        if not np.isfinite(self.min_excursion_atr) or self.min_excursion_atr < 0:
            raise ValueError("min_excursion_atr must be finite and >= 0")


DEFAULT_PIVOT_SPECS: tuple[PivotSpec, ...] = (
    PivotSpec("minor", "daily", left_bars=2, right_bars=2, atr_window=10, min_excursion_atr=0.75),
    PivotSpec("intermediate", "daily", left_bars=3, right_bars=3, atr_window=14, min_excursion_atr=1.00),
    PivotSpec("major", "daily", left_bars=5, right_bars=5, atr_window=20, min_excursion_atr=1.50),
    PivotSpec("primary", "weekly", left_bars=2, right_bars=2, atr_window=10, min_excursion_atr=1.00),
)


def _cutoff(value) -> pd.Timestamp | None:
    if value is None:
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert(None)
    return ts.normalize()


def _empty_daily() -> pd.DataFrame:
    return pd.DataFrame(columns=_DAILY_COLUMNS)


def prepare_daily_ohlcv(
    rows: Iterable[Mapping[str, object]],
    symbol: str,
    *,
    as_of=None,
    price_basis: str = "auto",
) -> pd.DataFrame:
    """Build a deterministic daily analysis frame from canonical price rows.

    ``price_basis`` can be:

    - ``auto``: use adjusted OHLC only when *all* retained sessions have a
      valid ``adj_close``; use raw OHLC when none do; fail closed on partial
      adjusted coverage.
    - ``adjusted``: require adjusted close on every retained session and scale
      O/H/L by the daily ``adj_close / close`` factor.
    - ``raw``: explicitly use the observed raw OHLC values.

    The function intentionally refuses incomplete O/H/L/C sessions rather than
    silently constructing bars.  No synthetic prices or sessions are created.
    """

    if price_basis not in {"auto", "adjusted", "raw"}:
        raise ValueError("price_basis must be auto, adjusted, or raw")

    valid, _issues = validated_rows(rows)
    wanted = str(symbol).strip()
    selected = [row for row in valid if row.get("symbol") == wanted]
    if not selected:
        return _empty_daily()

    frame = pd.DataFrame(selected).copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="raise").dt.normalize()
    cutoff = _cutoff(as_of)
    if cutoff is not None:
        frame = frame.loc[frame["date"] <= cutoff].copy()
    if frame.empty:
        return _empty_daily()

    frame = frame.sort_values("date", kind="stable").reset_index(drop=True)
    for field in ("open", "high", "low", "close", "volume", "adj_close"):
        if field not in frame:
            frame[field] = np.nan
        frame[field] = pd.to_numeric(frame[field], errors="coerce")

    if frame[["open", "high", "low", "close"]].isna().any(axis=None):
        raise ValueError("incomplete_ohlc_session")
    if (frame[["open", "high", "low", "close"]] <= 0).any(axis=None):
        raise ValueError("non_positive_ohlc_session")

    currencies = {str(v).strip() for v in frame.get("currency", pd.Series(dtype=str)) if str(v).strip()}
    if len(currencies) > 1:
        raise ValueError("mixed_currency_history")
    currency = next(iter(currencies), "")

    adj_present = frame["adj_close"].notna() & (frame["adj_close"] > 0)
    if price_basis == "auto":
        if adj_present.all():
            resolved_basis = "adjusted_via_adj_close_ratio"
        elif not adj_present.any():
            resolved_basis = "raw_unadjusted"
        else:
            raise ValueError("partial_adjusted_close_coverage")
    elif price_basis == "adjusted":
        if not adj_present.all():
            raise ValueError("adjusted_close_required_for_all_sessions")
        resolved_basis = "adjusted_via_adj_close_ratio"
    else:
        resolved_basis = "raw_unadjusted"

    if resolved_basis == "adjusted_via_adj_close_ratio":
        factor = frame["adj_close"] / frame["close"]
        for field in ("open", "high", "low"):
            frame[field] = frame[field] * factor
        frame["close"] = frame["adj_close"]

    out = pd.DataFrame(
        {
            "date": frame["date"],
            "symbol": wanted,
            "currency": currency,
            "open": frame["open"].astype(float),
            "high": frame["high"].astype(float),
            "low": frame["low"].astype(float),
            "close": frame["close"].astype(float),
            "volume": frame["volume"].astype(float),
            "timeframe": "daily",
            "price_basis": resolved_basis,
            # Daily bars are considered usable at their own completed session.
            "bar_confirmed_time": frame["date"],
            "session_count": 1,
        }
    )
    return out[_DAILY_COLUMNS].reset_index(drop=True)


def aggregate_weekly(
    daily: pd.DataFrame,
    *,
    as_of=None,
    require_confirmed: bool = True,
) -> pd.DataFrame:
    """Aggregate observed daily sessions to causal ISO-week bars.

    No Friday or other synthetic calendar date is invented.  ``date`` is the
    final observed session in the ISO week.  ``bar_confirmed_time`` is the first
    observed session of the following ISO week.  Therefore the newest partial
    week is excluded when ``require_confirmed=True``.
    """

    if daily.empty:
        return _empty_daily()

    work = daily.copy()
    work["date"] = pd.to_datetime(work["date"], errors="raise").dt.normalize()
    if "bar_confirmed_time" in work:
        work["bar_confirmed_time"] = pd.to_datetime(work["bar_confirmed_time"], errors="coerce").dt.normalize()
    else:
        work["bar_confirmed_time"] = work["date"]

    cutoff = _cutoff(as_of)
    if cutoff is not None:
        work = work.loc[work["bar_confirmed_time"].notna() & (work["bar_confirmed_time"] <= cutoff)].copy()
    if work.empty:
        return _empty_daily()

    work = work.sort_values("date", kind="stable").reset_index(drop=True)
    iso = work["date"].dt.isocalendar()
    work["_iso_year"] = iso.year.astype(int)
    work["_iso_week"] = iso.week.astype(int)

    grouped = list(work.groupby(["_iso_year", "_iso_week"], sort=False))
    rows: list[dict[str, object]] = []
    for index, (_key, group) in enumerate(grouped):
        group = group.sort_values("date", kind="stable")
        next_group = grouped[index + 1][1] if index + 1 < len(grouped) else None
        confirmed = pd.NaT if next_group is None else pd.Timestamp(next_group["date"].min()).normalize()
        if require_confirmed and pd.isna(confirmed):
            continue
        if cutoff is not None and (pd.isna(confirmed) or confirmed > cutoff):
            continue

        symbols = set(group["symbol"].astype(str))
        bases = set(group["price_basis"].astype(str))
        currencies = set(group["currency"].astype(str))
        if len(symbols) != 1 or len(bases) != 1 or len(currencies) != 1:
            raise ValueError("weekly_group_metadata_changed")

        rows.append(
            {
                "date": pd.Timestamp(group["date"].iloc[-1]).normalize(),
                "symbol": next(iter(symbols)),
                "currency": next(iter(currencies)),
                "open": float(group["open"].iloc[0]),
                "high": float(group["high"].max()),
                "low": float(group["low"].min()),
                "close": float(group["close"].iloc[-1]),
                "volume": float(group["volume"].sum(min_count=1)) if group["volume"].notna().any() else np.nan,
                "timeframe": "weekly",
                "price_basis": next(iter(bases)),
                "bar_confirmed_time": confirmed,
                "session_count": int(len(group)),
            }
        )

    return pd.DataFrame(rows, columns=_DAILY_COLUMNS)


def atr_series(frame: pd.DataFrame, window: int = 14) -> pd.Series:
    """Simple causal ATR estimate using only the current and prior bars."""

    if window < 1:
        raise ValueError("ATR window must be >= 1")
    if frame.empty:
        return pd.Series(dtype=float, index=frame.index)
    high = pd.to_numeric(frame["high"], errors="coerce")
    low = pd.to_numeric(frame["low"], errors="coerce")
    close = pd.to_numeric(frame["close"], errors="coerce")
    prev_close = close.shift(1)
    true_range = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return true_range.rolling(window=window, min_periods=window).mean()


def _strict_unique_extreme(values: np.ndarray, center: int, *, high: bool) -> bool:
    candidate = values[center]
    others = np.delete(values, center)
    if not np.isfinite(candidate) or not np.isfinite(others).all():
        return False
    return bool(candidate > others.max()) if high else bool(candidate < others.min())


def detect_confirmed_pivots(
    frame: pd.DataFrame,
    spec: PivotSpec,
    *,
    as_of=None,
) -> list[dict[str, object]]:
    """Detect fixed-window pivots without making them visible early.

    A pivot at bar ``i`` is evaluated only after ``right_bars`` subsequent bars
    are themselves confirmed.  The ATR/excursion filter is evaluated from data
    no later than ``pivot_time``.  Later bars never revise an already emitted
    pivot record.
    """

    if frame.empty:
        return []
    work = frame.copy()
    work["date"] = pd.to_datetime(work["date"], errors="raise").dt.normalize()
    if "bar_confirmed_time" not in work:
        work["bar_confirmed_time"] = work["date"]
    work["bar_confirmed_time"] = pd.to_datetime(work["bar_confirmed_time"], errors="coerce").dt.normalize()
    work = work.loc[work["bar_confirmed_time"].notna()].sort_values("date", kind="stable").reset_index(drop=True)

    cutoff = _cutoff(as_of)
    if cutoff is not None:
        work = work.loc[work["bar_confirmed_time"] <= cutoff].reset_index(drop=True)
    if work.empty:
        return []

    timeframes = set(work.get("timeframe", pd.Series([spec.timeframe] * len(work))).astype(str))
    if timeframes != {spec.timeframe}:
        raise ValueError("pivot_spec_timeframe_mismatch")

    atr = atr_series(work, spec.atr_window)
    left, right = spec.left_bars, spec.right_bars
    if len(work) < left + right + 1:
        return []

    highs = work["high"].to_numpy(dtype=float)
    lows = work["low"].to_numpy(dtype=float)
    pivots: list[dict[str, object]] = []

    for i in range(left, len(work) - right):
        high_window = highs[i - left : i + right + 1]
        low_window = lows[i - left : i + right + 1]
        is_high = _strict_unique_extreme(high_window, left, high=True)
        is_low = _strict_unique_extreme(low_window, left, high=False)
        if not is_high and not is_low:
            continue

        atr_value = float(atr.iloc[i]) if pd.notna(atr.iloc[i]) else np.nan
        past = work.iloc[i - left : i + 1]
        confirmed_time = pd.Timestamp(work["bar_confirmed_time"].iloc[i + right]).normalize()
        pivot_time = pd.Timestamp(work["date"].iloc[i]).normalize()
        source_start = pd.Timestamp(work["date"].iloc[i - left]).normalize()
        source_end = pd.Timestamp(work["date"].iloc[i + right]).normalize()
        symbol = str(work.get("symbol", pd.Series([""] * len(work))).iloc[i])
        price_basis = str(work.get("price_basis", pd.Series(["unknown"] * len(work))).iloc[i])
        ambiguous = bool(is_high and is_low)

        candidates: list[tuple[str, float, float]] = []
        if is_high:
            price = float(work["high"].iloc[i])
            excursion = price - float(past["low"].min())
            candidates.append(("high", price, excursion))
        if is_low:
            price = float(work["low"].iloc[i])
            excursion = float(past["high"].max()) - price
            candidates.append(("low", price, excursion))

        for kind, price, excursion in candidates:
            if spec.min_excursion_atr > 0:
                if not np.isfinite(atr_value) or atr_value <= 0:
                    continue
                excursion_atr = excursion / atr_value
                if excursion_atr + 1e-12 < spec.min_excursion_atr:
                    continue
            else:
                excursion_atr = excursion / atr_value if np.isfinite(atr_value) and atr_value > 0 else None

            pivots.append(
                {
                    "symbol": symbol,
                    "timeframe": spec.timeframe,
                    "degree": spec.degree,
                    "pivot_time": pivot_time.date().isoformat(),
                    "confirmed_time": confirmed_time.date().isoformat(),
                    "available_from": confirmed_time.date().isoformat(),
                    "price": price,
                    "kind": kind,
                    "atr_at_pivot": atr_value if np.isfinite(atr_value) else None,
                    "excursion_atr": float(excursion_atr) if excursion_atr is not None else None,
                    "left_bars": left,
                    "right_bars": right,
                    "atr_window": spec.atr_window,
                    "min_excursion_atr": spec.min_excursion_atr,
                    "parameter_validated": spec.validated,
                    "price_basis": price_basis,
                    "source_window_start": source_start.date().isoformat(),
                    "source_window_end": source_end.date().isoformat(),
                    "sequence_ambiguous": ambiguous,
                    "research_only": True,
                }
            )

    return sorted(pivots, key=lambda p: (p["confirmed_time"], p["pivot_time"], p["kind"], p["degree"]))


def detect_multidegree_pivots(
    daily: pd.DataFrame,
    *,
    specs: Sequence[PivotSpec] = DEFAULT_PIVOT_SPECS,
    as_of=None,
) -> list[dict[str, object]]:
    """Run the frozen research candidate grid on daily and causal weekly bars."""

    if daily.empty:
        return []
    cutoff = _cutoff(as_of)
    base = daily.copy()
    base["bar_confirmed_time"] = pd.to_datetime(base["bar_confirmed_time"], errors="coerce").dt.normalize()
    if cutoff is not None:
        base = base.loc[base["bar_confirmed_time"].notna() & (base["bar_confirmed_time"] <= cutoff)].copy()

    weekly = aggregate_weekly(base, as_of=cutoff, require_confirmed=True)
    pivots: list[dict[str, object]] = []
    for spec in specs:
        source = base if spec.timeframe == "daily" else weekly
        pivots.extend(detect_confirmed_pivots(source, spec, as_of=cutoff))
    return sorted(
        pivots,
        key=lambda p: (p["confirmed_time"], p["timeframe"], p["degree"], p["pivot_time"], p["kind"]),
    )
