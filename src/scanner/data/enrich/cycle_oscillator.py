"""CY-02: versioned, point-in-time conservative daily-price cycle oscillator.

New current-only formula. Its historical identity with legacy/market/cycle.py
is NOT proven. In particular, do not backfill old scanner observations.
The date index is a Yahoo *session label*, not a verified close timestamp;
only strictly prior UTC-calendar-date sessions are admitted.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import hashlib
import math

import pandas as pd

from scanner.data.enrich.cycle_exchange_calendar import (
    POLICY_VERSION, authorized_extended_equity_gap,
)

FORMULA_VERSION = "cycle_detrended_sma20_range40_v1"
CYCLE_SOURCE = "YAHOO_PIT_CYCLE_V1"
PRICE_SOURCE = "yfinance.download"
PRICE_BASIS = "1d_auto_adjust_true_close"
MIN_BARS = 60
META_FIELDS = (
    "cycle_formula_version", "cycle_price_source", "cycle_price_basis",
    "cycle_price_symbol", "cycle_currency", "cycle_last_bar",
    "cycle_as_of", "cycle_computed_at", "cycle_price_sha256",
    "cycle_eligible_bars", "cycle_quality_reason",
    "cycle_currency_lineage", "cycle_session_time_quality",
)


def _text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _weekday_lag(last: date, today: date) -> int:
    return sum((last + timedelta(days=n)).weekday() < 5
               for n in range(1, (today - last).days + 1))


def calculate_cycle(
    close: pd.Series | None,
    *,
    symbol: str,
    currency: str = "",
    is_crypto: bool,
    as_of: datetime,
) -> dict[str, object]:
    """Calculate a current, traceable value, or return a blocking status.

    Never substitutes an old oscillator. Same input/as_of => same value and
    exact used-series hash. computed_at describes the current invocation.
    """
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("as_of must be timezone-aware")
    as_of_utc = as_of.astimezone(timezone.utc)
    cutoff = as_of_utc.date()
    record: dict[str, object] = {
        "Zyklus %": float("nan"),
        "cycle_quality": "STALE",
        "cycle_source": CYCLE_SOURCE,
        "cycle_formula_version": FORMULA_VERSION,
        "cycle_price_source": PRICE_SOURCE,
        "cycle_price_basis": PRICE_BASIS,
        "cycle_price_symbol": symbol,
        "cycle_currency": _text(currency),
        # Separate a declared quote unit from independently provider-verified
        # currency; a symbol's original listing is not proven by this field.
        "cycle_currency_lineage": "WATCHLIST_DECLARED_ONLY" if _text(currency) else "NONE",
        "cycle_session_time_quality": "SESSION_DATE_CUTOFF_ONLY",
        "cycle_last_bar": "",
        "cycle_as_of": as_of_utc.isoformat(),
        "cycle_computed_at": datetime.now(timezone.utc).isoformat(),
        "cycle_price_sha256": "",
        "cycle_eligible_bars": 0,
        "cycle_quality_reason": "PROVIDER_UNAVAILABLE",
    }
    if not symbol:
        record.update(cycle_quality="MISSING_SOURCE", cycle_quality_reason="NO_YAHOO_SYMBOL")
        return record
    if close is None or not isinstance(close, pd.Series) or close.empty:
        return record
    if not isinstance(close.index, pd.DatetimeIndex) or close.index.hasnans:
        record.update(cycle_quality="INVALID_VALUE", cycle_quality_reason="INVALID_BAR_INDEX")
        return record

    # Daily yfinance indexes carry market-session DATE LABELS, not publish times.
    # Exclude labels dated today/future even if an intraday feed advertises them.
    frame = pd.DataFrame({
        "day": [item.date() for item in close.index],
        "price": pd.to_numeric(close, errors="coerce").to_numpy(),
    })
    frame = frame.loc[frame["day"] < cutoff].sort_values("day")
    if frame.empty:
        record.update(cycle_quality_reason="NO_COMPLETED_DAILY_BAR")
        return record
    record["cycle_last_bar"] = frame.iloc[-1]["day"].isoformat()
    record["cycle_eligible_bars"] = int(len(frame))
    if frame["day"].duplicated().any():
        record.update(cycle_quality="INVALID_VALUE", cycle_quality_reason="DUPLICATE_BAR_DATES")
        return record

    last = frame.iloc[-1]["day"]
    lag = (cutoff - last).days
    # Crypto has a seven-day market; stocks permit weekends and limited
    # exchange holidays, without guessing exchange-specific close times.
    if (is_crypto and lag != 1) or (not is_crypto and _weekday_lag(last, cutoff) > 3):
        record.update(cycle_quality_reason="BAR_STALE")
        return record
    if len(frame) < MIN_BARS:
        record.update(cycle_quality="INSUFFICIENT_HISTORY",
                      cycle_quality_reason="NEED_60_DAILY_BARS")
        return record
    # Cannot support an original-listing/quote-currency claim if the input
    # universe did not declare one. Never infer USD or apply an FX conversion.
    if not _text(currency):
        record.update(cycle_quality="MISSING_SOURCE", cycle_quality_reason="QUOTE_CURRENCY_UNKNOWN")
        return record
    if is_crypto and symbol.rsplit("-", 1)[-1].upper() != _text(currency).upper():
        record.update(cycle_quality="INVALID_VALUE", cycle_quality_reason="CURRENCY_SYMBOL_QUOTE_MISMATCH")
        return record
    sample = frame.tail(MIN_BARS).copy()
    prices = sample["price"].to_numpy(dtype=float)
    if not all(math.isfinite(v) and v > 0 for v in prices):
        record.update(cycle_quality="INVALID_VALUE", cycle_quality_reason="NAN_OR_NONPOSITIVE_CLOSE")
        return record
    dates = sample["day"].tolist()
    # For stocks at most one missing weekday between adjacent labels is
    # tolerated (exchange-specific holidays remain unverified); a raw
    # five-calendar-day threshold would silently hide 2-3 missing sessions.
    used_verified_calendar_exception = False
    for a, b in zip(dates, dates[1:]):
        if is_crypto:
            gap_invalid = (b - a).days != 1
        else:
            extended_gap = _weekday_lag(a, b) > 2
            authorized = (extended_gap and authorized_extended_equity_gap(
                a, b, symbol=symbol, currency=_text(currency)
            ))
            if authorized:
                used_verified_calendar_exception = True
            gap_invalid = extended_gap and not authorized
        if gap_invalid:
            record.update(cycle_quality="INSUFFICIENT_HISTORY",
                          cycle_quality_reason="GAP_IN_DAILY_BARS")
            return record
    # Very large unadjusted overnight jumps can indicate uncaught splits.
    # Yahoo auto_adjust=True is necessary, but not a historical split audit.
    if any(b / a > 4 or b / a < 0.25 for a, b in zip(prices, prices[1:])):
        record.update(cycle_quality="INVALID_VALUE",
                      cycle_quality_reason="SPLIT_OR_DISCONTINUITY_REVIEW")
        return record

    series = pd.Series(prices)
    detrended = series - series.rolling(20, min_periods=20).mean()
    window = detrended.tail(40)
    lower, upper = float(window.min()), float(window.max())
    if not (math.isfinite(lower) and math.isfinite(upper)) or upper <= lower:
        record.update(cycle_quality="INVALID_VALUE",
                      cycle_quality_reason="CONSTANT_OR_UNDEFINED_RANGE")
        return record

    current = float(detrended.iloc[-1])
    value = round(max(0.0, min(100.0, 100.0 * (current - lower) / (upper - lower))), 4)
    # Fingerprint exactly the 60 eligible market-bar inputs, not the entire
    # download or a later adjusted price series.
    payload = "\n".join(
        f"{day.isoformat()},{float(price):.17g}"
        for day, price in zip(dates, prices)
    )
    digest = hashlib.sha256(
        (FORMULA_VERSION + "\n" + PRICE_BASIS + "\n" + symbol
         + "\n" + _text(currency) + "\n" + payload + "\n").encode("utf-8")
    ).hexdigest()
    record.update({
        "Zyklus %": value,
        "cycle_quality": "VALID",
        "cycle_price_sha256": digest,
        # The distinct reason identifies the source-bound exception version
        # without pretending the unchanged mathematical formula has changed.
        "cycle_quality_reason": (
            "COMPLETED_DAILY_BARS_" + POLICY_VERSION
            if used_verified_calendar_exception else "COMPLETED_DAILY_BARS"
        ),
        # Transient audit evidence; never persists in the canonical scanner column.
        "_cycle_input_bars": [(day.isoformat(), f"{float(price):.17g}")
                              for day, price in zip(dates, prices)],
    })
    return record
