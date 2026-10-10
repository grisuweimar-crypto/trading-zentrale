"""CY-02: current-only, PIT-bound cycle v1 and fail-closed integration."""
from datetime import datetime, timezone
import math
import sys
from types import SimpleNamespace

import pandas as pd

from scanner.data.enrich.cycle_oscillator import (
    FORMULA_VERSION, CYCLE_SOURCE, calculate_cycle,
)
from scanner.data.enrich.yahoo_prices import enrich_watchlist_with_yahoo
from scanner.data.schema.cycle_quality import normalize_cycle_source
from scanner.reports.daily_research import ALIASES
from scanner.reports.research_views import KNOWN_COLUMNS


AS_OF = datetime(2026, 10, 10, 7, tzinfo=timezone.utc)


def series(*, end="2026-10-09", count=90, frequency="B"):
    idx = pd.date_range(end=end, periods=count, freq=frequency)
    values = [100 + 0.1 * n + 3 * math.sin(n * 0.27) for n in range(count)]
    return pd.Series(values, index=idx)


def evaluate(close, *, symbol="AAA", crypto=False, as_of=AS_OF):
    return calculate_cycle(close, symbol=symbol, currency="USD", is_crypto=crypto, as_of=as_of)


def test_fixed_gold_and_deterministic_replay():
    a = evaluate(series())
    b = evaluate(series())
    assert a["cycle_quality"] == b["cycle_quality"] == "VALID"
    assert a["Zyklus %"] == b["Zyklus %"] == 7.2258
    assert a["cycle_formula_version"] == FORMULA_VERSION
    assert a["cycle_price_basis"] == "1d_auto_adjust_true_close"
    assert a["cycle_price_sha256"] == b["cycle_price_sha256"]
    assert len(a["cycle_price_sha256"]) == 64
    assert a["cycle_eligible_bars"] == 90
    assert a["cycle_last_bar"] == "2026-10-09"
    assert a["cycle_price_symbol"] == "AAA"
    assert a["cycle_currency"] == "USD"
    assert a["cycle_currency_lineage"] == "WATCHLIST_DECLARED_ONLY"
    assert a["cycle_session_time_quality"] == "SESSION_DATE_CUTOFF_ONLY"
    assert a["cycle_source"] == CYCLE_SOURCE


def test_same_day_future_bar_excluded_even_if_download_contains_it():
    before = evaluate(series())
    with_today = pd.concat([series(), pd.Series([9000], index=[pd.Timestamp("2026-10-10")])])
    after = evaluate(with_today)
    assert after["Zyklus %"] == before["Zyklus %"]
    assert after["cycle_price_sha256"] == before["cycle_price_sha256"]
    assert after["cycle_last_bar"] == "2026-10-09"
    assert after["cycle_eligible_bars"] == 90


def test_bars_after_as_of_are_never_fed_into_formula():
    future = pd.Series([101.0], index=pd.DatetimeIndex(["2026-10-11"]))
    result = evaluate(future)
    assert result["cycle_quality"] == "STALE"
    assert math.isnan(result["Zyklus %"])


def test_short_history_is_not_a_neutral_fifty():
    result = evaluate(series(count=59))
    assert result["cycle_quality"] == "INSUFFICIENT_HISTORY"
    assert math.isnan(result["Zyklus %"])


def test_constant_series_returns_missing_not_fifty():
    close = pd.Series([100.0] * 90, index=series().index)
    result = evaluate(close)
    assert result["cycle_quality"] == "INVALID_VALUE"
    assert result["cycle_quality_reason"] == "CONSTANT_OR_UNDEFINED_RANGE"
    assert math.isnan(result["Zyklus %"])


def test_invalid_nan_and_negative_bars_fail_closed():
    for invalid in (float("nan"), -5, float("inf")):
        close = series().copy()
        close.iloc[-10] = invalid
        result = evaluate(close)
        assert result["cycle_quality"] == "INVALID_VALUE"
        assert math.isnan(result["Zyklus %"])


def test_duplicate_dates_and_extended_gaps_do_not_compress_missing_bars():
    close = series()
    dup = pd.concat([close, close.tail(1)])
    assert evaluate(dup)["cycle_quality_reason"] == "DUPLICATE_BAR_DATES"
    gap = close.drop(close.index[-5:])
    gap.loc[pd.Timestamp("2026-10-09")] = 110.0
    # Last retained bar is a week before the replacement Friday.
    assert evaluate(gap)["cycle_quality"] == "INSUFFICIENT_HISTORY"


def test_unadjusted_split_like_discontinuity_blocked_adjusted_series_ok():
    clean = series()
    assert evaluate(clean)["cycle_quality"] == "VALID"
    damaged = clean.copy()
    damaged.iloc[-1] = damaged.iloc[-2] / 10
    result = evaluate(damaged)
    assert result["cycle_quality"] == "INVALID_VALUE"
    assert result["cycle_quality_reason"] == "SPLIT_OR_DISCONTINUITY_REVIEW"


def test_provider_outage_does_not_reuse_legacy_cycle():
    no_prices = evaluate(None)
    assert no_prices["cycle_quality"] == "STALE"
    assert math.isnan(no_prices["Zyklus %"])
    assert no_prices["cycle_price_sha256"] == ""


def test_weekend_stock_friday_bar_and_seven_day_crypto():
    assert evaluate(series())["cycle_quality"] == "VALID"
    crypto = series(frequency="D")
    assert evaluate(crypto, symbol="BTC-USD", crypto=True)["cycle_quality"] == "VALID"
    assert evaluate(crypto.iloc[:-1], symbol="BTC-USD", crypto=True)["cycle_quality"] == "STALE"


def test_old_equity_bar_is_stale_even_with_sixty_records():
    close = series(end="2026-10-05")
    out = evaluate(close)
    assert out["cycle_quality"] == "STALE"
    assert math.isnan(out["Zyklus %"])


def test_missing_symbol_currency_listing_identity_and_aware_as_of():
    assert evaluate(series(), symbol="")["cycle_quality"] == "MISSING_SOURCE"
    try:
        evaluate(series(), as_of=datetime(2026, 10, 10))
    except ValueError as exc:
        assert "timezone-aware" in str(exc)
    else:
        raise AssertionError("Naive as_of accepted")
    changed = calculate_cycle(series(), symbol="AAA", currency="EUR",
                              is_crypto=False, as_of=AS_OF)
    orig = evaluate(series())
    assert changed["cycle_currency"] == "EUR"
    assert changed["cycle_price_sha256"] != orig["cycle_price_sha256"]


def test_enrichment_refreshes_cycle_and_preserves_quality_and_formula_source(monkeypatch):
    close = series()
    frame = pd.DataFrame({
        ("Close", "AAA"): close,
        ("Volume", "AAA"): pd.Series([1000] * len(close), index=close.index),
    })
    frame.columns = pd.MultiIndex.from_tuples(frame.columns)
    monkeypatch.setitem(sys.modules, "yfinance",
                        SimpleNamespace(download=lambda **kwargs: frame))
    inp = pd.DataFrame({
        "YahooSymbol": ["AAA"], "Currency": ["USD"], "Zyklus %": [65],
        "cycle": [0.0], "cycle_quality": ["MISSING_SOURCE"],
    })
    out, report = enrich_watchlist_with_yahoo(inp, enabled=True, as_of=AS_OF)
    assert report.tickers_fetched == 1
    assert out.loc[0, "Zyklus %"] == 7.2258
    assert out.loc[0, "cycle_quality"] == "VALID"
    assert out.loc[0, "cycle_price_symbol"] == "AAA"
    assert out.loc[0, "cycle_last_bar"] == "2026-10-09"
    normalized = normalize_cycle_source(out)
    assert normalized.loc[0, "cycle"] == 7.2258
    assert normalized.loc[0, "cycle_source"] == CYCLE_SOURCE


def test_missing_symbol_and_offline_enrichment_do_not_recycle_old_number():
    old = pd.DataFrame({
        "YahooSymbol": ["AAA", ""],
        "Zyklus %": [49.0, 0.0],
        "cycle": [49.0, 0.0],
    })
    out, _ = enrich_watchlist_with_yahoo(old, enabled=False, as_of=AS_OF)
    normalized = normalize_cycle_source(out)
    assert normalized["cycle"].isna().all()
    assert normalized["cycle_quality"].tolist() == ["STALE", "MISSING_SOURCE"]


def test_unknown_quote_currency_never_gains_valid_cycle():
    result = calculate_cycle(series(), symbol="AAA", currency="", is_crypto=False, as_of=AS_OF)
    assert result["cycle_quality"] == "MISSING_SOURCE"
    assert result["cycle_quality_reason"] == "QUOTE_CURRENCY_UNKNOWN"
    assert math.isnan(result["Zyklus %"])
    assert result["cycle_currency_lineage"] == "NONE"


def test_crypto_symbol_quote_currency_must_match_input_currency():
    result = calculate_cycle(series(frequency="D"), symbol="BTC-USD",
                             currency="EUR", is_crypto=True, as_of=AS_OF)
    assert result["cycle_quality"] == "INVALID_VALUE"
    assert result["cycle_quality_reason"] == "CURRENCY_SYMBOL_QUOTE_MISMATCH"


def test_conflicting_currency_declarations_for_one_yahoo_symbol_are_quarantined(monkeypatch):
    close = series()
    frame = pd.DataFrame({
        ("Close", "AAA"): close,
        ("Volume", "AAA"): pd.Series([500] * len(close), index=close.index),
    })
    frame.columns = pd.MultiIndex.from_tuples(frame.columns)
    monkeypatch.setitem(sys.modules, "yfinance",
                        SimpleNamespace(download=lambda **kwargs: frame))
    raw = pd.DataFrame({"YahooSymbol": ["AAA", "AAA"], "Currency": ["USD", "EUR"],
                        "Zyklus %": [0.0, 0.0]})
    result, _ = enrich_watchlist_with_yahoo(raw, enabled=True, as_of=AS_OF)
    assert result["cycle_quality"].tolist() == ["INVALID_VALUE", "INVALID_VALUE"]
    assert (result["cycle_quality_reason"] == "CONFLICTING_DECLARED_CURRENCIES").all()
    assert result["Zyklus %"].isna().all()


def test_missing_stock_weekdays_not_mistaken_for_exchange_holidays():
    close = series().copy()
    close = close.drop(pd.to_datetime(["2026-10-06", "2026-10-07"]))
    result = evaluate(close)
    assert result["cycle_quality"] == "INSUFFICIENT_HISTORY"
    assert result["cycle_quality_reason"] == "GAP_IN_DAILY_BARS"


def test_no_quotes_from_provider_fail_closed(monkeypatch):
    monkeypatch.setitem(sys.modules, "yfinance",
                        SimpleNamespace(download=lambda **kwargs: pd.DataFrame()))
    out, _ = enrich_watchlist_with_yahoo(
        pd.DataFrame({"YahooSymbol": ["AAA"], "Zyklus %": [0.0]}),
        enabled=True, as_of=AS_OF,
    )
    assert out.loc[0, "cycle_quality"] == "STALE"
    assert math.isnan(out.loc[0, "Zyklus %"])


def test_research_preserves_per_value_provenance_fields():
    fields = (
        "cycle_formula_version", "cycle_price_source", "cycle_price_basis",
        "cycle_price_symbol", "cycle_currency", "cycle_last_bar",
        "cycle_as_of", "cycle_computed_at", "cycle_price_sha256",
        "cycle_eligible_bars", "cycle_quality_reason",
        "cycle_currency_lineage", "cycle_session_time_quality",
    )
    for field in fields:
        assert field in ALIASES
        assert field in KNOWN_COLUMNS
