import pandas as pd
import pytest

from scanner.research.elliott_vnext.pivots import (
    DEFAULT_PIVOT_SPECS,
    PivotSpec,
    aggregate_weekly,
    detect_confirmed_pivots,
    prepare_daily_ohlcv,
)


def _row(day, close, *, high=None, low=None, open_=None, adj_close=None, symbol="TEST"):
    high = close + 1 if high is None else high
    low = close - 1 if low is None else low
    open_ = close if open_ is None else open_
    return {
        "date": day,
        "symbol": symbol,
        "currency": "USD",
        "open": str(open_),
        "high": str(high),
        "low": str(low),
        "close": str(close),
        "adj_close": "" if adj_close is None else str(adj_close),
        "volume": "1000",
        "retrieved_at": "2026-09-25T00:00:00Z",
    }


def _daily_frame():
    dates = pd.date_range("2026-01-05", periods=8, freq="D")
    high = [10.0, 11.0, 15.0, 12.0, 11.0, 10.0, 13.0, 11.0]
    low = [8.0, 9.0, 11.0, 10.0, 9.0, 7.0, 9.0, 8.0]
    close = [9.0, 10.0, 13.0, 11.0, 10.0, 8.0, 12.0, 9.0]
    return pd.DataFrame(
        {
            "date": dates,
            "symbol": "TEST",
            "currency": "USD",
            "open": close,
            "high": high,
            "low": low,
            "close": close,
            "volume": 1000.0,
            "timeframe": "daily",
            "price_basis": "raw_unadjusted",
            "bar_confirmed_time": dates,
            "session_count": 1,
        }
    )


def test_prepare_daily_adjusts_all_ohlc_with_adj_close_ratio():
    rows = [
        _row("2026-01-05", 100, high=110, low=90, open_=100, adj_close=50),
        _row("2026-01-06", 50, high=55, low=45, open_=50, adj_close=50),
    ]
    frame = prepare_daily_ohlcv(rows, "TEST", price_basis="auto")
    assert frame["price_basis"].unique().tolist() == ["adjusted_via_adj_close_ratio"]
    assert frame["close"].tolist() == [50.0, 50.0]
    assert frame["high"].tolist() == [55.0, 55.0]
    assert frame["low"].tolist() == [45.0, 45.0]


def test_prepare_daily_fails_closed_on_partial_adjusted_coverage():
    rows = [
        _row("2026-01-05", 100, adj_close=99),
        _row("2026-01-06", 101, adj_close=None),
    ]
    with pytest.raises(ValueError, match="partial_adjusted_close_coverage"):
        prepare_daily_ohlcv(rows, "TEST", price_basis="auto")


def test_weekly_bar_is_not_visible_until_next_week_is_observed():
    rows = [
        _row("2026-01-05", 10, adj_close=10),
        _row("2026-01-06", 11, adj_close=11),
        _row("2026-01-07", 12, adj_close=12),
        _row("2026-01-08", 13, adj_close=13),
        _row("2026-01-09", 14, adj_close=14),
        _row("2026-01-12", 15, adj_close=15),
    ]
    daily = prepare_daily_ohlcv(rows, "TEST")

    before_next_week = aggregate_weekly(daily, as_of="2026-01-09", require_confirmed=True)
    assert before_next_week.empty

    after_next_week_starts = aggregate_weekly(daily, as_of="2026-01-12", require_confirmed=True)
    assert len(after_next_week_starts) == 1
    bar = after_next_week_starts.iloc[0]
    assert bar["date"].date().isoformat() == "2026-01-09"
    assert bar["bar_confirmed_time"].date().isoformat() == "2026-01-12"
    assert bar["open"] == 10.0
    assert bar["close"] == 14.0
    assert bar["session_count"] == 5


def test_pivot_is_hidden_before_confirmed_time_and_visible_afterwards():
    frame = _daily_frame()
    spec = PivotSpec(
        degree="test",
        timeframe="daily",
        left_bars=1,
        right_bars=1,
        atr_window=1,
        min_excursion_atr=0.0,
    )

    before = detect_confirmed_pivots(frame, spec, as_of="2026-01-07")
    after = detect_confirmed_pivots(frame, spec, as_of="2026-01-08")

    assert not any(p["pivot_time"] == "2026-01-07" and p["kind"] == "high" for p in before)
    high = next(p for p in after if p["pivot_time"] == "2026-01-07" and p["kind"] == "high")
    assert high["confirmed_time"] == "2026-01-08"
    assert high["available_from"] == "2026-01-08"
    assert high["price"] == 15.0


def test_confirmed_pivot_history_is_prefix_invariant():
    frame = _daily_frame()
    spec = PivotSpec(
        degree="test",
        timeframe="daily",
        left_bars=1,
        right_bars=1,
        atr_window=1,
        min_excursion_atr=0.0,
    )
    cutoff = pd.Timestamp("2026-01-09")

    full_as_of = detect_confirmed_pivots(frame, spec, as_of=cutoff)
    prefix = frame.loc[frame["date"] <= cutoff].copy()
    prefix_result = detect_confirmed_pivots(prefix, spec)

    assert full_as_of == prefix_result


def test_default_degree_parameters_are_explicitly_unvalidated_research_candidates():
    assert DEFAULT_PIVOT_SPECS
    assert all(spec.validated is False for spec in DEFAULT_PIVOT_SPECS)
    assert {spec.timeframe for spec in DEFAULT_PIVOT_SPECS} == {"daily", "weekly"}


def test_pivot_records_remain_research_only_and_do_not_emit_actions():
    frame = _daily_frame()
    spec = PivotSpec("test", "daily", 1, 1, 1, 0.0)
    pivots = detect_confirmed_pivots(frame, spec, as_of="2026-01-12")
    assert pivots
    for pivot in pivots:
        assert pivot["research_only"] is True
        assert "trade_decision" not in pivot
        assert "order_instruction" not in pivot
