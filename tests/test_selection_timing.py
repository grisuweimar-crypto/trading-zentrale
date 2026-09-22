from __future__ import annotations

import pandas as pd

from scanner.reports.selection_timing import (
    Phase1AConfig,
    _score_percentile,
    build_events,
    cooldown_events,
    cross_sectional_summary,
)


def test_score_percentile_high_score_is_high_quality():
    s = pd.Series([10.0, 20.0, 30.0])
    p = _score_percentile(s)
    assert p.tolist() == [0.0, 0.5, 1.0]


def _synthetic():
    scanner_rows = []
    prices = []
    dates = pd.bdate_range("2026-04-15", periods=20)
    for symbol, score, slope in (("A", 90.0, 2.0), ("B", 50.0, 1.0), ("C", 10.0, -1.0)):
        for day in dates[:8]:
            scanner_rows.append({
                "date": day.date().isoformat(),
                "symbol": symbol,
                "name": symbol,
                "score": score,
                "currency": "USD",
                "observation_type": "observed_scanner",
            })
        for i, day in enumerate(dates):
            prices.append({
                "date": day.date().isoformat(),
                "symbol": symbol,
                "close": 100.0 + slope * i,
                "observation_type": "price_backfill",
            })
    return pd.DataFrame(scanner_rows), pd.DataFrame(prices)


def test_events_use_forward_trading_sessions_and_no_weekend_duplicates():
    history, prices = _synthetic()
    events = build_events(history, prices, Phase1AConfig(stable_start="2026-04-15"))
    assert set(events["symbol"]) == {"A", "B", "C"}
    assert events["start_market_date"].notna().all()
    assert events["return_5t"].notna().all()


def test_cooldown_reduces_serially_repeated_events():
    history, prices = _synthetic()
    events = build_events(history, prices, Phase1AConfig(stable_start="2026-04-15"))
    reduced = cooldown_events(events, prices, sessions=5)
    assert len(reduced) < len(events)
    assert reduced.groupby("symbol").size().max() <= 2


def test_cross_sectional_summary_keeps_quality_semantics_not_trade_signal():
    history, prices = _synthetic()
    events = build_events(history, prices, Phase1AConfig(stable_start="2026-04-15"))
    summary = cross_sectional_summary(events, 5, min_n=3)
    assert summary["days"] > 0
    assert summary["mean_daily_spearman"] > 0
