from __future__ import annotations

import pandas as pd

from scanner.reports.timing_patterns import (
    Phase1BConfig,
    feature_rows,
    build_timing_events,
    continuous_feature_summary,
)


def _synthetic():
    dates = pd.bdate_range("2026-04-15", periods=80)
    scanner = []
    prices = []
    for symbol, base, slope in (("A", 20.0, 1.0), ("B", 10.0, 0.4), ("C", 5.0, -0.2)):
        for i, day in enumerate(dates[:50]):
            scanner.append({
                "date": day.date().isoformat(),
                "symbol": symbol,
                "name": symbol,
                "score": base + 0.01 * i * i,
                "opportunity": 40 + i * 0.2,
                "risk": 50 - i * 0.1,
                "rs3m": -0.2 + i * 0.01,
                "trend200": -0.1 + i * 0.005,
                "cycle": float((i * 3) % 100),
                "currency": "USD",
                "r_code": "R3" if i < 25 else "R4",
                "observation_type": "observed_scanner",
            })
        for i, day in enumerate(dates):
            prices.append({
                "date": day.date().isoformat(),
                "symbol": symbol,
                "close": 100 + slope * i,
                "observation_type": "price_backfill",
            })
    return pd.DataFrame(scanner), pd.DataFrame(prices)


def test_feature_rows_create_point_in_time_deltas_and_r_changes():
    history, _ = _synthetic()
    frame, coverage = feature_rows(history)
    assert "score_d5" in frame.columns
    assert "rs3m_d10" in frame.columns
    assert "r_d1" in frame.columns
    assert coverage["sources"]["rs3m"] == "rs3m"
    assert frame["r_upgrade"].any()


def test_elliott_is_reported_missing_instead_of_invented():
    history, _ = _synthetic()
    _, coverage = feature_rows(history)
    assert coverage["sources"]["elliott"] is None
    assert coverage["non_null"]["elliott_state"] == 0


def test_build_timing_events_preserves_future_horizons():
    history, prices = _synthetic()
    events, coverage = build_timing_events(history, prices)
    assert len(events) > 0
    assert "score_d5" in events.columns
    assert events["return_20t"].notna().any()
    assert coverage["rows"] > 0


def test_continuous_summary_uses_deltas_not_score_as_signal():
    history, prices = _synthetic()
    events, _ = build_timing_events(history, prices)
    out = continuous_feature_summary(events, prices, 5, Phase1BConfig(min_single_n=5))
    assert "score_d5" in out
    assert "score" not in out


def test_same_day_rerun_uses_latest_row_via_phase1a_loader():
    history, _ = _synthetic()
    extra = history.iloc[[0]].copy()
    extra["score"] = 99.0
    history = pd.concat([history, extra], ignore_index=True)
    frame, _ = feature_rows(history)
    first_day = pd.Timestamp(history.iloc[0]["date"])
    row = frame[(frame["symbol"] == "A") & (frame["date"] == first_day)].iloc[0]
    assert row["score"] == 99.0
