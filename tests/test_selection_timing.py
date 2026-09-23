from __future__ import annotations

import json
import math

import pandas as pd

from scanner.reports.selection_timing import (
    Phase1AConfig,
    _peer_median,
    _peer_medians,
    _r_score_backbone,
    _scanner_rows,
    _score_percentile,
    analyze,
    build_events,
    cooldown_events,
    cross_sectional_summary,
    future_rank_bands,
)


def test_score_percentile_high_score_is_high_quality():
    s = pd.Series([10.0, 20.0, 30.0])
    assert _score_percentile(s).tolist() == [0.0, 0.5, 1.0]


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
            value = 100.0 + slope * i
            prices.append({
                "date": day.date().isoformat(),
                "symbol": symbol,
                "close": value,
                "adj_close": value,
                "observation_type": "price_backfill",
            })
    return pd.DataFrame(scanner_rows), pd.DataFrame(prices)


def test_latest_same_day_rerun_preserves_source_order():
    history = pd.DataFrame([
        {"date": "2026-09-18", "symbol": "A", "score": 10, "observation_type": "observed_scanner"},
        {"date": "2026-09-18", "symbol": "B", "score": 20, "observation_type": "observed_scanner"},
        {"date": "2026-09-18", "symbol": "A", "score": 30, "observation_type": "observed_scanner"},
        {"date": "2026-09-18", "symbol": "B", "score": 40, "observation_type": "observed_scanner"},
    ])
    rows = _scanner_rows(history).set_index("symbol")
    assert rows.loc["A", "score"] == 30
    assert rows.loc["B", "score"] == 40


def test_no_eligible_events_returns_zero_coverage():
    history, prices = _synthetic()
    result = analyze(history, prices, Phase1AConfig(stable_start="2030-01-01"))
    assert result["coverage"]["events"] == 0
    assert result["horizons"]["5"]["cross_sectional_selection"]["days"] == 0


def test_events_use_forward_trading_sessions():
    history, prices = _synthetic()
    events = build_events(history, prices, Phase1AConfig(stable_start="2026-04-15"))
    assert set(events["symbol"]) == {"A", "B", "C"}
    assert events["start_market_date"].notna().all()
    assert events["return_5t"].notna().all()


def test_forward_returns_use_adjusted_close_not_reverse_split_jump():
    dates = pd.bdate_range("2026-05-08", periods=7)
    history = pd.DataFrame([{
        "date": dates[0].date().isoformat(),
        "symbol": "SMX",
        "name": "SMX",
        "score": 50.0,
        "currency": "USD",
        "observation_type": "observed_scanner",
    }])
    prices = []
    for i, day in enumerate(dates):
        raw = 2.54 + i * 0.01 if i == 0 else 33.0 + i * 0.032
        adjusted = 33.0 + i * 0.032
        prices.append({
            "date": day.date().isoformat(),
            "symbol": "SMX",
            "close": raw,
            "adj_close": adjusted,
            "observation_type": "price_backfill",
        })
    events = build_events(history, pd.DataFrame(prices), Phase1AConfig(stable_start="2026-05-01"))
    assert len(events) == 1
    observed = float(events.iloc[0]["return_5t"])
    expected = prices[5]["adj_close"] / prices[0]["adj_close"] - 1.0
    raw_false_return = prices[5]["close"] / prices[0]["close"] - 1.0
    assert abs(observed - expected) < 1e-12
    assert raw_false_return > 10.0
    assert observed < 0.02


def test_missing_adjusted_close_never_falls_back_to_raw_close():
    history, prices = _synthetic()
    legacy = prices.drop(columns=["adj_close"])
    try:
        build_events(history, legacy, Phase1AConfig(stable_start="2026-04-15"))
    except ValueError as exc:
        assert "adjusted_close_required" in str(exc)
    else:
        raise AssertionError("legacy raw close must not be used as research return")


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


def test_leave_one_out_peer_uses_global_fallback_for_singleton_currency():
    day = pd.Timestamp("2026-05-01")
    events = pd.DataFrame([
        {"obs_date": day, "symbol": "BRL1", "currency": "BRL", "return_5t": 0.30},
        {"obs_date": day, "symbol": "USD1", "currency": "USD", "return_5t": 0.10},
        {"obs_date": day, "symbol": "USD2", "currency": "USD", "return_5t": 0.20},
    ])
    baselines, fallback = _peer_medians(events, "return_5t")
    brl = events.iloc[0]
    usd1 = events.iloc[1]
    # BRL1 has no BRL peer: exclude itself and use median(USD1, USD2)=0.15.
    assert abs(_peer_median(brl, baselines, fallback) - 0.15) < 1e-12
    # USD1 has a same-currency peer and therefore compares to USD2 only.
    assert abs(_peer_median(usd1, baselines, fallback) - 0.20) < 1e-12


def test_empty_quality_bands_do_not_emit_nan_json():
    day = pd.Timestamp("2026-05-01")
    events = pd.DataFrame({
        "obs_date": [day] * 20,
        "score_pct_full": [1.0] * 20,
        "return_5t": [i / 100.0 for i in range(20)],
    })
    bands = future_rank_bands(events, 5, min_n=20)
    assert set(bands) == {"Q5_high"}
    json.dumps(bands, allow_nan=False)
    assert not any(
        isinstance(value, float) and math.isnan(value)
        for metrics in bands.values() for value in metrics.values()
    )


def test_r_score_backbone_uses_r_code_percentile_thresholds_without_gates():
    assert _r_score_backbone(0, 0.99) == "B0_score0"
    assert _r_score_backbone(10, 0.19) == "B1"
    assert _r_score_backbone(10, 0.20) == "B2"
    assert _r_score_backbone(10, 0.45) == "B3"
    assert _r_score_backbone(10, 0.75) == "B4"
    assert _r_score_backbone(10, 0.90) == "B5"
