from __future__ import annotations

import pandas as pd

from scanner.reports.alpha_probability import (
    Phase2Config,
    _bh_adjust,
    _conservative_interval,
    _segment_stats,
    _selection_band,
    _selection_window,
)


def test_selection_bands_are_fixed_phase1a_backbone():
    assert _selection_band(0.0, 0.99) == "B0_score0"
    assert _selection_band(10.0, 0.19) == "B1"
    assert _selection_band(10.0, 0.20) == "B2"
    assert _selection_band(10.0, 0.45) == "B3"
    assert _selection_band(10.0, 0.75) == "B4"
    assert _selection_band(10.0, 0.90) == "B5"


def test_shrinkage_pulls_small_sample_toward_baseline():
    days = pd.bdate_range("2026-08-03", periods=4)
    frame = pd.DataFrame({
        "obs_date": days,
        "symbol": ["A", "B", "C", "D"],
        "peer_excess_5t": [0.1, 0.2, 0.3, -0.1],
    })
    out = _segment_stats(
        frame,
        "peer_excess_5t",
        baseline_rate=0.50,
        config=Phase2Config(prior_strength=20, bootstrap_rounds=30),
        label="unit",
    )
    assert out is not None
    assert out["raw_outperformance_probability"] == 0.75
    assert 0.50 < out["outperformance_probability"] < 0.75
    assert out["probability_advantage"] > 0
    assert out["uncertainty"]["probability_ci95"] is not None


def test_conservative_interval_envelopes_both_cluster_intervals():
    assert _conservative_interval([0.4, 0.6], [0.35, 0.55]) == [0.35, 0.6]
    assert _conservative_interval(None, [0.4, 0.5]) == [0.4, 0.5]


def test_bh_adjustment_is_monotone_and_bounded():
    adjusted = _bh_adjust([(0, 0.01), (1, 0.04), (2, 0.03)])
    assert 0 <= adjusted[0] <= adjusted[2] <= adjusted[1] <= 1
    assert round(adjusted[0], 6) == 0.03


def test_selection_window_keeps_selection_and_probability_separate():
    dates = pd.bdate_range("2026-08-03", periods=12)
    prices = []
    rows = []
    for symbol, score, pct, alpha in (
        ("LOW", 10.0, 0.10, -0.03),
        ("HIGH", 90.0, 0.95, 0.03),
    ):
        for i, day in enumerate(dates):
            prices.append({
                "date": day,
                "symbol": symbol,
                "close": 100 + i,
                "adj_close": 100 + i,
            })
            if i in (0, 5, 10):
                rows.append({
                    "obs_date": day,
                    "symbol": symbol,
                    "score": score,
                    "score_pct_full": pct,
                    "peer_excess_5t": alpha,
                    "start_market_date": day,
                })
    out = _selection_window(
        pd.DataFrame(rows),
        pd.DataFrame(prices),
        5,
        Phase2Config(
            cooldown_sessions=5,
            min_selection_n=2,
            prior_strength=2,
            bootstrap_rounds=30,
        ),
        "validation",
    )
    assert out["bands"]["B5"]["probability_advantage"] > 0
    assert out["bands"]["B1"]["probability_advantage"] < 0
    assert "selection_score" not in out["bands"]["B5"]
