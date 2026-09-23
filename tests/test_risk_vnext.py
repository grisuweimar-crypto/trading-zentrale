from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scanner.reports.risk_vnext import (
    Phase3Config,
    _attach_future_path_risk,
    _cluster_bootstrap_group_difference,
    _feature_stats,
    _horizon_time_blocks,
    analyze,
    risk_feature_rows,
)


def _synthetic_history_prices():
    dates = pd.bdate_range("2026-04-15", periods=150)
    history = []
    prices = []
    specs = (
        ("A", 20.0, 0.10),
        ("B", 35.0, 0.18),
        ("C", 50.0, 0.26),
        ("D", 65.0, 0.34),
        ("E", 80.0, 0.42),
    )
    for sidx, (symbol, risk, vol) in enumerate(specs):
        for i, day in enumerate(dates[:115]):
            history.append(
                {
                    "date": day.date().isoformat(),
                    "symbol": symbol,
                    "name": symbol,
                    "score": 70.0 - risk / 2.0 + i * 0.01,
                    "risk": risk,
                    "volatility": vol,
                    "drawdown": vol * 0.8,
                    "debt_ratio": 20.0 + sidx * 15.0,
                    "currency": "USD",
                    "r_code": "R3",
                    "observation_type": "observed_scanner",
                }
            )
        for i, day in enumerate(dates):
            trend = 100.0 + (0.24 - sidx * 0.035) * i
            wave = (sidx + 1) * 0.8 * np.sin(i / 4.0)
            value = max(5.0, trend + wave)
            prices.append(
                {
                    "date": day.date().isoformat(),
                    "symbol": symbol,
                    "close": value,
                    "adj_close": value,
                    "observation_type": "price_backfill",
                }
            )
    return pd.DataFrame(history), pd.DataFrame(prices)


def test_coverage_keeps_missing_factors_missing():
    history, _ = _synthetic_history_prices()
    rows, coverage = risk_feature_rows(history, Phase3Config(cluster_bootstrap_reps=0))
    assert coverage["features"]["aggregate_risk"]["non_null"] == len(rows)
    assert coverage["features"]["volatility"]["non_null"] == len(rows)
    assert coverage["features"]["liquidity_risk"]["non_null"] == 0
    assert coverage["features"]["liquidity_risk"]["source"] is None
    assert rows["liquidity_risk"].isna().all()
    assert coverage["missing_values_are_not_backfilled"] is True


def test_future_path_risk_uses_adjusted_path_without_shortening():
    dates = pd.bdate_range("2026-06-01", periods=70)
    values = np.full(len(dates), 100.0)
    values[1:6] = [98.0, 95.0, 90.0, 94.0, 110.0]
    prices = pd.DataFrame(
        {
            "date": dates,
            "symbol": "A",
            "close": values,
            "adj_close": values,
        }
    )
    events = pd.DataFrame(
        {
            "obs_date": [dates[0]],
            "symbol": ["A"],
            "start_market_date": [dates[0]],
        }
    )
    out = _attach_future_path_risk(events, prices)
    assert out.loc[0, "adverse_excursion_5t"] == pytest.approx(0.10)
    assert out.loc[0, "path_max_drawdown_5t"] == pytest.approx(0.10)

    prices.loc[3, "adj_close"] = np.nan
    missing = _attach_future_path_risk(events, prices)
    assert pd.isna(missing.loc[0, "adverse_excursion_5t"])
    assert pd.isna(missing.loc[0, "path_max_drawdown_5t"])


def test_phase3_is_research_only_and_separates_protection_from_return():
    history, prices = _synthetic_history_prices()
    result = analyze(
        history,
        prices,
        Phase3Config(min_feature_n=10, cluster_bootstrap_reps=20),
    )
    semantics = result["semantics"]
    assert semantics["research_only"] is True
    assert semantics["production_risk_weights_changed"] is False
    assert semantics["production_score_changed"] is False
    assert semantics["confidence_changed"] is False
    assert semantics["protection_and_return_effects_kept_separate"] is True
    assert semantics["return_and_protection_samples_decoupled"] is True
    assert semantics["uncertainty_method"] == "non-overlapping horizon-length observation-date block bootstrap"
    assert set(result["horizons"]) == {"5", "20", "40", "60"}
    assert result["horizons"]["20"]["bootstrap"]["block_length_sessions"] == 20

    stats = result["horizons"]["5"]["discovery"]["aggregate_risk"]
    assert stats is not None
    assert stats["N"] >= 10
    assert "return_N" in stats and "protection_N" in stats
    assert "spearman_risk_vs_adverse_excursion" in stats
    assert "spearman_risk_vs_peer_excess" in stats
    assert stats["quantile_status"] == "available"
    assert "protection_gap_bootstrap_95" in stats
    assert "low_risk_alpha_advantage_bootstrap_95" in stats


def test_return_and_protection_samples_are_independent():
    dates = pd.to_datetime(["2026-08-03"] * 10 + ["2026-08-10"] * 10)
    frame = pd.DataFrame(
        {
            "obs_date": dates,
            "symbol": [f"S{i}" for i in range(20)],
            "volatility": np.linspace(0.1, 0.9, 20),
            "peer_excess_5t": np.linspace(0.05, -0.05, 20),
            "return_5t": np.linspace(0.06, -0.04, 20),
            "adverse_excursion_5t": np.linspace(0.01, 0.20, 20),
            "path_max_drawdown_5t": np.linspace(0.02, 0.25, 20),
        }
    )
    frame.loc[:4, ["adverse_excursion_5t", "path_max_drawdown_5t"]] = np.nan
    stats = _feature_stats(
        frame,
        "volatility",
        5,
        Phase3Config(min_feature_n=5, cluster_bootstrap_reps=10),
        ("test",),
    )
    assert stats is not None
    assert stats["return_N"] == 20
    assert stats["protection_N"] == 15
    assert stats["return_quantile_status"] == "available"
    assert stats["protection_quantile_status"] == "available"


def test_unavailable_bootstrap_is_not_reported_as_zero_width_95_interval():
    frame = pd.DataFrame(
        {
            "obs_date": pd.to_datetime(["2026-08-03"] * 4),
            "target": [0.1, 0.2, -0.1, -0.2],
            "risk_group": ["high_risk", "high_risk", "low_risk", "low_risk"],
        }
    )
    assert _cluster_bootstrap_group_difference(
        frame, "target", "risk_group", "high_risk", "low_risk", 50, 1, 1
    ) is None

    two_days = frame.copy()
    two_days.loc[2:, "obs_date"] = pd.Timestamp("2026-08-10")
    assert _cluster_bootstrap_group_difference(
        two_days, "target", "risk_group", "high_risk", "low_risk", 0, 1, 1
    ) is None


def test_horizon_blocks_keep_dates_together_and_drop_partial_tail():
    dates = pd.bdate_range("2026-08-03", periods=12)
    rows = []
    for day in dates:
        rows.append({"obs_date": day, "marker": f"{day.date()}-A"})
        rows.append({"obs_date": day, "marker": f"{day.date()}-B"})
    frame = pd.DataFrame(rows)

    blocks = _horizon_time_blocks(frame, 5)
    assert len(blocks) == 2
    assert [block["obs_date"].nunique() for block in blocks] == [5, 5]
    assert [len(block) for block in blocks] == [10, 10]
    assert blocks[0]["obs_date"].min() == dates[0]
    assert blocks[0]["obs_date"].max() == dates[4]
    assert blocks[1]["obs_date"].min() == dates[5]
    assert blocks[1]["obs_date"].max() == dates[9]
    assert not any(block["obs_date"].isin(dates[10:]).any() for block in blocks)


def test_horizon_block_bootstrap_requires_two_complete_blocks():
    dates = pd.bdate_range("2026-08-03", periods=9)
    frame = pd.DataFrame(
        {
            "obs_date": np.repeat(dates, 2),
            "target": np.tile([0.1, -0.1], len(dates)),
            "risk_group": np.tile(["high_risk", "low_risk"], len(dates)),
        }
    )
    assert _cluster_bootstrap_group_difference(
        frame, "target", "risk_group", "high_risk", "low_risk", 50, 7, 5
    ) is None

    tenth = pd.DataFrame(
        {
            "obs_date": [pd.bdate_range("2026-08-03", periods=10)[-1]] * 2,
            "target": [0.1, -0.1],
            "risk_group": ["high_risk", "low_risk"],
        }
    )
    complete = pd.concat([frame, tenth], ignore_index=True)
    interval = _cluster_bootstrap_group_difference(
        complete, "target", "risk_group", "high_risk", "low_risk", 50, 7, 5
    )
    assert interval is not None
    assert len(interval) == 2


def test_invalid_phase3_config_rejected():
    with pytest.raises(ValueError):
        Phase3Config(quantile=0.6)
    with pytest.raises(ValueError):
        Phase3Config(cluster_bootstrap_reps=-1)
    with pytest.raises(ValueError):
        Phase3Config(tail_drawdown_threshold=1.0)
