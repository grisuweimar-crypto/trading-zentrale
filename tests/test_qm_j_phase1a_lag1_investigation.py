from __future__ import annotations

import pandas as pd

from scanner.research.governance.qm_j_phase1a_lag1_investigation import run_investigation


def _inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    symbols = [f"S{i:02d}" for i in range(20)]
    days = pd.bdate_range("2026-01-02", periods=90)
    prices = []
    for j, symbol in enumerate(symbols):
        for t, day in enumerate(days):
            close = 50.0 + j * 2.0 + (0.05 + j * 0.005) * t + 0.001 * (j + 1) * t**2
            prices.append({"date": day, "symbol": symbol, "close": close, "adj_close": close})
    history = []
    for k, day in enumerate(days[::2][:36]):
        for j, symbol in enumerate(symbols):
            history.append({
                "date": day,
                "symbol": symbol,
                "score": float((j * 5 + k * 2 + (j * k) % 7) % 100),
                "observation_type": "observed_scanner",
                "currency": "USD",
                "name": symbol,
                "sector": "test",
            })
    return pd.DataFrame(history), pd.DataFrame(prices)


def test_investigation_is_descriptive_and_nonmutating() -> None:
    history, prices = _inputs()
    before_history = history.copy(deep=True)
    before_prices = prices.copy(deep=True)
    result = run_investigation(
        history,
        prices,
        stable_start="2026-01-01",
        min_cross_section=20,
        horizon=20,
    )
    assert history.equals(before_history)
    assert prices.equals(before_prices)
    assert result["finding_id"] == "QM-H-QMJ-PHASE1A-LAG1-001"
    assert result["root_cause_assigned_by_code"] is False
    assert result["diagnostic_thresholds_introduced"] is False
    assert result["promotion_performed"] is False
    assert result["production_logic_changed"] is False
    assert result["interpretation_guard"]["descriptive_only"] is True
    assert result["interpretation_guard"]["post_trigger"] is True
    assert result["interpretation_guard"]["may_not_be_used_as_confirmation"] is True
    assert result["coverage"]["matched_observations"] > 0
    assert result["coverage"]["matched_days"] > 0


def test_investigation_is_deterministic() -> None:
    history, prices = _inputs()
    first = run_investigation(history, prices, stable_start="2026-01-01", min_cross_section=20, horizon=20)
    second = run_investigation(history, prices, stable_start="2026-01-01", min_cross_section=20, horizon=20)
    assert first == second
