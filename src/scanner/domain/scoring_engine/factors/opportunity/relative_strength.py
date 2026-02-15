# scoring_engine/factors/opportunity/relative_strength.py
from __future__ import annotations
from typing import Optional
import math
import pandas as pd


def _ret_over_days(close: pd.Series, days: int) -> Optional[float]:
    close = close.dropna()
    if len(close) < days + 2:
        return None
    last = float(close.iloc[-1])
    prev = float(close.iloc[-(days + 1)])
    if not (math.isfinite(last) and math.isfinite(prev)) or prev <= 0:
        return None
    return (last / prev) - 1.0


def rs_3m(asset_hist: pd.DataFrame, bench_hist: pd.DataFrame, days: int = 63) -> Optional[float]:
    """
    63 Handelstage ~ 3 Monate.
    Returns RS = asset_return - benchmark_return
    """
    if asset_hist is None or bench_hist is None:
        return None
    if "Close" not in asset_hist.columns or "Close" not in bench_hist.columns:
        return None

    a = _ret_over_days(asset_hist["Close"], days)
    b = _ret_over_days(bench_hist["Close"], days)
    if a is None or b is None:
        return None
    return float(a - b)
