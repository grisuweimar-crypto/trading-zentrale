from __future__ import annotations
import math
from typing import Dict, Any
import pandas as pd


def _is_finite(x: float) -> bool:
    return x is not None and isinstance(x, (int, float)) and math.isfinite(float(x))


def compute_returns(close: pd.Series) -> pd.Series:
    close = close.dropna()
    if close.empty:
        return close
    return close.pct_change().dropna()


def compute_volatility(returns: pd.Series) -> float | None:
    if returns is None or returns.empty:
        return None
    v = float(returns.std())
    return v if _is_finite(v) else None


def compute_downside_dev(returns: pd.Series) -> float | None:
    if returns is None or returns.empty:
        return None
    neg = returns[returns < 0]
    if neg.empty:
        return 0.0
    v = float(neg.std())
    return v if _is_finite(v) else None


def compute_max_drawdown(close: pd.Series) -> float | None:
    close = close.dropna()
    if close.empty:
        return None
    peak = close.cummax()
    dd = (close / peak) - 1.0
    mdd = float(dd.min())  # negative number (e.g. -0.22)
    if not _is_finite(mdd):
        return None
    return abs(mdd)  # return as positive drawdown magnitude (0..1+)


def price_risk_features_from_hist(hist: pd.DataFrame) -> Dict[str, Any]:
    """
    hist expected to have a 'Close' column (yfinance style).
    Returns RAW risk values (NOT scaled): volatility, downside_dev, max_drawdown
    """
    if hist is None or hist.empty or "Close" not in hist.columns:
        return {"volatility": None, "downside_dev": None, "max_drawdown": None}

    close = hist["Close"]
    rets = compute_returns(close)

    return {
        "volatility": compute_volatility(rets),
        "downside_dev": compute_downside_dev(rets),
        "max_drawdown": compute_max_drawdown(close),
    }
