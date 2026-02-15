# scoring_engine/regime/market_regime.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, Any
import math
import time

from market.yahoo import get_price_data
from scoring_engine.config.regime import REGIME_PARAMS, BENCHMARKS


@dataclass(frozen=True)
class RegimeResult:
    asset_class: str           # "stock" | "crypto"
    benchmark: str
    trend200: Optional[float]  # raw trend200 (e.g. 0.08)
    regime: str                # "bull" | "neutral" | "bear"
    opp_w: float
    risk_w: float
    risk_mult: float


# simple in-process cache (prevents repeated benchmark calls)
_CACHE: Dict[str, Dict[str, Any]] = {}  # key -> {"ts":..., "res": RegimeResult}
_CACHE_TTL_SECONDS = 60 * 30           # 30 minutes


def _trend200_from_hist(hist) -> Optional[float]:
    try:
        if hist is None or hist.empty or "Close" not in hist.columns:
            return None
        close = hist["Close"].dropna()
        if len(close) < 200:
            return None
        sma200 = float(close.tail(200).mean())
        last = float(close.iloc[-1])
        if not (math.isfinite(sma200) and math.isfinite(last)) or sma200 <= 0:
            return None
        return (last / sma200) - 1.0
    except Exception:
        return None


def classify_regime(trend200: Optional[float]) -> str:
    # conservative: if unknown => neutral
    if trend200 is None or not math.isfinite(float(trend200)):
        return "neutral"
    if trend200 < 0:
        return "bear"
    if trend200 < 0.05:
        return "neutral"
    return "bull"


def get_market_regime(asset_class: str) -> RegimeResult:
    """
    asset_class: "stock" or "crypto"
    Uses SPY for stocks, BTC-USD for crypto.
    Cached for TTL to keep scans fast.
    """
    asset_class = "crypto" if asset_class == "crypto" else "stock"
    key = asset_class

    now = time.time()
    cached = _CACHE.get(key)
    if cached and (now - cached["ts"] < _CACHE_TTL_SECONDS):
        return cached["res"]

    bench = BENCHMARKS[asset_class]
    hist = get_price_data(bench)  # your get_price_data returns a DataFrame
    t200 = _trend200_from_hist(hist)
    regime = classify_regime(t200)
    p = REGIME_PARAMS[regime]

    res = RegimeResult(
        asset_class=asset_class,
        benchmark=bench,
        trend200=t200,
        regime=regime,
        opp_w=float(p["opp_w"]),
        risk_w=float(p["risk_w"]),
        risk_mult=float(p["risk_mult"]),
    )
    _CACHE[key] = {"ts": now, "res": res}
    return res
