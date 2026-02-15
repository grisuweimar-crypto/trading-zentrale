# scoring_engine/config/regime.py

REGIME_THRESHOLDS = {
    "bear": 0.0,      # trend200 < 0.0
    "neutral": 0.05,  # 0.0 .. < 0.05
    "bull": 0.05,     # >= 0.05
}

REGIME_PARAMS = {
    "bull":    {"opp_w": 0.65, "risk_w": 0.35, "risk_mult": 0.60},
    "neutral": {"opp_w": 0.55, "risk_w": 0.45, "risk_mult": 0.70},
    "bear":    {"opp_w": 0.45, "risk_w": 0.55, "risk_mult": 0.85},
}

BENCHMARKS = {
    "stock": "SPY",
    "crypto": "BTC-USD",
}
