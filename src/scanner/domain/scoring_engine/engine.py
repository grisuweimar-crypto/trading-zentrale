"""scanner.domain.scoring_engine.engine

Canonical scoring entrypoint for Scanner_vNext.

Goals:
1) Never hide failures behind a fake 0-score.
2) Be self-contained (no legacy top-level imports like `scoring_engine.*` or `market.*`).
3) Keep outputs explainable: final score + opportunity/risk + factor breakdown + meta.
"""

from __future__ import annotations

from typing import Any, Dict, Optional
import math

from scanner.domain.scoring_engine.config.weights import OPPORTUNITY_WEIGHTS, RISK_WEIGHTS
from scanner.domain.scoring_engine.config.regime import REGIME_PARAMS, BENCHMARKS
from scanner.domain.scoring_engine.scoring.final_score import compute_scores
from scanner.domain.scoring_engine.factors.universe_csv import (
    Universe,
    load_universe,
    get_row_by_ticker,
    scale_from_universe,
)
from scanner.domain.scoring_engine.factors.map_from_csv import build_raw_from_csv_row
from scanner.domain.scoring_engine.quality.confidence import compute_confidence


def _is_crypto_identifier(s: str) -> bool:
    t = str(s).upper()
    return t.endswith("-USD") or t in {"BTC-USD", "ETH-USD"} or "CRYPTO" in t


def _infer_asset_class(identifier: str) -> str:
    return "crypto" if _is_crypto_identifier(identifier) else "stock"


def _classify_regime_from_trend200(trend200: Optional[float]) -> str:
    """Fallback regime classifier if regime label isn't present."""
    if trend200 is None:
        return "neutral"
    try:
        t = float(trend200)
        if not math.isfinite(t):
            return "neutral"
        if t < 0:
            return "bear"
        if t < 0.05:
            return "neutral"
        return "bull"
    except Exception:
        return "neutral"


def _regime_from_row(row, asset_class: str) -> Dict[str, Any]:
    """Use precomputed columns from the watchlist if present.

    Your CSV already contains:
      - MarketRegimeStock / MarketTrend200Stock
      - MarketRegimeCrypto / MarketTrend200Crypto
    We use those instead of fetching benchmarks online.
    """

    if asset_class == "crypto":
        regime = str(row.get("MarketRegimeCrypto", "") or "").strip().lower()
        trend200 = row.get("MarketTrend200Crypto", None)
        benchmark = BENCHMARKS["crypto"]
    else:
        regime = str(row.get("MarketRegimeStock", "") or "").strip().lower()
        trend200 = row.get("MarketTrend200Stock", None)
        benchmark = BENCHMARKS["stock"]

    # Normalize / fallback
    if regime not in {"bull", "neutral", "bear"}:
        regime = _classify_regime_from_trend200(trend200)

    p = REGIME_PARAMS.get(regime, REGIME_PARAMS["neutral"])

    return {
        "market_regime": regime,
        "market_trend200": None if trend200 is None else float(trend200),
        "market_benchmark": benchmark,
        "asset_class": asset_class,
        "opp_w": float(p["opp_w"]),
        "risk_w": float(p["risk_w"]),
        "risk_mult": float(p["risk_mult"]),
    }


def calculate_scores_v6_from_row(
    row,
    universe: Universe,
    identifier: str,
) -> Dict[str, Any]:
    """Score a single CSV row.

    Returns a dict with at least:
      - score (alias of final_score)
      - final_score, opportunity_score, risk_score
      - confidence_score / label / breakdown
      - meta + factor_breakdown

    On failure, returns {"error": "..."}.
    """

    try:
        raw = build_raw_from_csv_row(row)

        asset_class = _infer_asset_class(identifier)
        reg = _regime_from_row(row, asset_class)

        is_buy = "BUY" in str(raw.get("elliott_signal", "")).upper()

        # ---- Opportunity (0..1; higher = better) ----
        opportunity: Dict[str, float] = {
            "growth": scale_from_universe(universe, "Growth %", raw.get("growth_pct", None)),
            "roe": scale_from_universe(universe, "ROE %", raw.get("roe_pct", None)),
            "margin": scale_from_universe(universe, "Margin %", raw.get("margin_pct", None)),
            "mc_prob": scale_from_universe(universe, "MC-Chance", raw.get("mc_chance", None)),
            "analyst": 0.5,  # placeholder until you add an analyst column
            "elliott_quality": 0.7 if is_buy else 0.4,
            "trend_200dma": scale_from_universe(universe, "Trend200", raw.get("trend200", None)),
            "relative_strength": scale_from_universe(universe, "RS3M", raw.get("rs3m", None)),
            "target_distance": scale_from_universe(universe, "TargetDistance", raw.get("target_distance", None)),
            "upside": 0.5,
        }

        # Upside from Elliott target vs price (only if BUY)
        price = raw.get("current_price", None)
        target = raw.get("elliott_target", None)
        if is_buy and price is not None and target is not None and float(price) > 0:
            upside = (float(target) / float(price)) - 1.0
            # Clamp to 0..1 with 30% as full score
            opportunity["upside"] = max(0.0, min(upside / 0.30, 1.0))

        # ---- Risk (0..1; higher = riskier) ----
        crv_raw = raw.get("crv", None)
        if not is_buy:
            crv_fragility = 0.5
        else:
            if crv_raw is None or float(crv_raw) <= 0:
                crv_fragility = 0.5
            else:
                crv_scaled = scale_from_universe(universe, "CRV", float(crv_raw))
                crv_fragility = 1.0 - crv_scaled

        vol_scaled = scale_from_universe(universe, "Volatility", raw.get("volatility", None))
        down_scaled = scale_from_universe(universe, "DownsideDev", raw.get("downside_dev", None))
        mdd_scaled = scale_from_universe(universe, "MaxDrawdown", raw.get("max_drawdown", None))
        dv_scaled = scale_from_universe(universe, "DollarVolume", raw.get("dollar_volume", None))
        av_scaled = scale_from_universe(universe, "AvgVolume", raw.get("avg_volume", None))

        # Prefer DollarVolume; if missing, fallback to AvgVolume
        liq_good = dv_scaled if raw.get("dollar_volume", None) is not None else av_scaled
        liquidity_risk = 1.0 - liq_good

        risk: Dict[str, float] = {
            "debt_to_equity": scale_from_universe(universe, "Debt/Equity", raw.get("debt_to_equity", None)),
            "crv_fragility": crv_fragility,
            "volatility": vol_scaled,
            "downside_dev": down_scaled,
            "max_drawdown": mdd_scaled,
            "liquidity_risk": liquidity_risk,
            "beta": 0.5,
        }

        # ---- Score ----
        result = compute_scores(
            opportunity_factors_0_1=opportunity,
            risk_factors_0_1=risk,
            opp_weights=OPPORTUNITY_WEIGHTS,
            risk_weights=RISK_WEIGHTS,
            risk_multiplier=reg["risk_mult"],
            opp_weight=reg["opp_w"],
            risk_weight=reg["risk_w"],
        )

        # Meta enrichment
        result["meta"].update(
            {
                "market_regime": reg["market_regime"],
                "market_trend200": reg["market_trend200"],
                "market_benchmark": reg["market_benchmark"],
                "asset_class": reg["asset_class"],
            }
        )

        # Convenience alias (many parts of vNext expect `score`)
        result["score"] = result["final_score"]

        # ---- Confidence ----
        # Keep this self-contained: provide sane defaults in-code.
        confidence_config = {
            "CONFIDENCE_WEIGHTS": {
                "coverage": 0.25,
                "confluence": 0.25,
                "risk_clean": 0.20,
                "regime_align": 0.20,
                "liquidity": 0.10,
            },
            "CONFIDENCE_CORE_FACTORS": [
                "growth",
                "roe",
                "margin",
                "debt_to_equity",
                "volatility",
                "relative_strength",
                "trend_200dma",
            ],
            "CONFIDENCE_OPPORTUNITY_FACTORS": [
                "growth",
                "roe",
                "margin",
                "relative_strength",
                "trend_200dma",
            ],
            "CONFIDENCE_RISK_FACTORS": [
                "volatility",
                "max_drawdown",
                "debt_to_equity",
            ],
            "CONFIDENCE_THRESHOLDS": {"HIGH": 75, "MED": 50},
        }

        all_factors_0_1 = {**opportunity, **risk}
        conf = compute_confidence(all_factors_0_1, result["meta"], confidence_config)
        result["confidence_score"] = conf["confidence_score"]
        result["confidence_label"] = conf["confidence_label"]
        result["confidence_breakdown"] = conf["confidence_breakdown"]

        return result

    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


def calculate_final_score_v6_from_csv(
    ticker: str,
    csv_path: str = "watchlist.csv",
) -> Dict[str, Any]:
    """Backward compatible wrapper: load universe, find row, score it."""

    universe = load_universe(csv_path)
    row = get_row_by_ticker(universe, ticker)
    if row is None:
        return {"error": f"Asset '{ticker}' not found in {csv_path}"}

    return calculate_scores_v6_from_row(row, universe, identifier=ticker)
