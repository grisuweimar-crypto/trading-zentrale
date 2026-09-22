"""
Confidence Score Module für Setup-Qualität und Stabilität.

Bewertet nicht nur Score-Höhe, sondern auch Verlässlichkeit der Datenbasis.
Output: Confidence Score (0-100) + Badge + Breakdown.

Canonical factor names follow the current scoring engine. Legacy aliases remain
supported so older callers and historical code paths do not silently degrade.
"""

from typing import Any, Dict
import logging

import pandas as pd

logger = logging.getLogger(__name__)


# Canonical scoring-engine names first, legacy names second.
_FACTOR_ALIASES = {
    "relative_strength": ("relative_strength", "rs3m"),
    "rs3m": ("rs3m", "relative_strength"),
    "trend_200dma": ("trend_200dma", "trend200"),
    "trend200": ("trend200", "trend_200dma"),
    "debt_to_equity": ("debt_to_equity", "debt_ratio"),
    "debt_ratio": ("debt_ratio", "debt_to_equity"),
    "max_drawdown": ("max_drawdown", "drawdown"),
    "drawdown": ("drawdown", "max_drawdown"),
}


def _factor_value(factors_0_1: Dict[str, float], factor: str):
    """Return a present, non-NA factor value, honoring legacy aliases."""
    for key in _FACTOR_ALIASES.get(factor, (factor,)):
        if key in factors_0_1 and pd.notna(factors_0_1[key]):
            return factors_0_1[key]
    return None


def compute_confidence(factors_0_1: Dict[str, float], meta: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """Berechnet Confidence Score aus Datenqualität und Signal-Konsistenz."""
    coverage = compute_data_coverage(factors_0_1, config)
    confluence = compute_signal_confluence(factors_0_1, config)
    risk_clean = compute_risk_cleanliness(factors_0_1, config)
    regime_align = compute_regime_alignment(factors_0_1, meta, config)
    liquidity = compute_liquidity_sanity(factors_0_1, config)

    weights = config.get("CONFIDENCE_WEIGHTS", {
        "coverage": 0.25,
        "confluence": 0.25,
        "risk_clean": 0.20,
        "regime_align": 0.20,
        "liquidity": 0.10,
    })

    confidence_score = (
        coverage * weights["coverage"]
        + confluence * weights["confluence"]
        + risk_clean * weights["risk_clean"]
        + regime_align * weights["regime_align"]
        + liquidity * weights["liquidity"]
    ) * 100

    confidence_label = get_confidence_label(confidence_score, config)
    breakdown = {
        "coverage": coverage,
        "confluence": confluence,
        "risk_clean": risk_clean,
        "regime_align": regime_align,
        "liquidity": liquidity,
        "weights": weights,
    }

    result = {
        "confidence_score": round(confidence_score, 1),
        "confidence_label": confidence_label,
        "confidence_breakdown": breakdown,
    }

    logger.debug("Confidence computed: %.1f (%s)", confidence_score, confidence_label)
    return result


def compute_data_coverage(factors_0_1: Dict[str, float], config: Dict[str, Any]) -> float:
    """Berechnet, wie viele Kernfaktoren vorhanden sind."""
    core_factors = config.get("CONFIDENCE_CORE_FACTORS", [
        "growth",
        "roe",
        "margin",
        "debt_to_equity",
        "volatility",
        "relative_strength",
        "trend_200dma",
    ])
    if not core_factors:
        return 0.0

    present = sum(_factor_value(factors_0_1, factor) is not None for factor in core_factors)
    return min(present / len(core_factors), 1.0)


def compute_signal_confluence(factors_0_1: Dict[str, float], config: Dict[str, Any]) -> float:
    """Berechnet, wie viele Opportunity-Faktoren stark sind (>0.7)."""
    opp_factors = config.get("CONFIDENCE_OPPORTUNITY_FACTORS", [
        "growth",
        "roe",
        "margin",
        "relative_strength",
        "trend_200dma",
    ])
    if not opp_factors:
        return 0.0

    strong_signals = sum(
        1 for factor in opp_factors
        if (value := _factor_value(factors_0_1, factor)) is not None and value > 0.7
    )
    return min(strong_signals / len(opp_factors), 1.0)


def compute_risk_cleanliness(factors_0_1: Dict[str, float], config: Dict[str, Any]) -> float:
    """Berechnet, wie viele Risk-Faktoren okay sind (<0.6)."""
    risk_factors = config.get("CONFIDENCE_RISK_FACTORS", [
        "volatility",
        "max_drawdown",
        "debt_to_equity",
    ])
    if not risk_factors:
        return 0.0

    clean_risks = sum(
        1 for factor in risk_factors
        if (value := _factor_value(factors_0_1, factor)) is not None and value < 0.6
    )
    return min(clean_risks / len(risk_factors), 1.0)


def compute_regime_alignment(factors_0_1: Dict[str, float], meta: Dict[str, Any], config: Dict[str, Any]) -> float:
    """Prüft, ob das Asset zum Marktregime passt."""
    market_regime = str(meta.get("market_regime", "neutral")).lower()

    if market_regime == "bull":
        # Canonical engine keys are relative_strength and trend_200dma.
        momentum_score = 0.0
        relative_strength = _factor_value(factors_0_1, "relative_strength")
        trend_200dma = _factor_value(factors_0_1, "trend_200dma")
        if relative_strength is not None:
            momentum_score += relative_strength * 0.5
        if trend_200dma is not None:
            momentum_score += trend_200dma * 0.5
        return min(momentum_score, 1.0)

    if market_regime == "bear":
        defensive_score = 0.0
        volatility = _factor_value(factors_0_1, "volatility")
        roe = _factor_value(factors_0_1, "roe")
        margin = _factor_value(factors_0_1, "margin")
        if volatility is not None:
            defensive_score += (1 - volatility) * 0.4
        if roe is not None:
            defensive_score += roe * 0.3
        if margin is not None:
            defensive_score += margin * 0.3
        return min(defensive_score, 1.0)

    return 0.5


def compute_liquidity_sanity(factors_0_1: Dict[str, float], config: Dict[str, Any]) -> float:
    """Prüft, ob Liquidität ausreichend ist."""
    liquidity_risk = _factor_value(factors_0_1, "liquidity_risk")
    if liquidity_risk is None:
        return 0.5
    return min(max(0, 1 - liquidity_risk), 1.0)


def get_confidence_label(score: float, config: Dict[str, Any]) -> str:
    """Wandelt Score in Label um."""
    thresholds = config.get("CONFIDENCE_THRESHOLDS", {"HIGH": 75, "MED": 50})
    if score >= thresholds["HIGH"]:
        return "HIGH"
    if score >= thresholds["MED"]:
        return "MED"
    return "LOW"


def print_confidence_report(confidence_result: Dict[str, Any]) -> None:
    """Gibt Confidence-Report aus."""
    score = confidence_result["confidence_score"]
    label = confidence_result["confidence_label"]
    breakdown = confidence_result["confidence_breakdown"]

    print(f"\nCONFIDENCE SCORE: {score:.1f} ({label})")
    print("=" * 50)
    print("Breakdown:")
    for component, value in breakdown.items():
        if component != "weights":
            print(f"  {component}: {value:.2f}")
    print("=" * 50)
