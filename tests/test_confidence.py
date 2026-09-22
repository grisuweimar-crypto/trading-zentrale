import pytest

from scanner.domain.scoring_engine.quality.confidence import (
    compute_confidence,
    compute_data_coverage,
    compute_regime_alignment,
    compute_risk_cleanliness,
)


def test_bull_regime_uses_canonical_engine_factor_names():
    factors = {"relative_strength": 0.8, "trend_200dma": 0.6}
    assert compute_regime_alignment(factors, {"market_regime": "bull"}, {}) == pytest.approx(0.7)


def test_bull_regime_keeps_legacy_aliases_compatible():
    factors = {"rs3m": 0.8, "trend200": 0.6}
    assert compute_regime_alignment(factors, {"market_regime": "bull"}, {}) == pytest.approx(0.7)


def test_default_coverage_and_risk_keys_match_current_engine():
    factors = {
        "growth": 0.8,
        "roe": 0.8,
        "margin": 0.8,
        "debt_to_equity": 0.4,
        "volatility": 0.2,
        "relative_strength": 0.8,
        "trend_200dma": 0.6,
        "max_drawdown": 0.3,
    }
    assert compute_data_coverage(factors, {}) == pytest.approx(1.0)
    assert compute_risk_cleanliness(factors, {}) == pytest.approx(1.0)


def test_full_confidence_no_longer_loses_bull_regime_alignment():
    factors = {
        "growth": 0.8,
        "roe": 0.8,
        "margin": 0.8,
        "debt_to_equity": 0.4,
        "volatility": 0.2,
        "relative_strength": 0.8,
        "trend_200dma": 0.6,
        "max_drawdown": 0.3,
        "liquidity_risk": 0.1,
    }
    result = compute_confidence(factors, {"market_regime": "bull"}, {})
    assert result["confidence_breakdown"]["regime_align"] == pytest.approx(0.7)
    assert result["confidence_score"] == pytest.approx(88.0)
    assert result["confidence_label"] == "HIGH"
