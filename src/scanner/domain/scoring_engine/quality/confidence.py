"""
Confidence Score Module für Setup-Qualität und Stabilität

Bewertet nicht nur Score-Höhe, sondern auch Verlässlichkeit der Datenbasis.
Output: Confidence Score (0-100) + Badge + Breakdown.

Author: Trading-Zentrale v6
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

def compute_confidence(factors_0_1: Dict[str, float], meta: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Berechnet Confidence Score basierend auf Datenqualität und Signal-Konsistenz.
    
    Args:
        factors_0_1: Normalisierte Faktoren (0-1)
        meta: Metadaten (regime, etc.)
        config: Config Dictionary
    
    Returns:
        Dict mit confidence_score, label und breakdown
    """
    
    # 1. Data Coverage (0-1)
    coverage = compute_data_coverage(factors_0_1, config)
    
    # 2. Signal Confluence (0-1)
    confluence = compute_signal_confluence(factors_0_1, config)
    
    # 3. Risk Cleanliness (0-1)
    risk_clean = compute_risk_cleanliness(factors_0_1, config)
    
    # 4. Regime Alignment (0-1)
    regime_align = compute_regime_alignment(factors_0_1, meta, config)
    
    # 5. Liquidity Sanity (0-1)
    liquidity = compute_liquidity_sanity(factors_0_1, config)
    
    # Gewichte aus Config
    weights = config.get('CONFIDENCE_WEIGHTS', {
        'coverage': 0.25,
        'confluence': 0.25,
        'risk_clean': 0.20,
        'regime_align': 0.20,
        'liquidity': 0.10
    })
    
    # Weighted Score
    confidence_score = (
        coverage * weights['coverage'] +
        confluence * weights['confluence'] +
        risk_clean * weights['risk_clean'] +
        regime_align * weights['regime_align'] +
        liquidity * weights['liquidity']
    ) * 100
    
    # Label
    confidence_label = get_confidence_label(confidence_score, config)
    
    # Breakdown
    breakdown = {
        'coverage': coverage,
        'confluence': confluence,
        'risk_clean': risk_clean,
        'regime_align': regime_align,
        'liquidity': liquidity,
        'weights': weights
    }
    
    result = {
        'confidence_score': round(confidence_score, 1),
        'confidence_label': confidence_label,
        'confidence_breakdown': breakdown
    }
    
    logger.debug(f"Confidence computed: {confidence_score:.1f} ({confidence_label})")
    return result

def compute_data_coverage(factors_0_1: Dict[str, float], config: Dict[str, Any]) -> float:
    """
    Berechnet wie viele Kernfaktoren vorhanden sind.
    """
    core_factors = config.get('CONFIDENCE_CORE_FACTORS', [
        'growth', 'roe', 'margin', 'debt_ratio', 'volatility', 'rs3m', 'trend200'
    ])
    
    present = sum(1 for factor in core_factors 
                 if factor in factors_0_1 and pd.notna(factors_0_1[factor]))
    
    coverage = present / len(core_factors)
    return min(coverage, 1.0)

def compute_signal_confluence(factors_0_1: Dict[str, float], config: Dict[str, Any]) -> float:
    """
    Berechnet wie viele Opportunity-Faktoren stark sind (>0.7).
    """
    opp_factors = config.get('CONFIDENCE_OPPORTUNITY_FACTORS', [
        'growth', 'roe', 'margin', 'rs3m', 'trend200'
    ])
    
    strong_signals = sum(1 for factor in opp_factors 
                      if factor in factors_0_1 and 
                      pd.notna(factors_0_1[factor]) and 
                      factors_0_1[factor] > 0.7)
    
    confluence = strong_signals / len(opp_factors)
    return min(confluence, 1.0)

def compute_risk_cleanliness(factors_0_1: Dict[str, float], config: Dict[str, Any]) -> float:
    """
    Berechnet wie viele Risk-Faktoren okay sind (<0.6).
    """
    risk_factors = config.get('CONFIDENCE_RISK_FACTORS', [
        'volatility', 'drawdown', 'debt_ratio'
    ])
    
    clean_risks = sum(1 for factor in risk_factors 
                     if factor in factors_0_1 and 
                     pd.notna(factors_0_1[factor]) and 
                     factors_0_1[factor] < 0.6)
    
    risk_clean = clean_risks / len(risk_factors)
    return min(risk_clean, 1.0)

def compute_regime_alignment(factors_0_1: Dict[str, float], meta: Dict[str, Any], config: Dict[str, Any]) -> float:
    """
    Prüft ob Asset zum Marktregime passt.
    """
    market_regime = meta.get('market_regime', 'neutral').lower()
    
    if market_regime == 'bull':
        # Bull: prefer momentum signals; accept both canonical and legacy factor names.
        momentum_keys = config.get(
            'CONFIDENCE_MOMENTUM_FACTORS',
            ['relative_strength', 'trend_200dma', 'rs3m', 'trend200'],
        )
        vals = [
            float(factors_0_1[k])
            for k in momentum_keys
            if k in factors_0_1 and pd.notna(factors_0_1[k])
        ]
        if not vals:
            return 0.5
        return min(float(np.mean(vals)), 1.0)
    
    elif market_regime == 'bear':
        # Bear: Defensive/Quality preferred, lower volatility
        defensive_score = 0.0
        if 'volatility' in factors_0_1 and pd.notna(factors_0_1['volatility']):
            defensive_score += (1 - factors_0_1['volatility']) * 0.4
        if 'roe' in factors_0_1 and pd.notna(factors_0_1['roe']):
            defensive_score += factors_0_1['roe'] * 0.3
        if 'margin' in factors_0_1 and pd.notna(factors_0_1['margin']):
            defensive_score += factors_0_1['margin'] * 0.3
        
        return min(defensive_score, 1.0)
    
    else:
        # Neutral: Balanced approach
        return 0.5

def compute_liquidity_sanity(factors_0_1: Dict[str, float], config: Dict[str, Any]) -> float:
    """
    Prüft ob Liquidität ausreichend ist.
    """
    if 'liquidity_risk' not in factors_0_1 or pd.isna(factors_0_1['liquidity_risk']):
        return 0.5  # Neutral wenn unbekannt
    
    # Lower liquidity_risk = better
    liquidity_score = max(0, 1 - factors_0_1['liquidity_risk'])
    return min(liquidity_score, 1.0)

def get_confidence_label(score: float, config: Dict[str, Any]) -> str:
    """
    Wandelt Score in Label um.
    """
    thresholds = config.get('CONFIDENCE_THRESHOLDS', {
        'HIGH': 75,
        'MED': 50
    })
    
    if score >= thresholds['HIGH']:
        return 'HIGH'
    elif score >= thresholds['MED']:
        return 'MED'
    else:
        return 'LOW'

def print_confidence_report(confidence_result: Dict[str, Any]) -> None:
    """
    Gibt Confidence-Report aus.
    """
    score = confidence_result['confidence_score']
    label = confidence_result['confidence_label']
    breakdown = confidence_result['confidence_breakdown']
    
    print(f"\nCONFIDENCE SCORE: {score:.1f} ({label})")
    print("=" * 50)
    
    print("Breakdown:")
    for component, value in breakdown.items():
        if component != 'weights':
            print(f"  {component}: {value:.2f}")
    
    print("=" * 50)
