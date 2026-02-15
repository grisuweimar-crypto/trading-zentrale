"""
Rebalance Rules Module

Generiert kurze, verständliche Reasons für Rebalance-Actions.
Basiert auf Score, RS3M, Trend200 und Market Regime.

Author: Trading-Zentrale v6
"""

import pandas as pd
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

def generate_action_reason(
    action: str,
    ticker: str,
    target_weight: float,
    current_weight: float,
    watchlist_data: Optional[Dict[str, Any]] = None,
    market_regime: str = "neutral"
) -> str:
    """
    Generiert kurzen Reason für eine Rebalance-Action.
    
    Args:
        action: Art der Action (BUY/ADD, SELL/REMOVE, INCREASE, REDUCE)
        ticker: Ticker-Symbol
        target_weight: Ziel-Gewicht in %
        current_weight: Aktuelles Gewicht in %
        watchlist_data: Zusätzliche Daten aus watchlist.csv
        market_regime: Aktuelles Marktregime
        
    Returns:
        Kurzer Reason String (1 Zeile)
    """
    try:
        # Watchlist-Daten extrahieren
        score = watchlist_data.get('score', 0) if watchlist_data else 0
        rs3m = watchlist_data.get('rs3m', 0) if watchlist_data else 0
        trend200 = watchlist_data.get('trend200', 0) if watchlist_data else 0
        liquidity_risk = watchlist_data.get('liquidity_risk', 0) if watchlist_data else 0
        
        # Action-spezifische Reasons
        if action in ["BUY/ADD"]:
            return _generate_add_reason(ticker, score, rs3m, trend200, market_regime)
        
        elif action == "SELL/REMOVE":
            return _generate_remove_reason(ticker, score, rs3m, trend200, market_regime)
        
        elif action == "INCREASE":
            return _generate_increase_reason(ticker, score, rs3m, trend200, current_weight, target_weight)
        
        elif action == "REDUCE":
            return _generate_reduce_reason(ticker, score, rs3m, trend200, liquidity_risk, market_regime)
        
        else:
            return "rebalance adjustment"
            
    except Exception as e:
        logger.error(f"❌ Reason Generation Error for {ticker}: {e}")
        return "data analysis"

def _generate_add_reason(ticker: str, score: float, rs3m: float, trend200: float, market_regime: str) -> str:
    """Generiert Reason für neue Position."""
    reasons = []
    
    # Score-basiert
    if score >= 80:
        reasons.append("excellent score")
    elif score >= 60:
        reasons.append("strong score")
    elif score >= 40:
        reasons.append("moderate score")
    
    # Momentum-basiert
    if rs3m > 0.1:
        reasons.append("RS strong")
    elif rs3m > 0.05:
        reasons.append("RS positive")
    
    if trend200 > 0.1:
        reasons.append("trend strong")
    elif trend200 > 0.05:
        reasons.append("trend up")
    
    # Market-Kontext
    if market_regime == "bull":
        reasons.append("bull market")
    elif market_regime == "bear" and score >= 80:
        reasons.append("bear market gem")
    
    return ", ".join(reasons[:2]) if reasons else "meets criteria"

def _generate_remove_reason(ticker: str, score: float, rs3m: float, trend200: float, market_regime: str) -> str:
    """Generiert Reason für Entfernung."""
    if score < 20:
        return "score collapsed"
    elif score < 30:
        return "score too low"
    elif rs3m < -0.1:
        return "RS negative"
    elif trend200 < -0.1:
        return "downtrend"
    elif market_regime == "bear":
        return "bear market exit"
    else:
        return "portfolio optimization"

def _generate_increase_reason(ticker: str, score: float, rs3m: float, trend200: float, current_weight: float, target_weight: float) -> str:
    """Generiert Reason für Erhöhung."""
    reasons = []
    
    # Score-Verbesserung
    if score >= 70:
        reasons.append("top performer")
    elif score >= 50:
        reasons.append("strong score")
    
    # Momentum
    if rs3m > 0.1:
        reasons.append("RS strong")
    if trend200 > 0.05:
        reasons.append("trend up")
    
    # Gewicht-Veränderung
    weight_increase = target_weight - current_weight
    if weight_increase > 5:
        reasons.append("significant upsize")
    
    return ", ".join(reasons[:2]) if reasons else "position strength"

def _generate_reduce_reason(ticker: str, score: float, rs3m: float, trend200: float, liquidity_risk: float, market_regime: str) -> str:
    """Generiert Reason für Reduzierung."""
    reasons = []
    
    # Score-Probleme
    if score < 30:
        reasons.append("score weak")
    elif score < 40:
        reasons.append("score declined")
    
    # Momentum-Probleme
    if rs3m < -0.05:
        reasons.append("RS weak")
    if trend200 < -0.05:
        reasons.append("trend down")
    
    # Risiko-Management
    if liquidity_risk > 0.7:
        reasons.append("liquidity risk")
    if market_regime == "bear":
        reasons.append("risk reduction")
    
    return ", ".join(reasons[:2]) if reasons else "risk management"

def load_watchlist_data() -> Dict[str, Dict[str, Any]]:
    """
    Lädt watchlist.csv für Reason-Generation.
    
    Returns:
        Dict mit Ticker als Key und Metriken als Value
    """
    try:
        df = pd.read_csv("watchlist.csv")
        data = {}
        
        for _, row in df.iterrows():
            ticker = str(row.get('Ticker', '')).strip().upper()
            if ticker:
                data[ticker] = {
                    'score': _safe_float(row.get('Score', 0)),
                    'rs3m': _safe_float(row.get('RS3M', 0)),
                    'trend200': _safe_float(row.get('Trend200', 0)),
                    'liquidity_risk': _safe_float(row.get('LiquidityRisk', 0)),
                    'market_regime': str(row.get('MarketRegimeStock', 'neutral')).lower()
                }
        
        logger.info(f"📊 Loaded watchlist data: {len(data)} tickers")
        return data
        
    except Exception as e:
        logger.error(f"❌ Watchlist loading error: {e}")
        return {}

def _safe_float(value, default=0.0) -> float:
    """Sichere Konvertierung zu float."""
    try:
        if pd.isna(value) or value is None:
            return default
        return float(value)
    except (ValueError, TypeError):
        return default

def get_market_regime() -> str:
    """
    Holt aktuelles Marktregime aus watchlist.
    
    Returns:
        Market Regime String
    """
    try:
        df = pd.read_csv("watchlist.csv")
        if not df.empty and 'MarketRegimeStock' in df.columns:
            regime = str(df['MarketRegimeStock'].iloc[0]).strip().lower()
            logger.info(f"📊 Market Regime: {regime}")
            return regime
    except Exception as e:
        logger.error(f"❌ Market regime detection error: {e}")
    
    return "neutral"

def apply_bear_market_guardrails(
    actions: list, 
    watchlist_data: Dict[str, Dict[str, Any]], 
    market_regime: str
) -> list:
    """
    Wendet Bear-Market Guardrails an.
    
    Args:
        actions: Liste von Actions
        watchlist_data: Watchlist-Daten
        market_regime: Aktuelles Regime
        
    Returns:
        Gefilterte Actions
    """
    if market_regime != "bear":
        return actions
    
    filtered_actions = []
    for action in actions:
        ticker = action.get('ticker', '')
        action_type = action.get('action', '')
        
        # Im Bear-Market: Nur Adds bei sehr hohen Scores
        if action_type in ["BUY/ADD", "INCREASE"]:
            ticker_data = watchlist_data.get(ticker, {})
            score = ticker_data.get('score', 0)
            
            if score >= 80:
                filtered_actions.append(action)
            else:
                logger.info(f"🐻 Filtering {action_type} for {ticker} (score {score} < 80 in bear market)")
        else:
            # REDUCE/REMOVE immer erlauben im Bear-Market
            filtered_actions.append(action)
    
    if len(filtered_actions) < len(actions):
        logger.info(f"🐻 Bear-Market filter: {len(actions)} → {len(filtered_actions)} actions")
    
    return filtered_actions

def apply_liquidity_caps(actions: list, watchlist_data: Dict[str, Dict[str, Any]], max_add_pct: float = 2.0) -> list:
    """
    Begrenzt Adds bei illiquiden Positionen.
    
    Args:
        actions: Liste von Actions
        watchlist_data: Watchlist-Daten
        max_add_pct: Maximales Add bei illiquiden Assets
        
    Returns:
        Gefilterte Actions
    """
    capped_actions = []
    
    for action in actions:
        ticker = action.get('ticker', '')
        action_type = action.get('action', '')
        delta_pct = action.get('delta_pct', 0)
        
        if action_type in ["BUY/ADD", "INCREASE"]:
            ticker_data = watchlist_data.get(ticker, {})
            liquidity_risk = ticker_data.get('liquidity_risk', 0)
            
            if liquidity_risk > 0.7 and delta_pct > max_add_pct:
                # Delta cappen
                capped_action = action.copy()
                capped_action['delta_pct'] = max_add_pct
                capped_action['delta_value'] = action.get('delta_value', 0) * (max_add_pct / delta_pct)
                capped_action['target_weight'] = action.get('current_weight', 0) + max_add_pct
                
                capped_actions.append(capped_action)
                logger.info(f"💧 Capped {ticker} add: {delta_pct:.1f}% → {max_add_pct:.1f}% (liquidity risk {liquidity_risk:.2f})")
            else:
                capped_actions.append(action)
        else:
            capped_actions.append(action)
    
    return capped_actions
