"""
Diff Module

Berechnet Portfolio-Deltas und Turnover-Limits.
Vergleicht Ziel-Portfolio mit gematcheten Holdings.

Author: Trading-Zentrale v6
"""

import logging
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class Action:
    """Action-Datenstruktur."""
    ticker: str
    action: str  # BUY/ADD, SELL/REMOVE, INCREASE, REDUCE
    delta_pct: float
    delta_value: float
    current_weight: float
    target_weight: float
    current_value: float = 0.0
    target_value: float = 0.0
    asset_class: str = 'stock'

def build_rebalance_plan(
    target_positions: List[Dict[str, Any]],
    current_positions: List[Dict[str, Any]], 
    total_value: float,
    turnover_limit: float = 0.35,
    min_trade_pct: float = 1.0,
    min_trade_value: float = 25.0,
    market_regime: str = 'bull',
    liquidity_risk_threshold: float = 0.7,
    liquidity_risk_strict: float = 0.85,
    max_liquidity_add: float = 2.0
) -> Dict[str, Any]:
    """
    Erzeugt Rebalance-Plan mit Trade-Vorschlägen.
    
    Args:
        target_positions: Ziel-Portfolio aus Portfolio Builder (WeightPct)
        current_positions: Gematchete Holdings vom matcher
        total_value: Gesamtwert des Portfolios
        turnover_limit: Maximaler Turnover (0.35 = 35%)
        min_trade_pct: Minimale Trade-Größe (1.0 = 1%)
        min_trade_value: Minimaler Trade-Wert in EUR (default 25)
        market_regime: Market regime ('bull' oder 'bear')
        liquidity_risk_threshold: Liquidity risk threshold (0.7)
        liquidity_risk_strict: Strict liquidity threshold (0.85)
        max_liquidity_add: Max add for high liquidity risk (2.0%)
        
    Returns:
        Dict mit Metadaten und Actions
    """
    try:
        logger.info(f"🔄 Building Rebalance Plan (Total: {total_value:.2f}, "
                   f"Turnover Limit: {turnover_limit:.1%}, Regime: {market_regime})")
        
        # Position Maps erstellen
        target_map = {pos['Ticker']: pos for pos in target_positions}
        current_map = {pos['symbol']: pos for pos in current_positions}
        
        # Alle Ticker sammeln
        all_tickers = set(target_map.keys()) | set(current_map.keys())
        
        # Deltas berechnen
        deltas = []
        for ticker in all_tickers:
            target_pos = target_map.get(ticker)
            current_pos = current_map.get(ticker)
            
            # Gewichte berechnen
            target_weight = target_pos.get('WeightPct', 0) if target_pos else 0
            current_weight = (current_pos.get('value', 0) / total_value * 100) if current_pos else 0
            
            delta_pct = target_weight - current_weight
            delta_value = delta_pct / 100 * total_value
            
            # Min-Trade-Filter anwenden
            if abs(delta_pct) < min_trade_pct or abs(delta_value) < min_trade_value:
                continue
            
            # Regime Guardrails für BEAR market
            if market_regime == 'bear' and delta_pct > 0:
                # Nur erlauben wenn Score hoch und positiv trend
                score = target_pos.get('Score', 0) if target_pos else 0
                rs3m = target_pos.get('rs3m', 0) if target_pos else 0
                trend200 = target_pos.get('trend200', 0) if target_pos else 0
                
                if not (score >= 80 and rs3m > 0 and trend200 > 0):
                    if current_weight > 0:
                        # INCREASE -> REDUCE (nicht erhöhen)
                        delta_pct = min(delta_pct, -min_trade_pct)
                        delta_value = delta_pct / 100 * total_value
                    else:
                        # BUY/ADD -> überspringen
                        continue
            
            # Liquidity Guardrails
            liquidity_risk = target_pos.get('liquidity_risk', 0) if target_pos else 0
            if liquidity_risk > liquidity_risk_strict and delta_pct > 0:
                # Streng: keine positiven Deltas
                continue
            elif liquidity_risk > liquidity_risk_threshold and delta_pct > 0:
                # Clamp auf max_add
                delta_pct = min(delta_pct, max_liquidity_add)
                delta_value = delta_pct / 100 * total_value
            
            action = _determine_action_type(delta_pct, current_weight, target_weight)
            
            deltas.append({
                'ticker': ticker,
                'action': action,
                'delta_pct': round(delta_pct, 2),
                'delta_value': round(delta_value, 2),
                'current_weight': round(current_weight, 2),
                'target_weight': round(target_weight, 2),
                'current_value': current_pos.get('value', 0) if current_pos else 0,
                'target_value': round(target_weight / 100 * total_value, 2),
                'asset_class': current_pos.get('asset_class', 'stock') if current_pos else 'stock',
                'score': target_pos.get('Score', 0) if target_pos else 0,
                'liquidity_risk': liquidity_risk,
                'reason': _get_action_reason(action, delta_pct, market_regime, liquidity_risk)
            })
        
        logger.info(f"📊 Raw deltas: {len(deltas)} actions")
        
        # Turnover-Control anwenden
        controlled_deltas = _apply_turnover_control(deltas, turnover_limit)
        
        # Metadaten berechnen
        actual_turnover = _calculate_turnover(controlled_deltas)
        
        plan = {
            'meta': {
                'turnover': actual_turnover,
                'turnover_limit': turnover_limit,
                'min_trade_pct': min_trade_pct,
                'min_trade_value': min_trade_value,
                'market_regime': market_regime,
                'total_value': total_value,
                'actions_count': len(controlled_deltas),
                'target_positions': len(target_positions),
                'current_positions': len(current_positions)
            },
            'actions': controlled_deltas
        }
        
        logger.info(f"✅ Rebalance Plan: {len(controlled_deltas)} actions, "
                   f"turnover {actual_turnover:.1%}")
        
        return plan
        
    except Exception as e:
        logger.error(f"❌ Rebalance Plan Error: {e}")
        return {
            'meta': {'error': str(e)},
            'actions': []
        }

def _determine_action_type(delta_pct: float, current_weight: float, target_weight: float) -> str:
    """Bestimmt Action-Typ basierend auf Delta."""
    if current_weight == 0 and delta_pct > 0:
        return "BUY/ADD"
    elif target_weight == 0 and delta_pct < 0:
        return "SELL/REMOVE"
    elif delta_pct > 0:
        return "INCREASE"
    elif delta_pct < 0:
        return "REDUCE"
    else:
        return "HOLD"

def _calculate_turnover(deltas: List[Dict[str, Any]]) -> float:
    """Berechnet Turnover als Summe(|delta_pct|)/2."""
    return sum(abs(delta['delta_pct']) for delta in deltas) / 2

def _apply_turnover_control(deltas: List[Dict[str, Any]], turnover_limit: float) -> List[Dict[str, Any]]:
    """Wendet Turnover-Control an - skaliert Deltas proportional."""
    current_turnover = _calculate_turnover(deltas)
    
    if current_turnover <= turnover_limit:
        logger.info(f"✅ Turnover {current_turnover:.1%} within limit {turnover_limit:.1%}")
        return deltas
    
    # Turnover überschritten - skalieren
    scale_factor = turnover_limit / current_turnover
    logger.warning(f"⚠️ Turnover {current_turnover:.1%} > Limit {turnover_limit:.1%}, "
                   f"scaling by {scale_factor:.2f}")
    
    controlled = []
    for delta in deltas:
        scaled_delta = delta['delta_pct'] * scale_factor
        scaled_value = delta['delta_value'] * scale_factor
        
        controlled_delta = delta.copy()
        controlled_delta['delta_pct'] = round(scaled_delta, 2)
        controlled_delta['delta_value'] = round(scaled_value, 2)
        # Target Weight anpassen
        controlled_delta['target_weight'] = round(controlled_delta['current_weight'] + scaled_delta, 2)
        controlled_delta['target_value'] = round(controlled_delta['current_value'] + scaled_value, 2)
        controlled_delta['reason'] += f" (scaled by {scale_factor:.2f})"
        
        controlled.append(controlled_delta)
    
    # Verify new turnover
    new_turnover = _calculate_turnover(controlled)
    logger.info(f"✅ Scaled turnover: {new_turnover:.1%} (was {current_turnover:.1%})")
    
    return controlled

def _get_action_reason(action: str, delta_pct: float, market_regime: str, liquidity_risk: float) -> str:
    """Gibt Grund für Action zurück."""
    if action == "BUY/ADD":
        if market_regime == 'bear':
            return "New position (bear market exception)"
        return "New position"
    elif action == "SELL/REMOVE":
        return "Exit position"
    elif action == "INCREASE":
        if liquidity_risk > 0.7:
            return f"Increase (liquidity capped, risk={liquidity_risk:.2f})"
        return "Increase position"
    elif action == "REDUCE":
        if market_regime == 'bear' and delta_pct < 0:
            return "Reduce (bear market protection)"
        return "Reduce position"
    else:
        return "Hold"

def group_actions_by_type(actions: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Gruppiert Actions nach Typ für bessere Darstellung."""
    groups = {
        'buy_add': [],
        'increase': [],
        'reduce': [],
        'sell_remove': []
    }
    
    for action in actions:
        action_type = action.get('action', '').upper()
        
        if action_type in ['BUY/ADD', 'BUY', 'ADD']:
            groups['buy_add'].append(action)
        elif action_type == 'INCREASE':
            groups['increase'].append(action)
        elif action_type == 'REDUCE':
            groups['reduce'].append(action)
        elif action_type in ['SELL/REMOVE', 'SELL', 'REMOVE']:
            groups['sell_remove'].append(action)
    
    return groups

def calculate_portfolio_metrics(
    target_positions: List[Dict[str, Any]], 
    current_positions: List[Dict[str, Any]],
    total_value: float
) -> Dict[str, Any]:
    """
    Berechnet Portfolio-Metriken für Reporting.
    
    Returns:
        Dict mit diversen Metriken
    """
    # Current Metrics
    current_value = sum(pos.get('value', 0) for pos in current_positions)
    current_weights = {pos['symbol']: pos.get('value', 0) / total_value * 100 
                       for pos in current_positions}
    
    # Target Metrics  
    target_weights = {pos['Ticker']: pos.get('WeightPct', 0) for pos in target_positions}
    
    # Overlap Analysis
    current_tickers = set(current_weights.keys())
    target_tickers = set(target_weights.keys())
    
    overlap = current_tickers & target_tickers
    new_positions = target_tickers - current_tickers
    removed_positions = current_tickers - target_tickers
    
    return {
        'current_value': current_value,
        'current_positions_count': len(current_positions),
        'target_positions_count': len(target_positions),
        'overlap_count': len(overlap),
        'new_count': len(new_positions),
        'removed_count': len(removed_positions),
        'current_top5': sorted(current_weights.items(), key=lambda x: x[1], reverse=True)[:5],
        'target_top5': sorted(target_weights.items(), key=lambda x: x[1], reverse=True)[:5]
    }

def validate_rebalance_inputs(
    target_positions: List[Dict[str, Any]], 
    current_positions: List[Dict[str, Any]], 
    total_value: float
) -> Tuple[bool, str]:
    """
    Validiert Rebalance-Inputs.
    
    Returns:
        (is_valid, error_message)
    """
    if total_value <= 0:
        return False, "Total portfolio value must be positive"
    
    if not target_positions:
        return False, "Target positions cannot be empty"
    
    # Target weights should sum to ~100% (minus cash)
    target_weight_sum = sum(pos.get('WeightPct', 0) for pos in target_positions)
    if target_weight_sum < 80 or target_weight_sum > 105:  # Allow some tolerance
        return False, f"Target weights sum to {target_weight_sum:.1f}%, should be ~100%"
    
    # Check for duplicate tickers
    target_tickers = [pos.get('Ticker', '') for pos in target_positions]
    current_tickers = [pos.get('symbol', '') for pos in current_positions]
    
    if len(set(target_tickers)) != len(target_tickers):
        return False, "Duplicate tickers in target positions"
    
    if len(set(current_tickers)) != len(current_tickers):
        return False, "Duplicate tickers in current positions"
    
    return True, ""

def get_asset_class_distribution(positions: List[Dict[str, Any]]) -> Dict[str, float]:
    """
    Berechnet Asset-Class Verteilung.
    
    Args:
        positions: Liste von Positionen
        
    Returns:
        Dict mit Asset-Class Gewichten
    """
    total_value = sum(pos.get('value', 0) for pos in positions)
    if total_value == 0:
        return {}
    
    distribution = {}
    for pos in positions:
        asset_class = pos.get('asset_class', 'unknown')
        value = pos.get('value', 0)
        distribution[asset_class] = distribution.get(asset_class, 0) + value
    
    # In Prozent umrechnen
    for asset_class in distribution:
        distribution[asset_class] = (distribution[asset_class] / total_value) * 100
    
    return distribution
