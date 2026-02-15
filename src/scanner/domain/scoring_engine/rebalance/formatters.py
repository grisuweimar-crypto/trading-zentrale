"""
Formatters Module

Formatiert Rebalance-Pläne für Telegram-Nachrichten.
Kompakte, lesbare Darstellung mit Gruppen und Limits.

Author: Trading-Zentrale v6
"""

import logging
from typing import Dict, List, Any
from datetime import datetime

logger = logging.getLogger(__name__)

def format_rebalance_message(plan: Dict[str, Any], meta: Dict[str, Any]) -> str:
    """
    Formatiert Rebalance-Plan für Telegram-Nachricht.
    
    Args:
        plan: Rebalance-Plan mit Actions
        meta: Zusätzliche Metadaten (Market Regime etc.)
        
    Returns:
        Formatierte Nachricht für Telegram
    """
    try:
        # Header Informationen
        plan_meta = plan.get('meta', {})
        turnover_pct = plan_meta.get('turnover', 0) * 100
        turnover_limit_pct = plan_meta.get('turnover_limit', 0) * 100
        total_value = plan_meta.get('total_value', 0)
        actions_count = plan_meta.get('actions_count', 0)
        min_trade_pct = plan_meta.get('min_trade_pct', 0)
        min_trade_value = plan_meta.get('min_trade_value', 0)
        
        # Market Informationen
        market_regime = meta.get('market_regime', 'neutral').upper()
        crypto_regime = meta.get('crypto_regime', 'neutral').upper()
        
        # Actions gruppieren
        actions = plan.get('actions', [])
        grouped = _group_actions(actions)
        
        # Nachricht zusammenbauen
        lines = []
        
        # Header
        lines.append("🔄 *PORTFOLIO REBALANCE*")
        lines.append("")
        lines.append(f"📊 *Market*: {market_regime} | Crypto: {crypto_regime}")
        lines.append(f"💰 *Total*: {total_value:,.0f}€ | Turnover: {turnover_pct:.1f}% (Limit: {turnover_limit_pct:.1f}%)")
        lines.append(f"⚙️ *Limits*: Min Trade {min_trade_pct}% | €{min_trade_value:.0f}")
        lines.append(f"🔧 *Actions*: {actions_count}")
        lines.append("")
        
        # Action-Gruppen mit Limits
        total_shown = 0
        max_actions = 25
        
        # BUY/ADD
        if grouped.get('buy_add'):
            lines.append("✅ *BUY/ADD*")
            for action in grouped['buy_add'][:8]:
                if total_shown >= max_actions:
                    break
                lines.append(_format_action_line(action, total_value))
                total_shown += 1
            if len(grouped['buy_add']) > 8 and total_shown < max_actions:
                remaining = min(len(grouped['buy_add']) - 8, max_actions - total_shown)
                for action in grouped['buy_add'][8:8+remaining]:
                    lines.append(_format_action_line(action, total_value))
                    total_shown += 1
            if len(grouped['buy_add']) > 8 and total_shown >= max_actions:
                lines.append(f"   +{len(grouped['buy_add']) - 8} more...")
            lines.append("")
        
        # INCREASE
        if grouped.get('increase') and total_shown < max_actions:
            lines.append("➕ *INCREASE*")
            for action in grouped['increase'][:5]:
                if total_shown >= max_actions:
                    break
                lines.append(_format_action_line(action, total_value))
                total_shown += 1
            if len(grouped['increase']) > 5 and total_shown < max_actions:
                remaining = min(len(grouped['increase']) - 5, max_actions - total_shown)
                for action in grouped['increase'][5:5+remaining]:
                    lines.append(_format_action_line(action, total_value))
                    total_shown += 1
            if len(grouped['increase']) > 5 and total_shown >= max_actions:
                lines.append(f"   +{len(grouped['increase']) - 5} more...")
            lines.append("")
        
        # REDUCE
        if grouped.get('reduce') and total_shown < max_actions:
            lines.append("➖ *REDUCE*")
            for action in grouped['reduce'][:5]:
                if total_shown >= max_actions:
                    break
                lines.append(_format_action_line(action, total_value))
                total_shown += 1
            if len(grouped['reduce']) > 5 and total_shown < max_actions:
                remaining = min(len(grouped['reduce']) - 5, max_actions - total_shown)
                for action in grouped['reduce'][5:5+remaining]:
                    lines.append(_format_action_line(action, total_value))
                    total_shown += 1
            if len(grouped['reduce']) > 5 and total_shown >= max_actions:
                lines.append(f"   +{len(grouped['reduce']) - 5} more...")
            lines.append("")
        
        # SELL/REMOVE
        if grouped.get('sell_remove') and total_shown < max_actions:
            lines.append("❌ *SELL/REMOVE*")
            for action in grouped['sell_remove'][:8]:
                if total_shown >= max_actions:
                    break
                lines.append(_format_action_line(action, total_value))
                total_shown += 1
            if len(grouped['sell_remove']) > 8 and total_shown < max_actions:
                remaining = min(len(grouped['sell_remove']) - 8, max_actions - total_shown)
                for action in grouped['sell_remove'][8:8+remaining]:
                    lines.append(_format_action_line(action, total_value))
                    total_shown += 1
            if len(grouped['sell_remove']) > 8 and total_shown >= max_actions:
                lines.append(f"   +{len(grouped['sell_remove']) - 8} more...")
            lines.append("")
        
        # More Actions Indicator
        if total_shown < actions_count:
            lines.append(f"📋 *+{actions_count - total_shown} more actions not shown*")
            lines.append("")
        
        # Footer
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        lines.append(f"📅 *Generated*: {timestamp}")
        lines.append("")
        lines.append("_Trades are suggestions only_")
        
        return "\n".join(lines)
        
    except Exception as e:
        logger.error(f"❌ Message Formatting Error: {e}")
        return f"❌ *Rebalance Plan Error*\n\nCould not format message: {str(e)}"

def _group_actions(actions: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Gruppiert Actions nach Typ."""
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

def _format_action_line(action: Dict[str, Any], total_value: float) -> str:
    """Formatiert einzelne Action-Zeile."""
    ticker = action.get('ticker', 'UNKNOWN')
    delta_pct = action.get('delta_pct', 0)
    delta_value = action.get('delta_value', 0)
    current_weight = action.get('current_weight', 0)
    target_weight = action.get('target_weight', 0)
    reason = action.get('reason', '')
    
    # Delta-Formatierung
    delta_str = f"+{delta_pct:.1f}%" if delta_pct > 0 else f"{delta_pct:.1f}%"
    value_str = f"+{delta_value:,.0f}€" if delta_value > 0 else f"{delta_value:,.0f}€"
    
    # Hauptzeile
    line = f"{ticker} {delta_str} ({value_str}) {current_weight:.1f}%→{target_weight:.1f}%"
    
    # Reason bei Bedarf
    if reason and reason != "New position" and reason != "Exit position":
        line += f" - {reason}"
    
    return line

def format_summary_message(plan: Dict[str, Any], holdings: Dict[str, Any], target_portfolio: Dict[str, Any]) -> str:
    """
    Formatiert Zusammenfassung für Console/Log.
    
    Args:
        plan: Rebalance-Plan
        holdings: Aktuelle Holdings
        target_portfolio: Ziel-Portfolio
        
    Returns:
        Summary String
    """
    try:
        plan_meta = plan.get('meta', {})
        actions = plan.get('actions', [])
        
        lines = [
            "REBALANCE SUMMARY",
            "=" * 50,
            f"Total Value: {plan_meta.get('total_value', 0):,.2f}€",
            f"Current Positions: {holdings.get('stocks_count', 0) + holdings.get('crypto_count', 0)}",
            f"Target Positions: {plan_meta.get('target_positions', 0)}",
            f"Actions: {len(actions)}",
            f"Turnover: {plan_meta.get('turnover', 0) * 100:.1f}% (Limit: {plan_meta.get('turnover_limit', 0) * 100:.1f}%)",
            ""
        ]
        
        # Action-Gruppen
        grouped = _group_actions(actions)
        if grouped['buy_add']:
            lines.append(f"BUY/ADD: {len(grouped['buy_add'])}")
        if grouped['increase']:
            lines.append(f"INCREASE: {len(grouped['increase'])}")
        if grouped['reduce']:
            lines.append(f"REDUCE: {len(grouped['reduce'])}")
        if grouped['sell_remove']:
            lines.append(f"SELL/REMOVE: {len(grouped['sell_remove'])}")
        
        lines.append("=" * 50)
        
        return "\n".join(lines)
        
    except Exception as e:
        logger.error(f"Summary formatting error: {e}")
        return f"Summary Error: {str(e)}"

def format_error_message(error: str) -> str:
    """Formatiert Fehler-Nachricht."""
    lines = [
        "REBALANCE ERROR",
        "",
        f"Error: {error}",
        "",
        f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "",
        "_Check logs for details_"
    ]
    
    return "\n".join(lines)

def format_matching_summary(match_result: Dict[str, Any]) -> str:
    """
    Formatiert Matching-Ergebnisse für Reporting.
    
    Args:
        match_result: Ergebnis vom matcher
        
    Returns:
        Formatierter String
    """
    if "error" in match_result:
        return f"Matching failed: {match_result['error']}"
    
    stats = match_result.get('statistics', {})
    total = stats.get('total', 0)
    matched = stats.get('matched', 0)
    unmatched = stats.get('unmatched', 0)
    match_rate = stats.get('match_rate', 0)
    
    lines = [
        "MATCHING SUMMARY",
        "=" * 30,
        f"Total Positions: {total}",
        f"Matched: {matched}",
        f"Unmatched: {unmatched}",
        f"Match Rate: {match_rate:.1f}%",
        ""
    ]
    
    # Match-Methoden
    if match_result.get('matched_positions'):
        method_counts = {}
        for pos in match_result['matched_positions']:
            method = pos.get('match_method', 'unknown')
            method_counts[method] = method_counts.get(method, 0) + 1
        
        lines.append("Match Methods:")
        for method, count in sorted(method_counts.items()):
            lines.append(f"  • {method}: {count}")
    
    if unmatched > 0:
        lines.append("")
        lines.append(f"WARNING: {unmatched} positions unmatched - see data/holdings/unmatched.csv")
    
    lines.append("=" * 30)
    
    return "\n".join(lines)

def send_telegram_message(message: str) -> bool:
    """
    Sendet Nachricht an Telegram.
    
    Args:
        message: Zu sendende Nachricht
        
    Returns:
        True bei Erfolg, False bei Fehler
    """
    try:
        # Importiere hier für optional dependency
        try:
            from alerts.telegram import TOKEN, CHAT_ID
            import requests
        except ImportError:
            logger.warning("⚠️ Telegram module not available")
            return False
        
        if TOKEN == "DEIN_BOT_TOKEN":
            logger.info("📱 Telegram not configured - printing to console")
            print("\n" + "="*50)
            print(message.encode('utf-8', errors='replace').decode('utf-8'))
            print("="*50 + "\n")
            return True
        
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        payload = {
            "chat_id": CHAT_ID,
            "text": message,
            "parse_mode": "Markdown"
        }
        
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            logger.info("✅ Telegram message sent")
            return True
        else:
            logger.error(f"❌ Telegram API error: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Telegram send error: {e}")
        return False
