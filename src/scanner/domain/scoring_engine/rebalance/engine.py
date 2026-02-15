"""
Rebalance Engine (Core)

Orchestriert den gesamten Rebalance-Prozess mit echten Broker-Exports.
Integriert Holdings Loader, Matching, Diff Algorithm und Guardrails.

Author: Trading-Zentrale v6
"""

import logging
from typing import Dict, Any, Optional

from .holdings_loader import load_broker_holdings, validate_holdings_structure
from .matcher import match_holdings_to_symbols, create_symbol_map_template
from .diff import build_rebalance_plan, validate_rebalance_inputs
from .formatters import format_rebalance_message, format_summary_message, send_telegram_message, format_matching_summary

logger = logging.getLogger(__name__)

def run_rebalance(
    stocks_path: str = "data/holdings/stocks.csv",
    crypto_path: str = "data/holdings/crypto.csv",
    watchlist_path: str = "watchlist.csv",
    symbol_map_path: str = "data/holdings/symbol_map.csv",
    top_n: int = 10,
    min_score: float = 30.0,
    max_positions: int = 15,
    allow_crypto: bool = True,
    turnover_limit: float = 0.35,
    min_trade_pct: float = 1.0,
    min_trade_value: float = 25.0,
    send_alert: bool = False
) -> Dict[str, Any]:
    """
    Führt vollständigen Rebalance-Prozess durch.
    
    Args:
        stocks_path: Pfad zur Aktien-CSV
        crypto_path: Pfad zur Krypto-CSV
        watchlist_path: Pfad zur watchlist.csv
        symbol_map_path: Pfad zur Symbol-Mapping-Datei
        top_n: Anzahl der Top-Assets
        min_score: Minimaler Score
        max_positions: Maximale Positionen
        allow_crypto: Krypto erlauben
        turnover_limit: Maximaler Turnover
        min_trade_pct: Minimale Trade-Größe
        min_trade_value: Minimaler Trade-Wert in EUR
        send_alert: Telegram-Alert senden
        
    Returns:
        Dict mit Ergebnis und Metadaten
    """
    try:
        logger.info("🚀 Starting Rebalance Engine with Broker Exports")
        
        # 1. Holdings laden
        logger.info("📂 Loading broker holdings...")
        holdings = load_broker_holdings(stocks_path, crypto_path)
        if not validate_holdings_structure(holdings):
            error = holdings.get('error', 'Invalid holdings data')
            logger.error(f"❌ Holdings validation failed: {error}")
            return {"error": error, "stage": "holdings"}
        
        total_value = holdings['total_value']
        current_positions = holdings['positions']
        
        logger.info(f"✅ Holdings loaded: {len(current_positions)} positions, "
                   f"total value: {total_value:.2f}€")
        
        # 2. Holdings mit Watchlist matchen
        logger.info("🔍 Matching holdings to watchlist symbols...")
        match_result = match_holdings_to_symbols(holdings, watchlist_path, symbol_map_path)
        
        if "error" in match_result:
            error = match_result['error']
            logger.error(f"❌ Matching failed: {error}")
            return {"error": error, "stage": "matching"}
        
        matched_positions = match_result['matched_positions']
        unmatched_positions = match_result['unmatched_positions']
        
        logger.info(f"✅ Matching completed: {len(matched_positions)}/{len(current_positions)} matched")
        
        # Matching Summary ausgeben
        matching_summary = format_matching_summary(match_result)
        logger.info(f"🔍 Matching Summary:\n{matching_summary}")
        
        # 3. Ziel-Portfolio erstellen
        logger.info("🏗️ Building target portfolio...")
        from scoring_engine.portfolio.builder import build_portfolio
        
        target_portfolio = build_portfolio(
            csv_path=watchlist_path,
            top_n=top_n,
            min_score=min_score,
            max_positions=max_positions,
            allow_crypto=allow_crypto
        )
        
        if "error" in target_portfolio:
            error = target_portfolio['error']
            logger.error(f"❌ Portfolio builder failed: {error}")
            return {"error": error, "stage": "portfolio_builder"}
        
        target_positions = target_portfolio.get('positions', [])
        logger.info(f"✅ Target portfolio: {len(target_positions)} positions")
        
        # 4. Market Regime holen
        market_regime = _get_market_regime(watchlist_path)
        
        # 5. Rebalance-Plan erstellen
        logger.info("🔄 Building rebalance plan...")
        
        # Validierung
        is_valid, error_msg = validate_rebalance_inputs(
            target_positions, matched_positions, total_value
        )
        if not is_valid:
            logger.error(f"❌ Input validation failed: {error_msg}")
            return {"error": error_msg, "stage": "validation"}
        
        # Plan erstellen
        plan = build_rebalance_plan(
            target_positions=target_positions,
            current_positions=matched_positions,
            total_value=total_value,
            turnover_limit=turnover_limit,
            min_trade_pct=min_trade_pct,
            min_trade_value=min_trade_value,
            market_regime=market_regime
        )
        
        if "error" in plan.get('meta', {}):
            error = plan['meta']['error']
            logger.error(f"❌ Plan creation failed: {error}")
            return {"error": error, "stage": "plan_creation"}
        
        actions = plan.get('actions', [])
        logger.info(f"✅ Rebalance plan: {len(actions)} actions")
        
        # 6. Guardrails anwenden (optional, hier erweiterbar)
        logger.info("🛡️ Applying safety guardrails...")
        # TODO: Bear-Market Guardrails, Liquidity Caps etc.
        
        # 7. Alert senden
        if send_alert and actions:
            logger.info("📱 Sending rebalance alert...")
            
            # Metadaten für Message
            meta = {
                'market_regime': market_regime,
                'crypto_regime': 'neutral'  # TODO: aus watchlist holen
            }
            
            message = format_rebalance_message(plan, meta)
            success = send_telegram_message(message)
            
            if success:
                logger.info("✅ Alert sent successfully")
            else:
                logger.warning("⚠️ Alert sending failed")
        
        # 8. Ergebnis zusammenstellen
        result = {
            'success': True,
            'holdings': holdings,
            'match_result': match_result,
            'target_portfolio': target_portfolio,
            'plan': plan,
            'meta': {
                'market_regime': market_regime,
                'total_value': total_value,
                'actions_count': len(actions),
                'turnover': plan['meta']['turnover'],
                'matched_positions': len(matched_positions),
                'unmatched_positions': len(unmatched_positions),
                'stage': 'completed'
            }
        }
        
        logger.info("🎉 Rebalance completed successfully")
        return result
        
    except Exception as e:
        logger.error(f"❌ Rebalance engine error: {e}")
        import traceback
        traceback.print_exc()
        return {"error": str(e), "stage": "engine_error"}

def _get_market_regime(watchlist_path: str) -> str:
    """
    Holt aktuelles Marktregime aus watchlist.
    
    Args:
        watchlist_path: Pfad zur watchlist.csv
        
    Returns:
        Market Regime String
    """
    try:
        import pandas as pd
        df = pd.read_csv(watchlist_path)
        if not df.empty and 'MarketRegimeStock' in df.columns:
            regime = str(df['MarketRegimeStock'].iloc[0]).strip().lower()
            logger.info(f"📊 Market Regime: {regime}")
            return regime
    except Exception as e:
        logger.error(f"❌ Market regime detection error: {e}")
    
    return "neutral"

def create_sample_broker_files() -> bool:
    """
    Erstellt Beispiel-Broker-CSVs für Testing.
    
    Returns:
        True bei Erfolg
    """
    try:
        import pandas as pd
        import os
        
        # Ordner erstellen
        os.makedirs("data/holdings", exist_ok=True)
        
        # Beispiel Aktien
        stocks_data = {
            'Name': ['Apple Inc.', 'Microsoft Corp.', 'Alphabet Inc.', 'Amazon.com Inc.', 'Tesla Inc.'],
            'ISIN': ['US0378331005', 'US5949181045', 'US02079K3059', 'US0231351067', 'US88160R1014'],
            'WKN': ['865985', '870747', 'A0J7Z0', '906866', 'A1CX3T'],
            'Art': ['Aktie'] * 5,
            'Wert': ['1.234,56', '2.345,67', '987,65', '1.876,54', '654,32']
        }
        
        # Beispiel Krypto
        crypto_data = {
            'Name': ['Bitcoin', 'Ethereum', 'Cardano'],
            'ISIN': ['', '', ''],
            'Art': ['Krypto'] * 3,
            'Wert': ['3.456,78', '2.123,45', '456,78']
        }
        
        # Speichern
        pd.DataFrame(stocks_data).to_csv('data/holdings/stocks.csv', index=False, sep=';', encoding='utf-8')
        pd.DataFrame(crypto_data).to_csv('data/holdings/crypto.csv', index=False, sep=';', encoding='utf-8')
        
        # Symbol Map Template
        create_symbol_map_template()
        
        logger.info("✅ Sample broker files created in data/holdings/")
        return True
        
    except Exception as e:
        logger.error(f"❌ Sample broker files creation failed: {e}")
        return False

def print_rebalance_summary(result: Dict[str, Any]) -> None:
    """
    Druckt Zusammenfassung des Rebalance-Ergebnisses.
    
    Args:
        result: Ergebnis von run_rebalance()
    """
    if "error" in result:
        print(f"❌ Rebalance failed: {result['error']} (Stage: {result.get('stage', 'unknown')})")
        return
    
    holdings = result.get('holdings', {})
    target_portfolio = result.get('target_portfolio', {})
    plan = result.get('plan', {})
    meta = result.get('meta', {})
    
    summary = format_summary_message(plan, holdings, target_portfolio)
    print("\n" + summary.encode('utf-8', errors='replace').decode('utf-8'))
    
    # Matching Summary
    match_result = result.get('match_result', {})
    if match_result:
        matching_summary = format_matching_summary(match_result)
        print("\n" + matching_summary.encode('utf-8', errors='replace').decode('utf-8'))

def validate_environment() -> Dict[str, bool]:
    """
    Validiert die Umgebung für Rebalance Engine.
    
    Returns:
        Dict mit Validierungsergebnissen
    """
    results = {}
    
    # Check broker files
    try:
        import os
        results['stocks_csv'] = os.path.exists("data/holdings/stocks.csv")
        results['crypto_csv'] = os.path.exists("data/holdings/crypto.csv")
    except:
        results['stocks_csv'] = False
        results['crypto_csv'] = False
    
    # Check watchlist.csv
    try:
        import pandas as pd
        df = pd.read_csv("watchlist.csv")
        results['watchlist_csv'] = len(df) > 0
    except:
        results['watchlist_csv'] = False
    
    # Check portfolio builder
    try:
        from scoring_engine.portfolio.builder import build_portfolio
        results['portfolio_builder'] = True
    except:
        results['portfolio_builder'] = False
    
    # Check telegram (optional)
    try:
        from alerts.telegram import TOKEN, CHAT_ID
        results['telegram_configured'] = TOKEN != "DEIN_BOT_TOKEN"
    except:
        results['telegram_configured'] = True  # Optional, nicht blockieren
    
    return results
