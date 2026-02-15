"""
Holdings Loader Module

Liest und normalisiert holdings.csv mit Value/Quantity Support.
Robust gegen fehlende Daten, CASH-Handling, Kurs-Berechnung.

Author: Trading-Zentrale v6
"""

import pandas as pd
import logging
from typing import Dict, List, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

def load_holdings(path: str = "data/holdings.csv") -> Dict[str, Any]:
    """
    Lädt holdings.csv und normalisiert zu Value-basierten Positionen.
    
    Args:
        path: Pfad zur holdings.csv
        
    Returns:
        Dict mit positions, cash_value, total_value
    """
    try:
        file_path = Path(path)
        if not file_path.exists():
            logger.error(f"❌ Holdings file not found: {path}")
            return {"error": f"File not found: {path}"}
        
        # CSV laden
        df = pd.read_csv(path)
        logger.info(f"📊 Loaded holdings: {len(df)} rows from {path}")
        
        # Spalten identifizieren
        required_cols = ['Ticker']
        if not all(col in df.columns for col in required_cols):
            missing = [col for col in required_cols if col not in df.columns]
            logger.error(f"❌ Missing required columns: {missing}")
            return {"error": f"Missing columns: {missing}"}
        
        # Value vs Quantity Detection
        has_value = 'Value' in df.columns
        has_quantity = 'Quantity' in df.columns
        
        if not has_value and not has_quantity:
            logger.error("❌ Need either 'Value' or 'Quantity' column")
            return {"error": "Need Value or Quantity column"}
        
        # Daten normalisieren
        positions = []
        cash_value = 0.0
        total_value = 0.0
        
        for _, row in df.iterrows():
            ticker = str(row.get('Ticker', '')).strip().upper()
            if not ticker or ticker == 'NAN':
                logger.warning(f"⚠️ Skipping empty ticker")
                continue
            
            # CASH Handling
            if ticker == 'CASH':
                if has_value:
                    cash_val = _safe_float(row.get('Value', 0))
                elif has_quantity:
                    cash_val = _safe_float(row.get('Quantity', 0))
                else:
                    cash_val = 0.0
                
                if cash_val > 0:
                    cash_value += cash_val
                    logger.info(f"💰 Cash position: {cash_val:.2f}")
                continue
            
            # Value-basierte Position
            if has_value:
                value = _safe_float(row.get('Value', 0))
                if value <= 0:
                    logger.warning(f"⚠️ Skipping {ticker}: non-positive value {value}")
                    continue
                
                positions.append({
                    "ticker": ticker,
                    "value": value,
                    "source": "value"
                })
                total_value += value
            
            # Quantity-basierte Position
            elif has_quantity:
                quantity = _safe_float(row.get('Quantity', 0))
                if quantity <= 0:
                    logger.warning(f"⚠️ Skipping {ticker}: non-positive quantity {quantity}")
                    continue
                
                # Kurs aus watchlist.csv holen
                price = _get_current_price(ticker)
                if price is None:
                    logger.warning(f"⚠️ Skipping {ticker}: no price data available")
                    continue
                
                value = quantity * price
                positions.append({
                    "ticker": ticker,
                    "value": value,
                    "quantity": quantity,
                    "price": price,
                    "source": "quantity"
                })
                total_value += value
        
        # Total Value berechnen
        total_value += cash_value
        
        result = {
            "positions": positions,
            "cash_value": cash_value,
            "total_value": total_value,
            "positions_count": len(positions),
            "source": path
        }
        
        logger.info(f"✅ Holdings processed: {len(positions)} positions, "
                   f"cash: {cash_value:.2f}, total: {total_value:.2f}")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Holdings loading error: {e}")
        return {"error": str(e)}

def _safe_float(value, default=0.0) -> float:
    """Sichere Konvertierung zu float."""
    try:
        if pd.isna(value) or value is None:
            return default
        return float(value)
    except (ValueError, TypeError):
        return default

def _get_current_price(ticker: str) -> Optional[float]:
    """
    Holt aktuellen Preis aus watchlist.csv.
    
    Args:
        ticker: Ticker-Symbol
        
    Returns:
        Preis oder None wenn nicht gefunden
    """
    try:
        watchlist_path = "watchlist.csv"
        if not Path(watchlist_path).exists():
            logger.warning(f"⚠️ Watchlist not found for price lookup: {watchlist_path}")
            return None
        
        df = pd.read_csv(watchlist_path)
        ticker_row = df[df['Ticker'].str.upper() == ticker.upper()]
        
        if ticker_row.empty:
            logger.warning(f"⚠️ Ticker {ticker} not found in watchlist")
            return None
        
        # Versuche verschiedene Preis-Spalten
        price_cols = ['Price', 'Close', 'Last', 'Current', 'Aktuell']
        for col in price_cols:
            if col in ticker_row.columns:
                price = _safe_float(ticker_row[col].iloc[0])
                if price > 0:
                    return price
        
        # Fallback: Score als Indikator (nicht ideal, aber besser als nichts)
        if 'Score' in ticker_row.columns:
            logger.warning(f"⚠️ No price column for {ticker}, using score as proxy")
            return _safe_float(ticker_row['Score'].iloc[0])
        
        logger.warning(f"⚠️ No price data available for {ticker}")
        return None
        
    except Exception as e:
        logger.error(f"❌ Price lookup error for {ticker}: {e}")
        return None

def validate_holdings(holdings: Dict[str, Any]) -> bool:
    """
    Validiert holdings-Datenstruktur.
    
    Args:
        holdings: Holdings-Dict
        
    Returns:
        True wenn valide
    """
    if "error" in holdings:
        return False
    
    required_keys = ["positions", "cash_value", "total_value"]
    if not all(key in holdings for key in required_keys):
        return False
    
    if holdings["total_value"] <= 0:
        logger.error("❌ Total portfolio value must be positive")
        return False
    
    return True

def get_holdings_summary(holdings: Dict[str, Any]) -> str:
    """
    Erzeugt Zusammenfassung der Holdings.
    
    Args:
        holdings: Holdings-Dict
        
    Returns:
        Summary String
    """
    if "error" in holdings:
        return f"❌ {holdings['error']}"
    
    positions = holdings.get("positions", [])
    cash = holdings.get("cash_value", 0)
    total = holdings.get("total_value", 0)
    
    lines = [
        f"📊 Holdings Summary:",
        f"   Positions: {len(positions)}",
        f"   Cash: {cash:.2f}",
        f"   Total: {total:.2f}",
        f"   Cash %: {(cash/total*100):.1f}%" if total > 0 else "   Cash %: N/A"
    ]
    
    # Top 5 Positionen
    if positions:
        lines.append("   Top 5:")
        sorted_pos = sorted(positions, key=lambda x: x["value"], reverse=True)[:5]
        for pos in sorted_pos:
            weight = (pos["value"] / total * 100) if total > 0 else 0
            lines.append(f"     {pos['ticker']}: {pos['value']:.2f} ({weight:.1f}%)")
    
    return "\n".join(lines)
