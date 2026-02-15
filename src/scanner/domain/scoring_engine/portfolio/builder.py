"""
Portfolio Builder Module

Erzeugt handelbare Portfolios aus dem v6 Scoring System.
Fokus auf Score-basierte Selektion, Liquidity-Adjustment und Regime-Aware Exposure Control.

Author: Trading-Zentrale v6
"""

import pandas as pd
import datetime
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)

def _safe_float(value, default=0.0) -> float:
    """Sichere Konvertierung zu float mit Fallback"""
    try:
        if pd.isna(value) or value is None:
            return default
        return float(value)
    except (ValueError, TypeError):
        return default

def _safe_str(value, default="") -> str:
    """Sichere Konvertierung zu string mit Fallback"""
    try:
        if pd.isna(value) or value is None:
            return default
        return str(value).strip()
    except (ValueError, TypeError):
        return default

def _determine_asset_class(ticker: str) -> str:
    """Bestimmt Asset-Class aus Ticker-Symbol"""
    ticker_upper = ticker.upper()
    if "-USD" in ticker_upper or "BTC" in ticker_upper or "ETH" in ticker_upper:
        return "crypto"
    return "stock"

def _calculate_liquidity_risk(dollar_volume: float) -> float:
    """
    Berechnet Liquidity-Risk Score (0=sehr liquide, 1=sehr illiquide)
    Basierend auf Dollar Volume
    """
    if dollar_volume <= 0:
        return 0.5  # Default bei fehlenden Daten
    
    # Einfache Klassifikation basierend auf täglichem Dollar Volume
    if dollar_volume >= 50_000_000:  # > $50M
        return 0.1  # Sehr liquide
    elif dollar_volume >= 10_000_000:  # > $10M
        return 0.2  # Liquide
    elif dollar_volume >= 1_000_000:  # > $1M
        return 0.4  # Medium liquide
    elif dollar_volume >= 100_000:  # > $100k
        return 0.7  # Illiquide
    else:
        return 0.9  # Sehr illiquide

def _get_max_equity_exposure(market_regime: str) -> float:
    """Max Aktien-Exposure basierend auf Market Regime"""
    regime = market_regime.upper()
    if regime == "BULL":
        return 1.0  # 100%
    elif regime == "NEUTRAL":
        return 0.7  # 70%
    elif regime == "BEAR":
        return 0.4  # 40%
    else:
        return 0.7  # Default: Neutral

def _get_max_crypto_exposure(market_regime: str) -> float:
    """Max Crypto-Exposure basierend auf Market Regime"""
    regime = market_regime.upper()
    if regime == "BULL":
        return 0.15  # 15%
    elif regime == "NEUTRAL":
        return 0.10  # 10%
    elif regime == "BEAR":
        return 0.05  # 5%
    else:
        return 0.10  # Default: Neutral

def build_portfolio(
    csv_path: str = "watchlist.csv",
    top_n: int = 10,
    min_score: float = 30.0,
    max_positions: int = 20,
    allow_crypto: bool = True,
) -> Dict[str, Any]:
    """
    Baut ein Portfolio aus dem v6 Scoring System.
    
    Args:
        csv_path: Pfad zur Watchlist CSV
        top_n: Anzahl der Top-Assets die berücksichtigt werden
        min_score: Minimaler Score für Selektion
        max_positions: Maximale Anzahl an Positionen
        allow_crypto: Ob Krypto-Assets erlaubt sind
        
    Returns:
        Dict mit Portfolio-Metadaten und Positionen
    """
    
    try:
        # CSV laden
        df = pd.read_csv(csv_path)
        logger.info(f"📊 CSV geladen: {len(df)} Assets")
        
        if df.empty:
            logger.warning("⚠️ CSV ist leer")
            return {"error": "CSV ist leer"}
        
        # Grundlegende Filterung
        # Score filtern
        df = df[df['Score'] >= min_score].copy()
        logger.info(f"🔍 Score-Filter (≥{min_score}): {len(df)} Assets")
        
        # Optional: Bear-Market Filter für Aktien
        if 'MarketRegimeStock' in df.columns:
            stock_regime = df['MarketRegimeStock'].iloc[0] if not df.empty else 'neutral'
            if stock_regime.upper() == 'BEAR':
                # In Bear-Markets nur Assets mit Score > 50
                df = df[df['Score'] > 50].copy()
                logger.info(f"🐻 Bear-Market Filter: {len(df)} Assets")
        
        # Asset-Class bestimmen (falls nicht vorhanden)
        if 'AssetClass' not in df.columns:
            df['AssetClass'] = df['Ticker'].apply(_determine_asset_class)
        
        # Crypto filtern falls nicht erlaubt
        if not allow_crypto:
            df = df[df['AssetClass'] == 'stock'].copy()
            logger.info(f"🚫 No-Crypto Filter: {len(df)} Assets")
        
        # Nach Score sortieren
        df = df.sort_values('Score', ascending=False)
        
        # Top-N selektieren
        df = df.head(min(top_n, max_positions))
        logger.info(f"🏆 Top-{len(df)} selektiert")
        
        if df.empty:
            logger.warning("⚠️ Keine Assets nach Filterung übrig")
            return {"error": "Keine Assets nach Filterung übrig"}
        
        # Liquidity Risk berechnen
        if 'DollarVolume' in df.columns:
            df['LiquidityRisk'] = df['DollarVolume'].apply(_calculate_liquidity_risk)
        else:
            df['LiquidityRisk'] = 0.5  # Default
            logger.warning("⚠️ DollarVolume nicht gefunden, verwende Default LiquidityRisk")
        
        # Portfolio-Gewichtung berechnen
        positions = _calculate_weights(df)
        
        # Market Regime für Exposure Control
        market_regime = _safe_str(df['MarketRegimeStock'].iloc[0] if 'MarketRegimeStock' in df.columns else None, 'neutral')
        max_equity_exposure = _get_max_equity_exposure(market_regime)
        max_crypto_exposure = _get_max_crypto_exposure(market_regime)
        
        # Exposure anpassen (sowohl Equity als auch Crypto)
        positions, cash_pct = _apply_exposure_control(positions, max_equity_exposure, max_crypto_exposure)
        
        # Portfolio zusammenbauen
        portfolio = {
            "meta": {
                "market_regime": market_regime,
                "total_positions": len(positions),
                "cash_pct": round(cash_pct * 100, 2),
                "max_equity_exposure": round(max_equity_exposure * 100, 1),
                "max_crypto_exposure": round(max_crypto_exposure * 100, 1),
                "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
                "selection_criteria": {
                    "min_score": min_score,
                    "top_n": top_n,
                    "max_positions": max_positions,
                    "allow_crypto": allow_crypto
                }
            },
            "positions": positions
        }
        
        logger.info(f"✅ Portfolio erstellt: {len(positions)} Positionen, {cash_pct*100:.1f}% Cash")
        return portfolio
        
    except Exception as e:
        logger.error(f"❌ Portfolio Builder Fehler: {e}")
        return {"error": str(e)}

def _calculate_weights(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Berechnet score-basierte Gewichte mit Liquidity-Adjustment und finalen Clamps.
    """
    positions = []
    
    # Score-basierte Rohgewichte
    total_score = df['Score'].sum()
    df['RawWeight'] = df['Score'] / total_score
    
    # Liquidity-Adjustment
    df['AdjustedWeight'] = df['RawWeight'] * (1 - df['LiquidityRisk'])
    
    # Risk-Clamp: Max 15% pro Position
    max_weight = 0.15
    df['ClampedWeight'] = df['AdjustedWeight'].clip(upper=max_weight)
    
    # Renormalisieren auf 100%
    total_clamped = df['ClampedWeight'].sum()
    df['FinalWeight'] = df['ClampedWeight'] / total_clamped
    
    # Min-Gewicht: 1%
    min_weight = 0.01
    df['FinalWeight'] = df['FinalWeight'].clip(lower=min_weight)
    
    # Nochmal renormalisieren nach Min-Gewicht-Anpassung
    total_final = df['FinalWeight'].sum()
    df['FinalWeight'] = df['FinalWeight'] / total_final
    
    # Final Review: Asset-Class spezifische Limits
    for asset_class in ['stock', 'crypto']:
        class_positions = df[df['AssetClass'] == asset_class]
        if len(class_positions) > 0:
            class_total = class_positions['FinalWeight'].sum()
            logger.info(f"📊 {asset_class.upper()} Raw Total: {class_total*100:.1f}%")
    
    # Positionen bauen
    for _, row in df.iterrows():
        position = {
            "Ticker": _safe_str(row['Ticker']),
            "AssetClass": _safe_str(row.get('AssetClass', _determine_asset_class(_safe_str(row['Ticker'])))),
            "Score": _safe_float(row['Score']),
            "WeightPct": round(row['FinalWeight'] * 100, 2),
            "LiquidityRisk": round(_safe_float(row['LiquidityRisk']), 3),
            "RS3M": round(_safe_float(row.get('RS3M')), 4),
            "Trend200": round(_safe_float(row.get('Trend200')), 4),
            "DollarVolume": _safe_float(row.get('DollarVolume')),
            "RawWeight": round(row['RawWeight'] * 100, 2),
            "LiquidityAdjusted": round(row['AdjustedWeight'] * 100, 2)
        }
        positions.append(position)
    
    return positions

def _apply_exposure_control(positions: List[Dict[str, Any]], max_equity_exposure: float, max_crypto_exposure: float) -> tuple:
    """
    Wendet Regime-Aware Exposure Control an (sowohl Equity als auch Crypto).
    """
    equity_positions = [p for p in positions if p['AssetClass'] == 'stock']
    crypto_positions = [p for p in positions if p['AssetClass'] == 'crypto']
    
    # Equity Exposure Control
    if equity_positions:
        total_equity_weight = sum(p['WeightPct'] / 100 for p in equity_positions)
        
        if total_equity_weight > max_equity_exposure:
            # Aktien-Gewichte skalieren
            scale_factor = max_equity_exposure / total_equity_weight
            
            for pos in positions:
                if pos['AssetClass'] == 'stock':
                    old_weight = pos['WeightPct']
                    pos['WeightPct'] = round(old_weight * scale_factor, 2)
            
            logger.info(f"📊 Equity Exposure Control: {total_equity_weight*100:.1f}% → {max_equity_exposure*100:.1f}%")
    
    # Crypto Exposure Control
    if crypto_positions:
        total_crypto_weight = sum(p['WeightPct'] / 100 for p in crypto_positions)
        
        if total_crypto_weight > max_crypto_exposure:
            # Crypto-Gewichte skalieren
            scale_factor = max_crypto_exposure / total_crypto_weight
            
            for pos in positions:
                if pos['AssetClass'] == 'crypto':
                    old_weight = pos['WeightPct']
                    pos['WeightPct'] = round(old_weight * scale_factor, 2)
            
            logger.info(f"📊 Crypto Exposure Control: {total_crypto_weight*100:.1f}% → {max_crypto_exposure*100:.1f}%")
    
    # Cash-Quote berechnen
    total_weight_after_scaling = sum(p['WeightPct'] / 100 for p in positions)
    cash_pct = 1.0 - total_weight_after_scaling
    
    logger.info(f"📊 Final Exposure: Cash {cash_pct*100:.1f}%")
    
    return positions, cash_pct

def export_portfolio_to_csv(portfolio: Dict[str, Any], output_path: str = "portfolio.csv") -> bool:
    """
    Exportiert Portfolio in CSV-Format mit Metadaten.
    """
    try:
        if "positions" not in portfolio:
            logger.error("❌ Keine Positionen im Portfolio")
            return False
        
        positions = portfolio["positions"]
        if not positions:
            logger.error("❌ Portfolio ist leer")
            return False
        
        # DataFrame erstellen
        df = pd.DataFrame(positions)
        
        # Spalten anordnen
        columns = [
            'Ticker', 'AssetClass', 'Score', 'WeightPct', 
            'LiquidityRisk', 'RS3M', 'Trend200', 
            'DollarVolume', 'RawWeight', 'LiquidityAdjusted'
        ]
        
        # Nur vorhandene Spalten verwenden
        available_columns = [col for col in columns if col in df.columns]
        df = df[available_columns]
        
        # CSV speichern
        df.to_csv(output_path, index=False, encoding='utf-8')
        
        # Metadaten als separate Datei
        meta_path = output_path.replace('.csv', '_meta.json')
        import json
        with open(meta_path, 'w', encoding='utf-8') as f:
            json.dump(portfolio.get('meta', {}), f, indent=2, ensure_ascii=False)
        
        logger.info(f"✅ Portfolio exportiert: {output_path}")
        logger.info(f"✅ Metadaten exportiert: {meta_path}")
        
        # Verification
        with open(output_path, 'r', encoding='utf-8') as f:
            content = f.read()
            logger.info(f"� CSV Export: {len(content)} chars, {df.shape[0]} positions")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ CSV Export Fehler: {e}")
        import traceback
        traceback.print_exc()
        return False

# === TEST-FUNKTION ===
if __name__ == "__main__":
    print("🏗️  Portfolio Builder Test")
    print("=" * 50)
    
    # Portfolio mit Default-Parametern (BULL Market)
    print("\n📈 BULL MARKET TEST:")
    portfolio = build_portfolio(
        top_n=10,
        min_score=30.0,
        max_positions=15,
        allow_crypto=True
    )
    
    if "error" in portfolio:
        print(f"❌ Fehler: {portfolio['error']}")
    else:
        print(f"✅ Portfolio erstellt!")
        print(f"📊 Meta: {portfolio['meta']}")
        print(f"📈 Positionen ({len(portfolio['positions'])}):")
        
        for pos in portfolio['positions']:
            print(f"  • {pos['Ticker']} ({pos['AssetClass']}): {pos['WeightPct']}% | Score: {pos['Score']} | RS3M: {pos['RS3M']:+.1%}")
        
        # CSV Export
        export_portfolio_to_csv(portfolio)
        print(f"\n💾 Portfolio exportiert nach portfolio.csv")
    
    # Test mit BEAR Market (manueller Regime Override)
    print("\n\n🐻 BEAR MARKET TEST:")
    # Temporäres CSV mit BEAR Regime erstellen
    import pandas as pd
    df = pd.read_csv("watchlist.csv")
    df['MarketRegimeStock'] = 'bear'  # Override zu BEAR
    df.to_csv("watchlist_bear_test.csv", index=False)
    
    portfolio_bear = build_portfolio(
        csv_path="watchlist_bear_test.csv",
        top_n=10,
        min_score=30.0,
        max_positions=15,
        allow_crypto=True
    )
    
    if "error" not in portfolio_bear:
        print(f"✅ Bear-Market Portfolio erstellt!")
        print(f"📊 Meta: {portfolio_bear['meta']}")
        print(f"📈 Positionen ({len(portfolio_bear['positions'])}):")
        
        for pos in portfolio_bear['positions']:
            print(f"  • {pos['Ticker']} ({pos['AssetClass']}): {pos['WeightPct']}% | Score: {pos['Score']} | RS3M: {pos['RS3M']:+.1%}")
        
        # CSV Export
        export_portfolio_to_csv(portfolio_bear, "portfolio_bear.csv")
        print(f"\n💾 Bear-Market Portfolio exportiert nach portfolio_bear.csv")
    
    print("\n🎯 PORTFOLIO BUILDER ERFOLGREICH!")
