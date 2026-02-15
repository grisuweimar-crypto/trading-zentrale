"""
Matcher Module

Matcht Broker-Positionen mit Watchlist/Target-Portfolio.
ISIN-primäres Matching mit Fallbacks und Symbol-Mapping.

Author: Trading-Zentrale v6
"""

import pandas as pd
import logging
from typing import Dict, List, Any, Optional, Tuple

logger = logging.getLogger(__name__)

def match_holdings_to_symbols(
    holdings: Dict[str, Any],
    watchlist_path: str = "watchlist.csv",
    symbol_map_path: str = "data/holdings/symbol_map.csv"
) -> Dict[str, Any]:
    """
    Matcht Holdings-Positionen mit Watchlist-Symbolen.
    
    Args:
        holdings: Holdings-Dict vom holdings_loader
        watchlist_path: Pfad zur watchlist.csv
        symbol_map_path: Pfad zur Symbol-Mapping-Datei
        
    Returns:
        Dict mit gematchten Positionen und Statistik
    """
    try:
        logger.info("🔍 Starting holdings-to-symbols matching...")
        
        # Watchlist laden
        watchlist_df = _load_watchlist(watchlist_path)
        if watchlist_df is None:
            return {"error": "Could not load watchlist"}
        
        # Symbol-Map laden
        symbol_map = _load_symbol_map(symbol_map_path)
        
        # Matching durchführen
        matched_positions = []
        unmatched_positions = []
        
        for position in holdings.get('positions', []):
            match_result = _match_single_position(position, watchlist_df, symbol_map)
            
            if match_result['matched']:
                matched_position = {
                    **position,
                    'symbol': match_result['symbol'],
                    'match_method': match_result['method'],
                    'watchlist_data': match_result['watchlist_data']
                }
                matched_positions.append(matched_position)
            else:
                unmatched_positions.append({
                    **position,
                    'match_error': match_result.get('error', 'No match found')
                })
        
        # Statistik
        total_positions = len(holdings.get('positions', []))
        matched_count = len(matched_positions)
        unmatched_count = len(unmatched_positions)
        
        result = {
            'matched_positions': matched_positions,
            'unmatched_positions': unmatched_positions,
            'statistics': {
                'total': total_positions,
                'matched': matched_count,
                'unmatched': unmatched_count,
                'match_rate': (matched_count / total_positions * 100) if total_positions > 0 else 0
            }
        }
        
        logger.info(f"✅ Matching completed: {matched_count}/{total_positions} matched ({result['statistics']['match_rate']:.1f}%)")
        
        # Unmatched exportieren
        if unmatched_positions:
            from .holdings_loader import export_unmatched_positions
            export_unmatched_positions(unmatched_positions)
            logger.warning(f"⚠️ {unmatched_count} positions unmatched - see data/holdings/unmatched.csv")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Matching error: {e}")
        return {"error": str(e)}

def _load_watchlist(watchlist_path: str) -> Optional[pd.DataFrame]:
    """Lädt und normalisiert Watchlist."""
    try:
        df = pd.read_csv(watchlist_path)
        
        # Spalten normalisieren
        df.columns = df.columns.str.strip()
        
        # ISIN-Spalte sicherstellen
        if 'ISIN' not in df.columns:
            df['ISIN'] = ''
        
        # Ticker/Symbol-Spalte sicherstellen
        if 'Ticker' not in df.columns:
            if 'Symbol' in df.columns:
                df['Ticker'] = df['Symbol']
            else:
                df['Ticker'] = df.index.astype(str)
        
        logger.info(f"📊 Loaded watchlist: {len(df)} entries")
        return df
        
    except Exception as e:
        logger.error(f"❌ Error loading watchlist: {e}")
        return None

def _load_symbol_map(symbol_map_path: str) -> Dict[str, str]:
    """Lädt ISIN→YahooSymbol Mapping."""
    try:
        if not pd.io.common.file_exists(symbol_map_path):
            logger.info(f"📂 No symbol map found at {symbol_map_path}")
            return {}
        
        df = pd.read_csv(symbol_map_path, sep=';', encoding='utf-8')
        
        # Spalten normalisieren
        df.columns = df.columns.str.strip()
        
        # Akzeptiere beide Formate: ISIN;Symbol oder ISIN;YahooSymbol
        symbol_col = None
        if 'YahooSymbol' in df.columns:
            symbol_col = 'YahooSymbol'
        elif 'Symbol' in df.columns:
            symbol_col = 'Symbol'
            logger.info(f"📊 Using legacy Symbol column format in {symbol_map_path}")
        else:
            logger.warning(f"⚠️ Invalid symbol map format in {symbol_map_path}")
            return {}
        
        if 'ISIN' not in df.columns:
            logger.warning(f"⚠️ Missing ISIN column in {symbol_map_path}")
            return {}
        
        # Mapping erstellen
        symbol_map = {}
        for _, row in df.iterrows():
            isin = str(row['ISIN']).strip()
            symbol = str(row[symbol_col]).strip()
            if isin and symbol:
                symbol_map[isin] = symbol
        
        logger.info(f"📊 Loaded symbol map: {len(symbol_map)} mappings (using {symbol_col})")
        return symbol_map
        
    except Exception as e:
        logger.error(f"❌ Error loading symbol map: {e}")
        return {}

def _match_single_position(
    position: Dict[str, Any],
    watchlist_df: pd.DataFrame,
    symbol_map: Dict[str, str]
) -> Dict[str, Any]:
    """
    Matcht einzelne Position mit Watchlist.
    
    Priority: 1) ISIN, 2) WKN, 3) Name→Ticker, 4) Crypto-USD Rule
    """
    asset_class = position.get('asset_class', '').lower()
    isin = position.get('isin', '').strip()
    wkn = position.get('wkn', '').strip()
    name = position.get('name', '').strip()
    
    # 1) ISIN-Matching (primär)
    if isin:
        isin_matches = watchlist_df[watchlist_df['ISIN'].str.replace(' ', '') == isin.replace(' ', '')]
        if not isin_matches.empty:
            symbol = isin_matches.iloc[0]['Ticker']
            return {
                'matched': True,
                'symbol': symbol,
                'method': 'isin',
                'watchlist_data': isin_matches.iloc[0].to_dict()
            }
        
        # ISIN in Symbol-Map?
        if isin in symbol_map:
            mapped_symbol = symbol_map[isin]
            symbol_matches = watchlist_df[watchlist_df['Ticker'] == mapped_symbol]
            if not symbol_matches.empty:
                return {
                    'matched': True,
                    'symbol': mapped_symbol,
                    'method': 'isin_map',
                    'watchlist_data': symbol_matches.iloc[0].to_dict()
                }
    
    # 2) WKN-Matching (Sekundär für deutsche Aktien)
    if wkn and asset_class == 'stock':
        wkn_matches = watchlist_df[watchlist_df.get('WKN', pd.Series([''] * len(watchlist_df))) == wkn]
        if not wkn_matches.empty:
            symbol = wkn_matches.iloc[0]['Ticker']
            return {
                'matched': True,
                'symbol': symbol,
                'method': 'wkn',
                'watchlist_data': wkn_matches.iloc[0].to_dict()
            }
    
    # 3) Name→Ticker Matching (Fallback)
    if name:
        # Exakter Name-Match (Name column)
        name_matches = watchlist_df[watchlist_df['Name'].str.upper() == name.upper()]
        if not name_matches.empty:
            symbol = name_matches.iloc[0]['Ticker']
            return {
                'matched': True,
                'symbol': symbol,
                'method': 'name_exact',
                'watchlist_data': name_matches.iloc[0].to_dict()
            }
        
        # Exakter Name-Match (Ticker column)
        ticker_matches = watchlist_df[watchlist_df['Ticker'].str.upper() == name.upper()]
        if not ticker_matches.empty:
            symbol = ticker_matches.iloc[0]['Ticker']
            return {
                'matched': True,
                'symbol': symbol,
                'method': 'ticker_exact',
                'watchlist_data': ticker_matches.iloc[0].to_dict()
            }
        
        # Partial Name-Match (für Krypto)
        if asset_class == 'crypto':
            crypto_matches = watchlist_df[
                watchlist_df['Ticker'].str.contains(name.upper(), case=False, na=False)
            ]
            if not crypto_matches.empty:
                symbol = crypto_matches.iloc[0]['Ticker']
                return {
                    'matched': True,
                    'symbol': symbol,
                    'method': 'crypto_partial',
                    'watchlist_data': crypto_matches.iloc[0].to_dict()
                }
    
    # 4) Crypto-USD Rule (speziell für Krypto)
    if asset_class == 'crypto' and name:
        # BTC → BTC-USD, ETH → ETH-USD, etc.
        if not name.endswith('-USD'):
            usd_symbol = f"{name.upper()}-USD"
            usd_matches = watchlist_df[watchlist_df['Ticker'] == usd_symbol]
            if not usd_matches.empty:
                return {
                    'matched': True,
                    'symbol': usd_symbol,
                    'method': 'crypto_usd_rule',
                    'watchlist_data': usd_matches.iloc[0].to_dict()
                }
    
    # Kein Match gefunden
    return {
        'matched': False,
        'error': f"No match found for {name} (ISIN: {isin}, WKN: {wkn})",
        'method': 'none'
    }

def create_symbol_map_template(output_path: str = "data/holdings/symbol_map.csv") -> bool:
    """
    Erstellt Symbol-Map Template mit Header.
    
    Args:
        output_path: Ausgabepfad
        
    Returns:
        True bei Erfolg
    """
    try:
        template_data = {
            'ISIN': ['DE0007664039', 'US0378331005'],
            'Symbol': ['FME', 'AAPL']
        }
        
        df = pd.DataFrame(template_data)
        df.to_csv(output_path, index=False, sep=';', encoding='utf-8')
        
        logger.info(f"✅ Created symbol map template: {output_path}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create symbol map template: {e}")
        return False

def get_matching_statistics(match_result: Dict[str, Any]) -> str:
    """
    Formatiert Matching-Statistik für Logging.
    
    Args:
        match_result: Ergebnis von match_holdings_to_symbols
        
    Returns:
        Formatierter String
    """
    if "error" in match_result:
        return f"❌ Matching failed: {match_result['error']}"
    
    stats = match_result.get('statistics', {})
    total = stats.get('total', 0)
    matched = stats.get('matched', 0)
    unmatched = stats.get('unmatched', 0)
    match_rate = stats.get('match_rate', 0)
    
    lines = [
        "🔍 MATCHING STATISTICS",
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
    
    lines.append("=" * 30)
    
    return "\n".join(lines)
