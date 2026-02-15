"""
Watchlist Normalizer Utility

Stellt saubere Semantik der Identifier-Spalten in watchlist.csv sicher.
Feste Spalten: ISIN, Symbol, YahooSymbol mit klaren Regeln.

Author: Trading-Zentrale v6
"""

import logging
import re
import pandas as pd
from typing import Dict, List, Optional, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)

# ISIN Pattern: 2 Buchstaben + 10 alphanumerische Zeichen
ISIN_PATTERN = re.compile(r'^[A-Z]{2}[A-Z0-9]{10}$')

# Crypto Symbols die -USD Suffix benötigen
CRYPTO_SYMBOLS = {'BTC', 'ETH', 'XRP', 'SOL', 'DOGE', 'MANA', 'AVAX'}

def normalize_watchlist_inplace(path: str = "watchlist.csv") -> Dict[str, int]:
    """
    Normalisiert watchlist.csv inplace mit fester Spalten-Semantik.
    
    Args:
        path: Pfad zur watchlist.csv
        
    Returns:
        Report-Dict mit Änderungs-Statistiken
    """
    try:
        logger.info(f"🔧 Normalizing watchlist: {path}")
        
        # 1. CSV laden
        df = pd.read_csv(path, encoding='utf-8')
        original_shape = df.shape
        logger.info(f"📊 Loaded {original_shape[0]} rows, {original_shape[1]} columns")
        
        # 2. Spalten-Erkennung
        input_columns = _detect_input_columns(df.columns.tolist())
        logger.info(f"🔍 Detected input columns: {input_columns}")
        
        # 3. Zielspalten sicherstellen
        df = _ensure_target_columns(df, input_columns)
        
        # 4. Normalisierung durchführen
        report = _normalize_identifiers(df, input_columns)
        
        # 5. Spalten-Reihenfolge optimieren
        df = _optimize_column_order(df)
        
        # 6. Zurückschreiben
        df.to_csv(path, index=False, encoding='utf-8')
        
        # 7. Final Report
        report['original_rows'] = original_shape[0]
        report['original_columns'] = original_shape[1]
        report['final_columns'] = df.shape[1]
        
        logger.info(f"✅ Watchlist normalized: {report}")
        return report
        
    except Exception as e:
        logger.error(f"❌ Watchlist normalization failed: {e}")
        return {'error': str(e)}

def _detect_input_columns(columns: List[str]) -> Dict[str, str]:
    """
    Erkennt mögliche Input-Spalten für Identifier.
    
    Args:
        columns: Liste der Spaltennamen
        
    Returns:
        Dict mapping von Spaltentyp zu Spaltenname
    """
    # Normalisiere Spaltennamen für Matching (case-insensitive)
    normalized_cols = {col.lower(): col for col in columns}
    
    input_map = {}
    
    # ISIN-Spalten
    for possible in ['isin', 'is_in', 'isin_code']:
        if possible in normalized_cols:
            input_map['isin'] = normalized_cols[possible]
            break
    
    # Symbol-Spalten
    for possible in ['symbol', 'ticker', 'sym']:
        if possible in normalized_cols:
            input_map['symbol'] = normalized_cols[possible]
            break
    
    # Yahoo-Spalten
    for possible in ['yahoosymbol', 'yahoo', 'yahoo_symbol']:
        if possible in normalized_cols:
            input_map['yahoosymbol'] = normalized_cols[possible]
            break
    
    return input_map

def _ensure_target_columns(df: pd.DataFrame, input_columns: Dict[str, str]) -> pd.DataFrame:
    """
    Stellt sicher dass Zielspalten existieren.
    
    Args:
        df: DataFrame
        input_columns: Erkannte Input-Spalten
        
    Returns:
        DataFrame mit Zielspalten
    """
    # ISIN-Spalte sicherstellen
    if 'ISIN' not in df.columns:
        df['ISIN'] = ''
        logger.info("➕ Added ISIN column")
    elif input_columns.get('isin') != 'ISIN':
        # Wenn ISIN aus anderer Spalte kopiert werden soll
        if input_columns.get('isin'):
            df['ISIN'] = df[input_columns['isin']].fillna('')
            logger.info(f"📋 Copied ISIN from {input_columns['isin']}")
    
    # Symbol-Spalte sicherstellen
    if 'Symbol' not in df.columns:
        df['Symbol'] = ''
        logger.info("➕ Added Symbol column")
    elif input_columns.get('symbol') != 'Symbol':
        # Wenn Symbol aus anderer Spalte kopiert werden soll
        if input_columns.get('symbol'):
            df['Symbol'] = df[input_columns['symbol']].fillna('')
            logger.info(f"📋 Copied Symbol from {input_columns['symbol']}")
    
    # YahooSymbol-Spalte sicherstellen
    if 'YahooSymbol' not in df.columns:
        df['YahooSymbol'] = ''
        logger.info("➕ Added YahooSymbol column")
    elif input_columns.get('yahoosymbol') != 'YahooSymbol':
        # Wenn YahooSymbol aus anderer Spalte kopiert werden soll
        if input_columns.get('yahoosymbol'):
            df['YahooSymbol'] = df[input_columns['yahoosymbol']].fillna('')
            logger.info(f"📋 Copied YahooSymbol from {input_columns['yahoosymbol']}")
    
    return df

def _normalize_identifiers(df: pd.DataFrame, input_columns: Dict[str, str]) -> Dict[str, int]:
    """
    Führt die eigentliche Normalisierung durch.
    
    Args:
        df: DataFrame mit Zielspalten
        input_columns: Erkannte Input-Spalten
        
    Returns:
        Report-Dict mit Änderungs-Statistiken
    """
    report = {
        'count_isin_filled': 0,
        'count_symbol_filled': 0,
        'count_yahoo_filled': 0,
        'count_symbol_changed': 0,
        'count_yahoo_changed': 0,
        'count_rows_missing_symbol': 0
    }
    
    for idx, row in df.iterrows():
        # Aktuelle Werte holen (mit NaN-Handling)
        current_isin = _clean_value(row.get('ISIN', ''))
        current_symbol = _clean_value(row.get('Symbol', ''))
        current_yahoo = _clean_value(row.get('YahooSymbol', ''))
        
        # a) ISIN füllen wenn leer
        if not current_isin:
            # Suche ISIN in anderen Spalten
            for col_type in ['symbol', 'yahoosymbol']:
                source_col = input_columns.get(col_type)
                if source_col and source_col in df.columns:
                    candidate = _clean_value(row.get(source_col, ''))
                    if _is_valid_isin(candidate):
                        df.at[idx, 'ISIN'] = candidate
                        report['count_isin_filled'] += 1
                        current_isin = candidate
                        break
        
        # b) Symbol festlegen
        new_symbol = current_symbol
        if not new_symbol:
            # Versuche Symbol aus anderen Spalten
            for col_type in ['symbol', 'yahoosymbol']:
                source_col = input_columns.get(col_type)
                if source_col and source_col in df.columns:
                    candidate = _clean_value(row.get(source_col, ''))
                    if candidate:
                        # Wenn Kandidat wie ISIN aussieht, aber ISIN leer ist
                        if _is_valid_isin(candidate) and not current_isin:
                            new_symbol = candidate
                            df.at[idx, 'ISIN'] = candidate
                            report['count_isin_filled'] += 1
                        else:
                            new_symbol = candidate
                        break
        
        # Symbol setzen falls geändert
        if new_symbol and new_symbol != current_symbol:
            df.at[idx, 'Symbol'] = new_symbol
            report['count_symbol_filled'] += 1
            if current_symbol:  # Nur zählen wenn vorher vorhanden
                report['count_symbol_changed'] += 1
            current_symbol = new_symbol
        
        # c) YahooSymbol festlegen
        new_yahoo = current_yahoo
        if not new_yahoo:
            new_yahoo = current_symbol
        
        # Crypto Normalisierung
        if current_symbol:
            crypto_normalized = _normalize_crypto_symbol(current_symbol)
            if crypto_normalized != current_symbol:
                new_yahoo = crypto_normalized
                new_symbol = crypto_normalized
                df.at[idx, 'Symbol'] = new_symbol
                report['count_symbol_changed'] += 1
        
        # YahooSymbol setzen falls geändert
        if new_yahoo and new_yahoo != current_yahoo:
            df.at[idx, 'YahooSymbol'] = new_yahoo
            report['count_yahoo_filled'] += 1
            if current_yahoo:  # Nur zählen wenn vorher vorhanden
                report['count_yahoo_changed'] += 1
        
        # Prüfe ob Symbol immer noch fehlt
        if not _clean_value(df.at[idx, 'Symbol']):
            report['count_rows_missing_symbol'] += 1
    
    return report

def _clean_value(value) -> str:
    """
    Bereinigt einen Wert für Identifier-Verarbeitung.
    
    Args:
        value: Input-Wert
        
    Returns:
        Bereinigter String
    """
    if pd.isna(value) or value is None:
        return ''
    
    # Zu String konvertieren und trimmen
    str_value = str(value).strip()
    
    # Leerzeichen und Sonderzeichen entfernen
    str_value = re.sub(r'\s+', ' ', str_value).strip()
    
    return str_value

def _is_valid_isin(value: str) -> bool:
    """
    Prüft ob Wert ein gültiges ISIN-Pattern hat.
    
    Args:
        value: Zu prüfender Wert
        
    Returns:
        True wenn gültiges ISIN-Pattern
    """
    if not value:
        return False
    
    # Upper-case und prüfe Pattern
    return bool(ISIN_PATTERN.match(value.upper()))

def _normalize_crypto_symbol(symbol: str) -> str:
    """
    Normalisiert Crypto-Symbole mit -USD Suffix.
    
    Args:
        symbol: Input-Symbol
        
    Returns:
        Normalisiertes Symbol
    """
    if not symbol:
        return symbol
    
    # Upper-case für Vergleich
    symbol_upper = symbol.upper()
    
    # Prüfe ob es ein bekanntes Crypto-Symbol ist
    base_symbol = symbol_upper.replace('-USD', '')
    
    if base_symbol in CRYPTO_SYMBOLS and not symbol_upper.endswith('-USD'):
        return f"{base_symbol}-USD"
    
    return symbol

def _optimize_column_order(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optimiert die Spalten-Reihenfolge: Identifier zuerst.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame mit optimierter Reihenfolge
    """
    columns = df.columns.tolist()
    
    # Identifier-Spalten zuerst
    identifier_cols = []
    other_cols = []
    
    for col in ['ISIN', 'Symbol', 'YahooSymbol']:
        if col in columns:
            identifier_cols.append(col)
            columns.remove(col)
    
    # Restliche Spalten beibehalten
    other_cols = columns
    
    # Neue Reihenfolge zusammenbauen
    new_order = identifier_cols + other_cols
    
    return df[new_order]

def validate_watchlist_structure(path: str = "watchlist.csv") -> Dict[str, bool]:
    """
    Validiert watchlist.csv Struktur nach Normalisierung.
    
    Args:
        path: Pfad zur watchlist.csv
        
    Returns:
        Dict mit Validierungsergebnissen
    """
    try:
        df = pd.read_csv(path, encoding='utf-8')
        
        results = {
            'has_isin': 'ISIN' in df.columns,
            'has_symbol': 'Symbol' in df.columns,
            'has_yahoosymbol': 'YahooSymbol' in df.columns,
            'has_data': len(df) > 0,
            'rows_with_symbol': False,
            'rows_with_isin': False,
            'valid_isin_pattern': False
        }
        
        if results['has_data']:
            # Symbol-Prüfung
            symbol_col = df['Symbol'].fillna('')
            results['rows_with_symbol'] = (symbol_col != '').any()
            
            # ISIN-Prüfung
            isin_col = df['ISIN'].fillna('')
            results['rows_with_isin'] = (isin_col != '').any()
            
            # ISIN-Pattern-Prüfung
            valid_isins = isin_col.apply(lambda x: _is_valid_isin(str(x)))
            results['valid_isin_pattern'] = valid_isins.any()
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Watchlist validation failed: {e}")
        return {'error': str(e)}

def print_normalization_report(report: Dict[str, int]) -> None:
    """
    Druckt einen lesbaren Normalisierungs-Report.
    
    Args:
        report: Report-Dict von normalize_watchlist_inplace()
    """
    if 'error' in report:
        print(f"Normalization failed: {report['error']}")
        return
    
    print("WATCHLIST NORMALIZATION REPORT")
    print("=" * 50)
    print(f"Original: {report.get('original_rows', 0)} rows, {report.get('original_columns', 0)} columns")
    print(f"Final: {report.get('final_columns', 0)} columns")
    print("")
    print("Changes:")
    print(f"  • ISIN filled: {report.get('count_isin_filled', 0)}")
    print(f"  • Symbol filled: {report.get('count_symbol_filled', 0)}")
    print(f"  • YahooSymbol filled: {report.get('count_yahoo_filled', 0)}")
    print(f"  • Symbol changed: {report.get('count_symbol_changed', 0)}")
    print(f"  • YahooSymbol changed: {report.get('count_yahoo_changed', 0)}")
    print("")
    print("Issues:")
    print(f"  • Rows missing Symbol: {report.get('count_rows_missing_symbol', 0)}")
    print("=" * 50)
