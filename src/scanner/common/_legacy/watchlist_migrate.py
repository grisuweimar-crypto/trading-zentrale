"""
Watchlist Migration Utility

Einmalige Migration zur Säuberung der Identifier-Semantik in watchlist.csv.
Stellt klare Trennung sicher: ISIN (Matching), YahooSymbol (API), Symbol (Anzeige).

Author: Trading-Zentrale v6
"""

import logging
import re
import pandas as pd
import os
from typing import Dict, List, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

# ISIN Pattern: 2 Buchstaben + 10 alphanumerische Zeichen
ISIN_PATTERN = re.compile(r'^[A-Z]{2}[A-Z0-9]{10}$')

# Crypto Symbols die -USD Suffix benötigen
CRYPTO_SYMBOLS = {'BTC', 'ETH', 'XRP', 'SOL', 'DOGE', 'MANA', 'AVAX'}

def migrate_watchlist_inplace(path: str = "watchlist.csv") -> Dict[str, int]:
    """
    Führt einmalige Migration der watchlist.csv durch.
    
    Args:
        path: Pfad zur watchlist.csv
        
    Returns:
        Report-Dict mit Migrations-Statistiken
    """
    try:
        logger.info(f"🔄 Starting watchlist migration: {path}")
        
        # 1. CSV laden
        df = pd.read_csv(path, encoding='utf-8')
        original_shape = df.shape
        logger.info(f"📊 Loaded {original_shape[0]} rows, {original_shape[1]} columns")
        
        # 2. Zielspalten sicherstellen
        required_cols = ['ISIN', 'Symbol', 'YahooSymbol', 'Ticker', 'Yahoo']
        for col in required_cols:
            if col not in df.columns:
                df[col] = ''
                logger.info(f"➕ Added missing column: {col}")
        
        # 3. Migration durchführen
        report = _execute_migration(df)
        
        # 4. Spalten-Reihenfolge optimieren
        df = _optimize_column_order(df)
        
        # 5. Zurückschreiben
        df.to_csv(path, index=False, encoding='utf-8')
        
        # 6. Final Report
        report['original_rows'] = original_shape[0]
        report['original_columns'] = original_shape[1]
        report['final_columns'] = df.shape[1]
        
        logger.info(f"✅ Watchlist migration completed: {report}")
        return report
        
    except Exception as e:
        logger.error(f"❌ Watchlist migration failed: {e}")
        return {'error': str(e)}

def _execute_migration(df: pd.DataFrame) -> Dict[str, int]:
    """
    Führt die eigentlichen Migrationsregeln durch.
    
    Args:
        df: DataFrame mit allen Spalten
        
    Returns:
        Report-Dict mit Änderungs-Statistiken
    """
    report = {
        'count_isin_filled': 0,
        'count_yahoosymbol_from_real_source': 0,
        'count_yahoosymbol_from_map': 0,
        'count_yahoosymbol_cleared_isin': 0,
        'count_symbol_fixed_from_isin': 0,
        'count_rows_isin_only': 0,
        'count_crypto_usd_added': 0
    }
    
    def is_isin(x):
        if pd.isna(x): return False
        return bool(ISIN_PATTERN.match(str(x).strip()))
    
    def is_real_ticker(x):
        if pd.isna(x): return False
        s = str(x).strip()
        if s == '': return False
        return (not is_isin(s)) and any(ch.isalpha() for ch in s)
    
    # Lade symbol_map.csv falls vorhanden
    map_isin_to_yahoo = {}
    symbol_map_path = 'data/holdings/symbol_map.csv'
    if os.path.exists(symbol_map_path):
        try:
            symbol_map_df = pd.read_csv(symbol_map_path, sep=';', encoding='utf-8')
            for _, row in symbol_map_df.iterrows():
                isin_key = str(row.get('ISIN', '')).strip().upper()
                yahoo_val = str(row.get('YahooSymbol', '')).strip()
                if isin_key and yahoo_val:
                    map_isin_to_yahoo[isin_key] = yahoo_val
            logger.info(f"📋 Loaded {len(map_isin_to_yahoo)} mappings from symbol_map.csv")
        except Exception as e:
            logger.warning(f"⚠️ Could not load symbol_map.csv: {e}")
    
    for idx, row in df.iterrows():
        # Aktuelle Werte holen (mit NaN-Handling)
        current_isin = _clean_value(row.get('ISIN', ''))
        current_symbol = _clean_value(row.get('Symbol', ''))
        current_yahoo = _clean_value(row.get('YahooSymbol', ''))
        current_ticker = _clean_value(row.get('Ticker', ''))
        current_yahoo_col = _clean_value(row.get('Yahoo', ''))
        
        # Regel 1: ISIN füllen (aus echten Quellen)
        if not current_isin:
            if is_isin(current_symbol):
                df.at[idx, 'ISIN'] = current_symbol.upper()
                report['count_isin_filled'] += 1
                current_isin = current_symbol.upper()
            elif is_isin(current_ticker):
                df.at[idx, 'ISIN'] = current_ticker.upper()
                report['count_isin_filled'] += 1
                current_isin = current_ticker.upper()
            elif is_isin(current_yahoo):
                df.at[idx, 'ISIN'] = current_yahoo.upper()
                report['count_isin_filled'] += 1
                current_isin = current_yahoo.upper()
            elif is_isin(current_yahoo_col):
                df.at[idx, 'ISIN'] = current_yahoo_col.upper()
                report['count_isin_filled'] += 1
                current_isin = current_yahoo_col.upper()
        
        # Regel 2: YahooSymbol aus echten Quellen
        best_candidate = None
        for source in [current_yahoo, current_yahoo_col, current_symbol, current_ticker]:
            if source and is_real_ticker(source):
                best_candidate = source.strip()
                break
        
        if best_candidate and current_yahoo != best_candidate:
            df.at[idx, 'YahooSymbol'] = best_candidate
            report['count_yahoosymbol_from_real_source'] += 1
            current_yahoo = best_candidate
        
        # Regel 3: Apply symbol_map (hat Priorität)
        if current_isin and current_isin in map_isin_to_yahoo:
            mapped_yahoo = map_isin_to_yahoo[current_isin]
            if current_yahoo != mapped_yahoo:
                df.at[idx, 'YahooSymbol'] = mapped_yahoo
                report['count_yahoosymbol_from_map'] += 1
                current_yahoo = mapped_yahoo
        
        # Regel 4: HARD RULE - YahooSymbol nie ISIN
        if is_isin(current_yahoo):
            df.at[idx, 'YahooSymbol'] = ''
            report['count_yahoosymbol_cleared_isin'] += 1
            current_yahoo = ''
        
        # Regel 5: Symbol ent-ISIN-en (nur wenn YahooSymbol verfügbar)
        if is_isin(current_symbol) and current_yahoo:
            df.at[idx, 'Symbol'] = current_yahoo
            report['count_symbol_fixed_from_isin'] += 1
            current_symbol = current_yahoo
        
        # Regel 6: Crypto -USD Normalisierung
        crypto_normalized = False
        if current_symbol and not current_symbol.endswith('-USD'):
            symbol_base = current_symbol.upper()
            if symbol_base in CRYPTO_SYMBOLS:
                new_symbol = f"{symbol_base}-USD"
                df.at[idx, 'Symbol'] = new_symbol
                df.at[idx, 'YahooSymbol'] = new_symbol
                report['count_crypto_usd_added'] += 1
                crypto_normalized = True
                current_symbol = new_symbol
                current_yahoo = new_symbol
        
        # Auch YahooSymbol prüfen falls Symbol nicht crypto war
        if not crypto_normalized and current_yahoo and not current_yahoo.endswith('-USD'):
            yahoo_base = current_yahoo.upper()
            if yahoo_base in CRYPTO_SYMBOLS:
                new_yahoo = f"{yahoo_base}-USD"
                df.at[idx, 'YahooSymbol'] = new_yahoo
                df.at[idx, 'Symbol'] = new_yahoo
                report['count_crypto_usd_added'] += 1
                current_yahoo = new_yahoo
                current_symbol = new_yahoo
        
        # Regel 7: Final cleanup
        # ISIN uppercase
        if current_isin:
            df.at[idx, 'ISIN'] = current_isin.upper()
        
        # Symbol/YahooSymbol strip
        if current_symbol:
            df.at[idx, 'Symbol'] = current_symbol.strip()
        if current_yahoo:
            df.at[idx, 'YahooSymbol'] = current_yahoo.strip()
        
        # Statistiken sammeln
        if is_isin(_clean_value(df.at[idx, 'Symbol'])) and not _clean_value(df.at[idx, 'YahooSymbol']):
            report['count_rows_isin_only'] += 1
    
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
    
    for col in ['ISIN', 'Symbol', 'YahooSymbol', 'Ticker', 'Yahoo']:
        if col in columns:
            identifier_cols.append(col)
            columns.remove(col)
    
    # Restliche Spalten beibehalten
    other_cols = columns
    
    # Neue Reihenfolge zusammenbauen
    new_order = identifier_cols + other_cols
    
    return df[new_order]

def validate_migration_result(path: str = "watchlist.csv") -> Dict[str, bool]:
    """
    Validiert das Ergebnis der Migration.
    
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
            'rows_with_isin': False,
            'rows_with_yahoosymbol': False,
            'valid_isin_patterns': False,
            'no_isin_as_symbol': False,
            'crypto_have_usd': False
        }
        
        if results['has_data']:
            # ISIN-Prüfung
            isin_col = df['ISIN'].fillna('')
            results['rows_with_isin'] = (isin_col != '').any()
            
            # YahooSymbol-Prüfung
            yahoosymbol_col = df['YahooSymbol'].fillna('')
            results['rows_with_yahoosymbol'] = (yahoosymbol_col != '').any()
            
            # ISIN-Pattern-Prüfung
            valid_isins = isin_col.apply(lambda x: _is_valid_isin(str(x)) if str(x) else True)
            results['valid_isin_patterns'] = valid_isins.all()
            
            # Keine ISINs als Symbol
            symbol_col = df['Symbol'].fillna('')
            isin_as_symbols = symbol_col.apply(lambda x: _is_valid_isin(str(x)))
            results['no_isin_as_symbol'] = not isin_as_symbols.any()
            
            # Crypto haben -USD
            crypto_rows = df[symbol_col.apply(lambda x: str(x).upper() in CRYPTO_SYMBOLS)]
            if not crypto_rows.empty:
                crypto_with_usd = crypto_rows['Symbol'].apply(lambda x: str(x).endswith('-USD'))
                results['crypto_have_usd'] = crypto_with_usd.all()
            else:
                results['crypto_have_usd'] = True
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Migration validation failed: {e}")
        return {'error': str(e)}

def print_migration_report(report: Dict[str, int]) -> None:
    """
    Druckt einen lesbaren Migrations-Report.
    
    Args:
        report: Report-Dict von migrate_watchlist_inplace()
    """
    if 'error' in report:
        print(f"Migration failed: {report['error']}")
        return
    
    print("WATCHLIST MIGRATION REPORT")
    print("=" * 50)
    print(f"Original: {report.get('original_rows', 0)} rows, {report.get('original_columns', 0)} columns")
    print(f"Final: {report.get('final_columns', 0)} columns")
    print("")
    print("Changes:")
    print(f"  • ISIN filled: {report.get('count_isin_filled', 0)}")
    print(f"  • YahooSymbol from real source: {report.get('count_yahoosymbol_from_real_source', 0)}")
    print(f"  • YahooSymbol from symbol_map: {report.get('count_yahoosymbol_from_map', 0)}")
    print(f"  • YahooSymbol cleared (was ISIN): {report.get('count_yahoosymbol_cleared_isin', 0)}")
    print(f"  • Symbol fixed from ISIN: {report.get('count_symbol_fixed_from_isin', 0)}")
    print(f"  • ISIN-only rows: {report.get('count_rows_isin_only', 0)}")
    print(f"  • Crypto -USD added: {report.get('count_crypto_usd_added', 0)}")
    print("=" * 50)

def print_validation_report(validation: Dict[str, bool]) -> None:
    """
    Druckt einen lesbaren Validierungs-Report.
    
    Args:
        validation: Validierungsergebnisse
    """
    if 'error' in validation:
        print(f"Validation failed: {validation['error']}")
        return
    
    print("MIGRATION VALIDATION REPORT")
    print("=" * 50)
    
    for key, value in validation.items():
        status = 'OK' if value else 'FAIL'
        print(f"{status} {key}: {value}")
    
    print("=" * 50)
