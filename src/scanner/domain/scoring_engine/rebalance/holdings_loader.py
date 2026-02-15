"""
Holdings Loader Module

Liest echte Broker-Exports (stocks.csv, crypto.csv) mit robuster Verarbeitun
von deutschen CSV-Formaten (Semikolon, Komma-Dezimal).

Author: Trading-Zentrale v6
"""

import pandas as pd
import logging
from typing import Dict, List, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

def load_broker_holdings(
    stocks_path: str = "data/holdings/stocks.csv",
    crypto_path: str = "data/holdings/crypto.csv"
) -> Dict[str, Any]:
    """
    Lädt kombinierte Holdings aus Aktien- und Krypto-Depots.
    
    Args:
        stocks_path: Pfad zur Aktien-CSV
        crypto_path: Pfad zur Krypto-CSV
        
    Returns:
        Dict mit allen Positionen und Gesamtwert
    """
    try:
        all_positions = []
        total_value = 0.0
        
        # Aktien laden
        if Path(stocks_path).exists():
            stock_positions = _load_csv_holdings(stocks_path, 'stock')
            all_positions.extend(stock_positions)
            stock_value = sum(pos.get('value', 0) for pos in stock_positions)
            total_value += stock_value
            logger.info(f"📈 Loaded {len(stock_positions)} stock positions, value: {stock_value:.2f}€")
        else:
            logger.warning(f"⚠️ Stocks file not found: {stocks_path}")
        
        # Krypto laden
        if Path(crypto_path).exists():
            crypto_positions = _load_csv_holdings(crypto_path, 'crypto')
            all_positions.extend(crypto_positions)
            crypto_value = sum(pos.get('value', 0) for pos in crypto_positions)
            total_value += crypto_value
            logger.info(f"🪙 Loaded {len(crypto_positions)} crypto positions, value: {crypto_value:.2f}€")
        else:
            logger.warning(f"⚠️ Crypto file not found: {crypto_path}")
        
        if not all_positions:
            logger.error("❌ No positions found in any file")
            return {"error": "No positions found", "positions": [], "total_value": 0}
        
        result = {
            "positions": all_positions,
            "total_value": total_value,
            "stocks_count": len([p for p in all_positions if p.get('asset_class') == 'stock']),
            "crypto_count": len([p for p in all_positions if p.get('asset_class') == 'crypto'])
        }
        
        logger.info(f"✅ Total holdings: {len(all_positions)} positions, total value: {total_value:.2f}€")
        return result
        
    except Exception as e:
        logger.error(f"❌ Holdings loading error: {e}")
        return {"error": str(e), "positions": [], "total_value": 0}

def _load_csv_holdings(file_path: str, asset_class: str) -> List[Dict[str, Any]]:
    """
    Lädt einzelne CSV-Datei mit robuster Fehlerbehandlung.
    
    Args:
        file_path: Pfad zur CSV-Datei
        asset_class: 'stock' oder 'crypto'
        
    Returns:
        Liste von Positionen
    """
    try:
        # Deutsche CSV-Format robust einlesen
        df = pd.read_csv(
            file_path,
            sep=';',
            decimal=',',
            thousands='.',
            encoding='utf-8',
            encoding_errors='ignore',
            on_bad_lines='warn'
        )
        
        logger.info(f"📊 Loaded {len(df)} rows from {file_path}")
        
        if df.empty:
            return []
        
        # Spalten normalisieren
        df.columns = df.columns.str.strip()
        
        # Erforderliche Spalten prüfen
        required_cols = ['Name', 'Wert']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"❌ Missing columns in {file_path}: {missing_cols}")
            return []
        
        positions = []
        for _, row in df.iterrows():
            try:
                # Wert robust extrahieren
                value_raw = str(row.get('Wert', '')).strip()
                value = _parse_german_float(value_raw)
                
                if value is None or value <= 0:
                    logger.debug(f"⚠️ Skipping {row.get('Name', 'unknown')}: invalid value {value_raw}")
                    continue
                
                # Position erstellen
                position = {
                    "asset_class": asset_class,
                    "name": str(row.get('Name', '')).strip(),
                    "isin": str(row.get('ISIN', '')).strip(),
                    "wkn": str(row.get('WKN', '')).strip(),
                    "value": value,
                    "source_file": Path(file_path).name
                }
                
                # Zusätzliche Felder falls vorhanden
                optional_fields = ['Art', 'Stück', 'Kurs', 'Währung']
                for field in optional_fields:
                    if field in df.columns:
                        position[field.lower()] = row[field]
                
                positions.append(position)
                
            except Exception as e:
                logger.warning(f"⚠️ Error processing row in {file_path}: {e}")
                continue
        
        logger.info(f"✅ Processed {len(positions)} valid {asset_class} positions")
        return positions
        
    except Exception as e:
        logger.error(f"❌ Error loading {file_path}: {e}")
        return []

def _parse_german_float(value_str: str) -> Optional[float]:
    """
    Parst deutsche Fließkommazahlen robust.
    
    Args:
        value_str: String wie "1.234,56" oder "1234,56"
        
    Returns:
        Float oder None bei Fehler
    """
    if not value_str or value_str in ['', '-', '0', '0,0']:
        return None
    
    try:
        # Deutsche Formatierung: 1.234,56 → 1234.56
        cleaned = value_str.replace('.', '').replace(',', '.')
        return float(cleaned)
    except (ValueError, AttributeError):
        # Fallback: Versuch mit pandas
        try:
            return pd.to_numeric(value_str, decimal=',', thousands='.', errors='coerce')
        except:
            return None

def validate_holdings_structure(holdings: Dict[str, Any]) -> bool:
    """
    Validiert die Struktur der geladenen Holdings.
    
    Args:
        holdings: Holdings-Dict
        
    Returns:
        True wenn valide
    """
    if "error" in holdings:
        return False
    
    if not isinstance(holdings.get('positions'), list):
        return False
    
    if holdings.get('total_value', 0) <= 0:
        return False
    
    # Prüfe ob alle Positionen erforderliche Felder haben
    required_fields = ['asset_class', 'name', 'value']
    for pos in holdings['positions']:
        if not all(field in pos for field in required_fields):
            return False
        if pos['value'] <= 0:
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
    
    positions = holdings.get('positions', [])
    total = holdings.get('total_value', 0)
    stocks_count = holdings.get('stocks_count', 0)
    crypto_count = holdings.get('crypto_count', 0)
    
    lines = [
        "📊 BROKER HOLDINGS SUMMARY",
        "=" * 40,
        f"Total Value: {total:,.2f}€",
        f"Stocks: {stocks_count} positions",
        f"Crypto: {crypto_count} positions",
        f"Total: {len(positions)} positions",
        ""
    ]
    
    # Top 5 Positionen
    if positions:
        lines.append("Top 5:")
        sorted_pos = sorted(positions, key=lambda x: x["value"], reverse=True)[:5]
        for pos in sorted_pos:
            weight = (pos["value"] / total * 100) if total > 0 else 0
            name = pos['name'][:20] + "..." if len(pos['name']) > 20 else pos['name']
            lines.append(f"  • {name}: {pos['value']:,.2f}€ ({weight:.1f}%)")
    
    lines.append("=" * 40)
    
    return "\n".join(lines)

def export_unmatched_positions(unmatched: List[Dict[str, Any]], output_path: str = "data/holdings/unmatched.csv") -> bool:
    """
    Exportiert nicht gematchte Positionen zur Nachbearbeitung.
    
    Args:
        unmatched: Liste von nicht gematchten Positionen
        output_path: Ausgabepfad
        
    Returns:
        True bei Erfolg
    """
    try:
        if not unmatched:
            return True
        
        df = pd.DataFrame(unmatched)
        df.to_csv(output_path, index=False, sep=';', encoding='utf-8')
        logger.info(f"✅ Exported {len(unmatched)} unmatched positions to {output_path}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to export unmatched positions: {e}")
        return False
