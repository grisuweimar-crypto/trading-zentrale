import yfinance as yf
import pandas as pd

def get_ticker_symbol(row):
    """Mapping von ISIN/Name auf funktionierende Yahoo-Ticker."""
    isin = str(row.get('ISIN', ''))
    name = str(row.get('Name', '')).upper()
    
    # Bekannte Problemfälle und Korrekturen
    mapping = {
        "ALPHABET": "GOOGL",
        "APPLE": "AAPL",
        "ALLIANZ": "ALV.DE",
        "BASF": "BAS.DE",
        "BAYER": "BAYN.DE",      # Fix für Bayer
        "AUTOSTORE": "AUTO.OL",   # Fix für Autostore (Oslo)
        "BITCOIN": "BTC-EUR",
        "ETHEREUM": "ETH-EUR",
        "SOLANA": "SOL-EUR",
        "XRP": "XRP-EUR",
        "ADA": "ADA-EUR",         # Cardano Fix
        "DOGE": "DOGE-EUR"        # Dogecoin Fix
    }
    
    for key, ticker in mapping.items():
        if key in name: return ticker
        
    if isin.startswith('US'): return isin
    if isin.startswith('DE'): return isin + ".DE"
    return isin

def get_price_data(symbol: str):
    """Holt die historischen Kursdaten von Yahoo Finance."""
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="60d")
        if hist.empty:
            return None
        return hist
    except:
        return None