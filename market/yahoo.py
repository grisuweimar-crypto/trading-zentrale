import os
import sys
import yfinance as yf

# Yahoo arbeitet mit Tickern (z. B. DBK.DE), nicht mit ISIN. Bekannte ISIN → Yahoo-Symbol.
# Erweiterbar – bei "keine Kurse" den Ticker bei Yahoo suchen und hier eintragen.
ISIN_TO_YAHOO = {
    "DE0005140008": "DB1.DE",   # Deutsche Börse
    "DE0007100000": "SIE.DE",   # Siemens
    "DE0007236101": "P911.DE",  # Porsche
    "DE0007164600": "SAP.DE",   # SAP
    "DE0007500001": "ADS.DE",   # Adidas
    "DE0005810055": "DTE.DE",   # Deutsche Telekom
    "DE0005190003": "BMW.DE",   # BMW
    "DE0005192037": "VOW3.DE",  # VW Vz
    "DE0007664039": "ALV.DE",   # Allianz
    "DE000BASF111": "BAS.DE",   # BASF
    "DE000BAY0017": "BAYN.DE",  # Bayer
    "DE0008232125": "LHA.DE",   # Lufthansa
    "DE0008430026": "IFX.DE",   # Infineon
    "DE000A1EWWW0": "DBK.DE",   # Deutsche Bank
}

def get_ticker_symbol(row):
    """
    Ermittelt das Yahoo-Symbol: zuerst optionale Spalte 'Yahoo', dann Name/ISIN-Mapping.
    Yahoo nutzt Ticker (z. B. DBK.DE), keine ISIN – mit 'Yahoo' in der Watchlist geht der Link direkt auf die richtige Aktie.
    """
    # 1) Explizites Yahoo-Symbol aus der Watchlist (empfohlen bei ISIN-Einträgen)
    yahoo = str(row.get("Yahoo", "") or row.get("Symbol", "")).strip()
    if yahoo:
        return yahoo

    isin = str(row.get("ISIN", "") or "").strip().upper()
    name = str(row.get("Name", "") or "").upper()

    # 2) Bekannte ISIN → Yahoo (Yahoo akzeptiert ISIN.DE oft nicht)
    if isin and isin in ISIN_TO_YAHOO:
        return ISIN_TO_YAHOO[isin]

    # 3) Name-basiertes Mapping
    name_mapping = {
        "ALPHABET": "GOOGL", "APPLE": "AAPL", "ALLIANZ": "ALV.DE", "BASF": "BAS.DE",
        "BAYER": "BAYN.DE", "AUTOSTORE": "AUTO.OL", "BITCOIN": "BTC-EUR", "ETHEREUM": "ETH-EUR",
        "SOLANA": "SOL-EUR", "XRP": "XRP-EUR", "ADA": "ADA-EUR", "DOGE": "DOGE-EUR",
    }
    for key, sym in name_mapping.items():
        if key in name:
            return sym

    # 4) US/DE-Fallback (funktioniert nicht immer bei DE – dann 'Yahoo'-Spalte nutzen)
    if isin.startswith("US"):
        return isin
    if isin.startswith("DE"):
        return isin + ".DE"
    return isin if isin else ""

def get_price_data(symbol: str):
    """Holt Kurse und die dazugehörige Währung.
    Bei 404 / delisted unterdrücken wir die langen yfinance-Meldungen und geben nur eine kurze Info."""
    try:
        ticker = yf.Ticker(symbol)
        # yfinance gibt bei unbekannten/delisted Symbolen lange HTTP/Fehlermeldungen aus – kurz unterdrücken
        with open(os.devnull, "w", encoding="utf-8") as devnull:
            old_stderr = sys.stderr
            sys.stderr = devnull
            try:
                hist = ticker.history(period="60d")
                try:
                    currency = ticker.fast_info.get("currency", "USD")
                except Exception:
                    currency = "USD"
            finally:
                sys.stderr = old_stderr

        if hist is None or hist.empty:
            print(f"⏭️  {symbol}: keine Kurse (übersprungen)")
            return None

        hist.attrs["currency"] = currency
        return hist
    except Exception:
        print(f"⏭️  {symbol}: keine Kurse (übersprungen)")
        return None