import yfinance as yf

def get_usd_eur_rate():
    """Holt den aktuellen Wechselkurs von USD zu EUR."""
    try:
        ticker = yf.Ticker("USDEUR=X")
        data = ticker.history(period="1d")
        if not data.empty:
            return data['Close'].iloc[-1]
        return 0.92  # Fallback-Kurs, falls API hakt
    except Exception as e:
        print(f"⚠️ Forex-Fehler: {e}")
        return 0.92

def convert_to_eur(price_usd, rate):
    """Rechnet USD in EUR um."""
    return price_usd * rate