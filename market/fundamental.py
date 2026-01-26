import yfinance as yf

def get_fundamental_data(ticker):
    """
    Holt fundamentale Kennzahlen für das Scoring.
    """
    try:
        # Hier lag der Fehler: Wir müssen yf.Ticker(ticker) nutzen!
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # Sicherstellen, dass wir Werte haben, sonst 0 setzen
        data = {
            "upside": 0,
            "pe": info.get('forwardPE', 0) or 0,
            "growth": info.get('revenueGrowth', 0) or 0,
            "margin": info.get('profitMargins', 0) or 0,
            "recommendation": info.get('recommendationKey', 'none')
        }
        
        # Einfache Upside-Berechnung (Target Price vs Current)
        target = info.get('targetMeanPrice')
        current = info.get('currentPrice')
        if target and current:
            data["upside"] = round(((target / current) - 1) * 100, 2)
            
        return data

    except Exception as e:
        # Falls Yahoo blockt, liefern wir neutrale Werte zurück
        return {"upside": 0, "pe": 0, "growth": 0, "margin": 0, "recommendation": "none"}