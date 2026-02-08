import yfinance as yf
import requests


def get_fundamental_data(ticker):
    """
    Holt fundamentale Kennzahlen für das Scoring mit Session-Header.
    """
    try:
        # Hier wird die session an den Ticker übergeben
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # Sicherstellen, dass wir Werte haben, sonst 0 setzen
        data = {
            "upside": 0,
            "pe": info.get('forwardPE', 0) or 0,
            "growth": info.get('revenueGrowth', 0) or 0,
            "margin": info.get('profitMargins', 0) or 0,
            "recommendation": info.get('recommendationKey', 'none'),
            "sector": info.get('sector', 'Andere'),
            # 🔥 ROE + DEBT + DIVIDENDE HINZUFÜGEN
            "roe": info.get('returnOnEquity', 0) or 0,
            "debt_to_equity": info.get('debtToEquity', 100) or 100,
            "div_rendite": info.get('dividendYield', 0) or 0,
            # 🔥 WALLSTREETZEN-UPGRADE: FCF Yield + Rule of 40
            "fcf": info.get('freeCashflow', 0),
            "enterprise_value": info.get('enterpriseValue', 1),
            "revenue": info.get('totalRevenue', 1),
            "current_ratio": info.get('currentRatio', 1),
            "institutional_ownership": info.get('heldPercentInstitutions', 0),
        }

        
        # Einfache Upside-Berechnung (Target Price vs Current)
        target = info.get('targetMeanPrice')
        current = info.get('currentPrice')
        if target and current:
            data["upside"] = round(((target / current) - 1) * 100, 2)
            
        return data

    except Exception as e:
        # Falls Yahoo blockt, liefern wir neutrale Werte zurück
        print(f"⚠️ Yahoo-Fundamentaldaten-Fehler bei {ticker}: {e}")
        return {"upside": 0, "pe": 0, "growth": 0, "margin": 0, "recommendation": "none", "sector": "Andere"}