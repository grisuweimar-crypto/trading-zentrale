def get_fundamental_data(ticker_obj):
    """
    Sammelt alle fundamentalen Kriterien von Yahoo Finance in einem Rutsch.
    Gibt ein Dictionary zurück, das direkt vom Scoring-Modul gelesen werden kann.
    """
    try:
        # Einmaliger Zugriff auf die Info-API (spart Zeit)
        info = ticker_obj.info
        
        # Hilfsfunktion für die Upside-Berechnung
        current_price = info.get('currentPrice')
        target_price = info.get('targetMedianPrice')
        upside = 0.0
        if current_price and target_price:
            upside = round(((target_price - current_price) / current_price) * 100, 2)

        # Daten-Paket schnüren
        data = {
            'PE': info.get('trailingPE') or info.get('forwardPE') or 0.0,
            'DivRendite': round((info.get('dividendYield') or 0.0) * 100, 2),
            'Wachstum': round((info.get('earningsQuarterlyGrowth') or 0.0) * 100, 2),
            'Marge': round((info.get('profitMargins') or 0.0) * 100, 2),
            'Debt': info.get('debtToEquity') or 0.0,
            'AnalystRec': info.get('recommendationKey', 'none'),
            'Upside': upside,
            'Beta': info.get('beta') or 1.0  # Beta von 1.0 als neutraler Standard
        }
        
        return data

    except Exception as e:
        print(f"⚠️ Fehler beim Abrufen der Fundamentaldaten: {e}")
        # Rückgabe von Standardwerten bei Fehlern, damit der Scanner nicht stoppt
        return {
            'PE': 0.0, 'DivRendite': 0.0, 'Wachstum': 0.0, 
            'Marge': 0.0, 'Debt': 0.0, 'AnalystRec': 'none', 
            'Upside': 0.0, 'Beta': 1.0
        }