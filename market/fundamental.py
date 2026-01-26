def get_fundamental_data(ticker_obj):
    """Holt die Rohwerte für deine 9 Bewertungskategorien."""
    try:
        info = ticker_obj.info
        current_price = info.get('currentPrice', 1)
        target_price = info.get('targetMeanPrice', current_price)
        
        return {
            "PE": info.get('trailingPE', 999),             # 🏷️ KGV
            "AnalystRec": info.get('recommendationKey', 'none'), # 🏦 Analysten
            "Upside": ((target_price / current_price) - 1) * 100, # 🚀 Potenzial
            "Beta": info.get('beta', 1.0),                 # 🛡️ Sicherheit
            "DivRendite": info.get('dividendYield', 0) * 100, # 💸 Dividende
            "Wachstum": info.get('revenueGrowth', 0) * 100, # 🌱 Wachstum
            "Marge": info.get('profitMargins', 0) * 100    # 💰 Marge
        }
    except:
        return {}