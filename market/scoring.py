import pandas as pd

def calculate_final_score(ticker, elliott_data, fundamental_data, monte_carlo_data):
    """
    ERWEITERTE LOGIK: Hubi-Score v2.0
    Inklusive Konfluenz-Bonus und Sektor-Multiplikator.
    """
    score = 0.0

    # 1. MONTE CARLO CHANCE (Max 20 Punkte)
    mc_chance = float(monte_carlo_data.get('probability', 0) or 0)
    score += (mc_chance / 100) * 20

    # 2. UPSIDE / KURSZIEL (Max 15 Punkte)
    upside = float(fundamental_data.get('upside', 0) or 0)
    if upside > 0:
        score += min((upside / 30) * 15, 15)
    elif upside < -10:
        score -= 10  

    # 3. KGV / PE RATIO (Max 15 Punkte - SEKTOR-BEREINIGT)
    # Wir holen den Sektor aus den Fundamentaldaten oder der Watchlist
    sektor = str(fundamental_data.get('sector', 'Andere')).upper()
    pe = float(fundamental_data.get('pe', 0) or 0)
    
    if pe > 0:
        # Sektor-Multiplikator Logik: Tech darf teurer sein als Mining/Industrie
        if 'TECH' in sektor or 'GEHIRN' in sektor:
            if pe <= 25: score += 15
            elif pe <= 40: score += 10
            elif pe <= 60: score += 5
            elif pe > 80: score -= 15
        elif 'MINING' in sektor or 'EDELMETALLE' in sektor:
            if pe <= 10: score += 15
            elif pe <= 18: score += 10
            elif pe > 30: score -= 20 # Mining-Aktien mit PE > 30 sind oft massiv überhitzt
        else:
            # Standard-Logik bleibt erhalten
            if pe <= 15: score += 15
            elif pe <= 25: score += 10
            elif pe <= 40: score += 5
            elif pe > 60: score -= 15 

    # 4. WACHSTUM (Max 10 Punkte)
    growth = float(fundamental_data.get('growth', 0) or 0)
    score += min((growth / 20) * 10, 10)

    # 5. MARGE (Max 10 Punkte)
    marge = float(fundamental_data.get('margin', 0) or 0)
    if marge > 20:
        score += 10
    elif marge > 5:
        score += 5

    # 6. ANALYSTEN-BEWERTUNG (Max 10 Punkte)
    rec = str(fundamental_data.get('recommendation', 'none')).lower()
    if 'strong_buy' in rec: score += 10
    elif 'buy' in rec: score += 7
    elif 'hold' in rec: score += 2

    # 7. ELLIOTT WAVE (Max 20 Punkte)
    elliott_sig = str(elliott_data.get('signal', 'Warten')).upper()
    if elliott_sig == 'BUY':
        score += 20
    elif elliott_sig == 'SELL':
        score -= 20

    # ============================================================
    # NEU: KONFLUENZ-BONUS (Der "Bullseye"-Faktor)
    # Wenn Monte Carlo (>70%) UND Elliott (BUY) übereinstimmen -> Bonus
    # ============================================================
    if mc_chance > 70 and elliott_sig == 'BUY':
        score += 10
        # Begrenzung des Gesamtscores auf ca. 110-120 für absolute Top-Werte
    
    # NEU: KRYPTO-ADJUSTIERUNG (Failsafe für fehlende PEs)
    if 'KRYPTO' in sektor:
        # Da Kryptos kein PE/Wachstum im klassischen Sinne haben, 
        # gleichen wir das über die MC-Stärke aus, damit sie im Dashboard nicht absaufen.
        score += 15 

    return round(score, 2)