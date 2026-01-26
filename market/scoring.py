import pandas as pd

def calculate_final_score(ticker, elliott_data, fundamental_data, monte_carlo_data):
    """
    DEINE LOGIK: Das objektive Bewertungssystem.
    Nimmt die Daten aus den Modulen und berechnet den Score.
    """
    score = 0.0

    # 1. MONTE CARLO CHANCE (Max 20 Punkte)
    # Wir holen die 'probability' aus dem monte_carlo_data Paket
    mc_chance = float(monte_carlo_data.get('probability', 0) or 0)
    score += (mc_chance / 100) * 20

    # 2. UPSIDE / KURSZIEL (Max 15 Punkte)
    upside = float(fundamental_data.get('upside', 0) or 0)
    if upside > 0:
        score += min((upside / 30) * 15, 15)
    elif upside < -10:
        score -= 10  

    # 3. KGV / PE RATIO (Max 15 Punkte)
    pe = float(fundamental_data.get('pe', 0) or 0)
    if 0 < pe <= 15:
        score += 15
    elif 15 < pe <= 25:
        score += 10
    elif 25 < pe <= 40:
        score += 5
    elif pe > 60:
        score -= 15 

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
    if 'strong_buy' in rec:
        score += 10
    elif 'buy' in rec:
        score += 7
    elif 'hold' in rec:
        score += 2

    # 7. ELLIOTT WAVE TURBO (Max 20 Punkte)
    elliott = str(elliott_data.get('signal', '')).upper()
    if "JETZT KAUFEN" in elliott or "BUY" in elliott:
        score += 20
    elif "WARTEN" in elliott:
        score += 5

    # 8. ZIELZONEN-BONUS (Max 20 Punkte)
    # Wir nehmen hier die Daten aus der Elliott-Analyse
    try:
        current = float(elliott_data.get('current_price', 0) or 0)
        target_zone = float(elliott_data.get('entry', 0) or 0)
        if target_zone > 0 and current <= target_zone * 1.05:
            score += 20
    except:
        pass

    return round(score, 2)