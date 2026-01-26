import pandas as pd

def calculate_total_score(row):
    """
    Das objektive Bewertungssystem. 
    Startet bei 0 und bewertet rein nach Datenlage.
    Maximaler theoretischer Score: ~120 Punkte (bei extremen Top-Werten).
    """
    score = 0.0

    # 1. MONTE CARLO CHANCE (Max 20 Punkte)
    # Wer eine 100% Chance hat, bekommt 20 Punkte.
    mc_chance = float(row.get('MC_Chance', 0) or 0)
    score += (mc_chance / 100) * 20

    # 2. UPSIDE / KURSZIEL (Max 15 Punkte)
    # Wir werten eine Upside von 30% als "perfekt" (15 Pkt).
    upside = float(row.get('Upside', 0) or 0)
    if upside > 0:
        score += min((upside / 30) * 15, 15)
    elif upside < -10:
        score -= 10  # Malus bei hohem Risiko nach unten

    # 3. KGV / PE RATIO (Max 15 Punkte)
    # Je niedriger das KGV (solange > 0), desto besser.
    pe = float(row.get('PE', 0) or 0)
    if 0 < pe <= 15:
        score += 15
    elif 15 < pe <= 25:
        score += 10
    elif 25 < pe <= 40:
        score += 5
    elif pe > 60:
        score -= 15 # Überbewertet

    # 4. WACHSTUM (Max 10 Punkte)
    # 20% Wachstum oder mehr gibt die volle Punktzahl.
    growth = float(row.get('Wachstum', 0) or 0)
    score += min((growth / 20) * 10, 10)

    # 5. MARGE (Max 10 Punkte)
    # Profitabilität ist wichtig.
    marge = float(row.get('Marge', 0) or 0)
    if marge > 20:
        score += 10
    elif marge > 5:
        score += 5

    # 6. ANALYSTEN-BEWERTUNG (Max 10 Punkte)
    rec = str(row.get('AnalystRec', 'none')).lower()
    if 'strong_buy' in rec:
        score += 10
    elif 'buy' in rec:
        score += 7
    elif 'hold' in rec:
        score += 2

    # 7. ELLIOTT WAVE TURBO (Max 20 Punkte)
    # Das ist dein automatischer Scanner-Bonus.
    elliott = str(row.get('Elliott_Signal', '')).upper()
    if "JETZT KAUFEN" in elliott or "BUY" in elliott:
        score += 20
    elif "WARTEN" in elliott:
        score += 5

    # 8. ZIELZONEN-BONUS (Der "Kauf-Alarm")
    # Liegt der Kurs in deiner Zielzone? (+20 Punkte)
    # Wir vergleichen "Akt. Kurs [€]" mit "Zielzone [€]"
    try:
        current = float(row.get('Akt. Kurs [€]', 0) or 0)
        target_zone = float(row.get('Zielzone [€]', 0) or 0)
        if target_zone > 0 and current <= target_zone * 1.05: # Innerhalb oder max 5% drüber
            score += 20
    except:
        pass

    return round(score, 2)