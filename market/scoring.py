def calculate_total_score(row):
    """Berechnet den Score aus allen 9 Kategorien (Deine Formel)."""
    # Basis: 100 für DOPPEL-Typ, sonst 50
    score = 100 if "DOPPEL" in str(row.get('Typ', '')) else 50
    
    try:
        # 1. 🎲 Monte Carlo (Gewicht: +20)
        if float(row.get('MC_Chance', 0)) > 70: score += 20
        
        # 2. 🏦 Analysten (+10)
        if "buy" in str(row.get('AnalystRec', '')).lower(): score += 10
        
        # 3. 🚀 Potenzial (+15)
        if float(row.get('Upside', 0)) > 20: score += 15
        
        # 4. 🏷️ KGV / Preis (+15)
        pe = float(row.get('PE', 999))
        if 0 < pe < 20: score += 15
        
        # 5. 🛡️ Sicherheit (+10)
        if float(row.get('Beta', 1)) < 1.1: score += 10
        
        # 6. 💸 Dividende (+10)
        if float(row.get('DivRendite', 0)) > 2.5: score += 10
        
        # 7. 🌱 Wachstum (+10)
        if float(row.get('Wachstum', 0)) > 15: score += 10
        
        # 8. 💰 Marge (+10)
        if float(row.get('Marge', 0)) > 15: score += 10
        
        # 9. 📈 Elliott Wave Gewicht (+20)
        sig = str(row.get('Elliott_Signal', ''))
        if "Welle 3" in sig: score += 20

    except:
        pass
        
    return round(score, 2)