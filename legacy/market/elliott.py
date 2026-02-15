import numpy as np
import pandas as pd

def calculate_elliott(hist):
    """
    Professionelle Elliott-Wellen-Analyse v3.0.
    Bereinigt um Fake-Signale und Trend-Validierung.
    """
    if hist is None or len(hist) < 50:
        return {"signal": "Datenmangel", "entry": 0, "target": 0}

    close = hist['Close'].values
    highs = hist['High'].values
    lows = hist['Low'].values
    
    def find_pivots(n=5):
        pivots = []
        for i in range(n, len(close) - n):
            if all(highs[i] > highs[i-j] for j in range(1, n+1)) and \
               all(highs[i] > highs[i+j] for j in range(1, n+1)):
                pivots.append(('H', i, highs[i]))
            if all(lows[i] < lows[i-j] for j in range(1, n+1)) and \
               all(lows[i] < lows[i+j] for j in range(1, n+1)):
                pivots.append(('L', i, lows[i]))
        return pivots

    p = find_pivots()
    
    # 1. Zu wenig Struktur -> Warten (0 Punkte im Scoring)
    if len(p) < 4: 
        return {"signal": "Warten", "entry": 0, "target": 0}

    # Wir analysieren die letzten 4 Pivots (L1 -> H1 -> L2 -> H2)
    # p[-1] ist der aktuellste Pivot
    last_pivot_type = p[-1][0]
    
    # ELLIOTT WAVE SETUP (Welle 2 Korrektur)
    # Wir suchen: Tief (p-3) -> Hoch (p-2) -> höheres Tief (p-1)
    if p[-2][0] == 'H' and p[-1][0] == 'L':
        w1_start = p[-3][2] # Start der Welle 1
        w1_end = p[-2][2]   # Ende der Welle 1
        w2_end = p[-1][2]   # Ende der Welle 2 (aktuelles Tief)
        
        move_w1 = w1_end - w1_start
        
        # TREND-CHECK: Nur BUY, wenn Welle 1 wirklich nach oben ging
        # und das neue Tief (w2_end) HÖHER ist als der Start von Welle 1
        if move_w1 > 0 and w2_end > w1_start:
            retrace = (w1_end - w2_end) / move_w1
            
            # GOLDENER SCHNITT CHECK (Retracement zwischen 38% und 78%)
            if 0.38 <= retrace <= 0.78:
                target_w3 = w2_end + (move_w1 * 1.618)
                return {
                    "signal": "BUY",
                    "entry": round(w1_end, 2), # Einstieg bei Bruch des Wellen-Hochs
                    "target": round(target_w3, 2)
                }

    # 2. Wenn kein klares Muster da ist -> Seitwärts (0 Punkte im Scoring)
    return {"signal": "Seitwärts", "entry": 0, "target": 0}