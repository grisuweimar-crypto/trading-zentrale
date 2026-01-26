import numpy as np

def detect_elliott_wave(hist):
    """
    Professionelle Elliott-Wellen-Analyse mit Fibonacci-Zielen.
    Berechnet Einstieg (unten) und Ausstieg (oben).
    """
    if hist is None or len(hist) < 50:
        return {"signal": "Datenmangel", "entry_zone": "-", "target": 0}

    close = hist['Close'].values
    highs = hist['High'].values
    lows = hist['Low'].values
    
    # 1. Pivot-Erkennung (Suche nach markanten Hochs/Tiefs)
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
    if len(p) < 3: return {"signal": "Seitwärts", "entry_zone": "-", "target": 0}

    # BULLISCHES MUSTER: Welle 1 (Auf), Welle 2 (Ab) -> Welle 3 (Ziel oben)
    # Wir suchen: Letztes Tief (L1) -> Letztes Hoch (H1) -> Korrektur (L2)
    if p[-2][0] == 'H' and p[-1][0] == 'L':
        w1_start = 0
        for i in range(len(p)-2, -1, -1):
            if p[i][0] == 'L':
                w1_start = p[i][2]
                break
        
        w1_end = p[-2][2] # Hoch von Welle 1
        w2_end = p[-1][2] # Aktuelles Tief von Welle 2
        move_w1 = w1_end - w1_start
        
        if move_w1 > 0:
            retrace = (w1_end - w2_end) / move_w1
            
            # Einstiegszone (unten): 50% bis 78.6% Retracement
            if 0.45 <= retrace <= 0.85:
                # AUSSTIEGSZONE (oben): Welle 3 Ziel = 161.8% Extension
                target_w3 = w2_end + (move_w1 * 1.618)
                entry_high = w1_end - (move_w1 * 0.50)
                entry_low = w1_end - (move_w1 * 0.786)
                
                return {
                    "signal": "BUY",
                    "entry_zone": f"{entry_low:.2f} - {entry_high:.2f}",
                    "target": round(target_w3, 2),
                    "confidence": round((1 - abs(retrace - 0.618)) * 100, 1)
                }

    return {"signal": "Warten", "entry_zone": "-", "target": 0}