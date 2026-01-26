import numpy as np
import pandas as pd

def calculate_elliott(hist):
    """
    Deine professionelle Elliott-Wellen-Analyse.
    Gibt ein Dictionary zurück, das direkt in die CSV geschrieben werden kann.
    """
    if hist is None or len(hist) < 50:
        return {"signal": "Datenmangel", "entry": 0, "target": 0, "score": 0}

    close = hist['Close'].values
    highs = hist['High'].values
    lows = hist['Low'].values
    
    # Pivot-Erkennung
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
    if len(p) < 3: 
        return {"signal": "Seitwärts", "entry": 0, "target": 0, "score": 20}

    # Bullisches Muster Analyse
    if p[-2][0] == 'H' and p[-1][0] == 'L':
        w1_start = p[-3][2] if len(p) >= 3 else lows[0]
        w1_end = p[-2][2] 
        w2_end = p[-1][2] 
        move_w1 = w1_end - w1_start
        
        if move_w1 > 0:
            retrace = (w1_end - w2_end) / move_w1
            
            if 0.45 <= retrace <= 0.85:
                target_w3 = w2_end + (move_w1 * 1.618)
                entry_level = w1_end - (move_w1 * 0.618) # Der ideale Einstieg
                score = round((1 - abs(retrace - 0.618)) * 100, 1)
                
                return {
                    "signal": "BUY",
                    "entry": round(entry_level, 2),
                    "target": round(target_w3, 2),
                    "score": score
                }

    return {"signal": "Warten", "entry": 0, "target": 0, "score": 30}