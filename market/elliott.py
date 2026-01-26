import pandas as pd
import numpy as np

def detect_elliott_wave(history):
    """
    Einfache Erkennung von Impulswellen-Ansätzen.
    Sucht nach dem Verhältnis von Hochs und Tiefs.
    """
    try:
        closes = history['Close'].values
        if len(closes) < 20: return "Kein Muster"
        
        # Beispiel-Logik: Ist der Kurs über dem 20-Tage-Schnitt?
        sma20 = np.mean(closes[-20:])
        current = closes[-1]
        
        if current > sma20 * 1.05:
            return "Welle 3 Start?"
        elif current < sma20 * 0.95:
            return "Korrektur (ABC)"
        else:
            return "Seitwärts"
    except:
        return "Fehler"