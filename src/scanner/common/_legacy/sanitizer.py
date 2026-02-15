import pandas as pd

def get_logical_value(raw_val, raw_kaufwert):
    """
    Säubert den Wert basierend auf der Ziffernfolge und dem Kaufwert-Anker.
    Verhindert Komma-Fehler durch Google Sheets.
    """
    def to_num(x):
        if pd.isna(x): return 0.0
        # Nur Ziffern extrahieren
        s = "".join(c for c in str(x) if c.isdigit())
        return float(s) if s else 0.0

    val = to_num(raw_val)
    kauf = to_num(raw_kaufwert)
    
    if val <= 0: return 0.0
    
    # Skalierung anpassen: Wenn Wert > 10x Kaufwert, Komma schieben
    while val > kauf * 10 and val > 500: 
        val /= 10.0
    while val < kauf * 0.1 and val < 5:
        val *= 10.0
            
    return round(val, 2)