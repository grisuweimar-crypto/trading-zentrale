import pandas as pd

def force_logic_value(row):
    """
    Erzwingt einen logischen Wert, wenn Google Sheets das Komma gefressen hat.
    Nutzt die Formel: Wert = Anzahl * Kurs
    """
    def to_f(x):
        if pd.isna(x): return 0.0
        # Alle Zeichen entfernen außer Ziffern
        s = "".join(c for c in str(x) if c.isdigit())
        return float(s) if s else 0.0

    # Rohdaten ohne Kommas (z.B. 6811 statt 68.11)
    raw_anzahl = to_f(row.get('Anzahl'))
    raw_kurs   = to_f(row.get('Kurs'))
    raw_wert   = to_f(row.get('Wert'))
    raw_kauf   = to_f(row.get('Kaufwert'))

    if raw_wert == 0: return 0.0

    # Wir wissen: Ein normaler Wert im Depot liegt zwischen 10€ und 500€
    # Wir skalieren den Wert so lange durch 10, bis er in einen plausiblen 
    # Bereich zum Kaufwert passt.
    
    # Beispiel Vale: raw_wert = 6811, raw_kauf = 5892
    # Da 6811 > 5892 * 1.5 (Puffer), muss es skaliert werden.
    
    candidate = raw_wert
    # Solange der Wert astronomisch höher ist als der Kaufpreis (Faktor 10)
    # ODER absolut unrealistisch für eine Einzelposition (> 1000€)
    while (kauf_check := to_f(row.get('Kaufwert')) / 100) > 0 and candidate > kauf_check * 10:
         candidate /= 10.0
    
    # Letzte Sicherung: Wenn immer noch > 1000 (und nicht BTC), Komma schieben
    while candidate > 1000 and "BITCOIN" not in str(row.get('Name')).upper():
        candidate /= 10.0
        
    return round(candidate, 2)