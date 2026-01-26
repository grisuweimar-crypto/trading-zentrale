import os
import pandas as pd
from cloud.repository import TradingRepository

def cleanup():
    # Wir nehmen DEIN vorhandenes Secret
    mein_schluessel = os.environ.get('GOOGLE_CREDENTIALS')
    
    # Wir füttern dein System (auth.py) mit diesem Schlüssel
    if mein_schluessel:
        os.environ['GOOGLE_SHEETS_JSON'] = mein_schluessel
    else:
        print("❌ Fehler: GOOGLE_CREDENTIALS wurde nicht gefunden!")
        return

    print("🧹 Elliott-Cleanup gestartet...")
    try:
        repo = TradingRepository()
        df = repo.load_watchlist()
        
        # Die Spalten mit den Millionen-Fehlern
        cols = ['Elliott-Ausstieg', 'Elliott-Einstieg', 'Auto-Ausstieg 161%', 'Auto-Einstieg 61.8%']
        
        for col in cols:
            if col in df.columns and 'Akt. Kurs [€]' in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df['Akt. Kurs [€]'] = pd.to_numeric(df['Akt. Kurs [€]'], errors='coerce')
                
                # LOGIK: Wir schieben das Komma, bis die Zahl zum Kurs passt.
                def fix_scale(val, price):
                    if pd.isna(val) or pd.isna(price) or price <= 0 or val <= 0:
                        return val
                    temp = val
                    # Wenn Wert > 3x Kurs (z.B. 6 Mio bei 60€ Kurs) -> Komma schieben
                    while temp > (price * 3):
                        temp /= 10.0
                    return round(temp, 2)

                df[col] = df.apply(lambda x: fix_scale(x[col], x['Akt. Kurs [€]']), axis=1)

        repo.save_watchlist(df)
        print("✅ ERFOLG: Die Millionen sind weg!")

    except Exception as e:
        print(f"❌ Fehler: {e}")

if __name__ == "__main__":
    cleanup()
