import pandas as pd
from cloud.repository import TradingRepository

def cleanup():
    print("🧹 Elliott-Cleanup läuft über GOOGLE_CREDENTIALS...")
    try:
        repo = TradingRepository()
        df = repo.load_watchlist()
        
        # Liste der Spalten mit Millionen-Werten aus deiner Datei
        cols = ['Elliott-Ausstieg', 'Elliott-Einstieg', 'Auto-Ausstieg 161%', 'Auto-Einstieg 61.8%']
        
        for col in cols:
            if col in df.columns and 'Akt. Kurs [€]' in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                akt_kurs = pd.to_numeric(df['Akt. Kurs [€]'], errors='coerce')
                
                # Wenn Wert > 5x Kurs, ist das Komma verrutscht (z.B. 6483112 statt 64.83)
                mask = (df[col] > akt_kurs * 5) & (df[col] > 1000)
                df.loc[mask, col] = df.loc[mask, col] / 100000.0
                
        repo.save_watchlist(df)
        print("✅ Tabelle gesäubert!")
    except Exception as e:
        print(f"❌ Fehler: {e}")

if __name__ == "__main__":
    cleanup()