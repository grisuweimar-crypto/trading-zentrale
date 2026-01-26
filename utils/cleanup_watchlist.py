import pandas as pd
from cloud.repository import TradingRepository

def cleanup():
    repo = TradingRepository()
    df = repo.load_watchlist()
    
    # 1. Wir definieren NUR die Spalten, die wir wirklich brauchen
    # Alles andere (Duplikate) fliegt raus
    valid_columns = [
        'Säule / Bereich', 'Name', 'ISIN', 'Ticker', 'Zielzone [€]', 
        'Akt. Kurs [€]', 'Abstand [%]', 'Status / Signal', 'Trigger', 'Region',
        'Auto-Einstieg 61.8%', 'Auto-Ausstieg 161%', 'Elliott-Status',
        'Abstand-Elliott[%]', 'MC_Chance', 'Score', 'PE', 'Upside', 'Typ'
    ]
    
    # Behalte nur vorhandene Spalten aus der Liste
    df_clean = df[[c for c in valid_columns if c in df.columns]].copy()
    
    # 2. Wilde Zahlen nullen (Alles was unrealistisch hoch ist)
    cols_to_fix = ['Auto-Einstieg 61.8%', 'Auto-Ausstieg 161%']
    for col in cols_to_fix:
        if col in df_clean.columns:
            # Wenn Zahl > 10000 (und es keine Krypto ist), ist es ein Formatfehler
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
            df_clean.loc[df_clean[col] > 10000, col] = 0
            
    # 3. Speichern
    repo.save_watchlist(df_clean)
    print("✅ Watchlist bereinigt. Duplikate entfernt. Wilde Zahlen gelöscht.")

if __name__ == "__main__":
    cleanup()