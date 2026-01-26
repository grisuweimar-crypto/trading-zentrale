import pandas as pd
import os

class TradingRepository:
    def __init__(self):
        # Wir definieren nur noch den Namen der lokalen Datei
        self.filename = 'watchlist.csv'

    def load_watchlist(self):
        # Schaut nach, ob die CSV da ist, sonst erstellt sie eine leere
        if os.path.exists(self.filename):
            print(f"📂 Lade lokale Daten aus {self.filename}")
            return pd.read_csv(self.filename)
        else:
            print("⚠️ watchlist.csv nicht gefunden, erstelle neues Grundgerüst.")
            return pd.DataFrame(columns=['Ticker', 'Name', 'Akt. Kurs [€]', 'Score', 'Elliott-Signal'])

    def save_watchlist(self, df):
        # Speichert alles lokal. KEIN Google-Login nötig!
        df.to_csv(self.filename, index=False)
        print(f"✅ Erfolgreich lokal gespeichert in {self.filename}")