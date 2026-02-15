import pandas as pd
import os

# Wir gehen einen Ordner hoch (von utils/ zurück ins Hauptverzeichnis)
base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
csv_path = os.path.join(base_path, 'watchlist.csv')

# Die Struktur für deine 120 Aktien
columns = ['Ticker', 'Name', 'Akt. Kurs [€]', 'Score', 'Elliott-Signal', 'Elliott-Einstieg', 'Elliott-Ausstieg', 'Potential %']
df = pd.DataFrame(columns=columns)

# Speichern
df.to_csv(csv_path, index=False)
print(f"✅ watchlist.csv wurde hier erstellt: {csv_path}")