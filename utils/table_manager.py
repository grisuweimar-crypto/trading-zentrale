import pandas as pd
import os
import re

class TableManager:
    def __init__(self, csv_path='watchlist.csv'):
        self.csv_path = csv_path
        self.required_columns = [
            'Ticker', 'Name', 'Akt. Kurs [€]', 'Score', 
            'Elliott-Signal', 'Elliott-Einstieg', 'Elliott-Ausstieg', 'MC-Chance'
        ]

    def rebuild_from_master(self, master_path):
        """Baut die 114er Liste aus der Master-Vorlage neu auf."""
        if not os.path.exists(master_path):
            print(f"❌ Datei {master_path} nicht gefunden!")
            return

        print(f"🏗️ Lade Daten aus {master_path}...")
        # Wir laden die Master-CSV (oder Excel)
        df_master = pd.read_csv(master_path) if master_path.endswith('.csv') else pd.read_excel(master_path)
        
        new_data = []
        for _, row in df_master.iterrows():
            name = str(row['Name'])
            # Ticker aus der Spalte 'Elliott Status' extrahieren (z.B. AUTO(ABBN.SW))
            raw_status = str(row.get('Elliott Status', ''))
            match = re.search(r'AUTO\((.*?)\)', raw_status)
            ticker = match.group(1) if match else name # Fallback auf Name
            
            new_data.append({
                'Ticker': ticker,
                'Name': name,
                'Akt. Kurs [€]': row.get('Akt. Kurs [€]', 0),
                'Score': row.get('Score', 0),
                'Elliott-Signal': 'Warten',
                'Elliott-Einstieg': row.get('Elliott-Einstieg', 0),
                'Elliott-Ausstieg': row.get('Elliott-Ausstieg', 0),
                'MC-Chance': row.get('MC_Chance', 0)
            })
            
        # 114. Volkswagen hinzufügen
        if not any(d['Ticker'] == 'VOW3.DE' for d in new_data):
            new_data.append({
                'Ticker': 'VOW3.DE', 'Name': 'Volkswagen AG Vz.', 
                'Akt. Kurs [€]': 0, 'Score': 0, 'Elliott-Signal': 'Warten',
                'Elliott-Einstieg': 0, 'Elliott-Ausstieg': 0, 'MC-Chance': 0
            })

        df_final = pd.DataFrame(new_data)[self.required_columns]
        df_final.to_csv(self.csv_path, index=False)
        print(f"✅ ERFOLG: {len(df_final)} Zeilen in {self.csv_path} erstellt.")

if __name__ == "__main__":
    tm = TableManager()
    # Hier den Namen deiner Master-Datei einsetzen:
    tm.rebuild_from_master('Watchlist Master 2026-voll.xlsx')