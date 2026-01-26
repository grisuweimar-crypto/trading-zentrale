import pandas as pd
import os
import re

def fix_everything():
    # Wir nehmen die Datei als das, was sie ist: Eine Excel-Datei
    master_file = 'Watchlist Master 2026-voll.xlsx'
    output_file = 'watchlist.csv'
    
    if not os.path.exists(master_file):
        print(f"❌ Datei nicht gefunden!")
        return

    print(f"🏗️ Starte Excel-Direkt-Extraktion...")

    try:
        # Versuch 1: Als Excel lesen (falls nur die Endung falsch ist)
        try:
            df_master = pd.read_excel(master_file)
        except:
            # Versuch 2: Mit speziellem Engine für korrupte Dateien
            df_master = pd.read_csv(master_file, engine='python', on_bad_lines='skip', encoding_errors='ignore')
        
        print(f"📖 Daten geladen ({len(df_master)} Zeilen).")
    except Exception as e:
        print(f"❌ Kritischer Fehler: {e}")
        return

    # Das bewährte Mapping
    mapping = {
        "ABB LTD. NA": "ABBN.SW", "ALLIANZ SE": "ALV.DE", "AMAZON.COM": "AMZN",
        "ALPHABET INC.A": "GOOGL", "APPLE INC": "AAPL", "ASML HOLDING": "ASML",
        "AURUBIS AG": "NDA.DE", "BAYER AG": "BAYN.DE", "BITCOIN (KRYPTO)": "BTC-USD",
        "ETHEREUM (KRYPTO)": "ETH-USD", "SAP SE O.N.": "SAP.DE", "NVIDIA CORP.": "NVDA",
        "TESLA INC.": "TSLA", "VOLKSWAGEN AG VZ.": "VOW3.DE"
    }

    rows = []
    for _, row in df_master.iterrows():
        name = str(row.get('Name', 'Unbekannt')).strip()
        if name == "Unbekannt" or "Säule" in name: continue
        
        # Ticker aus AUTO(...) finden
        status_str = " ".join([str(val) for val in row.values])
        match = re.search(r'AUTO\((.*?)\)', status_str)
        ticker = match.group(1) if match else name
        
        # Mapping / ISIN / Ticker
        final_ticker = mapping.get(name.upper(), mapping.get(ticker.upper(), ticker))
        
        # ISIN Check
        isin = str(row.get('ISIN', '')).strip()
        if (len(final_ticker) > 10 or " " in final_ticker) and len(isin) > 5:
            final_ticker = isin

        rows.append({
            'Ticker': final_ticker, 'Name': name, 'Akt. Kurs [€]': 0,
            'Score': 0, 'Elliott-Signal': 'Warten', 'Elliott-Einstieg': 0,
            'Elliott-Ausstieg': 0, 'MC-Chance': 0
        })

    # VW ergänzen
    if not any("VOLKSWAGEN" in str(d['Name']).upper() for d in rows):
        rows.append({
            'Ticker': 'VOW3.DE', 'Name': 'Volkswagen AG Vz.', 
            'Akt. Kurs [€]': 0, 'Score': 0, 'Elliott-Signal': 'Warten'
        })

    pd.DataFrame(rows).to_csv(output_file, index=False, encoding='utf-8')
    print(f"✅ FERTIG! {len(rows)} Zeilen in sauberer watchlist.csv gespeichert.")

if __name__ == "__main__":
    fix_everything()