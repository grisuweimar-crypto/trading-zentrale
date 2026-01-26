import pandas as pd
import os

def final_repair():
    csv_path = 'watchlist.csv'
    if not os.path.exists(csv_path):
        print("❌ Datei watchlist.csv nicht gefunden!")
        return

    # Das ultimative Mapping für deine Liste
    mapping = {
        "ALLIANZ SE": "ALV.DE", "AMAZON.COM": "AMZN", "ALPHABET INC.A": "GOOGL",
        "APPLE INC": "AAPL", "ASML HOLDING": "ASML", "AURUBIS AG": "NDA.DE",
        "BAYER AG": "BAYN.DE", "BITCOIN (KRYPTO)": "BTC-USD", "ETHEREUM (KRYPTO)": "ETH-USD",
        "SAP SE O.N.": "SAP.DE", "NVIDIA CORP.": "NVDA", "TESLA INC.": "TSLA",
        "VOLKSWAGEN AG VZ.": "VOW3.DE", "BARRICK MINING": "GOLD", "FREEPORT-MCMORAN": "FCX",
        "FRESEN.MED.CARE": "FME.DE", "HECLA MNG": "HL", "INCYTE DL": "INCY",
        "JD.COM ADR": "JD", "LARGO INC.": "LGO", "LVMH EO": "MC.PA",
        "NOVO-NORDISK": "NVO", "TAKKT AG": "TTK.DE", "UMICORE": "UMI.BR",
        "UNITEDHEALTH": "UNH", "VALE S.A.": "VALE"
    }

    try:
        df = pd.read_csv(csv_path)
        print(f"🔄 Verarbeite {len(df)} Zeilen...")
        
        # Ersetze Namen durch Ticker
        def clean_ticker(row):
            t = str(row['Ticker']).strip().upper()
            n = str(row['Name']).strip().upper()
            if t in mapping: return mapping[t]
            if n in mapping: return mapping[n]
            return t

        df['Ticker'] = df.apply(clean_ticker, axis=1)
        
        # Speichern erzwingen
        df.to_csv(csv_path, index=False)
        print("✅ Watchlist erfolgreich repariert!")
        
    except Exception as e:
        print(f"❌ Fehler: {e}")

if __name__ == "__main__":
    final_repair()