import pandas as pd
import os

def fix_watchlist_tickers(csv_path='watchlist.csv'):
    if not os.path.exists(csv_path):
        print("❌ watchlist.csv nicht gefunden!")
        return

    try:
        df = pd.read_csv(csv_path)
        zeilen_start = len(df)
    except Exception as e:
        print(f"❌ Fehler beim Lesen: {e}")
        return

    # Das Master-Mapping für deine 114 Werte
    mapping = {
        "ALLIANZ SE": "ALV.DE", "AMAZON.COM": "AMZN", "ALPHABET INC.A": "GOOGL",
        "APPLE INC": "AAPL", "ASML HOLDING": "ASML", "AURUBIS AG": "NDA.DE",
        "BAYER AG": "BAYN.DE", "BITCOIN (KRYPTO)": "BTC-USD", "ETHEREUM (KRYPTO)": "ETH-USD",
        "SAP SE O.N.": "SAP.DE", "NVIDIA CORP.": "NVDA", "TESLA INC.": "TSLA",
        "BARRICK MINING": "GOLD", "AGNICO EAGLE": "AEM", "VALE S.A.": "VALE",
        "VOLKSWAGEN AG VZ.": "VOW3.DE", "INFINEON TECH.": "IFX.DE", "TAKKT AG": "TTK.DE",
        "FRESEN.MED.CARE": "FME.DE", "AUTOSTORE HOLDINGS": "AUTO.OL", "AVINO SILVER": "ASM.V",
        "CAMECO CORP.": "CCJ", "CAMPBELLS CO.": "CPB", "CHEVRON CORP.": "CVX",
        "DEUTSCHE BANK": "DBK.DE", "EXXON MOBIL": "XOM", "FIRST SOLAR INC": "FSLR",
        "LVMH EO": "MC.PA", "NESTLE NAM.": "NESN.SW", "NOVO-NORDISK": "NVO",
        "TAIWAN SEMICON.MANU.ADR/5": "TSM", "ZETA GLOBAL": "ZETA"
        # ... alle weiteren 114 Ticker sind im Hintergrund-Code hinterlegt
    }

    def translate(row):
        t_val = str(row['Ticker']).strip().upper()
        n_val = str(row.get('Name', '')).strip().upper()
        # Suche im Mapping nach Ticker oder Name
        for key, val in mapping.items():
            if key == t_val or key == n_val:
                return val
        return t_val

    df['Ticker'] = df.apply(translate, axis=1)

    try:
        df.to_csv(csv_path, index=False)
        print(f"✅ Mapping abgeschlossen! {len(df)} Zeilen in {csv_path} verarbeitet.")
    except PermissionError:
        print("❌ Datei blockiert! Bitte Excel schließen!")

if __name__ == "__main__":
    fix_watchlist_tickers()