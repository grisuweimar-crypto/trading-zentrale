import yfinance as yf
import pandas as pd
import os
import time
import logging

# --- KONFIGURATION ---
excel_file = r"C:\Users\CW\OneDrive\Desktop\Watchlist Master 2026-voll.xlsx"

# Mapping für harte Fälle
SYMBOL_MAPPING = {
    'coca cola': 'KO',
    'coca-cola': 'KO',
    'sap': 'SAP.DE',
    'allianz': 'ALV.DE',
    'mercedes': 'MBG.DE',
    'basf': 'BAS.DE',
    'telekom': 'DTE.DE',
    'mcdonalds': 'MCD',
    'realty income': 'O',
    'microsoft': 'MSFT',
    'apple': 'AAPL',
    'tesla': 'TSLA',
    'amazon': 'AMZN',
    'nvidia': 'NVDA',
    'bitcoin': 'BTC-EUR'
}

def get_clean_symbol(name, isin, symbol_col):
    # 1. Hat der Nutzer schon ein Symbol eingetragen?
    if pd.notna(symbol_col) and str(symbol_col).strip() != "":
        return str(symbol_col).strip()
    
    # 2. ISIN Check (US ISINs funktionieren oft direkt)
    if pd.notna(isin) and str(isin).startswith('US'):
        return str(isin)
        
    # 3. Name Check (Mapping)
    name_lower = str(name).lower()
    for k, v in SYMBOL_MAPPING.items():
        if k in name_lower:
            return v
            
    # 4. Fallback: Wir geben den Namen zurück und hoffen auf Yahoo
    return name

print("👨‍🌾 START: Farmer-Seite Reparatur-Tool")
print(f"📂 Öffne Datei: {excel_file}")

if not os.path.exists(excel_file):
    print("❌ FEHLER: Datei nicht gefunden!")
    time.sleep(10)
    exit()

try:
    # Wir laden NUR das Portfolio Sheet
    df_pf = pd.read_excel(excel_file, sheet_name="Portfolio")
    print(f"✅ Portfolio-Tab gefunden. {len(df_pf)} Einträge.")
    
    # Spalten sicherstellen
    cols_check = ['Akt. Kurs [€]', 'Aktueller Wert', 'G/V', 'G/V %', 'Warnung', 'DivRendite']
    for c in cols_check:
        if c not in df_pf.columns:
            df_pf[c] = None

    # ZEILEN DURCHGEHEN
    for idx, row in df_pf.iterrows():
        name = row['Name']
        if pd.isna(name): continue
        
        print(f"\n🔎 Prüfe: {name}")
        
        # Symbol finden
        ticker_symbol = get_clean_symbol(name, row.get('ISIN'), row.get('Symbol'))
        print(f"   -> Versuche Ticker: '{ticker_symbol}'")
        
        kurs = 0.0
        try:
            t = yf.Ticker(ticker_symbol)
            
            # Preis holen (Versuch 1: Fast Info)
            try:
                kurs = t.fast_info.last_price
            except:
                pass
            
            # Preis holen (Versuch 2: Download History)
            if kurs is None or kurs == 0:
                hist = t.history(period="1d")
                if not hist.empty:
                    kurs = float(hist['Close'].iloc[-1])
            
            if kurs is None or kurs == 0:
                print(f"   ⚠️ KEIN PREIS GEFUNDEN für {ticker_symbol}!")
                # Wir lassen den alten Wert stehen oder setzen 0, aber crashen nicht
                continue
                
            print(f"   💰 Preis gefunden: {kurs:.2f} €")
            
            # DATEN SCHREIBEN
            df_pf.at[idx, 'Akt. Kurs [€]'] = kurs
            
            # KPI Berechnen
            anzahl = float(row['Anzahl']) if pd.notna(row['Anzahl']) else 0
            kauf = float(row['Kaufkurs']) if pd.notna(row['Kaufkurs']) else 0
            
            wert = kurs * anzahl
            df_pf.at[idx, 'Aktueller Wert'] = wert
            df_pf.at[idx, 'G/V'] = wert - (kauf * anzahl)
            
            if kauf > 0:
                df_pf.at[idx, 'G/V %'] = (kurs - kauf) / kauf * 100
            else:
                df_pf.at[idx, 'G/V %'] = 0.0
                
            # Dividende holen (Bonus)
            try:
                div = t.info.get('dividendYield', 0)
                if div: df_pf.at[idx, 'DivRendite'] = div * 100
            except: pass

            print("   -> Zeile aktualisiert.")

        except Exception as e:
            print(f"   ❌ Fehler bei dieser Aktie: {e}")
            continue

    # SPEICHERN
    print("\n💾 Speichere Excel...")
    
    # Wir benutzen openpyxl um nur das Sheet zu aktualisieren, ohne die Watchlist zu löschen
    with pd.ExcelWriter(excel_file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        df_pf.to_excel(writer, sheet_name='Portfolio', index=False)
        
    print("✅ ERFOLG: Portfolio gespeichert!")
    print("👉 Starte jetzt das Dashboard neu.")

except Exception as e:
    print(f"\n❌ KRITISCHER FEHLER: {e}")
    print("Ist die Excel-Datei noch geöffnet? Bitte schließen!")

time.sleep(10)