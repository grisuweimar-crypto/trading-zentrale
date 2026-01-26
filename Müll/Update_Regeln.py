import pandas as pd
import os
import time

# --- KONFIGURATION ---
excel_file = r"C:\Users\CW\OneDrive\Desktop\Watchlist Master 2026-voll.xlsx"

# DEINE REGELN
STOP_LOSS_PCT = 0.20   # 20% unter Kurs
TAKE_PROFIT_PCT = 0.30 # 30% über Kaufkurs

print("👮 START: Regel-Wächter")
print(f"📂 Datei: {excel_file}")

if not os.path.exists(excel_file):
    print("❌ FEHLER: Datei nicht gefunden!")
    time.sleep(5)
    exit()

try:
    # 1. Alles laden
    with pd.ExcelFile(excel_file) as xls:
        df_wl = pd.read_excel(xls, sheet_name=0) # Watchlist merken
        df_pf = pd.read_excel(xls, sheet_name="Portfolio") # Portfolio bearbeiten

    print(f"✅ Portfolio geladen: {len(df_pf)} Einträge.")
    
    updates = 0

    # 2. Durch das Portfolio gehen
    for idx, row in df_pf.iterrows():
        name = row['Name']
        if pd.isna(name): continue
        
        # Daten holen
        kurs = row['Akt. Kurs [€]']
        kauf = row['Kaufkurs']
        anzahl = row['Anzahl']
        
        # Sicherheits-Check: Haben wir einen Kurs?
        if pd.isna(kurs) or kurs == 0:
            print(f"   ⚠️ Überspringe {name}: Kein Kurs vorhanden.")
            continue
            
        # --- REGEL 1: STOP LOSS (Wenn leer) ---
        if pd.isna(row['Stop Loss']):
            new_sl = kurs * (1 - STOP_LOSS_PCT)
            df_pf.at[idx, 'Stop Loss'] = new_sl
            print(f"   🛡️ {name}: Stop Loss gesetzt auf {new_sl:.2f} €")
            updates += 1
            
        # --- REGEL 2: TAKE PROFIT (Wenn leer) ---
        if pd.isna(row['Take Profit']):
            # Wenn Kaufkurs da ist, nehmen wir den, sonst den aktuellen Kurs als Basis
            basis = kauf if (pd.notna(kauf) and kauf > 0) else kurs
            new_tp = basis * (1 + TAKE_PROFIT_PCT)
            df_pf.at[idx, 'Take Profit'] = new_tp
            print(f"   💰 {name}: Take Profit gesetzt auf {new_tp:.2f} €")
            updates += 1

        # --- RECHNEN: WERTE & G/V ---
        wert_neu = kurs * anzahl
        df_pf.at[idx, 'Aktueller Wert'] = wert_neu
        
        if pd.notna(kauf) and kauf > 0:
            invest = kauf * anzahl
            gv = wert_neu - invest
            gv_pct = (gv / invest) * 100
            df_pf.at[idx, 'G/V'] = gv
            df_pf.at[idx, 'G/V %'] = gv_pct
        else:
            # Falls kein Kaufkurs da ist
            df_pf.at[idx, 'G/V'] = 0
            df_pf.at[idx, 'G/V %'] = 0

        # --- WARNUNGEN PRÜFEN ---
        sl = df_pf.at[idx, 'Stop Loss']
        tp = df_pf.at[idx, 'Take Profit']
        
        warnung = None
        if kurs <= sl:
            warnung = "STOP LOSS!"
        elif kurs >= tp:
            warnung = "ZIEL 30%!"
            
        df_pf.at[idx, 'Warnung'] = warnung

    # 3. SPEICHERN
    print(f"\n💾 Speichere {updates} Regel-Änderungen...")
    
    with pd.ExcelWriter(excel_file, engine='openpyxl', mode='w') as writer:
        df_wl.to_excel(writer, sheet_name=xls.sheet_names[0], index=False)
        df_pf.to_excel(writer, sheet_name='Portfolio', index=False)
        
    print("✅ FERTIG! Alle Lücken wurden gefüllt.")

except Exception as e:
    print(f"❌ FEHLER: {e}")
    print("⚠️ Bitte schließe die Excel-Datei!")

time.sleep(5)