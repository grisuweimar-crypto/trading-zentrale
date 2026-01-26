import pandas as pd
import yfinance as yf
import os

# --- PFAD KONFIGURATION ---
desktop = os.path.join(os.path.expanduser("~"), "OneDrive", "Desktop")
if not os.path.exists(desktop): desktop = os.path.join(os.path.expanduser("~"), "Desktop")
excel_file = os.path.join(desktop, "Watchlist Master 2026-voll.xlsx")

print("🧐 SMART DIVIDEND FILL (Robust & Manuell-Sicher)...")

if not os.path.exists(excel_file):
    print("❌ Excel-Datei nicht gefunden!")
    exit()

# Laden
try:
    xls = pd.ExcelFile(excel_file)
    df_wl = pd.read_excel(xls, sheet_name=0)
    df_pf = pd.read_excel(xls, sheet_name="Portfolio")
except Exception as e:
    print(f"❌ Fehler beim Laden: {e}")
    exit()

# Hilfsfunktion (Braucht keine ISIN mehr, Name reicht)
def get_symbol_by_name(name):
    n = str(name).upper()
    
    # KRYPTO (brauchen keine Dividende, aber zur Sicherheit)
    if 'BTC' in n or 'BITCOIN' in n: return 'BTC-EUR'
    
    # AKTIEN MAPPING (Yahoo Ticker)
    if 'VALE' in n: return 'VALE'
    if 'AURORA' in n: return 'ACB'
    if 'B2GOLD' in n: return 'BTG'
    if 'CANOPY' in n: return 'CGC'
    if 'BYD' in n: return 'BY6.F'
    if 'NIO' in n: return 'NIO'
    if 'XIAOMI' in n: return '1810.HK'
    
    # DEUTSCHE STANDARDWERTE
    if 'ALLIANZ' in n: return 'ALV.DE'
    if 'BASF' in n: return 'BAS.DE'
    if 'MERCEDES' in n: return 'MBG.DE'
    if 'SAP' in n: return 'SAP.DE'
    if 'TELEKOM' in n: return 'DTE.DE'
    if 'SIEMENS' in n: return 'SIE.DE'
    if 'VONOVIA' in n: return 'VNA.DE'
    if 'BAYER' in n: return 'BAYN.DE'
    if 'MUENCHENER' in n: return 'MUV2.DE'
    if 'DHL' in n: return 'DHL.DE'
    if 'INFINEON' in n: return 'IFX.DE'
    
    # US TECH
    if 'MICROSOFT' in n: return 'MSFT'
    if 'APPLE' in n: return 'AAPL'
    if 'TESLA' in n: return 'TSLA'
    if 'AMAZON' in n: return 'AMZN'
    if 'NVIDIA' in n: return 'NVDA'
    if 'PAYPAL' in n: return 'PYPL'
    if 'PALANTIR' in n: return 'PLTR'
    
    return ""

updates = 0
print(f"📂 Prüfe {len(df_pf)} Positionen...")

for idx, row in df_pf.iterrows():
    # Krypto überspringen
    asset_type = row.get('AssetType', '')
    if asset_type == 'CRYPTO': continue

    name = str(row['Name'])
    current_div = row.get('DivRendite', 0)
    
    # 1. CHECK: Hast du manuell was eingetragen? (> 0.1%)
    # Wir wandeln sicher in float um
    try: val = float(current_div)
    except: val = 0.0
    
    if val > 0.1:
        print(f"   🔒 {name}: Wert {val}% behalten (Manuell gesetzt).")
        continue
    
    # 2. SUCHE: Wenn 0, fragen wir Yahoo
    symbol = get_symbol_by_name(name)
    if not symbol:
        # Kein Symbol gefunden -> können nichts tun
        continue
    
    try:
        t = yf.Ticker(symbol)
        d = t.info.get('dividendYield', 0)
        
        # Sicherheits-Filter: Yahoo liefert oft Müll > 20%
        if d and 0 < d < 0.25:
            div_pct = d * 100
            df_pf.at[idx, 'DivRendite'] = div_pct
            print(f"   🖊️ {name} ({symbol}): Yahoo meldet {div_pct:.2f}% -> Eingetragen.")
            updates += 1
        elif d and d >= 0.25:
            print(f"   ⚠️ {name}: Yahoo meldet unrealistische {d*100:.0f}% -> Ignoriert.")
            
    except Exception as e:
        # Leise weitermachen, wenn Internet hakt
        pass

# Speichern
if updates > 0:
    try:
        with pd.ExcelWriter(excel_file, engine='openpyxl', mode='w') as writer:
            df_wl.to_excel(writer, sheet_name=xls.sheet_names[0], index=False)
            df_pf.to_excel(writer, sheet_name='Portfolio', index=False)
        print(f"\n✅ ERFOLG: {updates} fehlende Dividenden nachgetragen.")
    except:
        print("\n❌ Fehler beim Speichern (Datei offen?).")
else:
    print("\n✅ Fertig. Keine neuen Lücken gefüllt (oder keine Daten gefunden).")