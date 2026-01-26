import yfinance as yf
import pandas as pd
import numpy as np
import os
import requests
import time
import logging
from datetime import datetime

# Logging aus
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# --- KONFIGURATION ---
excel_file = r"C:\Users\CW\OneDrive\Desktop\Watchlist Master 2026-voll.xlsx"
TELEGRAM_TOKEN = "8507338681:AAGyiWZ4coOrwVemJujt-uT2uZmT6V66NqQ" 
CHAT_ID = "1369830507"

# --- EINSTELLUNGEN ---
MC_DAYS = 30
MC_SIMULATIONS = 500

# Gewichte (Sync mit Dashboard)
STD_ANALYST = 10
STD_UPSIDE  = 10
STD_PE      = 15
STD_DEBT    = 5
STD_DIV     = 5
STD_GROWTH  = 10
STD_MARGIN  = 10
STD_MC      = 20
STD_ELLIO = 0  # Gewicht für Elliott-Confidence (0–50)

print("🚀 STARTE SCANNER (V24 - Mit OneDrive-Geduld)...")

# --- TELEGRAM ---
def telegram_send(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        requests.post(url, data=data)
    except Exception as e:
        print(f"❌ Telegram Fehler: {e}")

# --- MATHE ---

def zigzag(prices, threshold=0.05):
    swings = []
    last_pivot = prices[0]
    last_type = None  # 'high' oder 'low'

    for i, price in enumerate(prices[1:], start=1):
        change = (price - last_pivot) / last_pivot

        if last_type != 'high' and change >= threshold:
            swings.append((i, price, 'high'))
            last_pivot = price
            last_type = 'high'

        elif last_type != 'low' and change <= -threshold:
            swings.append((i, price, 'low'))
            last_pivot = price
            last_type = 'low'

    return swings


def detect_impulse(swings):
    if len(swings) < 6:
        return None

    last = swings[-6:]
    types = [s[2] for s in last]

    if types != ['low', 'high', 'low', 'high', 'low', 'high']:
        return None

    w1 = last[1][1] - last[0][1]
    w3 = last[3][1] - last[2][1]
    w5 = last[5][1] - last[4][1]

    if last[2][1] <= last[0][1]:
        return None

    if w3 <= min(w1, w5):
        return None

    if last[4][1] <= last[1][1]:
        return None

    return {
        "type": "IMPULS",
        "confidence": 0.7,
        "wave_4_low": last[4][1],
        "wave_5_high": last[5][1]
    }


    swings = []
    last_pivot = prices[0]
    last_type = None  # 'high' or 'low'

    for i, price in enumerate(prices[1:], start=1):
        change = (price - last_pivot) / last_pivot

        if last_type != 'high' and change >= threshold:
            swings.append((i, price, 'high'))
            last_pivot = price
            last_type = 'high'

        elif last_type != 'low' and change <= -threshold:
            swings.append((i, price, 'low'))
            last_pivot = price
            last_type = 'low'

    return swings


def calculate_monte_carlo_prob(history):
    if len(history) < MC_DAYS: return 0
    try:
        returns = history['Close'].pct_change().dropna()
        mean = returns.mean()
        std = returns.std()
        simulations = np.random.normal(mean, std, (MC_DAYS, MC_SIMULATIONS))
        paths = (1 + simulations).cumprod(axis=0)
        final_prices = paths[-1] * history['Close'].iloc[-1]
        win_prob = np.mean(final_prices > history['Close'].iloc[-1]) * 100
        return win_prob
    except: return 0

def calculate_score(row):
    def safe_float(val):
        try: return float(val)
        except: return 0.0

    score = 0
    typ = str(row.get('Typ', ''))
    if "DOPPEL" in typ:
        score += 100
    else:
        score += 50

    # Monte Carlo
    score += safe_float(row.get('MC_Chance', 0)) * (STD_MC / 100)

    # Analystenempfehlung
    rec = str(row.get('AnalystRec', '')).lower()
    if 'buy' in rec or 'strong' in rec:
        score += STD_ANALYST

    # Upside
    up = safe_float(row.get('Upside', 0))
    if up > 5: score += STD_UPSIDE
    elif up < 0: score -= STD_UPSIDE

    # PE
    pe = safe_float(row.get('PE', 999))
    if 0 < pe < 25: score += STD_PE
    elif pe > 60: score -= STD_PE

    # Debt
    debt = safe_float(row.get('Debt', 0))
    if debt < 80: score += STD_DEBT
    elif debt > 150: score -= STD_DEBT

    # Dividende
    div = safe_float(row.get('DivRendite', 0))
    if div > 2.0: score += STD_DIV

    # Wachstum
    if safe_float(row.get('Wachstum', 0)) > 5: score += STD_GROWTH

    # Marge
    if safe_float(row.get('Marge', 0)) > 0: score += STD_MARGIN

    # 🔹 Elliott jetzt korrekt
    elliott_conf = safe_float(row.get('Elliott_Confidence', 0))
    score += elliott_conf * STD_ELLIO  # Standardgewicht 30, kann später über Slider geändert werden

    return score


    score = 0
    typ = str(row.get('Typ', ''))
    if "DOPPEL" in typ: score += 100
    # Elliott wird NUR über Confidence bewertet
    else: score += 50
    score += safe_float(row.get('MC_Chance', 0)) * (STD_MC / 100)
    rec = str(row.get('AnalystRec', '')).lower()
    if 'buy' in rec or 'strong' in rec: score += STD_ANALYST
    up = safe_float(row.get('Upside', 0))
    if up > 5: score += STD_UPSIDE
    elif up < 0: score -= STD_UPSIDE
    pe = safe_float(row.get('PE', 999))
    if 0 < pe < 25: score += STD_PE
    elif pe > 60: score -= STD_PE
    debt = safe_float(row.get('Debt', 0))
    if debt < 80: score += STD_DEBT
    elif debt > 150: score -= STD_DEBT
    div = safe_float(row.get('DivRendite', 0))
    if div > 2.0: score += STD_DIV
    if safe_float(row.get('Wachstum', 0)) > 5: score += STD_GROWTH
    if safe_float(row.get('Marge', 0)) > 0: score += STD_MARGIN
    return score

def elliott_wave_signal(row, symbol):
    try:
        data = yf.download(symbol, period="6mo")
        if len(data) > 50:
            high = data['High'].tail(60).max()
            low = data['Low'].tail(60).min()
            range_size = high - low
            elliott_entry = high - range_size * 0.5  # Fib 50%
            kurs = float(row['Akt. Kurs [€]'])
            abstand = (kurs - elliott_entry) / kurs * 100
            
            if abs(abstand) < 10:
                return "🔥 ELLIOTT KAUFEN", elliott_entry
            return "⏳ ELLIOTT WARTEN", elliott_entry
    except:
        pass
    return "FEHLER", 0


# --- SYMBOL MAPPING ---
def get_symbol(name, isin, asset_type='STOCK'):
    n = str(name).upper()
    if asset_type == 'CRYPTO':
        if 'BTC' in n: return 'BTC-EUR'
        if 'ETH' in n: return 'ETH-EUR'
        if 'SOL' in n: return 'SOL-EUR'
        if 'XRP' in n: return 'XRP-EUR'
        if 'ADA' in n: return 'ADA-EUR'
        return f"{n}-EUR"

    if 'UNITEDHEALTH' in n: return 'UNH.F'
    if 'INGREDION' in n: return 'CHI.F'
    if 'PHILIP MORRIS' in n: return '4I1.F'
    if 'JOHNSON' in n: return 'JNJ.F'
    if 'PROCTER' in n: return 'PRG.F'
    if 'COCA' in n: return 'CCC3.F'
    if 'PEPSI' in n: return 'PEP.F'
    if 'XIAOMI' in n: return '3CP.F'     
    if 'BYD' in n: return 'BY6.F'        
    if 'ALIBABA' in n: return 'AHLA.F'   
    if 'JD.COM' in n: return '099.F'
    if 'NIO' in n: return 'N3IA.F'
    if 'TENCENT' in n: return 'NNnD.F'
    if 'VALE' in n: return 'CVLC.F'      
    if 'B2GOLD' in n: return 'A2D.F'
    if 'AURORA' in n: return '21P.F'
    if 'CANOPY' in n: return '11L1.F'
    if 'NEL' in n: return 'D7G.F'        
    if 'NOVO' in n: return 'NOVC.F'      
    if 'PAYPAL' in n: return '2PP.F'
    if 'MICROSOFT' in n: return 'MSF.F'
    if 'APPLE' in n: return 'APC.F'
    if 'TESLA' in n: return 'TL0.F'
    if 'AMAZON' in n: return 'AMZ.F'
    if 'NVIDIA' in n: return 'NVD.F'
    if 'PALANTIR' in n: return 'PTX.F'
    if 'NIKE' in n: return 'NKE.F'
    if 'CHEVRON' in n: return 'CHV.F'
    if 'CENTENE' in n: return 'C26.F'
    if 'ALLIANZ' in n: return 'ALV.DE'
    if 'BASF' in n: return 'BAS.DE'
    if 'MERCEDES' in n: return 'MBG.DE'
    if 'SAP' in n: return 'SAP.DE'
    if 'TELEKOM' in n: return 'DTE.DE'
    if 'SIEMENS' in n: return 'SIE.DE'
    if 'BAYER' in n: return 'BAYN.DE'
    if 'VONOVIA' in n: return 'VNA.DE'
    if 'FRESENIUS' in n: return 'FME.DE'
    if 'SUSS' in n: return 'SMHN.DE'
    if 'MUENCHENER' in n: return 'MUV2.DE'
    if 'INFINEON' in n: return 'IFX.DE'
    if 'DHL' in n: return 'DHL.DE'
    if 'MINISO' in n: return 'MNS.F'
    if 'RELIANCE' in n: return 'RIGD.F' 
    if 'FIRST SOLAR' in n: return 'F3A.F'
    if 'ALPHABET' in n: return 'GOOGL'


    if str(isin).startswith('US'): return str(isin)
    return ""

# --- HAUPTPROGRAMM ---

try:
    xls = pd.ExcelFile(excel_file)
    df_wl = pd.read_excel(xls, sheet_name=0)
    df_pf = pd.read_excel(xls, sheet_name="Portfolio")
    
    # 🔧 NaN-Fix
    for col in ['MC_Chance', 'Upside', 'PE', 'Debt', 'DivRendite', 'Wachstum', 'Marge']:
        if col in df_wl.columns:
            df_wl[col] = df_wl[col].fillna(0)
            
    print(f"✅ Geladen. WL: {len(df_wl)}, PF: {len(df_pf)}")
except Exception as e:
    print(f"❌ Fehler beim Laden: {e}")
    exit()

# 1. WATCHLIST SCAN
print("\n🔭 Scanne Watchlist...")

# --- WATCHLIST SCAN (mit Elliott) ---
print("\n🔭 Scanne Watchlist...")

# Spalten für Elliott vorbereiten
for c in ['Elliott_Signal', 'Elliott_Confidence', 'Elliott_Entry']:
    if c not in df_wl.columns:
        df_wl[c] = 0 if 'Confidence' in c else "-"

for idx, row in df_wl.iterrows():
    try:
        symbol = get_symbol(row.get('Name'), row.get('ISIN'), 'STOCK')
        if not symbol:
            continue

        t = yf.Ticker(symbol)
        try:
            price = t.fast_info.last_price
        except:
            h = t.history(period='5d')
            if h.empty:
                raise ValueError("Kein Preis")
            price = h['Close'].iloc[-1]

        info = t.info
        hist = t.history(period=f"{MC_DAYS+10}d")
        if hist.empty or 'Close' not in hist:
            raise ValueError("Keine Kursdaten")

        # 🔹 Elliott Analyse
        prices = hist['Close'].values
        if len(prices) > 50:
            swings = zigzag(prices)
            elliott = detect_impulse(swings)

            if elliott:
                df_wl.at[idx, 'Elliott_Signal'] = elliott['type']        # z.B. "IMPULS"
                df_wl.at[idx, 'Elliott_Confidence'] = elliott['confidence']
                df_wl.at[idx, 'Elliott_Entry'] = elliott['wave_4_low']
            else:
                df_wl.at[idx, 'Elliott_Signal'] = "NONE"
                df_wl.at[idx, 'Elliott_Confidence'] = 0
                df_wl.at[idx, 'Elliott_Entry'] = None

        # 🔹 Fundamentale Daten
        if price > 0:
            df_wl.at[idx, 'Akt. Kurs [€]'] = price

        df_wl.at[idx, 'PE'] = info.get('trailingPE', 999)
        if info.get('targetMeanPrice') and price > 0:
            df_wl.at[idx, 'Upside'] = ((info.get('targetMeanPrice') - price) / price) * 100

        df_wl.at[idx, 'AnalystRec'] = info.get('recommendationKey', 'none')
        df_wl.at[idx, 'DivRendite'] = info.get('dividendYield', 0) * 100 if info.get('dividendYield') else 0
        df_wl.at[idx, 'Marge'] = info.get('profitMargins', 0) * 100 if info.get('profitMargins') else 0
        df_wl.at[idx, 'Wachstum'] = info.get('revenueGrowth', 0) * 100 if info.get('revenueGrowth') else 0
        df_wl.at[idx, 'Debt'] = info.get('debtToEquity', 0)
        df_wl.at[idx, 'ProfiZiel'] = info.get('targetMeanPrice', 0)

        # 🔹 Monte Carlo
        mc = calculate_monte_carlo_prob(hist)
        if mc > 0:
            df_wl.at[idx, 'MC_Chance'] = mc

        # 🔹 Score Berechnen inkl. Elliott
        df_wl.at[idx, 'Score'] = calculate_score(df_wl.loc[idx])

    except Exception as e:
        print(f"⚠️ Fehler bei {row.get('Name')}: {e}")
        continue



# 2. PORTFOLIO SCAN
print("\n💼 Scanne Portfolio...")
pf_alarms = []
for idx, row in df_pf.iterrows():
    asset_type = row.get('AssetType', 'STOCK')
    symbol = get_symbol(row['Name'], row.get('ISIN'), asset_type)
    if not symbol: continue
    try:
        t = yf.Ticker(symbol)
        try: kurs = t.fast_info.last_price
        except: kurs = t.history(period='1d')['Close'].iloc[-1]
        
        if kurs > 0: df_pf.at[idx, 'Akt. Kurs [€]'] = kurs
        
        if asset_type == 'STOCK':
            current_div = row.get('DivRendite', 0)
            try: val = float(current_div)
            except: val = 0.0
            if val <= 0.1: 
                d = t.info.get('dividendYield', 0)
                if d and 0 < d < 0.25: df_pf.at[idx, 'DivRendite'] = d * 100

        sl = row.get('Stop Loss')
        tp = row.get('Take Profit')
        if pd.notna(sl) and kurs <= sl:
            pf_alarms.append(f"🚨 STOP LOSS: {row['Name']} ({kurs:.2f} < {sl})")
            df_pf.at[idx, 'Warnung'] = "STOP LOSS!"
        elif pd.notna(tp) and kurs >= tp:
            pf_alarms.append(f"💰 TAKE PROFIT: {row['Name']} ({kurs:.2f} > {tp})")
            df_pf.at[idx, 'Warnung'] = "ZIEL ERREICHT!"
        else:
            df_pf.at[idx, 'Warnung'] = None
    except: pass

# 3. SPEICHERN (MIT WIEDERHOLUNG)
print("\n💾 Speichern...")
df_pf['Aktueller Wert'] = df_pf['Anzahl'] * df_pf['Akt. Kurs [€]']
df_pf['Invest'] = df_pf['Anzahl'] * df_pf['Kaufkurs']
df_pf['G/V'] = df_pf['Aktueller Wert'] - df_pf['Invest']
total_wert = df_pf['Aktueller Wert'].sum()
total_gv = df_pf['G/V'].sum()

save_success = False
max_retries = 5

for attempt in range(max_retries):
    try:
        with pd.ExcelWriter(excel_file, engine='openpyxl', mode='w') as writer:
            df_wl.to_excel(writer, sheet_name=xls.sheet_names[0], index=False)
            df_pf.to_excel(writer, sheet_name='Portfolio', index=False)
        print(f"✅ Datei erfolgreich gespeichert (Versuch {attempt+1}).")
        save_success = True
        break
    except Exception as e:
        print(f"⏳ Schreibfehler (Versuch {attempt+1}/{max_retries}): {e}")
        print("   Warte 3 Sekunden auf OneDrive/Excel...")
        time.sleep(3)

if save_success:
    # --- TELEGRAM ---
    warn_text = "Keine" if not pf_alarms else "\n".join(pf_alarms)
    msg_depot = (
        f"🏦 **DEPOT UPDATE**\n"
        f"Depot: {total_wert:,.2f} €\n"
        f"G/V: {total_gv:,.2f} €\n"
        f"Warnsignale: {warn_text}"
    )
    telegram_send(msg_depot)
    
    top3 = df_wl.sort_values(by='Score', ascending=False).head(3)
    for i, (idx, row) in enumerate(top3.iterrows()):
        medals = ["🥇", "🥈", "🥉"]
        einstieg = row.get('Auto-Einstieg 50%', 0)
        if pd.isna(einstieg) or einstieg == 0: einstieg = row.get('Akt. Kurs [€]', 0)
        
        profi_ziel = row.get('ProfiZiel', 0)
        if pd.isna(profi_ziel) or profi_ziel == 0: p_ziel_txt = "n/a"
        else: p_ziel_txt = f"{profi_ziel:.2f} €"

        dein_ziel = row.get('Ziel', 'n/a')
        if pd.isna(dein_ziel): dein_ziel = "n/a"

        msg_top = (
            f"{medals[i]} {row['Name']} (Score: {int(row['Score'])})\n"
            f"Einstieg: {einstieg:.2f} €\n"
            f"Aktuell: {row['Akt. Kurs [€]']:.2f} €\n"
            f"Dein Ziel: {dein_ziel} €\n"
            f"Profi-Ziel: {p_ziel_txt}"
        )
        telegram_send(msg_top)
    print("🚀 Fertig.")
else:
    print("❌❌❌ KONNTE NICHT SPEICHERN. KEIN TELEGRAM GESENDET. ❌❌❌")
    print("Bitte prüfe, ob Excel oder OneDrive die Datei blockiert.")