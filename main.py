import pandas as pd
import os
import sys
import time

# --- PFAD-FIX FÜR DEN HUB (ROOT-EBENE) ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from cloud.repository import TradingRepository
from market.yahoo import get_price_data, get_ticker_symbol
from market.elliott import calculate_elliott
from market.fundamental import get_fundamental_data
from market.montecarlo import run_monte_carlo
from market.scoring import calculate_final_score
from market.cycle import compute_cycle_oscillator, classify_cycle
from alerts.telegram import send_signal
# DASHBOARD IMPORT
from dashboard_gen import generate_dashboard
from market.crv import calculate_crv

def main():
    print("🚀 TRADING-ZENTRALE: AKTIVIERE SCAN...")
    repo = TradingRepository()
    df = repo.load_watchlist()
    df['Zyklus %'] = 50.0
    df['Zyklus-Status'] = 'neutral'
    
    if df.empty:
        print("❌ FEHLER: watchlist.csv leer.")
        return

    print(f"📊 Analyse von {len(df)} Werten gestartet...")

    for index, row in df.iterrows():
        ticker = str(row['Ticker']).strip()
        stock_name = str(row['Name']).strip()
        # Yahoo nutzt Ticker, keine ISIN: zuerst Spalte "Yahoo", sonst Auflösung aus Name/ISIN
        row_dict = row.to_dict()
        has_isin = row_dict.get('ISIN') is not None and not (isinstance(row_dict.get('ISIN'), float) and pd.isna(row_dict.get('ISIN'))) and str(row_dict.get('ISIN', '')).strip()
        if not has_isin and len(ticker) >= 9 and ticker[:2].isalpha() and ticker[2:].replace(' ', '').isalnum():
            row_dict['ISIN'] = ticker
        symbol_for_yahoo = (str(row.get('Yahoo', '') or '').strip() or get_ticker_symbol(row_dict) or ticker)

        print(f"🔍 [{(index+1)}/{(len(df))}] Scanne {ticker}...")

        try:
            hist = get_price_data(symbol_for_yahoo)
            if hist is None or hist.empty:
                continue

            # --- ZYKLUS BERECHNEN ---
            cycle_value = compute_cycle_oscillator(hist, period=20)
            cycle_status = classify_cycle(cycle_value)
            
            # 1. Preis fixieren & WÄHRUNG HOLEN
            current_price = float(hist['Close'].iloc[-1]) 
            # Holt die Währung aus den Attributen, die wir in yahoo.py gesetzt haben
            currency_code = hist.attrs.get('currency', 'USD') 
            
            # 2. Daten sammeln
            elliott = calculate_elliott(hist)
            fundamentals = get_fundamental_data(symbol_for_yahoo)
            monte_carlo = run_monte_carlo(hist)
            
            # --- NEU: CRV BERECHNEN ---
            e_target = elliott.get('target', 0)
            crv_value = calculate_crv(current_price, e_target) 
            
            # 3. Score berechnen (mit Preis & CRV Übergabe)
            final_calculated_score = calculate_final_score(
                symbol_for_yahoo, elliott, fundamentals, monte_carlo, current_price, crv_value
            )
            
            # 4. Performance
            perf_pct = 0.0
            if len(hist) > 1:
                perf_pct = ((current_price - hist['Close'].iloc[-2]) / hist['Close'].iloc[-2]) * 100

            # 5. Daten in Zeile schreiben (Yahoo-Symbol speichern → Link im Dashboard geht direkt auf die richtige Aktie)
            df.at[index, 'Yahoo'] = symbol_for_yahoo
            df.at[index, 'Akt. Kurs'] = round(current_price, 2)
            df.at[index, 'Währung'] = currency_code
            df.at[index, 'Perf %'] = round(perf_pct, 2)
            df.at[index, 'Score'] = final_calculated_score
            df.at[index, 'CRV'] = crv_value
            df.at[index, 'Elliott-Signal'] = elliott.get('signal', 'Warten')
            df.at[index, 'Elliott-Einstieg'] = elliott.get('entry', 0)
            df.at[index, 'Elliott-Ausstieg'] = elliott.get('target', 0)
            df.at[index, 'MC-Chance'] = monte_carlo.get('probability', 0)
            
            # --- ZYKLUS-SPALTE ---
            df.loc[index, 'Zyklus %'] = round(cycle_value, 1)
            df.loc[index, 'Zyklus-Status'] = cycle_status
            
            

            # 6. TELEGRAM (Nutzt jetzt die Variable von oben)
            # Wir prüfen das Signal direkt aus den Elliott-Daten
            if elliott.get('signal') == "BUY" and final_calculated_score > 75:
                send_signal(ticker, elliott, final_calculated_score, name=stock_name, currency=currency_code)
                print(f"📲 Telegram-Alarm für {stock_name} raus (Score: {final_calculated_score})!")

            time.sleep(0.5)

        except Exception as e:
            print(f"❌ Fehler bei {ticker}: {e}")
            

    # SPEICHERN
    final_df = df
    repo.save_watchlist(final_df)
    
    # DASHBOARD GENERIEREN
    try:
        print("🏗️ Erstelle Dashboard...")
        generate_dashboard() 
    except Exception as e:
        print(f"⚠️ Dashboard-Fehler: {e}")

    print("🏁 SCAN BEENDET. Alle Module erfolgreich ausgeführt!")

if __name__ == "__main__":
    main()