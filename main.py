import pandas as pd
import os
import sys
import time

# --- PFAD-FIX FÜR DEN HUB (ROOT-EBENE) ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from cloud.repository import TradingRepository
from market.yahoo import get_price_data
from market.elliott import calculate_elliott
from market.fundamental import get_fundamental_data
from market.montecarlo import run_monte_carlo
from market.scoring import calculate_final_score
from alerts.telegram import send_signal
# DASHBOARD IMPORT
from dashboard_gen import generate_dashboard

def main():
    print("🚀 TRADING-ZENTRALE: AKTIVIERE SCAN...")
    repo = TradingRepository()
    df = repo.load_watchlist()
    
    if df.empty:
        print("❌ FEHLER: watchlist.csv leer.")
        return

    print(f"📊 Analyse von {len(df)} Werten gestartet...")
    results = []

    for index, row in df.iterrows():
        ticker = str(row['Ticker']).strip()
        stock_name = str(row['Name']).strip() # Namen für Telegram sichern
        
        print(f"🔍 [{(index+1)}/{(len(df))}] Scanne {ticker}...")
        
        try:
            hist = get_price_data(ticker)
            if hist is None or hist.empty:
                results.append(row)
                continue
            
            elliott = calculate_elliott(hist)
            fundamentals = get_fundamental_data(ticker)
            monte_carlo = run_monte_carlo(hist)
            score = calculate_final_score(ticker, elliott, fundamentals, monte_carlo)
            
            # --- NEU: 24H PERFORMANCE BERECHNUNG ---
            # Wir nutzen die geladenen Historien-Daten (hist)
            current_price = hist['Close'].iloc[-1]
            if len(hist) > 1:
                previous_close = hist['Close'].iloc[-2]
                perf_pct = ((current_price - previous_close) / previous_close) * 100
            else:
                perf_pct = 0.0
            # ----------------------------------------
            
            row['Akt. Kurs [€]'] = round(current_price, 2)
            row['Perf %'] = round(perf_pct, 2) # Speichern für das Dashboard
            row['Score'] = score
            row['Elliott-Signal'] = elliott.get('signal', 'Warten')
            row['Elliott-Ausstieg'] = elliott.get('target', 0)
            row['MC-Chance'] = monte_carlo.get('probability', 0)
            
            results.append(row)

            # TELEGRAM MIT NAMEN-FIX
            if elliott.get('signal') == "BUY" and score > 75:
                send_signal(ticker, elliott, score, name=stock_name)
                print(f"📲 Telegram-Alarm für {stock_name} raus!")

            time.sleep(0.5)

        except Exception as e:
            print(f"❌ Fehler bei {ticker}: {e}")
            results.append(row)

    # SPEICHERN
    final_df = pd.DataFrame(results)
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