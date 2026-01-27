import pandas as pd
import os
import sys
import time

# Pfad-Fix für den Hub
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)
sys.path.append(os.path.join(BASE_DIR, 'utils')) # Explizit utils hinzufügen

from cloud.repository import TradingRepository
from market.yahoo import get_price_data
from market.elliott import calculate_elliott
from market.fundamental import get_fundamental_data
from market.montecarlo import run_monte_carlo
from market.scoring import calculate_final_score
from alerts.telegram import send_signal
# DASHBOARD IMPORT
from utils.dashboard_gen import generate_dashboard

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
            
            row['Akt. Kurs [€]'] = round(hist['Close'].iloc[-1], 2)
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