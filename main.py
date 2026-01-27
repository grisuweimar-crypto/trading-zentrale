import pandas as pd
import os
import sys
import time
from utils.dashboard_gen import generate_dashboard

# Pfade für Thonny sicherstellen
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# ALLE DEINE MODULE LADEN
from cloud.repository import TradingRepository
from market.yahoo import get_price_data
from market.elliott import calculate_elliott
from market.fundamental import get_fundamental_data
from market.montecarlo import run_monte_carlo
from market.scoring import calculate_final_score
from alerts.telegram import send_signal

def main():
    print("🚀 TRADING-ZENTRALE: AKTIVIERE SCAN...")
    repo = TradingRepository()
    
    # 1. Ticker aus der CSV laden
    df = repo.load_watchlist()
    
    if df.empty:
        print("❌ FEHLER: watchlist.csv ist leer. Nutze den TableManager zum Initialisieren!")
        return

    print(f"📊 Analyse von {len(df)} Werten aus der Tabelle gestartet...")
    results = []

    for index, row in df.iterrows():
        ticker = str(row['Ticker']).strip()
        
        # --- DER FIX: WIR ENTFERNEN DIE BREMSE ---
        # Namen mit Leerzeichen (wie 'ABB LTD. NA') werden jetzt NICHT mehr übersprungen, 
        # weil fix_everything.py sie zuvor in echte Kürzel (ABBN.SW) umgewandelt hat.
        
        print(f"🔍 [{(index+1)}/{(len(df))}] Scanne {ticker}...")
        
        try:
            hist = get_price_data(ticker)
            if hist is None or hist.empty:
                print(f"⚠️ Keine Daten für {ticker}")
                results.append(row) 
                continue
            
            # ANALYSE-MODULE (Dein Original-Umfang)
            elliott = calculate_elliott(hist)
            fundamentals = get_fundamental_data(ticker)
            monte_carlo = run_monte_carlo(hist)
            score = calculate_final_score(ticker, elliott, fundamentals, monte_carlo)
            
            # TABELLE BEFÜLLEN
            row['Akt. Kurs [€]'] = round(hist['Close'].iloc[-1], 2)
            row['Score'] = score
            row['Elliott-Signal'] = elliott.get('signal', 'Warten')
            row['Elliott-Ausstieg'] = elliott.get('target', 0)
            row['MC-Chance'] = monte_carlo.get('probability', 0)
            
            results.append(row)

            # TELEGRAM ALERT (Deine korrigierte Logik)
            # Das Signal wird nur gesendet, wenn BUY und Score > 75
            if elliott.get('signal') == "BUY" and score > 75:
                send_signal(ticker, elliott, score)
                print(f"📲 Telegram-Alarm für {ticker} raus!")

            time.sleep(0.5) # Yahoo Finance Schongang

        except Exception as e:
            print(f"❌ Fehler bei {ticker}: {e}")
            results.append(row)

    # 3. SPEICHERN & FINISH
    final_df = pd.DataFrame(results)
    repo.save_watchlist(final_df)
    print("🏁 SCAN BEENDET. Alle Module erfolgreich ausgeführt!")
    
    # --- HIER PASSIERT DIE MAGIE ---
    try:
        print("🏗️ Erstelle Dashboard...")
        generate_dashboard() # Diese Funktion aus deinem utils-Ordner aufrufen
        print("📊 Dashboard index.html wurde generiert!")
    except Exception as e:
        print(f"⚠️ Dashboard konnte nicht erstellt werden: {e}")

    print("🏁 SCAN BEENDET. Alle Module erfolgreich ausgeführt!")

if __name__ == "__main__":
    main()