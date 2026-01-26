import pandas as pd
import yfinance as yf
from cloud.repository import TradingRepository
from market.yahoo import get_ticker_symbol, get_price_data
from market.montecarlo import calculate_probability
from market.fundamental import get_fundamental_data
from market.scoring import calculate_total_score
from market.elliott import detect_elliott_wave
from market.forex import get_usd_eur_rate, convert_to_eur 
import config
from alerts.telegram import send_summary

def run_scanner():
    print("🚀 TRADING SCANNER V27 - BROKER-IMPORT MODUS")
    repo = TradingRepository()
    df_wl = repo.load_watchlist()
    
    # 1. Watchlist Scan (Deine bewährte Logik)
    fx_rate = get_usd_eur_rate()
    for idx, row in df_wl.iterrows():
        symbol = row.get('Ticker') if pd.notna(row.get('Ticker')) and row.get('Ticker') != "" else get_ticker_symbol(row)
        hist = get_price_data(symbol)
        if hist is not None:
            ticker_obj = yf.Ticker(symbol)
            raw_price = float(hist['Close'].iloc[-1])
            currency = ticker_obj.info.get('currency', 'EUR')
            price_eur = convert_to_eur(raw_price, fx_rate) if currency == 'USD' else raw_price
            df_wl.at[idx, 'Akt. Kurs [€]'] = price_eur
            df_wl.at[idx, 'Score'] = calculate_total_score(df_wl.loc[idx])

    # --- 2. SCHRITT: PORTFOLIO AUS BROKER_IMPORT (Der Fix) ---
    print("📊 Berechne Portfolio aus Broker_Import Tab...")
    # Nutzt die load_import_data Methode, die wir in repository.py erstellt haben
    df_import = repo.load_import_data()
    
    total_value = 0.0
    if not df_import.empty:
        for idx, row in df_import.iterrows():
            # Wir nehmen die Spalte 'Wert', da Zero hier die Summe pro Position liefert
            raw_wert = str(row.get('Wert', '0'))
            
            # REINIGUNG: Tausender-Punkt weg, Komma zu Punkt
            # Macht aus "1.365,40" -> "1365.40"
            clean_wert = raw_wert.replace('.', '').replace(',', '.')
            
            try:
                # Wichtig: Explizit in Zahl (float) umwandeln BEVOR gerechnet wird!
                val = float(pd.to_numeric(clean_wert, errors='coerce') or 0.0)
                total_value += val
            except:
                continue
        print(f"✅ Depot-Summe erfolgreich berechnet.")
    else:
        print("⚠️ Broker_Import Tab ist leer oder konnte nicht gelesen werden!")

    # --- 3. SCHRITT: SPEICHERN & TELEGRAM ---
    total_value = round(total_value, 2)
    repo.save_history(total_value) 
    repo.save_watchlist(df_wl)
    
    print(f"📤 Sende Bericht an Telegram... (Depotwert: {total_value} €)")
    try:
        df_wl['Score'] = pd.to_numeric(df_wl['Score'], errors='coerce').fillna(0)
        send_summary(df_wl.nlargest(5, 'Score'), total_value) 
    except Exception as e:
        print(f"❌ Telegram-Fehler: {e}")

    print(f"🏁 Update abgeschlossen. Realwert: {total_value:.2f} €")

if __name__ == "__main__":
    run_scanner()