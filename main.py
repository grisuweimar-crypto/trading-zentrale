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
    print("🚀 TRADING SCANNER V27 - CLOUD SYNC AKTIVIERT")
    repo = TradingRepository()
    df_wl = repo.load_watchlist()
    
    # Spalten sicherstellen
    expected_cols = ['Akt. Kurs [€]', 'PE', 'DivRendite', 'Wachstum', 'Marge', 
                     'Upside', 'Beta', 'AnalystRec', 'MC_Chance', 'Elliott_Signal', 'Score']
    for col in expected_cols:
        if col not in df_wl.columns:
            df_wl[col] = 0.0

    fx_rate = get_usd_eur_rate()
    print(f"💱 Kurs: 1 USD = {fx_rate:.4f} EUR")

    # 1. Watchlist Scan
    for idx, row in df_wl.iterrows():
        symbol = row.get('Ticker') if pd.notna(row.get('Ticker')) and row.get('Ticker') != "" else get_ticker_symbol(row)
        hist = get_price_data(symbol)
        if hist is not None:
            ticker_obj = yf.Ticker(symbol)
            raw_price = float(hist['Close'].iloc[-1])
            currency = ticker_obj.info.get('currency', 'EUR')
            price_eur = convert_to_eur(raw_price, fx_rate) if currency == 'USD' else raw_price

            df_wl.at[idx, 'Akt. Kurs [€]'] = price_eur
            df_wl.at[idx, 'MC_Chance'] = float(calculate_probability(hist))
            df_wl.at[idx, 'Elliott_Signal'] = detect_elliott_wave(hist)
            
            fund_data = get_fundamental_data(ticker_obj)
            for key, val in fund_data.items():
                if key in df_wl.columns: df_wl.at[idx, key] = val
            
            df_wl.at[idx, 'Score'] = calculate_total_score(df_wl.loc[idx])
            print(f"✅ {row.get('Name', 'Aktie')} ({symbol}) bewertet.")

    # --- 2. SCHRITT: PORTFOLIO-LOGIK (Radikale Reinigung) ---
    print("📊 Berechne Portfolio-Performance...")
    df_pf = repo.load_portfolio()
    
    total_value = 0.0
    for idx, row in df_pf.iterrows():
        # Wir lesen ALLES als String ein, um volle Kontrolle zu haben
        anzahl_str = str(row.get('Anzahl', '0')).replace('.', '').replace(',', '.')
        kurs_str = str(row.get('Akt. Kurs [€]', '0')).replace('.', '').replace(',', '.')
        
        # Umwandlung in echte Python-Zahlen
        try:
            anzahl = float(anzahl_str)
            current_price = float(kurs_str)
            
            # Sicherheits-Check: Wenn der Kurs durch falsche Punkte zu hoch ist
            # Beispiel: 1.365,40 wurde zu 136540.0 -> wir rücken das Komma zurecht.
            if current_price > 10000 and "BTC" not in str(row.get('Symbol')):
                current_price = current_price / 100 
                
            total_value += (anzahl * current_price)
        except:
            continue

    # --- 3. SCHRITT: SPEICHERN & TELEGRAM ---
    total_value = round(total_value, 2)
    repo.save_history(total_value) 
    
    print(f"📤 Sende Bericht... Realwert: {total_value} €")
    # Hier rufen wir Telegram auf
    try:
        df_wl['Score'] = pd.to_numeric(df_wl['Score'], errors='coerce').fillna(0)
        send_summary(df_wl.nlargest(5, 'Score'), total_value) 
    except Exception as e:
        print(f"❌ Telegram-Fehler: {e}")

    print(f"🏁 Update fertig. Depot: {total_value:.2f} €")

if __name__ == "__main__":
    run_scanner()