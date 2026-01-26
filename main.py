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

    # --- 2. SCHRITT: PORTFOLIO AUS ZWEI REITERN ---
    print("📊 Berechne Portfolio aus getrennten Import-Tabs...")
    
    total_value = 0.0
    
    # 1. Aktien berechnen
    df_a = repo.load_import_aktien()
    if not df_a.empty:
        for val in df_a['Wert']:
            clean = str(val).replace('.', '').replace(',', '.')
            num = pd.to_numeric(clean, errors='coerce')
            if pd.notna(num): total_value += float(num)
        print(f"✅ Aktien-Wert addiert.")

    # 2. Krypto berechnen
    df_k = repo.load_import_krypto()
    if not df_k.empty:
        for val in df_k['Wert']:
            clean = str(val).replace('.', '').replace(',', '.')
            num = pd.to_numeric(clean, errors='coerce')
            if pd.notna(num): total_value += float(num)
        print(f"✅ Krypto-Wert addiert.")

    total_value = round(total_value, 2)
    # --- WEITER MIT SPEICHERN & TELEGRAM ---

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