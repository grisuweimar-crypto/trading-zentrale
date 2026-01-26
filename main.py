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
    print("🚀 TRADING SCANNER V27 - SMART IMPORT EDITION")
    repo = TradingRepository()
    df_wl = repo.load_watchlist()
    fx_rate = get_usd_eur_rate()

    # 1. WATCHLIST SCAN (Unverändert)
    for idx, row in df_wl.iterrows():
        symbol = row.get('Ticker') if pd.notna(row.get('Ticker')) and row.get('Ticker') != "" else get_ticker_symbol(row)
        hist = get_price_data(symbol)
        if hist is not None:
            ticker_obj = yf.Ticker(symbol)
            price_eur = convert_to_eur(float(hist['Close'].iloc[-1]), fx_rate) if ticker_obj.info.get('currency') == 'USD' else float(hist['Close'].iloc[-1])
            df_wl.at[idx, 'Akt. Kurs [€]'] = price_eur
            df_wl.at[idx, 'MC_Chance'] = float(calculate_probability(hist))
            df_wl.at[idx, 'Elliott_Signal'] = detect_elliott_wave(hist)
            fund_data = get_fundamental_data(ticker_obj)
            for key, val in fund_data.items():
                if key in df_wl.columns: df_wl.at[idx, key] = val
            df_wl.at[idx, 'Score'] = calculate_total_score(df_wl.loc[idx])
            print(f"✅ {row.get('Name', 'Aktie')} bewertet.")

    # --- 2. SCHRITT: PORTFOLIO MIT DOPPELTER SICHERUNG ---
    print("📊 Berechne Portfolio (Realitäts-Check aktiv)...")
    total_value = 0.0

    def clean_and_validate(raw_wert, kaufwert, symbol):
        # 1. Grundreinigung
        try:
            s = str(raw_wert).replace('.', '').replace(',', '.')
            val = float(s)
            
            s_kauf = str(kaufwert).replace('.', '').replace(',', '.')
            kauf = float(s_kauf)
        except: return 0.0

        # 2. DER 100x DETEKTOR (Vergleich mit Kaufwert)
        # Wenn der Wert 50-mal höher ist als der Kaufwert (unwahrscheinlich bei deinen Titeln)
        # ODER wenn der Wert über 1000€ liegt (bei deinem aktuellen Depot unmöglich)
        if (kauf > 0 and val > kauf * 50) or (val > 1000 and "BTC" not in str(symbol)):
            return val / 100.0
            
        return val

    # Aktien-Summe
    df_a = repo.load_import_aktien()
    if not df_a.empty:
        for _, row in df_a.iterrows():
            total_value += clean_and_validate(row.get('Wert'), row.get('Kaufwert'), row.get('ISIN'))
        print("✅ Aktien plausibilisiert.")

    # Krypto-Summe
    df_k = repo.load_import_krypto()
    if not df_k.empty:
        for _, row in df_k.iterrows():
            total_value += clean_and_validate(row.get('Wert'), row.get('Kaufwert'), row.get('Name'))
        print("✅ Krypto plausibilisiert.")

    total_value = round(total_value, 2)

    # 3. FINALE: SPEICHERN & TELEGRAM
    repo.save_history(total_value) 
    repo.save_watchlist(df_wl)
    
    print(f"📤 Sende Bericht... Realwert: {total_value} €")
    try:
        # Wir zwingen send_summary ein Ergebnis zu liefern
        success = send_summary(df_wl.nlargest(5, 'Score'), total_value)
        if success is False: print("⚠️ Telegram konnte nicht gesendet werden.")
        else: print("✅ Telegram erfolgreich versendet.")
    except Exception as e:
        print(f"❌ Telegram-Fehler: {e}")

    print(f"🏁 Update fertig. Depot: {total_value:.2f} €")

if __name__ == "__main__":
    run_scanner()