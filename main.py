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

    # 2. PORTFOLIO-BERECHNUNG (Die Perplexity Plausibilitäts-Sicherung)
    print("📊 Berechne Portfolio mit Live-Kurs Plausibilitäts-Check...")
    total_value = 0.0

    def get_plausible_value(sheet_val, symbol, anzahl, fx_rate):
        # Rohwert aus Sheet säubern
        try:
            s = str(sheet_val).replace('.', '').replace(',', '.')
            val_sheet = float(s)
        except: return 0.0

        # Erwartungswert berechnen (Anzahl * Live-Kurs)
        p_data = get_price_data(symbol)
        if p_data is None: return val_sheet # Fallback
        
        live_price = float(p_data['Close'].iloc[-1])
        # Währungskorrektur für US-Aktien im Depot
        ticker_obj = yf.Ticker(symbol)
        if ticker_obj.info.get('currency') == 'USD':
            live_price *= fx_rate
        
        expected = anzahl * live_price
        
        # PLAUSIBILITÄTS-CHECK:
        # Wenn der Sheet-Wert ca. 100x höher ist als erwartet -> durch 100 teilen
        if val_sheet > expected * 50: # Faktor 50 als Sicherheitspuffer
            print(f"⚠️ Korrektur: {symbol} Wert von {val_sheet} auf {val_sheet/100} gesenkt (100x Fehler).")
            return val_sheet / 100
        return val_sheet

    # Summe Aktien
    df_a = repo.load_import_aktien()
    if not df_a.empty:
        for _, row in df_a.iterrows():
            sym = row.get('ISIN') or row.get('Name') # ISIN funktioniert oft direkt bei Yahoo
            anz_str = str(row.get('Anzahl', '0')).replace(',', '.')
            anz = float(pd.to_numeric(anz_str, errors='coerce') or 0.0)
            total_value += get_plausible_value(row.get('Wert'), sym, anz, fx_rate)

    # Summe Krypto
    df_k = repo.load_import_krypto()
    if not df_k.empty:
        for _, row in df_k.iterrows():
            sym = row.get('Symbol') # z.B. BTC-EUR
            anz_str = str(row.get('Anzahl', '0')).replace(',', '.')
            anz = float(pd.to_numeric(anz_str, errors='coerce') or 0.0)
            total_value += get_plausible_value(row.get('Wert'), sym, anz, 1.0)

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