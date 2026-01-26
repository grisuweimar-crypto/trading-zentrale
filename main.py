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
    print("🚀 TRADING SCANNER V28 - LIVE-VALUATION MODE")
    repo = TradingRepository()
    df_wl = repo.load_watchlist()
    fx_rate = get_usd_eur_rate()

    # 1. TICKER-MAPPING ERSTELLEN
    # Wir erstellen ein Wörterbuch Name -> Ticker aus der Watchlist
    ticker_map = {}
    for _, row in df_wl.iterrows():
        if pd.notna(row.get('Name')) and pd.notna(row.get('Ticker')):
            ticker_map[str(row['Name']).strip()] = str(row['Ticker']).strip()

    # 2. WATCHLIST SCAN & LIVE-PREISE SAMMELN
    live_prices = {}
    for idx, row in df_wl.iterrows():
        symbol = row.get('Ticker') if pd.notna(row.get('Ticker')) and row.get('Ticker') != "" else get_ticker_symbol(row)
        hist = get_price_data(symbol)
        if hist is not None:
            price = float(hist['Close'].iloc[-1])
            ticker_obj = yf.Ticker(symbol)
            if ticker_obj.info.get('currency') == 'USD':
                price *= fx_rate
            
            df_wl.at[idx, 'Akt. Kurs [€]'] = price
            live_prices[symbol] = price
            # Auch unter dem Namen speichern für den Import-Abgleich
            live_prices[str(row.get('Name')).strip()] = price
            
            df_wl.at[idx, 'Score'] = calculate_total_score(df_wl.loc[idx])
            print(f"✅ {row.get('Name', 'Aktie')} bewertet.")

    # 3. DEPOT-BERECHNUNG (Rein auf Live-Kursen basierend!)
    print("📊 Berechne Portfolio basierend auf Live-Börsenkursen...")
    total_value = 0.0
    total_invest = 0.0

    def get_live_valuation(df, fx_rate, is_krypto=False):
        current_sum = 0.0
        invest_sum = 0.0
        if df.empty: return 0.0, 0.0
        
        for _, row in df.iterrows():
            # 1. Anzahl & Kaufwert sicher auslesen
            anz_str = str(row.get('Anzahl', '0')).replace(',', '.')
            anzahl = pd.to_numeric(anz_str, errors='coerce') or 0.0
            
            kauf_str = str(row.get('Kaufwert', '0')).replace('.', '').replace(',', '.')
            kaufwert = pd.to_numeric(kauf_str, errors='coerce') or 0.0
            
            # 2. Live-Kurs holen
            lookup = row.get('ISIN') if not is_krypto else row.get('Symbol')
            if not lookup or pd.isna(lookup): lookup = row.get('Name')
                
            hist = get_price_data(lookup)
            if hist is not None:
                price_eur = float(hist['Close'].iloc[-1])
                ticker_obj = yf.Ticker(lookup)
                if ticker_obj.info.get('currency') == 'USD':
                    price_eur *= fx_rate
                
                # RECHNUNG: Stück * Kurs (Das kann nicht falsch formatiert werden!)
                pos_wert = anzahl * price_eur
                current_sum += pos_wert
                invest_sum += kaufwert
                print(f"🔹 {row.get('Name')}: {anzahl} Stk. * {price_eur:.2f}€ = {pos_wert:.2f}€")
            else:
                current_sum += kaufwert # Not-Fallback
                invest_sum += kaufwert

        return current_sum, invest_sum

    # Ausführung
    val_aktien, inv_aktien = get_live_valuation(repo.load_import_aktien(), fx_rate, is_krypto=False)
    val_krypto, inv_krypto = get_live_valuation(repo.load_import_krypto(), 1.0, is_krypto=True)

    total_value = round(val_aktien + val_krypto, 2)
    total_invest = round(inv_aktien + inv_krypto, 2)
    total_profit_eur = round(total_value - total_invest, 2)
    total_profit_pct = round((total_profit_eur / total_invest * 100), 2) if total_invest > 0 else 0

    print(f"💰 Depotwert: {total_value}€ | Invest: {total_invest}€")
    # --- 4. FINALE: SPEICHERN & TELEGRAM ---
    # Speichert den neuen Wert in den Reiter 'Historie'
    repo.save_history(total_value) 
    
    # Speichert die aktualisierten Kurse/Scores in der 'Watchlist'
    repo.save_watchlist(df_wl)
    
    print(f"📤 Sende Bericht an Telegram... Realer Depotwert: {total_value} €")
    
    # Schickt die Top 5 Aktien und den Depotwert an dein Handy
    try:
        # Falls die Spalte Score NaN ist, auf 0 setzen
        df_wl['Score'] = pd.to_numeric(df_wl['Score'], errors='coerce').fillna(0)
        send_summary(df_wl.nlargest(5, 'Score'), total_value) 
        print("✅ Telegram erfolgreich versendet.")
    except Exception as e:
        print(f"❌ Telegram-Fehler: {e}")

    print(f"🏁 Update abgeschlossen. Depot: {total_value:.2f} €")

if __name__ == "__main__":
    run_scanner()