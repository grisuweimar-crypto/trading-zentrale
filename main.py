import pandas as pd
import yfinance as yf
from cloud.repository import TradingRepository
from market.yahoo import get_ticker_symbol, get_price_data
from market.montecarlo import calculate_probability
from market.fundamental import get_fundamental_data
from market.scoring import calculate_total_score
from market.elliott import detect_elliott_wave
from market.forex import get_usd_eur_rate, convert_to_eur # Wichtig!
import config
from alerts.telegram import send_summary

def run_scanner():
    print("🚀 TRADING SCANNER V27 - CLOUD SYNC AKTIVIERT")
    repo = TradingRepository()
    df_wl = repo.load_watchlist()
    
    # Sicherstellen, dass alle nötigen Spalten existieren
    expected_cols = ['Akt. Kurs [€]', 'PE', 'DivRendite', 'Wachstum', 'Marge', 
                     'Upside', 'Beta', 'AnalystRec', 'MC_Chance', 'Elliott_Signal', 'Score']
    for col in expected_cols:
        if col not in df_wl.columns:
            df_wl[col] = 0.0

    # --- 1. SCHRITT: WECHSELKURS LADEN ---
    fx_rate = get_usd_eur_rate()
    print(f"💱 Aktueller Wechselkurs USD/EUR: {fx_rate:.4f}")
    print(f"🔭 Scanne {len(df_wl)} Aktien...")

    for idx, row in df_wl.iterrows():
        # --- 2. SCHRITT: TICKER LOGIK ---
        symbol = row.get('Ticker') if pd.notna(row.get('Ticker')) and row.get('Ticker') != "" else get_ticker_symbol(row)
        
        hist = get_price_data(symbol)
        
        if hist is not None:
            ticker_obj = yf.Ticker(symbol)
            raw_price = float(hist['Close'].iloc[-1])
            
            # Währung abfragen
            try:
                currency = ticker_obj.info.get('currency', 'EUR')
            except:
                currency = 'EUR'
                
            # --- 3. SCHRITT: DIE KONVERTIERUNG ---
            if currency == 'USD':
                price_eur = convert_to_eur(raw_price, fx_rate)
                print(f"🔄 {symbol}: {raw_price:.2f} USD -> {price_eur:.2f} EUR")
            else:
                price_eur = raw_price

            # Daten ins DataFrame schreiben
            df_wl.at[idx, 'Akt. Kurs [€]'] = price_eur
            df_wl.at[idx, 'MC_Chance'] = float(calculate_probability(hist))
            df_wl.at[idx, 'Elliott_Signal'] = detect_elliott_wave(hist)
            
            # Fundamentaldaten
            fund_data = get_fundamental_data(ticker_obj)
            for key, val in fund_data.items():
                if key in df_wl.columns:
                    df_wl.at[idx, key] = val
            
            # 3. Das Finale Scoring (DEIN SYSTEM)
            df_wl.at[idx, 'Score'] = calculate_total_score(df_wl.loc[idx])
            
            print(f"✅ {row.get('Name', 'Aktie')} ({symbol}) bewertet.")

    # 4. Der Moment der Wahrheit: Upload & Telegram
    print("💾 Synchronisiere Daten mit Google Sheets...")
    repo.save_watchlist(df_wl)
    
    print("📤 Sende Top 5 Ergebnisse an Telegram...")
    try:
        # NEU: Sicherstellen, dass 'Score' eine Zahl ist (behebt den Dtype-Fehler)
        df_wl['Score'] = pd.to_numeric(df_wl['Score'], errors='coerce').fillna(0)
        
        # Jetzt die Top 5 filtern
        top_5 = df_wl.nlargest(5, 'Score')
        send_summary(top_5)
    except Exception as e:
        print(f"❌ Fehler beim Telegram-Aufruf: {e}")

    print("🏁 Cloud-Update abgeschlossen. Dein Dashboard ist nun aktuell!")

if __name__ == "__main__":
    run_scanner()