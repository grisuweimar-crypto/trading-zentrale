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

    # START DER WATCHLIST-SCHLEIFE
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
            
            # Das Finale Scoring
            df_wl.at[idx, 'Score'] = calculate_total_score(df_wl.loc[idx])
            
            print(f"✅ {row.get('Name', 'Aktie')} ({symbol}) bewertet.")
    
    # <--- HIER IST DER WATCHLIST-SCAN ZU ENDE! --->
    # Alles was jetzt kommt, passiert nur EINMAL nach dem Scan.

    # --- 2. SCHRITT: PORTFOLIO-UPDATE ---
    print("📊 Berechne Portfolio-Performance...")
    df_pf = repo.load_portfolio()
    
    total_value = 0.0
    for idx, row in df_pf.iterrows():
        symbol = row.get('Symbol')
        # Sicherstellen, dass Anzahl eine saubere Zahl ist
        anzahl = str(row.get('Anzahl')).replace(',', '.')
        anzahl = pd.to_numeric(anzahl, errors='coerce') or 0.0
        
        current_price = 0.0
        if pd.notna(symbol) and symbol != "":
            p_data = get_price_data(symbol)
            if p_data is not None:
                current_price = float(p_data['Close'].iloc[-1])
                df_pf.at[idx, 'Akt. Kurs [€]'] = current_price
        
        # Fallback & Sanity Check: Wenn der Kurs aus dem Sheet kommt
        if current_price == 0.0:
            raw_val = str(row.get('Akt. Kurs [€]')).replace('.', '').replace(',', '.')
            current_price = pd.to_numeric(raw_val, errors='coerce') or 0.0
            
            # Korrektur für "verschluckte" Kommas (z.B. 13654 -> 13.65)
            # Wenn der Preis über 5000 ist und es nicht Bitcoin/Ethereum ist
            if current_price > 5000 and "BTC" not in str(symbol) and "ETH" not in str(symbol):
                current_price = current_price / 1000 # Verschiebung um 3 Stellen
        
        total_value += (anzahl * current_price)

    # --- 3. SCHRITT: HISTORIE & UPLOAD ---
    # Hier runden wir auf 2 Nachkommastellen
    total_value = round(total_value, 2)
    repo.save_history(total_value) 
    repo.save_watchlist(df_wl)
    repo.save_portfolio(df_pf)
    
    # --- 4. SCHRITT: TELEGRAM ---
    print("📤 Sende Bericht an Telegram...")
    try:
        df_wl['Score'] = pd.to_numeric(df_wl['Score'], errors='coerce').fillna(0)
        top_5 = df_wl.nlargest(5, 'Score')
        send_summary(top_5, total_value) 
    except Exception as e:
        print(f"❌ Fehler beim Telegram-Aufruf: {e}")

    print(f"🏁 Cloud-Update abgeschlossen. Realer Depotwert: {total_value:.2f} €")

if __name__ == "__main__":
    run_scanner()