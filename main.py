import pandas as pd
import yfinance as yf
from cloud.repository import TradingRepository
from market.yahoo import get_ticker_symbol, get_price_data
from market.montecarlo import calculate_probability
from market.fundamental import get_fundamental_data
from market.scoring import calculate_total_score
from market.elliott import detect_elliott_wave
import config
from alerts.telegram import send_summary

def run_scanner():
    print("🚀 TRADING SCANNER V27 - CLOUD SYNC AKTIVIERT")
    repo = TradingRepository()
    df_wl = repo.load_watchlist()
    
    # Sicherstellen, dass alle nötigen Spalten existieren (verhindert Fehler)
    expected_cols = ['Akt. Kurs [€]', 'PE', 'DivRendite', 'Wachstum', 'Marge', 
                     'Upside', 'Beta', 'AnalystRec', 'MC_Chance', 'Elliott_Signal', 'Score']
    for col in expected_cols:
        if col not in df_wl.columns:
            df_wl[col] = 0.0

    print(f"🔭 Scanne {len(df_wl)} Aktien...")

    # --- 1. SCHRITT: WECHSELKURS LADEN (VOR DER SCHLEIFE) ---
fx_rate = get_usd_eur_rate()
print(f"💱 Aktueller Wechselkurs USD/EUR: {fx_rate:.4f}")

print(f"🔭 Scanne {len(df_wl)} Aktien...")

for idx, row in df_wl.iterrows():
    # --- 2. SCHRITT: TICKER LOGIK (DIE STELLE!) ---
    # Wir prüfen erst die neue Spalte 'Ticker', sonst ISIN
    symbol = row.get('Ticker') if pd.notna(row.get('Ticker')) and row.get('Ticker') != "" else get_ticker_symbol(row)
    
    hist = get_price_data(symbol)
    
    if hist is not None:
        ticker_obj = yf.Ticker(symbol)
        
        # Rohpreis von Yahoo (kann USD oder EUR sein)
        raw_price = float(hist['Close'].iloc[-1])
        
        # Währung abfragen
        try:
            currency = ticker_obj.info.get('currency', 'EUR')
        except:
            currency = 'EUR' # Fallback
            
        # --- 3. SCHRITT: DIE KONVERTIERUNG ---
        if currency == 'USD':
            price_eur = convert_to_eur(raw_price, fx_rate)
            print(f"🔄 {symbol}: {raw_price:.2f} USD -> {price_eur:.2f} EUR")
        else:
            price_eur = raw_price

        # Jetzt den bereinigten Euro-Preis ins DataFrame schreiben
        df_wl.at[idx, 'Akt. Kurs [€]'] = price_eur
        
        # --- AB HIER GEHT DEIN BESTEHENDES SCORING WEITER ---
        df_wl.at[idx, 'MC_Chance'] = float(calculate_probability(hist))
        # ... usw.
            
            # 1. Kurs & MC Chance
            df_wl.at[idx, 'Akt. Kurs [€]'] = float(hist['Close'].iloc[-1])
            df_wl.at[idx, 'MC_Chance'] = float(calculate_probability(hist))
            
            # 2. Elliott & Fundamentales
            df_wl.at[idx, 'Elliott_Signal'] = detect_elliott_wave(hist)
            fund_data = get_fundamental_data(ticker)
            
            # Alle Kennzahlen in das DataFrame schreiben
            for key, val in fund_data.items():
                df_wl.at[idx, key] = val
            
            # 3. Das Finale Scoring (alle 9 Kategorien fließen ein)
            df_wl.at[idx, 'Score'] = calculate_total_score(df_wl.loc[idx])
            
            print(f"✅ {row.get('Name', 'Aktie')} ({symbol}) bewertet.")

    # 4. Der Moment der Wahrheit: Upload
    print("💾 Synchronisiere Daten mit Google Sheets...")
    repo.save_watchlist(df_wl)
    
    # --- DIESE ZEILEN MÜSSEN EINGERÜCKT SEIN (8 Leerzeichen) ---
    print("📤 Sende Top 5 Ergebnisse an Telegram...")
    try:
        from alerts.telegram import send_summary
        top_5 = df_wl.nlargest(5, 'Score')
        send_summary(top_5)
    except Exception as e:
        print(f"❌ Fehler beim Telegram-Aufruf: {e}")
    # ---------------------------------------------------------

    print("🏁 Cloud-Update abgeschlossen. Dein Dashboard ist nun aktuell!")
    
    # Ganz am Ende der main.py
if __name__ == "__main__":
    # ... dein bisheriger Scan-Code ...
    
    # Der entscheidende Aufruf:
    if 'top_5' in locals() or 'top_5' in globals():
        from alerts.telegram_bot import send_summary
        send_summary(top_5) # Hier wird die Nachricht abgeschickt!
    else:
        print("⚠️ Keine Top 5 Liste zum Versenden gefunden.")

if __name__ == "__main__":
    run_scanner()