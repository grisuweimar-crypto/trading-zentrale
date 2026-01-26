import pandas as pd
import yfinance as yf
from cloud.repository import TradingRepository
from market.yahoo import get_ticker_symbol, get_price_data
from market.scoring import calculate_total_score
from market.elliott import detect_elliott_wave
from market.forex import get_usd_eur_rate
from alerts.telegram import send_summary

def run_scanner():
    print("🚀 TRADING SCANNER - MARKET ANALYSIS ONLY")
    repo = TradingRepository()
    df_wl = repo.load_watchlist()
    fx_rate = get_usd_eur_rate()

    # 1. REINER MARKT-SCAN (Verlässt sich NICHT auf deine Depot-Daten)
    print(f"🔭 Scanne {len(df_wl)} Aktien auf Elliott-Wellen-Signale...")
    for idx, row in df_wl.iterrows():
        symbol = row.get('Ticker') or get_ticker_symbol(row)
        hist = get_price_data(symbol)
        
        if hist is not None:
            ticker_obj = yf.Ticker(symbol)
            # Preis in EUR umrechnen
            price = float(hist['Close'].iloc[-1])
            if ticker_obj.info.get('currency') == 'USD':
                price *= fx_rate
            
            # Signale berechnen
            df_wl.at[idx, 'Akt. Kurs [€]'] = price
            df_wl.at[idx, 'Elliott_Signal'] = detect_elliott_wave(hist)
            df_wl.at[idx, 'Score'] = calculate_total_score(df_wl.loc[idx])
            
            print(f"✅ {row.get('Name', 'Aktie')} analysiert.")

    # 2. SPEICHERN & SENDEN (Nur die Watchlist)
    repo.save_watchlist(df_wl)
    
    print("📤 Sende Top-Signale an Telegram...")
    try:
        # Wir filtern die Top 5 nach deinem Scoring-System
        df_wl['Score'] = pd.to_numeric(df_wl['Score'], errors='coerce').fillna(0)
        top_picks = df_wl.nlargest(5, 'Score')
        
        # Wir senden 0.0 als Depotwert, da wir ihn nicht mehr berechnen
        send_summary(top_picks, 0.0) 
        print("✅ Bericht erfolgreich versendet.")
    except Exception as e:
        print(f"❌ Fehler: {e}")

    print("🏁 Markt-Update abgeschlossen.")

if __name__ == "__main__":
    run_scanner()