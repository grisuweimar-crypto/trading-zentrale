import pandas as pd
import yfinance as yf
from cloud.repository import TradingRepository
from market.yahoo import get_ticker_symbol, get_price_data
from market.scoring import calculate_total_score
from market.forex import get_usd_eur_rate, convert_to_eur 
import config
from alerts.telegram import send_summary

def run_scanner():
    print("🚀 TRADING SCANNER V31 - FINAL FIX")
    repo = TradingRepository()
    df_wl = repo.load_watchlist()
    fx_rate = get_usd_eur_rate()

    # 1. WATCHLIST SCAN (Unverändert, da stabil)
    for idx, row in df_wl.iterrows():
        symbol = row.get('Ticker') or get_ticker_symbol(row)
        hist = get_price_data(symbol)
        if hist is not None:
            price = float(hist['Close'].iloc[-1])
            ticker_obj = yf.Ticker(symbol)
            if ticker_obj.info.get('currency') == 'USD': price *= fx_rate
            df_wl.at[idx, 'Akt. Kurs [€]'] = price
            df_wl.at[idx, 'Score'] = calculate_total_score(df_wl.loc[idx])

    # 2. PORTFOLIO: DER EISERNE BESEN
    print("📊 Berechne Portfolio mit absolutem Realitäts-Filter...")
    total_value = 0.0

    def force_real_value(raw_wert, kaufwert_raw):
        """Korrektur basierend auf der Ziffernfolge."""
        def to_num(x):
            s = "".join(c for c in str(x) if c.isdigit() or c in '.-,')
            s = s.replace(',', '.')
            try: return float(s)
            except: return 0.0

        val = to_num(raw_wert)
        kauf = to_num(kaufwert_raw)
        
        if val <= 0: return 0.0
        
        # Logik: Wenn der Wert durch Google-Sheets-Fehler (Punkt weg) 
        # mehr als 10x so hoch ist wie der Kaufwert, korrigieren wir.
        # Beispiel: Wert 6811 vs Kaufwert 58.92 -> 68.11
        while val > kauf * 5 and val > 500: # 500€ als Kappungsgrenze für deine Positionen
            val /= 10.0
        
        # Spezialfall: Wenn val viel zu klein wurde (0.68 statt 68)
        while val < kauf * 0.1 and val < 5:
            val *= 10.0
            
        return val

    # Berechnung
    for tab_func in [repo.load_import_aktien, repo.load_import_krypto]:
        df = tab_func()
        if not df.empty:
            for _, row in df.iterrows():
                # Wir vertrauen der Spalte 'Wert' wieder, aber mit der 10x-Sperre
                clean_val = force_real_value(row.get('Wert'), row.get('Kaufwert'))
                total_value += clean_val
                print(f"🔹 {row.get('Name')}: {clean_val:.2f} €")

    total_value = round(total_value, 2)

    # 3. FINALE
    repo.save_history(total_value) 
    repo.save_watchlist(df_wl)
    print(f"📤 Sende Bericht... Realwert: {total_value} €")
    send_summary(df_wl.nlargest(5, 'Score'), total_value) 
    print(f"🏁 Update abgeschlossen. Depot: {total_value:.2f} €")

if __name__ == "__main__":
    run_scanner()