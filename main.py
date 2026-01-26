import pandas as pd
import yfinance as yf
from cloud.repository import TradingRepository
from market.yahoo import get_price_data
from market.scoring import calculate_total_score
from market.forex import get_usd_eur_rate
from alerts.telegram import send_summary

def run_scanner():
    print("🚀 TRADING SCANNER V36 - UNIVERSAL FORMAT FIX")
    repo = TradingRepository()
    df_wl = repo.load_watchlist()
    fx_rate = get_usd_eur_rate()

    # 1. WATCHLIST SCAN (Preise direkt von Yahoo - immer sicher)
    for idx, row in df_wl.iterrows():
        symbol = row.get('Ticker')
        hist = get_price_data(symbol)
        if hist is not None:
            price = float(hist['Close'].iloc[-1])
            if "US" in str(row.get('ISIN', '')): price *= fx_rate
            df_wl.at[idx, 'Akt. Kurs [€]'] = price
            df_wl.at[idx, 'Score'] = calculate_total_score(df_wl.loc[idx])

    # 2. PORTFOLIO: DIE MATHE-GEWALT
    print("📊 Berechne Portfolio...")
    total_value = 0.0

    def clean_to_real_value(val_str, ref_str):
        """Entfernt allen Format-Müll und gleicht Ziffern mit dem Kaufwert ab."""
        def get_digits(x):
            return "".join(c for c in str(x) if c.isdigit())
        
        digits_val = get_digits(val_str)
        digits_ref = get_digits(ref_str)
        
        if not digits_val or not digits_ref: return 0.0
        
        num_val = float(digits_val)
        num_ref = float(digits_ref)
        
        # Wir skalieren den Wert so lange, bis er im Bereich 
        # von 10% bis 1000% des Kaufwerts liegt.
        # Beispiel: 6811 (Wert) vs 5892 (Kaufwert) -> passt.
        # Beispiel: 681100 (Wert) vs 5892 (Kaufwert) -> 6811.00
        while num_val > num_ref * 10 and num_val > 500: # 500€ Cap für Plausibilität
            num_val /= 10.0
            
        # Falls durch falsche Skalierung zu klein:
        while num_val < num_ref * 0.01 and num_val < 1:
            num_val *= 10.0
            
        return num_val / 100.0 if num_ref > 100 else num_val # Meist 2 Dezimalstellen

    # Berechnung
    for tab_func in [repo.load_import_aktien, repo.load_import_krypto]:
        df = tab_func()
        if df.empty: continue
        for _, row in df.iterrows():
            # Wir erzwingen die Logik: Wert muss zum Kaufwert passen!
            val = clean_to_real_value(row.get('Wert'), row.get('Kaufwert'))
            total_value += val
            print(f"🔹 {row.get('Name')}: {val:.2f} €")

    total_value = round(total_value, 2)
    repo.save_history(total_value)
    send_summary(df_wl.nlargest(5, 'Score'), total_value)
    print(f"🏁 Fertig! Depotwert: {total_value} €")

if __name__ == "__main__":
    run_scanner()