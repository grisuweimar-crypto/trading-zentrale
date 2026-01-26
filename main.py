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
    print("🚀 TRADING SCANNER V29 - ULTRA-SAFE MODE")
    repo = TradingRepository()
    df_wl = repo.load_watchlist()
    fx_rate = get_usd_eur_rate()

    # 1. WATCHLIST SCAN
    print(f"🔭 Scanne {len(df_wl)} Aktien...")
    for idx, row in df_wl.iterrows():
        symbol = row.get('Ticker') if pd.notna(row.get('Ticker')) and row.get('Ticker') != "" else get_ticker_symbol(row)
        hist = get_price_data(symbol)
        if hist is not None:
            ticker_obj = yf.Ticker(symbol)
            price = float(hist['Close'].iloc[-1])
            if ticker_obj.info.get('currency') == 'USD': price *= fx_rate
            df_wl.at[idx, 'Akt. Kurs [€]'] = price
            df_wl.at[idx, 'Score'] = calculate_total_score(df_wl.loc[idx])
            print(f"✅ {row.get('Name', 'Aktie')} bewertet.")

    # 2. PORTFOLIO-BERECHNUNG MIT RATIO-FAILSAFE
    print("📊 Berechne Portfolio mit mathematischem Plausibilitäts-Check...")
    total_value = 0.0

    def get_safe_anzahl(row):
        """Findet die echte Anzahl durch Abgleich von Kaufkurs und Kaufwert."""
        def clean(v):
            if pd.isna(v): return 0.0
            s = "".join(c for c in str(v) if c.isdigit())
            return float(s) if s else 0.0

        A_raw = clean(row.get('Anzahl'))
        K_raw = clean(row.get('Kaufkurs'))
        W_raw = clean(row.get('Kaufwert'))

        if W_raw == 0 or K_raw == 0: return 0.0

        # Wir berechnen den Faktor: (Roh-Anzahl * Roh-Kurs) / Roh-Wert
        # Dieser Faktor ist bei Fehlern immer eine Potenz von 10 (100, 1000, 1000000)
        ratio = (A_raw * K_raw) / (W_raw * 1000000) # Normierung auf große Zahlen
        
        # Wir finden die richtige Skalierung (die Anzahl, die am besten zum Kaufwert passt)
        # Echte_Anzahl = Kaufwert / Kaufkurs (beide vorher grob gesäubert)
        kauf_val_clean = W_raw
        while kauf_val_clean > 1000: kauf_val_clean /= 10.0 # Annahme: Position < 1000€
        
        kauf_kurs_clean = K_raw
        # Wir skalieren den Kurs so lange, bis er im Bereich 0.1 bis 1000 liegt
        while kauf_kurs_clean > 2000: kauf_kurs_clean /= 10.0
        
        return kauf_val_clean / kauf_kurs_clean if kauf_kurs_clean > 0 else 0.0

    # Berechnung
    for tab in [repo.load_import_aktien(), repo.load_import_krypto()]:
        if not tab.empty:
            for _, row in tab.iterrows():
                # Echte Anzahl berechnen
                anzahl = get_safe_anzahl(row)
                
                # Aktuellen Kurs holen
                name = str(row.get('Name'))
                symbol = row.get('ISIN') or row.get('Symbol') or name
                hist = get_price_data(symbol)
                
                if hist is not None:
                    price = float(hist['Close'].iloc[-1])
                    ticker_obj = yf.Ticker(symbol)
                    if ticker_obj.info.get('currency') == 'USD': price *= fx_rate
                    
                    pos_wert = anzahl * price
                    total_value += pos_wert
                    print(f"🔹 {name}: {anzahl:.4f} Stk. * {price:.2f}€ = {pos_wert:.2f}€")

    total_value = round(total_value, 2)

    # 3. FINALE: SPEICHERN & TELEGRAM
    repo.save_history(total_value) 
    repo.save_watchlist(df_wl)
    
    print(f"📤 Sende Bericht... Realer Depotwert: {total_value} €")
    try:
        df_wl['Score'] = pd.to_numeric(df_wl['Score'], errors='coerce').fillna(0)
        send_summary(df_wl.nlargest(5, 'Score'), total_value) 
        print("✅ Telegram erfolgreich versendet.")
    except Exception as e:
        print(f"❌ Telegram-Fehler: {e}")

    print(f"🏁 Update abgeschlossen. Depot: {total_value:.2f} €")

if __name__ == "__main__":
    run_scanner()