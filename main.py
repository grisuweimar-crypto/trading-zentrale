import pandas as pd
import yfinance as yf
import config
from cloud.repository import TradingRepository
from market.yahoo import get_ticker_symbol, get_price_data
from market.scoring import calculate_total_score
from market.forex import get_usd_eur_rate
from alerts.telegram import send_summary

def run_scanner():
    print("🚀 TRADING SCANNER V33 - PROFESSIONAL MODE (USA LOCALE)")
    repo = TradingRepository()
    df_wl = repo.load_watchlist()
    fx_rate = get_usd_eur_rate()

    # 1. WATCHLIST SCAN
    print(f"🔭 Analysiere {len(df_wl)} Aktien via Elliott-Wave...")
    for idx, row in df_wl.iterrows():
        symbol = row.get('Ticker') or get_ticker_symbol(row)
        hist = get_price_data(symbol)
        if hist is not None:
            ticker_obj = yf.Ticker(symbol)
            price = float(hist['Close'].iloc[-1])
            # Währungskorrektur
            if ticker_obj.info.get('currency') == 'USD':
                price *= fx_rate
            
            df_wl.at[idx, 'Akt. Kurs [€]'] = price
            # Der Score berechnet jetzt alles (Elliott, Monte Carlo, Fundamentaldaten)
            df_wl.at[idx, 'Score'] = calculate_total_score(df_wl.loc[idx])
            print(f"✅ {row.get('Name', 'Aktie')} bewertet.")

    # 2. PORTFOLIO BERECHNUNG (Jetzt ganz einfach!)
    print("📊 Berechne Portfolio-Summe...")
    total_value = 0.0

    # Wir laden die Daten. Da das Sheet auf USA steht, 
    # wandelt gspread/pandas diese automatisch in korrekte Zahlen um.
    for tab in [repo.load_import_aktien(), repo.load_import_krypto()]:
        if not tab.empty:
            # Wir nehmen einfach die Summe der Spalte 'Wert'
            # (errors='coerce' macht ungültige Einträge zu 0)
            tab_sum = pd.to_numeric(tab['Wert'], errors='coerce').sum()
            total_value += tab_sum

    total_value = round(total_value, 2)

    # 3. SPEICHERN & TELEGRAM
    repo.save_history(total_value)
    repo.save_watchlist(df_wl)
    
    print(f"📤 Sende Bericht... Depotwert: {total_value} €")
    try:
        df_wl['Score'] = pd.to_numeric(df_wl['Score'], errors='coerce').fillna(0)
        send_summary(df_wl.nlargest(5, 'Score'), total_value)
        print("✅ Telegram-Nachricht verschickt.")
    except Exception as e:
        print(f"❌ Telegram-Fehler: {e}")

    print(f"🏁 Update abgeschlossen. Realwert: {total_value:.2f} €")

if __name__ == "__main__":
    run_scanner()