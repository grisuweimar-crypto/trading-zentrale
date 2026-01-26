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
    print("🚀 TRADING SCANNER V27 - BROKER-IMPORT MODUS")
    repo = TradingRepository()
    df_wl = repo.load_watchlist()
    
    fx_rate = get_usd_eur_rate()
    print(f"🔭 Scanne {len(df_wl)} Aktien...")

    # 1. WATCHLIST SCAN
    for idx, row in df_wl.iterrows():
        symbol = row.get('Ticker') if pd.notna(row.get('Ticker')) and row.get('Ticker') != "" else get_ticker_symbol(row)
        hist = get_price_data(symbol)
        if hist is not None:
            ticker_obj = yf.Ticker(symbol)
            raw_price = float(hist['Close'].iloc[-1])
            currency = ticker_obj.info.get('currency', 'EUR')
            price_eur = convert_to_eur(raw_price, fx_rate) if currency == 'USD' else raw_price
            df_wl.at[idx, 'Akt. Kurs [€]'] = price_eur
            df_wl.at[idx, 'Score'] = calculate_total_score(df_wl.loc[idx])
            print(f"✅ {row.get('Name', 'Aktie')} bewertet.")

    # 2. PORTFOLIO-BERECHNUNG (Der Fix für die 125.000 €)
    print("📊 Berechne Portfolio aus getrennten Import-Tabs...")
    total_value = 0.0

    def smart_clean(v):
        s = str(v).strip().replace('€', '').replace(' ', '')
        if not s or s.lower() == 'nan': return 0.0
        
        # Logik: Nur wenn Punkt UND Komma da sind, löschen wir den Punkt.
        # Wenn nur ein Punkt da ist (Zero-Stil: 68.11), bleibt er!
        if '.' in s and ',' in s:
            if s.rfind('.') < s.rfind(','): # Format 1.256,96
                s = s.replace('.', '').replace(',', '.')
            else: # Format 1,256.96
                s = s.replace(',', '')
        elif ',' in s:
            s = s.replace(',', '.')
            
        try: return float(s)
        except: return 0.0

    # Aktien & Krypto addieren
    tabs = [repo.load_import_aktien(), repo.load_import_krypto()]
    for df in tabs:
        if not df.empty and 'Wert' in df.columns:
            for val in df['Wert']:
                total_value += smart_clean(val)

    total_value = round(total_value, 2)

    # 3. SPEICHERN & TELEGRAM
    repo.save_history(total_value) 
    repo.save_watchlist(df_wl)
    
    print(f"📤 Sende Bericht... Realwert: {total_value} €")
    try:
        top_5 = df_wl.nlargest(5, 'Score')
        # Telegram Aufruf mit Bestätigung
        success = send_summary(top_5, total_value)
        if success: print("✅ Telegram Nachricht wurde erfolgreich abgesetzt.")
        else: print("⚠️ Telegram Dienst meldet Probleme (Check config.py)")
    except Exception as e:
        print(f"❌ Schwerer Fehler beim Telegram-Versand: {e}")

    print(f"🏁 Update abgeschlossen. Depot: {total_value:.2f} €")

if __name__ == "__main__":
    run_scanner()