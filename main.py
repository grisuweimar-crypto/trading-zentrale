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
    
    # 1. Watchlist Scan (Deine bewährte Logik)
    fx_rate = get_usd_eur_rate()
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

    # --- 2. SCHRITT: PORTFOLIO-BERECHNUNG (Zwei-Reiter-Modus) ---
    print("📊 Berechne Portfolio aus getrennten Import-Tabs...")
    total_value = 0.0

    def smart_clean(v):
        """Bereinigt Zahlen-Strings sicher und erkennt Punkt/Komma korrekt."""
        s = str(v).strip().replace('€', '').replace('%', '').strip()
        if not s or s.lower() == 'nan':
            return 0.0
        
        # Logik für Zero-Exporte: Punkt ist oft Dezimaltrenner (68.11)
        # Wir prüfen, ob ein Punkt vorhanden ist und kein Komma
        if '.' in s and ',' not in s:
            # Punkt bleibt Punkt (Dezimalzeichen)
            pass
        elif ',' in s and '.' in s:
            # Deutsches Format: 1.365,40 -> Punkt weg, Komma zu Punkt
            s = s.replace('.', '').replace(',', '.')
        elif ',' in s:
            # Einfaches Komma: 68,11 -> 68.11
            s = s.replace(',', '.')
            
        try:
            return float(s)
        except:
            return 0.0

    # 1. Aktien aus Reiter 'Import_Aktien'
    df_a = repo.load_import_aktien()
    if not df_a.empty and 'Wert' in df_a.columns:
        for v in df_a['Wert']:
            val = smart_clean(v)
            total_value += val
        print(f"✅ Aktien-Wert erfolgreich addiert.")

    # 2. Krypto aus Reiter 'Import_Krypto'
    df_k = repo.load_import_krypto()
    if not df_k.empty and 'Wert' in df_k.columns:
        for v in df_k['Wert']:
            val = smart_clean(v)
            total_value += val
        print(f"✅ Krypto-Wert erfolgreich addiert.")

    # Gesamtwert runden
    total_value = round(total_value, 2)
    
    # --- 3. SCHRITT: SPEICHERN & TELEGRAM ---
    repo.save_history(total_value) 
    repo.save_watchlist(df_wl)
    
    print(f"📤 Sende Bericht an Telegram... (Depotwert: {total_value} €)")
    try:
        df_wl['Score'] = pd.to_numeric(df_wl['Score'], errors='coerce').fillna(0)
        send_summary(df_wl.nlargest(5, 'Score'), total_value) 
    except Exception as e:
        print(f"❌ Telegram-Fehler: {e}")

    print(f"🏁 Update abgeschlossen. Realwert: {total_value:.2f} €")

if __name__ == "__main__":
    run_scanner()