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
            df_wl.at[idx, 'MC_Chance'] = float(calculate_probability(hist))
            df_wl.at[idx, 'Elliott_Signal'] = detect_elliott_wave(hist)
            
            fund_data = get_fundamental_data(ticker_obj)
            for key, val in fund_data.items():
                if key in df_wl.columns: df_wl.at[idx, key] = val
            
            df_wl.at[idx, 'Score'] = calculate_total_score(df_wl.loc[idx])
            print(f"✅ {row.get('Name', 'Aktie')} bewertet.")

    # 2. PORTFOLIO BERECHNUNG (Zwei-Reiter & Punkt-Fix)
    print("📊 Berechne Portfolio aus getrennten Import-Tabs...")
    total_value = 0.0

    def smart_clean(v):
        """Wandelt Text-Zahlen sicher in echte Zahlen um."""
        s = str(v).strip().replace('€', '').replace(' ', '')
        if not s or s.lower() == 'nan': return 0.0
        
        # WICHTIG: Wenn nur ein Punkt da ist (z.B. 68.11), 
        # ist es ein Dezimalzeichen. Wir lassen ihn so!
        if '.' in s and ',' not in s:
            pass 
        # Wenn Punkt UND Komma da sind (z.B. 1.250,50), 
        # ist der Punkt ein Tausendertrenner -> weg damit.
        elif ',' in s:
            s = s.replace('.', '').replace(',', '.')
            
        try: return float(s)
        except: return 0.0

    # Summe Aktien
    df_a = repo.load_import_aktien()
    if not df_a.empty and 'Wert' in df_a.columns:
        for v in df_a['Wert']:
            total_value += smart_clean(v)
        print("✅ Aktien-Import summiert.")

    # Summe Krypto
    df_k = repo.load_import_krypto()
    if not df_k.empty and 'Wert' in df_k.columns:
        for v in df_k['Wert']:
            total_value += smart_clean(v)
        print("✅ Krypto-Import summiert.")

    total_value = round(total_value, 2)

    # 3. FINALE: SPEICHERN & TELEGRAM
    repo.save_history(total_value) 
    repo.save_watchlist(df_wl)
    
    print(f"📤 Sende Bericht... Realwert: {total_value} €")
    try:
        df_wl['Score'] = pd.to_numeric(df_wl['Score'], errors='coerce').fillna(0)
        top_5 = df_wl.nlargest(5, 'Score')
        send_summary(top_5, total_value) 
    except Exception as e:
        print(f"❌ Telegram-Fehler: {e}")

    print(f"🏁 Update abgeschlossen. Depot: {total_value:.2f} €")

if __name__ == "__main__":
    run_scanner()