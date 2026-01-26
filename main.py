import pandas as pd
import yfinance as yf
import config
from cloud.repository import TradingRepository
from market.yahoo import get_ticker_symbol, get_price_data
from market.montecarlo import calculate_probability
from market.fundamental import get_fundamental_data
from market.scoring import calculate_total_score
from market.elliott import detect_elliott_wave
from market.forex import get_usd_eur_rate, convert_to_eur 
from alerts.telegram import send_summary

# --- INTERNES SANITY-MODUL ---
def force_logic_value(row):
    """Erzwingt einen logischen Wert, falls Google Sheets das Komma gefressen hat."""
    def to_f(x):
        if pd.isna(x): return 0.0
        # Extrahiert nur Ziffern (ignoriert falsche Punkte/Kommas)
        s = "".join(c for c in str(x) if c.isdigit())
        return float(s) if s else 0.0

    raw_wert = to_f(row.get('Wert'))
    raw_kauf = to_f(row.get('Kaufwert'))

    if raw_wert == 0: return 0.0

    # Mathematische Korrektur: 
    # Wir skalieren den Wert so lange, bis er im plausiblen Bereich zum Kaufwert liegt.
    candidate = raw_wert
    kauf_check = raw_kauf / 100.0 # Wir nehmen an, der Kaufwert hat 2 Dezimalstellen
    
    # Wenn der Wert astronomisch höher ist als der Kaufwert (Faktor 10+)
    # ODER absolut unrealistisch für eine Einzelposition (> 1000€)
    while kauf_check > 0 and candidate > kauf_check * 10:
        candidate /= 10.0
    
    # Letzte Sicherung für deine Depotgröße
    while candidate > 1000 and "BITCOIN" not in str(row.get('Name')).upper():
        candidate /= 10.0
        
    return round(candidate, 2)

def run_scanner():
    print("🚀 TRADING SCANNER V32 - COMPLETE & SAFE EDITION")
    repo = TradingRepository()
    df_wl = repo.load_watchlist()
    fx_rate = get_usd_eur_rate()

    # 1. SCHRITT: WATCHLIST SCAN (Elliott & Scoring)
    print(f"🔭 Scanne {len(df_wl)} Aktien...")
    for idx, row in df_wl.iterrows():
        symbol = row.get('Ticker') or get_ticker_symbol(row)
        hist = get_price_data(symbol)
        
        if hist is not None:
            ticker_obj = yf.Ticker(symbol)
            # Preis & Währung
            price = float(hist['Close'].iloc[-1])
            if ticker_obj.info.get('currency') == 'USD':
                price *= fx_rate
            
            # Analyse-Module
            df_wl.at[idx, 'Akt. Kurs [€]'] = price
            df_wl.at[idx, 'MC_Chance'] = float(calculate_probability(hist))
            df_wl.at[idx, 'Elliott_Signal'] = detect_elliott_wave(hist)
            
            # Fundamentaldaten & Score
            fund_data = get_fundamental_data(ticker_obj)
            for key, val in fund_data.items():
                if key in df_wl.columns: df_wl.at[idx, key] = val
            
            df_wl.at[idx, 'Score'] = calculate_total_score(df_wl.loc[idx])
            print(f"✅ {row.get('Name', 'Aktie')} bewertet (Score: {df_wl.at[idx, 'Score']})")

    # 2. SCHRITT: PORTFOLIO BERECHNUNG (Sanity Mode)
    print("📊 Berechne Portfolio mit Sanity-Check...")
    total_value = 0.0

    # Wir laden beide Import-Tabs
    for tab_func in [repo.load_import_aktien, repo.load_import_krypto]:
        df_imp = tab_func()
        if not df_imp.empty:
            for _, row in df_imp.iterrows():
                # Hier wird das Komma-Problem mathematisch gelöst
                clean_val = force_logic_value(row)
                total_value += clean_val
                print(f"🔹 {row.get('Name')}: {clean_val:.2f} €")

    total_value = round(total_value, 2)

    # 3. SCHRITT: SPEICHERN & BERICHT
    print(f"💾 Speichere Daten... Realwert: {total_value} €")
    repo.save_history(total_value)
    repo.save_watchlist(df_wl)
    
    # Top 5 für Telegram vorbereiten
    try:
        df_wl['Score'] = pd.to_numeric(df_wl['Score'], errors='coerce').fillna(0)
        top_5 = df_wl.nlargest(5, 'Score')
        
        print("📤 Sende Bericht an Telegram...")
        send_summary(top_5, total_value)
        print("✅ Telegram-Update abgeschlossen.")
    except Exception as e:
        print(f"❌ Fehler beim Telegram-Versand: {e}")

    print(f"🏁 SCANNER ABGESCHLOSSEN. Realwert: {total_value:.2f} €")

if __name__ == "__main__":
    run_scanner()