import pandas as pd
from cloud.repository import TradingRepository
from market.yahoo import get_price_data
from market.scoring import calculate_total_score
from market.forex import get_usd_eur_rate
from market.elliott import detect_elliott_wave  # <--- HIER IMPORTIEREN
from utils.sanitizer import get_logical_value
from alerts.telegram import send_summary

def run_scanner():
    print("🚀 SCANNER AKTIV - MODULARE STRUKTUR")
    repo = TradingRepository()
    fx_rate = get_usd_eur_rate()
    
    # --- TEIL 1: WATCHLIST (Markt-Analyse & Elliott) ---
    df_wl = repo.load_watchlist()
    print(f"🔭 Scanne {len(df_wl)} Werte auf Elliott-Muster...")

    for idx, row in df_wl.iterrows():
        ticker = row.get('Ticker')
        hist = get_price_data(ticker)
        
        if hist is not None:
            # 1. ELLIOTT LOGIK AUFRUFEN
            # Diese Funktion berechnet jetzt Entry (unten) und Target (oben)
            elliott_res = detect_elliott_wave(hist)
            
            # 2. DATEN IN DEINE SPALTEN SCHREIBEN
            # Wir füllen exakt die Spalten, die du in deinem Sheet hast
            df_wl.at[idx, 'Elliott-Signal'] = elliott_res['signal']
            df_wl.at[idx, 'Auto-Ausstieg 161%'] = elliott_res['target']
            
            # Den Einstiegs-Bereich (unten) splitten oder als Range eintragen
            df_wl.at[idx, 'Elliott-Einstieg'] = elliott_res['entry_zone']
            
            # 3. SCORE BERECHNEN
            # Wir übergeben die neue Zeile (inkl. Elliott-Signal) an das Scoring
            df_wl.at[idx, 'Score'] = calculate_total_score(df_wl.loc[idx])
            
            if "BUY" in str(elliott_res['signal']):
                print(f"🎯 SIGNAL: {row.get('Name')} | Ziel: {elliott_res['target']}€")

    # --- TEIL 2: PORTFOLIO (Dein stabiler Teil) ---
    total_value = 0.0
    for df_p in [repo.load_import_aktien(), repo.load_import_krypto()]:
        if not df_p.empty:
            for _, row_p in df_p.iterrows():
                total_value += get_logical_value(row_p.get('Wert'), row_p.get('Kaufwert'))

    # --- TEIL 3: ABSCHLUSS & SPEICHERN ---
    # Hier werden alle Elliott-Marken zurück in dein Google Sheet geschrieben!
    repo.save_watchlist(df_wl)
    repo.save_history(total_value)
    
    # Telegram Bericht
    send_summary(df_wl.nlargest(5, 'Score'), total_value)
    print(f"🏁 Fertig. Depot: {total_value} €")

if __name__ == "__main__":
    run_scanner()