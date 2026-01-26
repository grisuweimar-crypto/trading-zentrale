import pandas as pd
import yfinance as yf
from cloud.repository import TradingRepository
from market.yahoo import get_price_data
from market.scoring import calculate_total_score
from market.forex import get_usd_eur_rate
from alerts.telegram import send_summary

# --- DAS KOMMA-REPARATUR-MODUL (DER "PROFI-CLEANER") ---
def get_real_number(val, reference=None):
    """Extrahiert die Ziffern und setzt das Komma logisch passend zum Referenzwert."""
    if pd.isna(val) or val == 0: return 0.0
    
    # Alle Zeichen entfernen außer Ziffern
    s = "".join(c for c in str(val) if c.isdigit())
    if not s: return 0.0
    num = float(s)
    
    # Wenn wir keinen Referenzwert haben (z.B. für Kaufwert selbst)
    if reference is None:
        # Wir gehen davon aus, dass Kaufwerte meistens 2 Dezimalstellen haben (5000 -> 50.00)
        # und im Bereich 10-1000 liegen.
        while num > 2000: num /= 10.0
        return round(num, 2)
    
    # Wenn wir eine Referenz haben (z.B. Anzahl passend zum Kaufwert/Kurs)
    # Wir schieben das Komma so lange, bis (Anzahl * Kurs) in der Nähe vom Kaufwert liegt.
    if reference <= 0: return num
    
    # Beispiel: Anzahl 172486356, Kurs 0.28, Kaufwert 50.00
    # Wir wollen eine Anzahl, die ca. 50 / 0.28 = 178 ergibt.
    candidate = num
    target = reference
    while candidate > target * 100:
        candidate /= 10.0
    while candidate < target / 10 and candidate > 0:
        candidate *= 10.0
    
    return round(candidate, 6)

def run_scanner():
    print("🚀 TRADING SCANNER V34 - REALITY CHECK ACTIVE")
    repo = TradingRepository()
    df_wl = repo.load_watchlist()
    fx_rate = get_usd_eur_rate()

    # 1. WATCHLIST SCAN (Elliott & Scores)
    for idx, row in df_wl.iterrows():
        symbol = row.get('Ticker')
        hist = get_price_data(symbol)
        if hist is not None:
            price = float(hist['Close'].iloc[-1])
            if "US" in str(row.get('ISIN', '')): price *= fx_rate
            df_wl.at[idx, 'Akt. Kurs [€]'] = price
            df_wl.at[idx, 'Score'] = calculate_total_score(df_wl.loc[idx])

    # 2. PORTFOLIO BERECHNUNG (Die "Erzwingung")
    print("📊 Berechne Depot-Performance...")
    total_value = 0.0
    total_invest = 0.0

    for tab_func in [repo.load_import_aktien, repo.load_import_krypto]:
        df = tab_func()
        if df.empty: continue
        
        for _, row in df.iterrows():
            # A. Wir bestimmen zuerst den Kaufwert (unser Anker)
            invest = get_real_number(row.get('Kaufwert'))
            
            # B. Wir bestimmen die echte Anzahl (passend zum Kaufwert/Kaufkurs)
            kaufkurs = get_real_number(row.get('Kaufkurs'))
            target_qty = invest / kaufkurs if kaufkurs > 0 else 1.0
            anzahl = get_real_number(row.get('Anzahl'), reference=target_qty)
            
            # C. Wir holen den aktuellen Live-Kurs
            symbol = row.get('ISIN') or row.get('Symbol') or row.get('Name')
            hist = get_price_data(symbol)
            
            if hist is not None:
                live_price = float(hist['Close'].iloc[-1])
                # Währung fixen
                t_obj = yf.Ticker(symbol)
                if t_obj.info.get('currency') == 'USD': live_price *= fx_rate
                
                akt_wert = anzahl * live_price
                total_value += akt_wert
                total_invest += invest
                print(f"🔹 {row.get('Name')}: {anzahl:.2f} Stk * {live_price:.2f}€ = {akt_wert:.2f}€")

    # 3. STATISTIKEN & BERICHT
    total_value = round(total_value, 2)
    total_invest = round(total_invest, 2)
    profit_eur = round(total_value - total_invest, 2)
    profit_pct = round((profit_eur / total_invest * 100), 2) if total_invest > 0 else 0

    repo.save_history(total_value)
    repo.save_watchlist(df_wl)
    
    print(f"💰 Gesamt: {total_value} € | G/V: {profit_eur} € ({profit_pct}%)")
    
    try:
        df_wl['Score'] = pd.to_numeric(df_wl['Score'], errors='coerce').fillna(0)
        send_summary(df_wl.nlargest(5, 'Score'), total_value)
        print("✅ Telegram gesendet.")
    except Exception as e:
        print(f"❌ Fehler: {e}")

if __name__ == "__main__":
    run_scanner()