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

# --- DETERMINISTISCHE KORREKTUR-FUNKTION ---
def get_logical_quantity(row):
    """Erzwingt eine logische Stückzahl durch Skalierungs-Abgleich."""
    def to_float(x):
        if pd.isna(x): return None
        s = str(x).replace('€', '').replace(' ', '').replace('"', '')
        if ',' in s and '.' in s:
            if s.rfind('.') < s.rfind(','): s = s.replace('.', '').replace(',', '.')
            else: s = s.replace(',', '')
        elif ',' in s: s = s.replace(',', '.')
        try: return float(s)
        except: return None

    raw_qty   = to_float(row.get("Anzahl"))
    raw_price = to_float(row.get("Kaufkurs"))
    raw_value = to_float(row.get("Kaufwert"))

    if raw_price is None or raw_value is None or raw_price <= 0:
        return raw_qty if raw_qty else 0.0

    # Teste Skalierungsfaktoren (10er Potenzen)
    SCALE_FACTORS = [1, 10, 100, 1000, 10000, 100000, 1000000]
    best_candidate = raw_qty
    best_score = float("inf")

    for v_scale in SCALE_FACTORS:
        for p_scale in SCALE_FACTORS:
            value = raw_value / v_scale
            price = raw_price / p_scale
            if price <= 0: continue
            
            qty = value / price
            # Realismus-Check (Aktien/Krypto meist < 1 Mio Stück)
            if qty <= 0 or qty > 1000000: continue

            # Score: Bevorzuge kleine Skalierung und Nähe zum Original
            score = abs(v_scale - 1) * 0.1 + abs(p_scale - 1) * 0.1
            if raw_qty: score += abs(qty - raw_qty) / max(raw_qty, 1)
            
            if score < best_score:
                best_score = score
                best_candidate = qty

    return round(best_candidate, 6)

def run_scanner():
    print("🚀 TRADING SCANNER V30 - DETERMINISTIC MATH MODE")
    repo = TradingRepository()
    df_wl = repo.load_watchlist()
    fx_rate = get_usd_eur_rate()

    # 1. WATCHLIST SCAN
    print(f"🔭 Scanne {len(df_wl)} Aktien...")
    for idx, row in df_wl.iterrows():
        symbol = row.get('Ticker') or get_ticker_symbol(row)
        hist = get_price_data(symbol)
        if hist is not None:
            ticker_obj = yf.Ticker(symbol)
            price = float(hist['Close'].iloc[-1])
            if ticker_obj.info.get('currency') == 'USD': price *= fx_rate
            df_wl.at[idx, 'Akt. Kurs [€]'] = price
            df_wl.at[idx, 'Score'] = calculate_total_score(df_wl.loc[idx])
            print(f"✅ {row.get('Name', 'Aktie')} bewertet.")

    # 2. PORTFOLIO-BERECHNUNG (Mathematisch erzwungen)
    print("📊 Berechne Portfolio via Ratio-Failsafe...")
    total_value = 0.0
    
    # Import-Tabs laden
    tabs = [
        (repo.load_import_aktien(), False), 
        (repo.load_import_krypto(), True)
    ]
    
    for df, is_krypto in tabs:
        if not df.empty:
            for _, row in df.iterrows():
                # Die "mündige" Anzahl berechnen
                anzahl = get_logical_quantity(row)
                
                # Live-Kurs für die Bewertung
                lookup = row.get('ISIN') if not is_krypto else row.get('Symbol')
                if not lookup or pd.isna(lookup): lookup = row.get('Name')
                
                # Krypto-Ticker fixen (z.B. BTC -> BTC-EUR)
                if is_krypto and "-" not in str(lookup): lookup = f"{lookup}-EUR"
                
                hist = get_price_data(lookup)
                if hist is not None:
                    price = float(hist['Close'].iloc[-1])
                    ticker_obj = yf.Ticker(lookup)
                    if ticker_obj.info.get('currency') == 'USD': price *= fx_rate
                    
                    pos_wert = anzahl * price
                    total_value += pos_wert
                    print(f"🔹 {row.get('Name')}: {anzahl:.4f} Stk. * {price:.2f}€ = {pos_wert:.2f}€")

    total_value = round(total_value, 2)

    # 3. FINALE: SPEICHERN & TELEGRAM
    repo.save_history(total_value) 
    repo.save_watchlist(df_wl)
    
    print(f"📤 Sende Bericht... Realwert: {total_value} €")
    try:
        df_wl['Score'] = pd.to_numeric(df_wl['Score'], errors='coerce').fillna(0)
        send_summary(df_wl.nlargest(5, 'Score'), total_value) 
        print("✅ Telegram erfolgreich versendet.")
    except Exception as e:
        print(f"❌ Telegram-Fehler: {e}")

    print(f"🏁 Update abgeschlossen. Depot: {total_value:.2f} €")

if __name__ == "__main__":
    run_scanner()