import pandas as pd
import os
import sys
import time
import json

# --- PFAD-FIX FÜR DEN HUB (ROOT-EBENE) ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from cloud.repository import TradingRepository
from market.yahoo import get_price_data, get_ticker_symbol
from market.elliott import calculate_elliott
from market.fundamental import get_fundamental_data
from market.montecarlo import run_monte_carlo
from market.scoring import calculate_final_score
from market.cycle import compute_cycle_oscillator, classify_cycle
from alerts.telegram import send_signal
# DASHBOARD IMPORT
from dashboard_gen import generate_dashboard
from market.crv import calculate_crv

def main():
    print("🚀 TRADING-ZENTRALE: AKTIVIERE SCAN...")
    repo = TradingRepository()
    df = repo.load_watchlist()
    
    if df.empty:
        print("❌ FEHLER: watchlist.csv leer.")
        return
    
    # --- Spalten-Initialisierung vor der Schleife (dtype-safe) ---
    # Text-Spalten auf object dtype
    text_cols = ['Elliott-Signal', 'Elliott-Einstieg', 'Elliott-Ausstieg', 'Zyklus-Status', 'Yahoo', 'Währung', 'Radar Vector']
    for col in text_cols:
        if col not in df.columns:
            df[col] = pd.NA
        else:
            df[col] = df[col].astype('object')
    
    # Numerische Spalten auf float64 (außer wenn leer, dann benutzerdefiniert)
    numeric_cols = [
        'Akt. Kurs', 'Perf %', 'Score', 'CRV', 'MC-Chance', 'Zyklus %',
        'ROE %', 'Debt/Equity', 'Div. Rendite %', 'FCF', 'Enterprise Value', 'Revenue',
        'FCF Yield %', 'Growth %', 'Margin %', 'Rule of 40', 'Current Ratio', 'Institutional Ownership %'
    ]
    for col in numeric_cols:
        if col not in df.columns:
            df[col] = pd.NA
        else:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Initialisiere Zyklus-Spalten (falls nicht vorhanden oder fehlerhaft)
    df['Zyklus %'] = df['Zyklus %'].fillna(50.0)
    df['Zyklus-Status'] = df['Zyklus-Status'].fillna('neutral').astype('object')

    print(f"📊 Analyse von {len(df)} Werten gestartet...")
    
    # Dubletten-Schutz: Set der bereits verarbeiteten Ticker/Symbole
    processed_symbols = set()

    for index, row in df.iterrows():
        ticker = str(row['Ticker']).strip()
        stock_name = str(row['Name']).strip()
        # Yahoo nutzt Ticker, keine ISIN: zuerst Spalte "Yahoo", sonst Auflösung aus Name/ISIN
        row_dict = row.to_dict()
        has_isin = row_dict.get('ISIN') is not None and not (isinstance(row_dict.get('ISIN'), float) and pd.isna(row_dict.get('ISIN'))) and str(row_dict.get('ISIN', '')).strip()
        if not has_isin and len(ticker) >= 9 and ticker[:2].isalpha() and ticker[2:].replace(' ', '').isalnum():
            row_dict['ISIN'] = ticker
        symbol_for_yahoo = (str(row.get('Yahoo', '') or '').strip() or get_ticker_symbol(row_dict) or ticker)

        # Dubletten-Schutz: Überspringe, wenn bereits gescannt
        symbol_key = symbol_for_yahoo.upper()
        if symbol_key in processed_symbols:
            print(f"⏭️  [{(index+1)}/{(len(df))}] {ticker} bereits gescannt, überspringe...")
            continue
        processed_symbols.add(symbol_key)

        print(f"🔍 [{(index+1)}/{(len(df))}] Scanne {ticker}...")

        try:
            hist = get_price_data(symbol_for_yahoo)
            if hist is None or hist.empty:
                continue

            # --- ZYKLUS BERECHNEN ---
            cycle_value = compute_cycle_oscillator(hist, period=20)
            cycle_status = classify_cycle(cycle_value)
            
            # 1. Preis fixieren & WÄHRUNG HOLEN
            current_price = float(hist['Close'].iloc[-1]) 
            # Holt die Währung aus den Attributen, die wir in yahoo.py gesetzt haben
            currency_code = hist.attrs.get('currency', 'USD') 
            
            # 2. Daten sammeln
            elliott = calculate_elliott(hist)
            fundamentals = get_fundamental_data(symbol_for_yahoo)
            monte_carlo = run_monte_carlo(hist)
            
            # --- NEU: CRV BERECHNEN ---
            e_target = elliott.get('target', 0)
            crv_value = calculate_crv(current_price, e_target) 
            
            # 3. Score berechnen (mit Preis & CRV Übergabe)
            final_calculated_score = calculate_final_score(
                symbol_for_yahoo, elliott, fundamentals, monte_carlo, current_price, crv_value
            )
            
            # 4. Performance
            perf_pct = 0.0
            if len(hist) > 1:
                perf_pct = ((current_price - hist['Close'].iloc[-2]) / hist['Close'].iloc[-2]) * 100

            # 5. Daten in Zeile schreiben (Yahoo-Symbol speichern → Link im Dashboard geht direkt auf die richtige Aktie)
            # Nutze .loc mit expliziter Typ-Konvertierung, um dtype-Konflikte zu vermeiden
            try:
                df.loc[index, 'Yahoo'] = str(symbol_for_yahoo)
                df.loc[index, 'Akt. Kurs'] = float(round(current_price, 2))
                df.loc[index, 'Währung'] = str(currency_code)
                df.loc[index, 'Perf %'] = float(round(perf_pct, 2))
                df.loc[index, 'Score'] = float(final_calculated_score)
                df.loc[index, 'CRV'] = float(crv_value)
            except Exception as e:
                print(f"⚠️ Warnung bei Zuweisung für {ticker}: {e}")
            # Fundamentale Kennzahlen (für CSV & Dashboard)
            try:
                roe_pct = round(float(fundamentals.get('roe', 0) or 0) * 100, 2)
            except Exception:
                roe_pct = 0.0
            try:
                debt_eq = fundamentals.get('debt_to_equity', 100) or 100
            except Exception:
                debt_eq = 100
            try:
                div_pct = round(float(fundamentals.get('div_rendite', 0) or 0) * 100, 2)
            except Exception:
                div_pct = 0.0
            fcf = fundamentals.get('fcf', 0)
            enterprise_value = fundamentals.get('enterprise_value', 1) or 1
            revenue = fundamentals.get('revenue', 1) or 1
            try:
                fcf_yield = round((float(fcf) / float(enterprise_value)) * 100, 2) if enterprise_value else 0.0
            except Exception:
                fcf_yield = 0.0
            try:
                growth_pct = round(float(fundamentals.get('growth', 0) or 0) * 100, 2)
            except Exception:
                growth_pct = 0.0
            try:
                margin_pct = round(float(fundamentals.get('margin', 0) or 0) * 100, 2)
            except Exception:
                margin_pct = 0.0
            rule40 = round(growth_pct + margin_pct, 2)
            current_ratio = fundamentals.get('current_ratio', '')
            inst_own = round(float(fundamentals.get('institutional_ownership', 0) or 0) * 100, 2)

            # --- Radar-Vektor (normalisiert 0-100 für 5 Achsen) ---
            # Achsen: Wachstum, Rentabilität (ROE), Sicherheit (1/Debt), Technik (Elliott/Zyklus), Bewertung (Upside/PE)
            try:
                # Wachstum (growth_pct ist bereits in Prozent, clamp 0..50 -> 0..100)
                growth_norm = max(0.0, min(growth_pct, 50.0)) / 50.0 * 100.0
            except Exception:
                growth_norm = 0.0
            try:
                roe_norm = max(0.0, min(roe_pct, 50.0)) / 50.0 * 100.0
            except Exception:
                roe_norm = 0.0
            try:
                # Sicherheit: geringere Verschuldung -> höherer Score. debt_eq is Debt/Equity.
                de = float(debt_eq or 100)
                safety_norm = 0.0
                if de <= 0:
                    safety_norm = 100.0
                else:
                    # Map de: 0 ->100, 0.5->75, 1->50, 2->0, >2->0
                    safety_norm = max(0.0, min((2.0 - de) / 2.0, 1.0)) * 100.0
            except Exception:
                safety_norm = 0.0
            try:
                # Technik: niedriger Zyklus% ist besser. Zyklus % liegt in df['Zyklus %'] (0-100). Elliott BUY adds bonus.
                cycle_pct = float(cycle_value if 'cycle_value' in locals() else df.loc[index, 'Zyklus %'] or 50.0)
                tech_base = max(0.0, min(100.0, 100.0 - cycle_pct))
                e_sig = str(elliott.get('signal', '')).upper()
                tech_norm = min(100.0, tech_base + (20.0 if e_sig == 'BUY' else 0.0))
            except Exception:
                tech_norm = 50.0
            try:
                # Bewertung: Upside normalisiert (-50..150 -> 0..100) minus PE-Penalty
                upside_val = float(fundamentals.get('upside', 0) or 0)
                pe_val = float(fundamentals.get('pe', 0) or 0)
                upside_clamped = max(-50.0, min(upside_val, 150.0))
                upside_norm = (upside_clamped + 50.0) / 200.0 * 100.0
                pe_pen = min(pe_val / 2.0, 50.0)
                valuation_norm = max(0.0, min(100.0, upside_norm - pe_pen))
            except Exception:
                valuation_norm = 0.0

            radar_vector = [
                round(growth_norm, 2),
                round(roe_norm, 2),
                round(safety_norm, 2),
                round(tech_norm, 2),
                round(valuation_norm, 2)
            ]

            # Store as JSON string in CSV-friendly column
            try:
                df.loc[index, 'Radar Vector'] = str(json.dumps(radar_vector))
                df.loc[index, 'ROE %'] = float(roe_pct)
                df.loc[index, 'Debt/Equity'] = float(debt_eq) if debt_eq else 0.0
                df.loc[index, 'Div. Rendite %'] = float(div_pct)
                df.loc[index, 'FCF'] = float(fcf) if fcf else 0.0
                df.loc[index, 'Enterprise Value'] = float(enterprise_value) if enterprise_value else 0.0
                df.loc[index, 'Revenue'] = float(revenue) if revenue else 0.0
                df.loc[index, 'FCF Yield %'] = float(fcf_yield)
                df.loc[index, 'Growth %'] = float(growth_pct)
                df.loc[index, 'Margin %'] = float(margin_pct)
                df.loc[index, 'Rule of 40'] = float(rule40)
                df.loc[index, 'Current Ratio'] = float(current_ratio) if isinstance(current_ratio, (int, float)) else current_ratio
                df.loc[index, 'Institutional Ownership %'] = float(inst_own)
                df.loc[index, 'Elliott-Signal'] = str(elliott.get('signal', 'Warten'))
                df.loc[index, 'Elliott-Einstieg'] = float(elliott.get('entry', 0))
                df.loc[index, 'Elliott-Ausstieg'] = float(elliott.get('target', 0))
                df.loc[index, 'MC-Chance'] = float(monte_carlo.get('probability', 0))
                # --- ZYKLUS-SPALTE ---
                df.loc[index, 'Zyklus %'] = float(round(cycle_value, 1))
                df.loc[index, 'Zyklus-Status'] = str(cycle_status)
            except Exception as e:
                print(f"⚠️ Warnung bei Fundamental-Zuweisung für {ticker}: {e}")
            
            

            # 6. TELEGRAM (Nutzt jetzt die Variable von oben)
            # Wir prüfen das Signal direkt aus den Elliott-Daten
            if elliott.get('signal') == "BUY" and final_calculated_score > 75:
                send_signal(ticker, elliott, final_calculated_score, name=stock_name, currency=currency_code)
                print(f"📲 Telegram-Alarm für {stock_name} raus (Score: {final_calculated_score})!")

            time.sleep(0.5)

        except Exception as e:
            print(f"❌ Fehler bei {ticker}: {e}")
            

    # SPEICHERN
    final_df = df
    repo.save_watchlist(final_df)
    
    # DASHBOARD GENERIEREN
    try:
        print("🏗️ Erstelle Dashboard...")
        generate_dashboard() 
    except Exception as e:
        print(f"⚠️ Dashboard-Fehler: {e}")

    print("🏁 SCAN BEENDET. Alle Module erfolgreich ausgeführt!")

if __name__ == "__main__":
    main()