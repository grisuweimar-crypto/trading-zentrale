import streamlit as st
import pandas as pd
import plotly.express as px
import os
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- HILFSFUNKTIONEN FÜR BACKTEST ---
def get_ticker(name):
    n = str(name).upper()
    mapping = {
        # Tech & USA
        'MICROSOFT': 'MSF.F', 'APPLE': 'APC.F', 'NVIDIA': 'NVD.F', 
        'ALPHABET': 'GOOGL', 'AMAZON': 'AMZ.F', 'TESLA': 'TL0.F',
        'PALANTIR': 'PTX.F', 'PAYPAL': '2PP.F', 'META': 'FB2.F',
        
        # Deutschland (DAX/Vorgaben)
        'ALLIANZ': 'ALV.DE', 'SAP': 'SAP.DE', 'MERCEDES': 'MBG.DE',
        'TELEKOM': 'DTE.DE', 'SIEMENS': 'SIE.DE', 'BASF': 'BAS.DE',
        'BAYER': 'BAYN.DE', 'VONOVIA': 'VNA.DE', 'DHL': 'DHL.DE',
        
        # China / Asien (aus deinem Scanner)
        'ALIBABA': 'AHLA.F', 'XIAOMI': '3CP.F', 'BYD': 'BY6.F',
        'JD.COM': '099.F', 'TENCENT': 'NNnD.F', 'NIO': 'N3IA.F',
        'MINISO': 'MNS.F', 'RELIANCE': 'RIGD.F',
        
        # Krypto
        'BITCOIN': 'BTC-EUR', 'ETHEREUM': 'ETH-EUR', 'SOLANA': 'SOL-EUR',
        'CARDANO': 'ADA-EUR', 'RIPPLE': 'XRP-EUR',
        
        # Gesundheit & Konsum
        'NOVO': 'NOVC.F', 'UNITEDHEALTH': 'UNH.F', 'COCA': 'CCC3.F',
        'PEPSI': 'PEP.F', 'PHILIP MORRIS': '4I1.F'
    }
    
    # Suche nach Teilübereinstimmungen
    for key, ticker in mapping.items():
        if key in n:
            return ticker
    return None

@st.cache_data(ttl=3600)
def get_30d_perf(symbol):
    try:
        t = yf.Ticker(symbol)
        h = t.history(period="35d")
        if len(h) < 20: return 0
        price_now = h['Close'].iloc[-1]
        price_old = h['Close'].iloc[0]
        return ((price_now - price_old) / price_old) * 100
    except: return 0
    
# --- KONFIGURATION ---
desktop = os.path.join(os.path.expanduser("~"), "OneDrive", "Desktop")
if not os.path.exists(desktop): desktop = os.path.join(os.path.expanduser("~"), "Desktop")
EXCEL_FILE = os.path.join(desktop, "Watchlist Master 2026-voll.xlsx")

st.set_page_config(page_title="CW Trading Zentrale", page_icon="🚀", layout="wide")

# --- STYLING ---
st.markdown("""
<style>
    .metric-card { background-color: #1E1E1E; padding: 15px; border-radius: 10px; border: 1px solid #333; margin-bottom: 10px; }
    .stDataFrame { overflow-x: auto; }
</style>
""", unsafe_allow_html=True)

# --- DATEN LADEN ---
def load_data_cloud():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name('google_key.json', scope)
        client = gspread.authorize(creds)
        sheet = client.open("Trading_Zentrale_Cloud")
        
        # Watchlist und Portfolio laden
        df_wl_raw = pd.DataFrame(sheet.worksheet("Watchlist").get_all_records())
        df_pf_raw = pd.DataFrame(sheet.worksheet("Portfolio").get_all_records())
        
        # WICHTIG: Text in Zahlen umwandeln (behebt die "verstümmelte" Ansicht)
        for df_temp in [df_wl_raw, df_pf_raw]:
            for col in df_temp.columns:
                if any(x in col for x in ['Kurs', 'Score', 'Anzahl', 'Kaufkurs', 'PE', 'Upside', 'Div', 'G/V', 'Marge', 'Wachstum', 'Debt']):
                    df_temp[col] = pd.to_numeric(df_temp[col].astype(str).str.replace('.', '').str.replace(',', '.'), errors='coerce').fillna(0)
        
        return df_wl_raw, df_pf_raw
    except Exception as e:
        st.error(f"❌ Cloud-Verbindung fehlgeschlagen: {e}")
        return None, None

# --- SIDEBAR (MISCHPULT) ---
with st.sidebar:
    st.header("🎛️ Strategie-Mischpult")
    
    defaults = {
        "w_analyst": 10, "w_upside": 10, "w_pe": 15, "w_debt": 5,
        "w_div": 5, "w_growth": 10, "w_margin": 10, "w_mc": 20,
        "w_elliott": 30, "w_min_score": 140
    }

    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val

    if st.button("↺ Mischpult zurücksetzen"):
        for key, val in defaults.items():
            st.session_state[key] = val
        st.rerun()

    st.divider()
    st.write("Bewertungskriterien:")

    # Slider mit 'help' Parameter für die Mouseover-Funktion
    w_mc = st.slider("🎲 Monte Carlo", 0, 40, key="w_mc", 
                     help="Wahrscheinlichkeit für Kursgewinne basierend auf 500 Simulationen der letzten 30 Tage.")
    
    w_analyst = st.slider("🏦 Analysten", 0, 20, key="w_analyst", 
                          help="Punkte für positive Kaufempfehlungen (Buy/Strong Buy) von Bank-Analysten.")
    
    w_upside = st.slider("🚀 Potenzial", 0, 20, key="w_upside", 
                         help="Bewertet das Kursziel der Profis. Punkte gibt es ab +5% Kurspotenzial.")
    
    w_pe = st.slider("🏷️ KGV (Preis)", 0, 30, key="w_pe", 
                     help="Kurs-Gewinn-Verhältnis. Belohnt günstige Aktien (KGV < 25), bestraft teure (> 60).")
    
    w_debt = st.slider("🛡️ Sicherheit", 0, 20, key="w_debt", 
                       help="Verschuldungsgrad. Weniger Schulden (< 80% Eigenkapital-Verhältnis) geben mehr Punkte.")
    
    w_div = st.slider("💸 Dividende", 0, 20, key="w_div", 
                      help="Bonuspunkte für Aktien mit einer Dividendenrendite von über 2,0%.")
    
    w_growth = st.slider("🌱 Wachstum", 0, 20, key="w_growth", 
                         help="Umsatzwachstum im Vergleich zum Vorjahr. Belohnt expandierende Unternehmen.")
    
    w_margin = st.slider("💰 Marge", 0, 20, key="w_margin", 
                         help="Nettogewinn-Marge. Gibt Punkte, solange das Unternehmen profitabel arbeitet (> 0%).")
    
    w_elliott = st.slider("📈 Elliott Wave Gewicht", 0, 50, key="w_elliott", 
                          help="Gewichtet die Stärke des Elliott-Wave-Signals (Impuls, Welle 3 oder ABC-Korrektur).")
    
    st.divider()
    w_min_score = st.slider("🎯 Elite-Filter", 0, 200, key="w_min_score",
                            help="Filtert die Top-Picks. Nur Aktien, die diesen Score erreichen, erscheinen als 🔥 Treffer.")

# --- APP START ---
st.title("🚀 CW Trading Zentrale")
df_wl, df_pf = load_data_cloud()
if df_pf is None or df_wl is None:
    st.error("❌ Keine Daten gefunden.")
    st.stop()

# --- DATEN VORBEREITUNG ---
for c in ['Anzahl', 'Kaufkurs', 'Akt. Kurs [€]', 'DivRendite']:
    if c not in df_pf.columns: df_pf[c] = 0.0
    df_pf[c] = pd.to_numeric(df_pf[c], errors='coerce').fillna(0)

df_pf['RealWert'] = df_pf['Anzahl'] * df_pf['Akt. Kurs [€]']
df_pf['Invest'] = df_pf['Anzahl'] * df_pf['Kaufkurs']
df_pf['Gewinn'] = df_pf['RealWert'] - df_pf['Invest']
df_pf['Div_Euro_Calc'] = df_pf['RealWert'] * (df_pf['DivRendite'] / 100)

df_stocks = df_pf[df_pf['AssetType'] == 'STOCK'].copy()
df_crypto = df_pf[df_pf['AssetType'] == 'CRYPTO'].copy()

warn_stocks = df_stocks['Warnung'].notna().sum() if 'Warnung' in df_stocks.columns else 0
warn_crypto = df_crypto['Warnung'].notna().sum() if 'Warnung' in df_pf.columns else 0
warn_total = df_pf['Warnung'].notna().sum() if 'Warnung' in df_pf.columns else 0

# --- Watchlist Score & Elliott ---
for col in ['Score','MC_Chance','Upside','PE','Typ','Beschreibung','DivRendite','Marge','AnalystRec','Debt','Wachstum','Ziel']:
    if col not in df_wl.columns: df_wl[col] = 0
df_wl['Kurs'] = df_wl.get('Akt. Kurs [€]', df_wl.get('Kurs', 0.0))
df_wl['Elliott_Signal'] = df_wl.get('Elliott_Signal', "-")
df_wl['Elliott_Confidence'] = df_wl.get('Elliott_Confidence', 0.0)
df_wl['Elliott_Entry'] = df_wl.get('Elliott_Entry', 0.0)

def calculate_live_score(row):
    score = 0
    typ = str(row['Typ'])
    score += 100 if typ == "DOPPEL" else 80 if typ == "ELLIOTT" else 50
    score += row['MC_Chance'] * (w_mc / 100)
    if 'buy' in str(row['AnalystRec']).lower(): score += w_analyst
    if row['Upside'] > 5: score += w_upside
    elif row['Upside'] < 0: score -= w_upside
    pe = row['PE']
    if 0 < pe < 25: score += w_pe
    elif pe > 60: score -= w_pe 
    debt = row['Debt']
    if debt < 80: score += w_debt
    elif debt > 150: score -= w_debt
    if row['DivRendite'] > 2.0: score += w_div
    if row['Wachstum'] > 5.0: score += w_growth
    if row['Marge'] > 0: score += w_margin

    elliott_conf = float(row.get('Elliott_Confidence', 0))
    score += elliott_conf * w_elliott
    return score

df_wl['LiveScore'] = df_wl.apply(calculate_live_score, axis=1)
candidates = df_wl[df_wl['LiveScore'] >= w_min_score].copy().sort_values(by='LiveScore', ascending=False)

# --- Neue Elliott-Logik ---
def elliott_badge(row):
    sig = str(row.get('Elliott_Signal', "")).upper()
    conf = float(row.get('Elliott_Confidence', 0))
    
    if "IMPULS" in sig: return "🟢 Impuls"
    elif "WELLE 3" in sig: return "🔥 W3 Start"
    elif "ABC" in sig: return "🔵 ABC Ende"
    elif conf > 0.4: return "🟡 Analyse..."
    else: return "⚪ Kein Signal"

candidates['Elliott_Badge'] = candidates.apply(elliott_badge, axis=1)


# --- TABS ---
tab_watch, tab_port = st.tabs(["🔭 WATCHLIST", "💼 MEIN PORTFOLIO"])

with tab_watch:
    c1,c2,c3,c4 = st.columns([1,1,2,1])
    c1.metric("Gescannt", len(df_wl))
    c2.metric("🔥 Treffer", len(candidates))
    with c3:
        if not candidates.empty:
            tops = [candidates.iloc[i]['Name'] if i<len(candidates) else "-" for i in range(3)]
            st.markdown(f"<div style='text-align:center; background:#262730; border-radius:10px; padding:5px; border:1px solid #444;'><h2 style='margin:0;color:#FFD700;'>🥇 {tops[0]}</h2><p style='margin:0;color:#ccc;'>🥈 {tops[1]} | 🥉 {tops[2]}</p></div>", unsafe_allow_html=True)
        else:
            st.metric("🥇 Top Pick","-")
    c4.metric("Ø Chance", f"{candidates['Upside'].mean():.1f}%" if not candidates.empty else "-")
    
    # --- 3. BACKTEST-LIGHT (STRATEGIE-VALIDIERUNG) ---
    if not candidates.empty:
        top_3_backtest = candidates.head(3).copy()
        results = []
        
        for _, row in top_3_backtest.iterrows():
            sym = get_ticker(row['Name'])
            if sym:
                perf = get_30d_perf(sym)
                results.append(perf)
        
        if results:
            avg_perf = sum(results) / len(results)
            
            # Anzeige in einer schicken Box
            color = "green" if avg_perf > 0 else "red"
            st.markdown(f"""
                <div style='background-color: #1E1E1E; padding: 15px; border-radius: 10px; border-left: 5px solid {color};'>
                    <h3 style='margin:0; color:{color};'>📊 Strategie-Check (30 Tage)</h3>
                    <p style='margin:0; color:#ccc;'>Deine aktuellen Top 3 Picks hätten in den letzten 30 Tagen 
                    <b style='color:white; font-size:1.2em;'>{avg_perf:+.1f}%</b> erzielt.</p>
                </div>
            """, unsafe_allow_html=True)

    st.divider()
    if not candidates.empty:
        col_chart, col_list = st.columns([1,2])
        with col_chart:
            st.subheader("💰 Gewinn-Potential")
            top10 = df_wl.sort_values(by='Upside', ascending=False).head(15)
            fig = px.bar(top10, x='Name', y='Upside', color='Upside', color_continuous_scale=['red','yellow','green','magenta'], title="Analysten Ziele")
            st.plotly_chart(fig, width="stretch")
        
        with col_list:
            st.subheader("📋 Top Kandidaten")
            candidates['PE_Display'] = candidates['PE'].apply(lambda x: "-" if x>900 else f"{x:.1f}")
            st.dataframe(
                candidates[['Name','Kurs','LiveScore','Upside','PE_Display','Elliott_Badge']],
                column_config={
                    "LiveScore": st.column_config.ProgressColumn("Score", min_value=0, max_value=200, color="green"),
                    "Upside": st.column_config.NumberColumn("Upside %", format="%.1f%%"),
                    "Kurs": st.column_config.NumberColumn("Kurs €", format="%.2f €"),
                },
                hide_index=True,
                use_container_width=True
            )

        # --- DEEP DIVE BLOCK (EXAKT HIER) ---
        st.subheader("🔎 Deep Dive")
        for i, (index,row) in enumerate(candidates.head(5).iterrows()):
            score_int = int(row['LiveScore'])
            with st.expander(f"{['🥇','🥈','🥉','4️⃣','5️⃣'][i]} {row['Name']} (Score: {score_int})"):
                # Header-Bereich
                c_a, c_b, c_c = st.columns(3)
                c_a.info(f"Signal: {row['Typ']} | {row['Elliott_Badge']}")
                c_b.success(f"Ziel: {row['Ziel']:.2f}€")
                c_c.warning(f"Upside: {row['Upside']:.1f}%")
                
                # Mittelteil
                desc = str(row['Beschreibung']) if pd.notna(row['Beschreibung']) and row['Beschreibung']!=0 else "Keine Kurzbeschreibung verfügbar."
                st.markdown(f"**Über das Unternehmen:**\n{desc}")
                
                # Trading-Strategie
                sig = str(row.get('Elliott_Signal', '')).upper()
                if "WELLE 3" in sig:
                    st.warning("🚀 **Trading-Strategie:** Starker Trendbeginn. Über Kaufeinstieg nachdenken, Stop-Loss unter letztes Tief.")
                elif "ABC" in sig:
                    st.info("🛒 **Trading-Strategie:** Korrektur scheint beendet. Bodenbildung für Einstieg beobachten.")
                elif "IMPULS" in sig:
                    st.success("📈 **Trading-Strategie:** Trend ist intakt. Gewinne laufen lassen, Stops nachziehen.")
                
                # Fußzeile
                st.caption(f"Fundamental: KGV {row['PE_Display']} | Marge {row['Marge']:.1f}% | Wachstum {row['Wachstum']:.1f}% | Schulden {row['Debt']:.0f}%")

    else:
        st.info("Keine Favoriten gefunden. Verringere evtl. den Mindest-Score im Mischpult.")

with tab_port:
    # --- DIESE ZEILE MUSS ZUERST KOMMEN ---
    sub_total, sub_stock, sub_crypto = st.tabs(["🏠 Gesamt", "🏭 Aktien", "🪙 Krypto"])
# --- 1. REITER: GESAMT (Das Cockpit) ---
    with sub_total:
        # Daten konsolidieren
        df_all = pd.concat([df_stocks, df_crypto], ignore_index=True) if (not df_stocks.empty or not df_crypto.empty) else pd.DataFrame()
        
        if not df_all.empty:
            # 1. Globale Kennzahlen berechnen
            total_invest = df_all['Invest'].sum()
            total_val = df_all['RealWert'].sum()
            total_gv = total_val - total_invest
            total_perf = (total_gv / total_invest * 100) if total_invest > 0 else 0
            
            # --- 1. Risiko-Berechnung für den Banner ---
            df_sorted = df_all.sort_values(by='RealWert', ascending=False)
            top_pos = df_sorted.iloc[0]
            top_percent = (top_pos['RealWert'] / total_val) * 100
            
            if top_percent > 30:
                risk_status, risk_emoji = "Kritisch", "🔴"
            elif top_percent > 15:
                risk_status, risk_emoji = "Warnung", "🟡"
            else:
                risk_status, risk_emoji = "Sicher", "🟢"

            # --- 2. Der 4-Spalten Banner (AKTUALISIERT mit Dividende) ---
            b1, b2, b3, b4 = st.columns(4)
            b1.metric("Gesamtvermögen", f"{total_val:,.2f} €")
            b2.metric("Performance", f"{total_gv:,.2f} €", f"{total_perf:.2f} %")
            
            # Risiko-Anzeige
            b3.metric("Risiko-Status", f"{risk_emoji} {risk_status}", f"{top_percent:.1f}% Klumpen", 
                      help=f"Größte Position: {top_pos['Name']}. Ein Wert unter 15% gilt als sicher.")
            
            # NEU: Dividenden-Vorschau (Berechnung basierend auf deinen Spalten)
            total_div_year = df_all['Div_Euro_Calc'].sum() if 'Div_Euro_Calc' in df_all.columns else 0
            b4.metric("Dividende p.a.", f"{total_div_year:,.2f} €", f"~ {total_div_year/12:.2f} € / Mon")

            st.divider()

            # --- NEU: DEPOT-PERFORMANCE CHART (DIREKT IM SICHTFELD) ---
            history_file = os.path.join(desktop, "depot_history.csv")
            if os.path.exists(history_file):
                df_hist = pd.read_csv(history_file, sep=';')
                st.subheader("📈 Depot-Performance")
                fig_hist = px.line(df_hist, x='Datum', y='Wert', line_shape="spline")
                fig_hist.update_traces(line_color='#00FF00', fill='tozeroy', fillcolor='rgba(0,255,0,0.1)')
                fig_hist.update_layout(height=300, margin=dict(l=0, r=0, t=20, b=0), xaxis_title=None, yaxis_title="Euro")
                st.plotly_chart(fig_hist, use_container_width=True)
                st.divider()

            # --- AB HIER BLEIBT ALLES EXAKT WIE GEHABT ---

            # 3. Treemap (Klumpenrisiko)
            st.subheader("🗺️ Portfolio-Struktur")
            fig_tree = px.treemap(df_all, path=['AssetType', 'Name'], values='RealWert',
                                  color='G/V %', color_continuous_scale='RdYlGn', color_continuous_midpoint=0)
            fig_tree.update_layout(margin=dict(t=0, b=0, l=0, r=0), height=350)
            st.plotly_chart(fig_tree, use_container_width=True)

            st.divider()
            # --- 3b. RAKETEN VS. U-BOOTE ---
            st.subheader("🚀 Top Performer vs. ⚓ Bremsklötze")
            col_win, col_loss = st.columns(2)
            
            # Sortierung nach G/V %
            df_perf = df_all.sort_values(by='G/V %', ascending=False)
            
            with col_win:
                st.write("**Top 3 Raketen (Best Performance)**")
                for i in range(min(3, len(df_perf))):
                    row = df_perf.iloc[i]
                    st.success(f"{row['Name']}: **{row['G/V %']:+.1f}%**")

            with col_loss:
                st.write("**Top 3 U-Boote (Größte Verlierer)**")
                df_flops = df_perf.tail(3).iloc[::-1] # Die letzten 3 umdrehen
                for i in range(min(3, len(df_flops))):
                    row = df_flops.iloc[i]
                    st.error(f"{row['Name']}: **{row['G/V %']:+.1f}%**")

            # 4. DIE ALARME (Deine 30% Regel & Stop-Loss)
            st.subheader("🚩 Aktuelle Alarme & Handlungen")
            col_a, col_b = st.columns(2)
            
            has_alarms = False
            for _, row in df_all.iterrows():
                perf = row['G/V %']
                name = row['Name']
                
                if perf >= 30:
                    has_alarms = True
                    with col_a:
                        st.warning(f"🎯 **{name}**: {perf:.1f}% Profit! \n\n **AKTION:** TEILVERKAUF (50%) & STOP DYNAMISCH ANPASSEN")
                
                elif perf <= -15:
                    has_alarms = True
                    with col_b:
                        st.error(f"⚠️ **{name}**: {perf:.1f}% Verlust! \n\n **AKTION:** STOP ÜBERPRÜFEN / NACHKAUF-SIGNAL ABWARTEN")

            if not has_alarms:
                st.success("✅ Alle Positionen liegen innerhalb deiner Toleranzgrenzen. Aktuell kein Handlungsbedarf.")
            
            # --- Hier folgen dann Rebalancing etc. ---
                
                                 
            # =========================================================                 
            # ⚖️ REBALANCING ASSISTENT (JETZT AN DER RICHTIGEN STELLE)
            # =========================================================
            st.divider()
            st.subheader("⚖️ Rebalancing-Assistent (Gleichverteilung)")
            
            n_pos = len(df_all)
            target_value_per_pos = total_val / n_pos if n_pos > 0 else 0
            
            rebalance_data = []
            for _, row in df_all.iterrows():
                diff_euro = target_value_per_pos - row['RealWert']
                diff_units = diff_euro / row['Akt. Kurs [€]'] if row['Akt. Kurs [€]'] > 0 else 0
                
                # Zeige nur signifikante Abweichungen (> 10%)
                if abs(diff_euro) > (target_value_per_pos * 0.1): 
                    action = "🛒 KAUFEN" if diff_euro > 0 else "💰 VERKAUFEN"
                    rebalance_data.append({
                        "Name": row['Name'],
                        "Ist-Wert": f"{row['RealWert']:,.2f} €",
                        "Abweichung": f"{diff_euro:,.2f} €",
                        "Aktion": action,
                        "Menge": f"{abs(diff_units):,.2f} Stk"
                    })

            if rebalance_data:
                st.dataframe(pd.DataFrame(rebalance_data), hide_index=True, use_container_width=True)
                st.caption(f"Basis: Gleichverteilung. Zielwert: {target_value_per_pos:,.2f} € pro Position.")
            else:
                st.success("✅ Dein Depot ist perfekt balanciert (Abweichungen < 10%).")
            # =========================================================
            
            # --- 5. DIVERSIFIKATIONS-CHECK (EINZELWERT-FOKUS) ---
            st.divider()
            st.subheader("🕵️ Einzelwert-Klumpenrisiko")

            if not df_all.empty:
                # Wir suchen die größte Einzelposition im Depot
                df_sorted = df_all.sort_values(by='RealWert', ascending=False)
                top_pos = df_sorted.iloc[0]
                top_name = top_pos['Name']
                top_percent = (top_pos['RealWert'] / total_val) * 100
                
                c_risk1, c_risk2 = st.columns([1, 2])
                
                with c_risk1:
                    # Grenzwerte für Einzelaktien (nicht für die Assetklasse!)
                    if top_percent > 30:
                        risk_lvl = "🔴 Kritisch"
                        r_color = "red"
                    elif top_percent > 15:
                        risk_lvl = "🟡 Warnung"
                        r_color = "orange"
                    else:
                        risk_lvl = "🟢 Sicher"
                        r_color = "green"
                    
                    st.markdown(f"**Risiko-Status:** <span style='color:{r_color};'>{risk_lvl}</span>", unsafe_allow_html=True)
                    st.metric("Größte Position", top_name, f"{top_percent:.1f}%")

                with c_risk2:
                    if top_percent > 20:
                        st.warning(f"⚠️ **{top_name}** nimmt {top_percent:.1f}% deines Depots ein. "
                                   "Das ist riskant, falls diese Firma spezifische Probleme bekommt.")
                    else:
                        st.success(f"✅ Deine größte Position ({top_name}) ist mit {top_percent:.1f}% gut im Rahmen.")

                # Zeige die Top 3 Klumpenrisiken als Balken
                st.write("**Top 3 Konzentration:**")
                for i in range(min(3, len(df_sorted))):
                    row = df_sorted.iloc[i]
                    p = (row['RealWert'] / total_val)
                    st.text(f"{row['Name']} ({p*100:.1f}%)")
                    st.progress(p)
            
        else:
            st.info("Noch keine Daten im Portfolio vorhanden.")
            
            # --- 4. REBALANCING ASSISTENT ---
            st.divider()
            st.subheader("⚖️ Rebalancing-Assistent (Gleichverteilung)")
            
            # Berechnung des Soll-Werts pro Position
            n_pos = len(df_all)
            target_value_per_pos = total_val / n_pos if n_pos > 0 else 0
            
            rebalance_data = []
            for _, row in df_all.iterrows():
                diff_euro = target_value_per_pos - row['RealWert']
                # Wie viele Stücke muss ich kaufen/verkaufen?
                diff_units = diff_euro / row['Akt. Kurs [€]'] if row['Akt. Kurs [€]'] > 0 else 0
                
                if abs(diff_euro) > (target_value_per_pos * 0.1): # Nur anzeigen, wenn Abweichung > 10%
                    action = "🛒 KAUFEN" if diff_euro > 0 else "💰 VERKAUFEN"
                    rebalance_data.append({
                        "Name": row['Name'],
                        "Ist-Wert": f"{row['RealWert']:,.2f} €",
                        "Abweichung": f"{diff_euro:,.2f} €",
                        "Aktion": action,
                        "Menge": f"{abs(diff_units):,.2f} Stk"
                    })

            if rebalance_data:
                st.dataframe(pd.DataFrame(rebalance_data), hide_index=True, use_container_width=True)
                st.caption(f"Basis: Jede der {len(df_all)} Positionen sollte idealerweise {target_value_per_pos:,.2f} € wert sein.")
            else:
                st.success("✅ Dein Depot ist perfekt balanciert (Abweichungen < 10%).")
            
    # --- 2. REITER: AKTIEN ---
    with sub_stock:
        if not df_stocks.empty:
            val_s = df_stocks['RealWert'].sum()
            gv_s = df_stocks['Gewinn'].sum()
            perf_s = (gv_s / df_stocks['Invest'].sum() * 100) if df_stocks['Invest'].sum() > 0 else 0
            div_s = (df_stocks['DivRendite'] * df_stocks['Invest'] / 100).sum()

            m1, m2, m3 = st.columns(3)
            m1.metric("Aktien Wert", f"{val_s:,.2f} €")
            m2.metric("Gewinn/Verlust", f"{gv_s:,.2f} €", f"{perf_s:.2f} %")
            m3.metric("Dividende p.a.", f"{div_s:,.2f} €")

            st.divider()
            st.dataframe(df_stocks[['Name', 'Anzahl', 'Akt. Kurs [€]', 'RealWert', 'G/V %']], hide_index=True, use_container_width=True)
            st.divider()
    
    # --- 3. REITER: KRYPTO ---
    with sub_crypto:
        if not df_crypto.empty:
            val_c = df_crypto['RealWert'].sum()
            gv_c = df_crypto['Gewinn'].sum()
            perf_c = (gv_c / df_crypto['Invest'].sum() * 100) if df_crypto['Invest'].sum() > 0 else 0

            c1, c2 = st.columns(2)
            c1.metric("Krypto Wert", f"{val_c:,.2f} €")
            c2.metric("Gewinn/Verlust", f"{gv_c:,.2f} €", f"{perf_c:.2f} %")

            st.divider()
            st.dataframe(df_crypto[['Name', 'Anzahl', 'Akt. Kurs [€]', 'RealWert', 'G/V %']], hide_index=True, use_container_width=True)
