import streamlit as st
import pandas as pd
import plotly.express as px
import os

# --- KONFIGURATION ---
# Wir suchen automatisch auf dem Desktop
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
def load_data():
    if not os.path.exists(EXCEL_FILE): return None, None
    try:
        xls = pd.ExcelFile(EXCEL_FILE)
        return pd.read_excel(xls, sheet_name=0), pd.read_excel(xls, sheet_name="Portfolio")
    except: return None, None

# --- SIDEBAR (MISCHPULT) ---
# Standardwerte
defaults = {'w_mc': 20, 'w_analyst': 10, 'w_upside': 10, 'w_pe': 15, 'w_debt': 5, 'w_div': 5, 'w_growth': 10, 'w_margin': 10}
for key, val in defaults.items():
    if key not in st.session_state: st.session_state[key] = val

with st.sidebar:
    st.header("🎛️ Strategie-Mischpult")
    
    # Reset Button
    if st.button("↺ Mischpult zurücksetzen", help="Setzt alle Regler auf die Standard-Einstellung zurück."):
        for key, val in defaults.items(): st.session_state[key] = val
        st.rerun()
    
    st.divider()
    st.write("Bewertungskriterien:")
    
    # Regler mit Tooltips (Mouseover)
    w_mc = st.slider("🎲 Monte Carlo", 0, 40, key="w_mc", help="Gewichtet die mathematische Gewinnwahrscheinlichkeit der Simulation.")
    w_analyst = st.slider("🏦 Analysten", 0, 20, key="w_analyst", help="Punkte, wenn die Mehrheit der Analysten zum Kauf rät.")
    w_upside = st.slider("🚀 Potenzial", 0, 20, key="w_upside", help="Gewichtet das Kursziel-Potential (Abstand zum Analysten-Ziel).")
    w_pe = st.slider("🏷️ KGV (Preis)", 0, 30, key="w_pe", help="Belohnt günstige Aktien (niedriges Kurs-Gewinn-Verhältnis).")
    w_debt = st.slider("🛡️ Sicherheit", 0, 20, key="w_debt", help="Bestraft hohe Schulden, belohnt solide Bilanzen.")
    w_div = st.slider("💸 Dividende", 0, 20, key="w_div", help="Gewichtet die Dividendenrendite.")
    w_growth = st.slider("🌱 Wachstum", 0, 20, key="w_growth", help="Belohnt Umsatz- und Gewinnwachstum.")
    w_margin = st.slider("💰 Marge", 0, 20, key="w_margin", help="Belohnt profitable Unternehmen (hohe Nettomarge).")

# --- APP START ---
st.title("🚀 CW Trading Zentrale")

df_wl, df_pf = load_data()

if df_pf is None:
    st.error("❌ Keine Daten gefunden. Bitte erst Reset_Portfolio_Final.py ausführen!")
    st.stop()

# --- DATEN VORBEREITUNG (Portfolio) ---
# Zahlen erzwingen
for c in ['Anzahl', 'Kaufkurs', 'Akt. Kurs [€]', 'DivRendite']:
    if c not in df_pf.columns: df_pf[c] = 0.0
    df_pf[c] = pd.to_numeric(df_pf[c], errors='coerce').fillna(0)

# Live Berechnung
df_pf['RealWert'] = df_pf['Anzahl'] * df_pf['Akt. Kurs [€]']
df_pf['Invest'] = df_pf['Anzahl'] * df_pf['Kaufkurs']
df_pf['Gewinn'] = df_pf['RealWert'] - df_pf['Invest']
# Dividende berechnen (Prozent / 100 * Wert)
df_pf['Div_Euro_Calc'] = df_pf['RealWert'] * (df_pf['DivRendite'] / 100)

# Trennung für die Reiter
df_stocks = df_pf[df_pf['AssetType'] == 'STOCK'].copy()
df_crypto = df_pf[df_pf['AssetType'] == 'CRYPTO'].copy()

# Warnungen zählen
warn_stocks = df_stocks['Warnung'].notna().sum() if 'Warnung' in df_stocks.columns else 0
warn_crypto = df_crypto['Warnung'].notna().sum() if 'Warnung' in df_crypto.columns else 0
warn_total = df_pf['Warnung'].notna().sum() if 'Warnung' in df_pf.columns else 0

# --- HAUPT TABS ---
tab_watch, tab_port = st.tabs(["🔭 WATCHLIST (Jäger)", "💼 MEIN PORTFOLIO (Farmer)"])

# ==============================================================================
# TAB 1: WATCHLIST
# ==============================================================================
with tab_watch:
    # Daten putzen
    for c in ['Score', 'MC_Chance', 'Upside', 'PE', 'Typ', 'Beschreibung', 'DivRendite', 'Marge', 'AnalystRec', 'Debt', 'Wachstum', 'Ziel']:
        if c not in df_wl.columns: df_wl[c] = 0
    if 'Akt. Kurs [€]' in df_wl.columns: df_wl = df_wl.rename(columns={'Akt. Kurs [€]': 'Kurs'})
    elif 'Kurs' not in df_wl.columns: df_wl['Kurs'] = 0.0

    # Live Score Berechnung
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
        return score

    df_wl['LiveScore'] = df_wl.apply(calculate_live_score, axis=1)
    candidates = df_wl[df_wl['LiveScore'] > 20].copy().sort_values(by='LiveScore', ascending=False)

    # BANNER
    c1, c2, c3, c4 = st.columns([1, 1, 2, 1])
    c1.metric("Gescannt", len(df_wl))
    c2.metric("🔥 Treffer", len(candidates))
    
    with c3:
        if not candidates.empty:
            top1 = candidates.iloc[0]['Name']
            top2 = candidates.iloc[1]['Name'] if len(candidates) > 1 else "-"
            top3 = candidates.iloc[2]['Name'] if len(candidates) > 2 else "-"
            
            st.markdown(f"""
            <div style="text-align: center; background-color: #262730; border-radius: 10px; padding: 5px; border: 1px solid #444;">
                <h2 style="margin:0; color: #FFD700; text-shadow: 0px 0px 10px rgba(255, 215, 0, 0.5);">🥇 {top1}</h2>
                <p style="margin:0; font-size: 0.9em; color: #cccccc;">🥈 {top2} &nbsp;&nbsp;|&nbsp;&nbsp; 🥉 {top3}</p>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.metric("🥇 Top Pick", "-")

    c4.metric("Ø Chance", f"{candidates['Upside'].mean():.1f}%" if not candidates.empty else "-")
    
    st.divider()

    if not candidates.empty:
        # CHART & TABELLE
        col_chart, col_list = st.columns([1, 2])
        with col_chart:
            st.subheader("💰 Gewinn-Potential")
            top10 = df_wl.sort_values(by='Upside', ascending=False).head(15)
            fig = px.bar(top10, x='Name', y='Upside', color='Upside', 
                         color_continuous_scale=['red', 'yellow', 'green', 'magenta'],
                         title="Analysten Ziele")
            # FIX: width="stretch" statt use_container_width=True
            st.plotly_chart(fig, width="stretch")
            
        with col_list:
            st.subheader("📋 Top Kandidaten")
            candidates['PE_Display'] = candidates['PE'].apply(lambda x: "-" if x > 900 else f"{x:.1f}")
            
            # FIX: width="stretch" statt use_container_width=True
            st.dataframe(
                candidates[['Name', 'Kurs', 'LiveScore', 'Upside', 'PE_Display', 'DivRendite']],
                width="stretch", hide_index=True,
                column_config={
                    "Name": st.column_config.TextColumn("Aktie", help="Name des Unternehmens"),
                    "Kurs": st.column_config.NumberColumn("Preis", format="%.2f €"),
                    "LiveScore": st.column_config.ProgressColumn("Score", format="%d", min_value=0, max_value=200, help="Dein individueller Score (0-200)"),
                    "Upside": st.column_config.NumberColumn("Potential", format="%.1f %%", help="Kurschance bis zum Analystenziel"),
                    "PE_Display": st.column_config.TextColumn("KGV", help="Kurs-Gewinn-Verhältnis (<25 ist gut)"),
                    "DivRendite": st.column_config.NumberColumn("Div %", format="%.2f %%", help="Erwartete Dividendenrendite")
                }
            )
        
        # DEEP DIVE
        st.divider()
        st.subheader("🔎 Deep Dive")
        for i, (index, row) in enumerate(candidates.head(5).iterrows()):
            score_int = int(row['LiveScore'])
            with st.expander(f"{['🥇','🥈','🥉', '4️⃣', '5️⃣'][i]} {row['Name']} (Score: {score_int})"):
                c_a, c_b, c_c = st.columns(3)
                c_a.info(f"Signal: {row['Typ']}")
                c_b.success(f"Ziel: {row['Ziel']:.2f}€")
                c_c.warning(f"Upside: {row['Upside']:.1f}%")
                
                desc = str(row['Beschreibung']) if pd.notna(row['Beschreibung']) and row['Beschreibung'] != 0 else "Keine Kurzbeschreibung verfügbar."
                st.markdown(f"**Über das Unternehmen:**\n{desc}")
                st.caption(f"Fundamental: KGV {row['PE_Display']} | Marge {row['Marge']:.1f}% | Wachstum {row['Wachstum']:.1f}% | Schulden {row['Debt']:.0f}%")

# ==============================================================================
# TAB 2: PORTFOLIO
# ==============================================================================
with tab_port:
    # Unter-Reiter
    sub_stock, sub_crypto, sub_total = st.tabs(["🏭 Aktien", "🪙 Krypto-Währungen", "🏠 Gesamt-Übersicht"])

    # === 1. AKTIEN ===
    with sub_stock:
        if not df_stocks.empty:
            # KPIs
            val_s = df_stocks['RealWert'].sum()
            gv_s = df_stocks['Gewinn'].sum()
            div_s = df_stocks['Div_Euro_Calc'].sum()
            count_s = len(df_stocks)
            
            # BANNER AKTIEN
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Gesamtwert (Aktien)", f"{val_s:.2f} €")
            k2.metric("Gewinn / Verlust", f"{gv_s:.2f} €", delta_color="normal")
            k3.metric("⚠️ Warnungen", int(warn_stocks), delta_color="inverse")
            k4.metric("📊 Aufteilung", f"{count_s} Positionen")
            
            st.divider()
            
            # Extra Metriken
            e1, e2 = st.columns(2)
            avg_div = df_stocks[df_stocks['DivRendite'] > 0]['DivRendite'].mean() if not df_stocks.empty else 0
            e1.metric("Ø Dividenden-Rendite", f"{avg_div:.2f} %")
            e2.metric("💰 Erwarteter Cashflow (Jahr)", f"{div_s:.2f} €", help="Summe der Dividenden (Basierend auf Rendite * Wert)")
            
            st.subheader("Aktien Bestände")
            # FIX: width="stretch" statt use_container_width=True
            st.dataframe(
                df_stocks[['Name', 'Anzahl', 'Akt. Kurs [€]', 'RealWert', 'Gewinn', 'DivRendite', 'Div_Euro_Calc']],
                width="stretch", hide_index=True,
                column_config={
                    "Name": st.column_config.TextColumn("Name"),
                    "Akt. Kurs [€]": st.column_config.NumberColumn("Kurs", format="%.2f €"),
                    "RealWert": st.column_config.NumberColumn("Wert", format="%.2f €"),
                    "Gewinn": st.column_config.NumberColumn("G/V", format="%.2f €"),
                    "DivRendite": st.column_config.NumberColumn("Div %", format="%.2f %%"),
                    "Div_Euro_Calc": st.column_config.NumberColumn("Div €", format="%.2f €", help="Jährliche Auszahlung in Euro"),
                }
            )
        else:
            st.info("Keine Aktien im Portfolio.")

    # === 2. KRYPTO ===
    with sub_crypto:
        if not df_crypto.empty:
            # KPIs
            val_c = df_crypto['RealWert'].sum()
            gv_c = df_crypto['Gewinn'].sum()
            count_c = len(df_crypto)
            
            # BANNER KRYPTO
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Gesamtwert (Krypto)", f"{val_c:.2f} €")
            k2.metric("Gewinn / Verlust", f"{gv_c:.2f} €", delta_color="normal")
            k3.metric("⚠️ Warnungen", int(warn_crypto), delta_color="inverse")
            k4.metric("📊 Aufteilung", f"{count_c} Coins")
            
            st.divider()
            
            st.subheader("Krypto Bestände")
            # FIX: width="stretch" statt use_container_width=True
            st.dataframe(
                df_crypto[['Name', 'Anzahl', 'Akt. Kurs [€]', 'RealWert', 'Gewinn']],
                width="stretch", hide_index=True,
                column_config={
                    "Name": st.column_config.TextColumn("Coin"),
                    "Akt. Kurs [€]": st.column_config.NumberColumn("Kurs", format="%.4f €"),
                    "RealWert": st.column_config.NumberColumn("Wert", format="%.2f €"),
                    "Gewinn": st.column_config.NumberColumn("G/V", format="%.2f €"),
                }
            )
        else:
            st.info("Keine Krypto-Währungen im Portfolio.")

    # === 3. GESAMT ===
    with sub_total:
        # KPIs Gesamt
        total_val = df_pf['RealWert'].sum()
        total_gv = df_pf['Gewinn'].sum()
        
        # BANNER GESAMT
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Gesamtwert (Portfolio)", f"{total_val:.2f} €")
        k2.metric("Gewinn / Verlust", f"{total_gv:.2f} €", delta_color="normal")
        k3.metric("⚠️ Warnungen", int(warn_total), delta_color="inverse")
        k4.metric("📊 Aufteilung", f"{len(df_stocks)} Aktien / {len(df_crypto)} Krypto")
        
        st.divider()
        
        c_chart, c_list = st.columns([1, 1])
        
        with c_chart:
            st.subheader("Portfolio Verteilung")
            if total_val > 0:
                # Gewichtung nach Wert
                fig_pie = px.pie(df_pf, values='RealWert', names='Name', title="Gewichtung nach Wert", hole=0.4)
                # FIX: width="stretch" statt use_container_width=True
                st.plotly_chart(fig_pie, width="stretch")
        
        with c_list:
            st.subheader("🚨 Handlungsbedarf")
            warns = df_pf[df_pf['Warnung'].notna()]
            if not warns.empty:
                st.error(f"{len(warns)} aktive Signale!")
                # FIX: width="stretch" statt use_container_width=True
                st.dataframe(
                    warns[['Name', 'Warnung', 'Akt. Kurs [€]', 'Stop Loss']],
                    width="stretch", hide_index=True,
                    column_config={
                        "Warnung": st.column_config.TextColumn("Signal"),
                        "Akt. Kurs [€]": st.column_config.NumberColumn("Kurs", format="%.2f €"),
                        "Stop Loss": st.column_config.NumberColumn("Limit", format="%.2f €"),
                    }
                )
            else:
                st.success("✅ Alles ruhig. Keine Warnungen.")