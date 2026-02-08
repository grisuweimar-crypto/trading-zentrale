import streamlit as st
import pandas as pd
import json
import plotly.graph_objects as go
from pathlib import Path
import re

# ===== CONFIG =====
CSV_PATH = 'watchlist.csv'
PAGE_TITLE = "Trading Zentrale — Interactive Radar Dashboard"

# Sektor-Konfiguration
DISPLAY_SEKTOREN = [
    ("automation", "Automation & Robotik 🤖"),
    ("ki_chips", "KI, Chips & Cloud 🧠"),
    ("energie", "Energie & Speicher ⚡"),
    ("metalle", "Metalle & Rohstoffe 🔩"),
    ("gold_silber", "Gold & Silber ⛏️"),
    ("konsum", "Konsum & Marken 🛒"),
    ("finanzen", "Finanzen & Zahlungsverkehr 💳"),
    ("gesundheit", "Gesundheit & Biotech 💊"),
    ("infra", "Infra & Versorger 🏭"),
    ("krypto_core", "Krypto Core 🪙"),
    ("krypto_sat", "Krypto Satelliten 🪙"),
    ("experimente", "Experimente & High Risk 🧪"),
    ("medien", "Medien & Digitales 📱"),
    ("andere", "Andere 🌐"),
]
DISPLAY_BY_KEY = dict(DISPLAY_SEKTOREN)

SECTOR_COLORS = {
    'ki_chips': '#3b82f6',
    'gold_silber': '#f59e0b',
    'energie': '#f97316',
    'konsum': '#0ea5a4',
    'finanzen': '#ef4444',
    'gesundheit': '#8b5cf6',
    'automation': '#06b6d4',
    'metalle': '#64748b',
    'infra': '#10b981',
    'krypto_core': '#8b5cf6',
    'krypto_sat': '#8b5cf6',
    'experimente': '#ec4899',
    'medien': '#06b6d4',
    'andere': '#475569'
}

# ===== HELPER FUNCTIONS =====
def _norm(s):
    """Sektor-String für Abgleich: Klein, ohne Emojis."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s = str(s).strip().upper()
    s = re.sub(r"[^\w\s&/-]", "", s)
    return s.replace(" ", "")

def normalize_sektor(row):
    """Ordnet einen Watchlist-Eintrag einer festen Sektor-Säule zu."""
    sektor_raw = row.get("Sektor", "")
    name = str(row.get("Name", "") or "").upper()
    ticker = str(row.get("Ticker", "") or "").upper()
    s = _norm(sektor_raw)

    if "BITCOIN" in name or "BTC" in ticker or (s and "KRYPTO" in s and "BITCOIN" in name):
        return "krypto_core", DISPLAY_BY_KEY["krypto_core"]
    if "ETHEREUM" in name or "ETH" in ticker:
        return "krypto_core", DISPLAY_BY_KEY["krypto_core"]
    if any(x in s for x in ["KRYPTO"]) or any(x in ticker for x in ["SOL", "ADA", "DOGE", "XRP", "COIN"]):
        return "krypto_sat", DISPLAY_BY_KEY["krypto_sat"]

    if "KOREA" in name or ("ELECTRIC" in name and "POWER" in name) or "KEP" in ticker:
        return "infra", DISPLAY_BY_KEY["infra"]
    if "VERSORGER" in s or "INFRA" in s:
        return "infra", DISPLAY_BY_KEY["infra"]

    if "EXPERIMENTE" in s:
        return "experimente", DISPLAY_BY_KEY["experimente"]
    if any(x in name for x in ["FLUENCE", "LARGO", "QUANTUMSCAPE", "SOLVAY"]):
        return "experimente", DISPLAY_BY_KEY["experimente"]
    if "INDUSTRIE" in s and "AUTO" in s:
        return "experimente", DISPLAY_BY_KEY["experimente"]

    if "HARDWARE" in s or "ROBOTIK" in s or "AUTOMATION" in s:
        return "automation", DISPLAY_BY_KEY["automation"]
    if any(x in name for x in ["COGNEX", "FANUC", "YASKAWA", "AUTOSTORE", "ABB", "TERADYNE", "KEYENCE"]):
        return "automation", DISPLAY_BY_KEY["automation"]

    if "GEHIRN" in s or "TECH" in s or "SOFTWARE" in s or "E-COMMERCE" in str(sektor_raw) or "ECOM" in s:
        return "ki_chips", DISPLAY_BY_KEY["ki_chips"]
    if any(x in name for x in ["ASML", "ALPHABET", "AMAZON", "INFINEON", "SAP", "MICROSOFT", "NVIDIA", "ORACLE"]):
        return "ki_chips", DISPLAY_BY_KEY["ki_chips"]

    if "ENERGIE" in s or "ENERGY" in s:
        return "energie", DISPLAY_BY_KEY["energie"]
    if any(x in name for x in ["CAMECO", "EXXON", "FIRST SOLAR", "CATL", "OCCIDENTAL", "CHEVRON"]):
        return "energie", DISPLAY_BY_KEY["energie"]

    if "FUNDAMENT" in s or "RECYCLING" in s or "ROHSTOFFE" in s:
        return "metalle", DISPLAY_BY_KEY["metalle"]
    if any(x in name for x in ["FREEPORT", "AURUBIS", "CONSTELLIUM", "VALE", "POSCO", "UMICORE"]):
        return "metalle", DISPLAY_BY_KEY["metalle"]

    if "EDELMETALLE" in s or "GOLD" in s or "SILBER" in s:
        return "gold_silber", DISPLAY_BY_KEY["gold_silber"]
    if any(x in name for x in ["SILVER", "GOLD", "AGNICO", "HECLA", "BARRICK", "NEWMONT"]):
        return "gold_silber", DISPLAY_BY_KEY["gold_silber"]

    if "KONSUM" in s or "LIFESTYLE" in s:
        return "konsum", DISPLAY_BY_KEY["konsum"]
    if any(x in name for x in ["CAMPBELL", "LVMH", "NESTLE", "NIKE", "COCA", "PEPSI"]):
        return "konsum", DISPLAY_BY_KEY["konsum"]

    if "FINANZEN" in s or "FINTECH" in s:
        return "finanzen", DISPLAY_BY_KEY["finanzen"]
    if any(x in name for x in ["ALLIANZ", "DEUTSCHE BANK", "BLOCK", "COINBASE", "MASTERCARD", "PAYPAL"]):
        return "finanzen", DISPLAY_BY_KEY["finanzen"]

    if "PHARMA" in s or "GESUNDHEIT" in s or "HEALTH" in s:
        return "gesundheit", DISPLAY_BY_KEY["gesundheit"]
    if any(x in name for x in ["BAYER", "PFIZER", "NOVARTIS", "ROCHE", "JOHNSON", "ABBOTT"]):
        return "gesundheit", DISPLAY_BY_KEY["gesundheit"]

    if "MEDIEN" in s or "DIGITALES" in s:
        return "medien", DISPLAY_BY_KEY["medien"]
    if any(x in name for x in ["NETFLIX", "SPOTIFY", "LUMEN"]):
        return "medien", DISPLAY_BY_KEY["medien"]

    return "andere", DISPLAY_BY_KEY["andere"]

def create_radar_chart(radar_vector, sektor_key, benchmark_by_sector, benchmark_global):
    """Erstellt ein Plotly Radar-Chart mit Asset + Benchmark."""
    if not radar_vector or not isinstance(radar_vector, list) or len(radar_vector) != 5:
        return None
    
    labels = ['Wachstum', 'Rentabilität', 'Sicherheit', 'Technik', 'Bewertung']
    
    # Benchmark: Sektor oder Global
    benchmark = benchmark_by_sector.get(sektor_key, benchmark_global) if sektor_key and benchmark_by_sector else benchmark_global
    if not benchmark or not isinstance(benchmark, list) or len(benchmark) != 5:
        benchmark = benchmark_global
    
    fig = go.Figure()
    
    # Benchmark (Hintergrund — grau/transparent)
    fig.add_trace(go.Scatterpolar(
        r=benchmark,
        theta=labels,
        fill='toself',
        name='Benchmark',
        line=dict(color='rgba(148,163,184,0.5)', width=1.5),
        fillcolor='rgba(148,163,184,0.08)',
        hovertemplate='<b>%{theta}</b><br>Benchmark: %{r:.1f}<extra></extra>'
    ))
    
    # Asset-Daten (Vordergrund — blau/gold)
    fig.add_trace(go.Scatterpolar(
        r=radar_vector,
        theta=labels,
        fill='toself',
        name='Dieses Asset',
        line=dict(color='#3b82f6', width=2.5),
        fillcolor='rgba(59,130,246,0.25)',
        marker=dict(color='#f59e0b', size=8),
        hovertemplate='<b>%{theta}</b><br>Wert: %{r:.1f}<extra></extra>'
    ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100],
                gridcolor='rgba(255,255,255,0.08)',
                linecolor='rgba(255,255,255,0.1)',
                tickcolor='rgba(148,163,184,0.6)',
                tickfont=dict(size=9, color='rgba(148,163,184,0.8)')
            ),
            angularaxis=dict(
                gridcolor='rgba(255,255,255,0.05)',
                linecolor='rgba(255,255,255,0.1)',
                tickfont=dict(size=11, color='#cbd5e1', family='Arial')
            ),
            bgcolor='rgba(0,0,0,0)'
        ),
        paper_bgcolor='rgba(15,23,42,0.95)',
        plot_bgcolor='rgba(15,23,42,0.95)',
        font=dict(color='#cbd5e1', family='Arial'),
        hovermode='closest',
        showlegend=True,
        legend=dict(
            x=0.5,
            y=-0.15,
            xanchor='center',
            yanchor='top',
            orientation='h',
            font=dict(size=10),
            bgcolor='rgba(0,0,0,0)',
            bordercolor='rgba(148,163,184,0.2)',
            borderwidth=0
        ),
        margin=dict(l=40, r=40, t=40, b=80),
        height=380
    )
    
    return fig

@st.cache_data
def load_data():
    """Lädt CSV und berechnet Benchmarks."""
    try:
        df = pd.read_csv(CSV_PATH)
    except FileNotFoundError:
        st.error(f"CSV nicht gefunden: {CSV_PATH}")
        return None, {}, {}
    
    # Sektor-Mapping
    df['Sektor_Key'], df['Sektor'] = zip(*df.apply(normalize_sektor, axis=1))
    df = df.sort_values(by='Score', ascending=False)
    
    # Benchmark: Parse Radar Vectors
    sector_sums = {}
    sector_counts = {}
    overall_sum = [0.0, 0.0, 0.0, 0.0, 0.0]
    total_count = 0
    
    for _, row in df.iterrows():
        rv = row.get('Radar Vector', '')
        if not rv:
            continue
        try:
            vec = json.loads(rv) if isinstance(rv, str) else rv
        except Exception:
            continue
        if not isinstance(vec, (list, tuple)) or len(vec) != 5:
            continue
        
        total_count += 1
        for i in range(5):
            overall_sum[i] += float(vec[i] or 0)
        
        sk = row.get('Sektor_Key', 'andere')
        if sk not in sector_sums:
            sector_sums[sk] = [0.0, 0.0, 0.0, 0.0, 0.0]
            sector_counts[sk] = 0
        for i in range(5):
            sector_sums[sk][i] += float(vec[i] or 0)
        sector_counts[sk] += 1
    
    overall_avg = [round(overall_sum[i] / total_count, 2) if total_count else 0.0 for i in range(5)]
    sector_avgs = {}
    for sk, sums in sector_sums.items():
        cnt = sector_counts.get(sk, 0)
        sector_avgs[sk] = [round(sums[i] / cnt, 2) if cnt else 0.0 for i in range(5)]
    
    return df, sector_avgs, overall_avg

# ===== STREAMLIT PAGE CONFIG =====
st.set_page_config(
    page_title=PAGE_TITLE,
    layout="wide",
    initial_sidebar_state="expanded"
)

# ===== CUSTOM CSS (Dark Theme) =====
st.markdown("""
<style>
    [data-testid="stAppViewContainer"] {
        background-color: #020617;
        color: #f1f5f9;
    }
    [data-testid="stSidebar"] {
        background-color: #0f1729;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: transparent;
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 8px;
        padding: 8px 16px;
        color: #94a3b8;
    }
    .stTabs [aria-selected="true"] [data-baseweb="tab"] {
        background-color: rgba(16,185,129,0.15);
        color: #10b981;
        border-color: rgba(16,185,129,0.5);
    }
    .stDataFrame {
        background-color: #0f1729;
    }
    /* Metric cards */
    [data-testid="metric-container"] {
        background-color: rgba(15,23,42,0.8);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 12px;
        padding: 16px;
    }
</style>
""", unsafe_allow_html=True)

# ===== LOAD DATA =====
df, sector_avgs, overall_avg = load_data()

if df is None:
    st.stop()

# ===== SESSION STATE für Row-Click =====
if 'selected_ticker' not in st.session_state:
    st.session_state.selected_ticker = df.iloc[0]['Ticker'] if len(df) > 0 else None

# ===== HEADER =====
col1, col2 = st.columns([3, 1])
with col1:
    st.title("🚀 Trading Zentrale")
    st.caption("*Interactive Multi-Factor Radar Dashboard — Real-time Watchlist*")
with col2:
    st.info(f"📊 {len(df)} Assets")

st.divider()

# ===== SIDEBAR: FILTER + INFO =====
with st.sidebar:
    st.subheader("⚙️ Filter & Einstellungen")
    
    selected_sektoren = st.multiselect(
        "Nach Sektor filtern:",
        options=[key for key, _ in DISPLAY_SEKTOREN],
        format_func=lambda x: DISPLAY_BY_KEY.get(x, x),
        default=[key for key, _ in DISPLAY_SEKTOREN]
    )
    
    st.markdown("---")
    st.subheader("📖 Über dieses System")
    with st.expander("Radar Erklärung", expanded=False):
        st.markdown("""
        **5-Faktor Radar:**
        - **Wachstum**: Umsatzwachstum & zukünftiges Potenzial (0–100)
        - **Rentabilität**: ROE und Gewinn-Marge (0–100)
        - **Sicherheit**: 1 / Debt-to-Equity Ratio (niedrig verschuldet = höher)
        - **Technik**: Zyklus-Position + Elliott-Setup-Bonus (0–100)
        - **Bewertung**: Upside-Potenzial vs. aktueller Kurs (0–100)
        
        **Benchmark:** Grauer Bereich = Durchschnitt dieses Sektors (oder global)
        """)
    
    with st.expander("Scoring System", expanded=False):
        st.markdown("""
        Der Gesamtscore (0–145) kombiniert:
        - **Technik** (Elliott + Monte-Carlo) = 0–30 Punkte
        - **Fundamental** (ROE, Wachstum, Marge) = 0–50 Punkte
        - **CRV-Ranking** = ±15 Punkte
        - **Konfluenz-Bonus** = +15 Punkte
        - **Spezial-Boni** (Krypto, Newbuys) = +15 Punkte
        
        **Signale:**
        - *BUY*: Elliott Wave Setup (Welle 2 Korrektur) erkannt
        - *NO SETUP*: Keine klare Einstiegsmöglichkeit
        """)

st.markdown("---")

# ===== FILTER ANWENDEN =====
df_filtered = df[df['Sektor_Key'].isin(selected_sektoren)].copy()

# ===== MAIN CONTENT: RADAR + TABLE =====
# --- RADAR SECTION (oben) ---
st.subheader("📊 Multi-Factor Radar — Live Ansicht")

selected_row = None
if st.session_state.selected_ticker:
    matching_rows = df_filtered[df_filtered['Ticker'] == st.session_state.selected_ticker]
    if not matching_rows.empty:
        selected_row = matching_rows.iloc[0]

if selected_row is not None:
    name = selected_row.get('Name', '?')
    ticker = selected_row.get('Ticker', '?')
    sektor_key = selected_row.get('Sektor_Key', 'andere')
    radar_json = selected_row.get('Radar Vector', '')
    
    try:
        radar_vector = json.loads(radar_json) if isinstance(radar_json, str) else None
    except Exception:
        radar_vector = None
    
    col1, col2 = st.columns([2, 1])
    with col1:
        st.write(f"**{name}** ({ticker})")
    with col2:
        sektor_label = DISPLAY_BY_KEY.get(sektor_key, sektor_key)
        st.write(f"*{sektor_label}*")
    
    if radar_vector:
        fig = create_radar_chart(radar_vector, sektor_key, sector_avgs, overall_avg)
        if fig:
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
        else:
            st.warning("Radar-Daten ungültig oder fehlernd.")
    else:
        st.warning("Keine Radar-Vektordaten für dieses Asset verfügbar.")
else:
    st.info("Wählen Sie eine Aktie aus der Tabelle unten aus, um das Radar zu sehen.")

st.markdown("---")

# --- TABLE SECTION (darunter) ---
st.subheader("📋 Watchlist — Interaktive Tabelle")

if len(df_filtered) == 0:
    st.warning("Keine Assets in dieser Sektor-Auswahl gefunden.")
else:
    # Tabelle mit Auswahl
    display_cols = ['Name', 'Ticker', 'Sektor', 'Akt. Kurs', 'ROE %', 'Debt/Equity', 'Score', 'Elliott-Signal', 'CRV', 'Zyklus %']
    available_cols = [col for col in display_cols if col in df_filtered.columns]
    
    table_data = df_filtered[available_cols].copy()
    
    # Formatierung für Anzeige
    table_data_display = table_data.copy()
    for col in table_data_display.columns:
        if col in ['Akt. Kurs', 'ROE %', 'Debt/Equity', 'Score', 'CRV', 'Zyklus %']:
            try:
                table_data_display[col] = table_data_display[col].apply(lambda x: f"{float(x):.2f}" if pd.notna(x) else "–")
            except Exception:
                pass
    
    # Markiere die ausgewählte Reihe
    st.dataframe(
        table_data_display,
        use_container_width=True,
        height=400,
        column_config={
            'Name': st.column_config.TextColumn("Asset"),
            'Ticker': st.column_config.TextColumn("Symbol"),
            'Score': st.column_config.NumberColumn("Score", format="%d"),
        }
    )
    
    st.write("---")
    st.write("**Klick auf einen Ticker unten, um das Radar zu aktualisieren:**")
    
    # Ticker-Buttons
    cols = st.columns(6)
    for idx, ticker in enumerate(df_filtered['Ticker'].head(12).values):
        col_idx = idx % 6
        with cols[col_idx]:
            if st.button(ticker, key=f"ticker_{ticker}"):
                st.session_state.selected_ticker = ticker
                st.rerun()

st.divider()

# ===== FOOTER =====
st.markdown("""
<div style="text-align:center; margin-top:40px; color:#64748b; font-size:11px;">
    ⚠️ <strong>Disclaimer:</strong> Private Nutzung — keine Anlageberatung. 
    Experimentelles System; eigene Due Diligence erforderlich.
    <br><br>
    📧 Für Fehler oder Feedback: scanner-support@local
</div>
""", unsafe_allow_html=True)
