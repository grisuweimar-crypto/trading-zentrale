import pandas as pd
import datetime
import re
import json
import logging

logger = logging.getLogger(__name__)

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

def _norm(s):
    """Sektor-String normalisieren."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s = str(s).strip().upper()
    s = re.sub(r"[^\w\s&/-]", "", s)
    return s.replace(" ", "")

def normalize_sektor(row):
    """Ordnet Watchlist-Eintrag einer Sektor-Säule zu."""
    sektor_raw = row.get("Sektor", "")
    name = str(row.get("Name", "") or "").upper()
    ticker = str(row.get("Ticker", "") or "").upper()
    s = _norm(sektor_raw)

    # Krypto Core vs Satelliten
    if "BITCOIN" in name or "BTC" in ticker:
        return "krypto_core", DISPLAY_BY_KEY["krypto_core"]
    if "ETHEREUM" in name or "ETH" in ticker:
        return "krypto_core", DISPLAY_BY_KEY["krypto_core"]
    if any(x in s for x in ["KRYPTO"]) or any(x in ticker for x in ["SOL", "ADA", "DOGE", "XRP"]):
        return "krypto_sat", DISPLAY_BY_KEY["krypto_sat"]

    # Infra
    if "KOREA" in name or ("ELECTRIC" in name and "POWER" in name) or "KEP" in ticker:
        return "infra", DISPLAY_BY_KEY["infra"]
    if "VERSORGER" in s or "INFRA" in s:
        return "infra", DISPLAY_BY_KEY["infra"]

    # Experimente
    if "EXPERIMENTE" in s or any(x in name for x in ["FLUENCE", "LARGO", "SOLVAY"]):
        return "experimente", DISPLAY_BY_KEY["experimente"]

    # Automation
    if "HARDWARE" in s or "ROBOTIK" in s or any(x in name for x in ["COGNEX", "FANUC", "ABB"]):
        return "automation", DISPLAY_BY_KEY["automation"]

    # KI & Chips
    if "GEHIRN" in s or "TECH" in s or "SOFTWARE" in s:
        return "ki_chips", DISPLAY_BY_KEY["ki_chips"]
    if any(x in name for x in ["ASML", "ALPHABET", "AMAZON", "MICROSOFT", "NVIDIA"]):
        return "ki_chips", DISPLAY_BY_KEY["ki_chips"]

    # Energie
    if "ENERGIE" in s or "ENERGY" in s:
        return "energie", DISPLAY_BY_KEY["energie"]
    if any(x in name for x in ["CAMECO", "EXXON", "CHEVRON"]):
        return "energie", DISPLAY_BY_KEY["energie"]

    # Metalle
    if "FUNDAMENT" in s or "RECYCLING" in s:
        return "metalle", DISPLAY_BY_KEY["metalle"]

    # Gold & Silber
    if "EDELMETALLE" in s or "GOLD" in s or "SILBER" in s:
        return "gold_silber", DISPLAY_BY_KEY["gold_silber"]

    # Konsum
    if "KONSUM" in s or any(x in name for x in ["NESTLE", "NIKE", "COCA"]):
        return "konsum", DISPLAY_BY_KEY["konsum"]

    # Finanzen
    if "FINANZEN" in s or "FINTECH" in s:
        return "finanzen", DISPLAY_BY_KEY["finanzen"]

    # Gesundheit
    if "PHARMA" in s or "GESUNDHEIT" in s:
        return "gesundheit", DISPLAY_BY_KEY["gesundheit"]

    # Medien
    if "MEDIEN" in s or any(x in name for x in ["NETFLIX", "SPOTIFY"]):
        return "medien", DISPLAY_BY_KEY["medien"]

    return "andere", DISPLAY_BY_KEY["andere"]

def generate_dashboard(csv_path='watchlist.csv', output_path='index.html'):
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        logger.error(f"Fehler beim Lesen von {csv_path}: {e}")
        return

    # Sektor-Mapping
    out = df.apply(normalize_sektor, axis=1)
    df["Sektor_Key"] = [o[0] for o in out]
    df["Sektor"] = [o[1] for o in out]

    def get_empfehlung(row, display_currency="€"):
        """Empfehlung basierend auf Score, Signal, Zyklus."""
        score = float(row.get('Score', 0))
        signal = str(row.get('Elliott-Signal', '')).upper()
        cycle = float(row.get('Zyklus %', 50))
        e_entry = row.get('Elliott-Einstieg', 0)
        e_entry = float(e_entry) if e_entry else 0

        if signal != "BUY":
            return {"badge": "Kein Setup", "badge_class": "bg-slate-700 text-slate-400 border border-slate-600", "line2": "–"}

        if cycle <= 20 and score >= 95:
            return {"badge": "Starkes Setup", "badge_class": "bg-emerald-500/20 text-emerald-400 border border-emerald-500/40", "line2": f"ab {e_entry:.2f} {display_currency}"}
        elif cycle <= 40 and score >= 75:
            return {"badge": "Setup ok", "badge_class": "bg-amber-500/20 text-amber-400 border border-amber-500/40", "line2": f"ab {e_entry:.2f} {display_currency}"}
        elif cycle > 60:
            return {"badge": "Vorsicht: Hoch", "badge_class": "bg-rose-500/20 text-rose-400 border border-rose-500/40", "line2": "nur Korrektur"}
        else:
            return {"badge": "Beobachten", "badge_class": "bg-slate-500/20 text-slate-300 border border-slate-500/40", "line2": "–"}

    # Sektor-Farben
    sector_colors = {
        'ki_chips': '#3b82f6', 'gold_silber': '#f59e0b', 'energie': '#f97316',
        'konsum': '#0ea5a4', 'finanzen': '#ef4444', 'gesundheit': '#8b5cf6',
        'automation': '#06b6d4', 'metalle': '#64748b', 'infra': '#10b981',
        'krypto_core': '#8b5cf6', 'krypto_sat': '#8b5cf6', 'experimente': '#ec4899',
        'medien': '#06b6d4', 'andere': '#475569'
    }

    # Filter-Buttons
    present_keys = set(df["Sektor_Key"].unique())
    neutral = sector_colors.get('andere', '#475569')
    filter_buttons_html = f'<button onclick="filterSektor(\'Alle\')" class="px-4 py-2 rounded-full text-xs font-bold sector-btn" style="border:2px solid {neutral}; color:#f1f5f9; background:transparent;">Alle</button>\n'
    for key, label in DISPLAY_SEKTOREN:
        if key in present_keys:
            color = sector_colors.get(key, neutral)
            filter_buttons_html += f'                    <button onclick="filterSektor(\'{key}\')" class="px-4 py-2 rounded-full text-xs font-bold sector-btn" style="border:2px solid {color}; color:#f1f5f9; background:transparent;" data-sektor="{key}">{label}</button>\n'

    df = df.sort_values(by='Score', ascending=False)
    
    # Benchmark-Berechnung
    sector_sums = {}
    sector_counts = {}
    overall_sum = [0.0, 0.0, 0.0, 0.0, 0.0]
    total_count = 0
    
    for _, r in df.iterrows():
        rv = r.get('Radar Vector', '')
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
        
        sk = r.get('Sektor_Key', 'andere')
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

    timestamp = datetime.datetime.now().strftime('%d.%m.%Y um %H:%M:%S')
    
    logger.info("Dashboard-Generierung gestartet...")
    
    html_template = f"""
    <!DOCTYPE html>
    <html lang="de">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Trading-Zentrale Ultimate</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <script src="https://cdn.jsdelivr.net/npm/apexcharts@3.45.1/dist/apexcharts.min.js"></script>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
        <style>
            :root {{
                --bg: #020617;
                --muted: #94a3b8;
                --emerald: #10b981;
                --ki-blue: #3b82f6;
                --gold: #ffd700;
                --info-bg: #1e293b;
                --info-accent: #334155;
                --info-text: #cbd5e1;
            }}
            body {{ background: var(--bg); color: #f1f5f9; font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; }}
            .glass {{ background: rgba(15, 23, 42, 0.8); backdrop-filter: blur(10px); border: 1px solid rgba(255,255,255,0.06); }}
            /* Desktop: Header bleibt oben kleben */
            @media (min-width: 1024px) {{
                .sticky-header {{
                    position: sticky;
                    top: 0;
                    z-index: 50;
                    background: var(--bg);
                    padding-bottom: 1rem;
                }}
            }}

            /* Handy: Header scrollt mit, damit Platz für die Tabelle ist */
            @media (max-width: 1023px) {{
                .sticky-header {{
                    position: relative !important;
                    margin-bottom: 1rem;
                }}
                .filter-container {{
                    display: flex;
                    flex-wrap: wrap !important;
                    gap: 6px !important;
                    padding: 10px 0;
                }}
                .sector-btn {{
                    padding: 6px 10px !important;
                    font-size: 10px !important;
                }}
                h1 {{ font-size: 1.5rem !important; }}
            }}

            /* Der entscheidende Container für das seitliche Wischen */
            .table-container {{
                width: 100%;
                overflow-x: auto;
                -webkit-overflow-scrolling: touch;
                margin-top: 10px;
                border-radius: 1rem;
                background: rgba(15, 23, 42, 0.5);
            #mainTable {{
                width: 100%; /* Am PC nutzt sie den verfügbaren Platz */
                table-layout: auto !important;
            }}

            /* Nur auf mobilen Geräten erzwingen wir die Breite zum Wischen */
            @media (max-width: 1023px) {{
                #mainTable {{
                    min-width: 1100px; 
                }}
            }}
            .tooltip {{ visibility: hidden; opacity: 0; transition: opacity 0.18s; position: absolute; z-index: 99999; pointer-events: none; }}
            .has-tooltip:hover .tooltip, .has-tooltip:active .tooltip {{ visibility: visible; opacity: 1; }}
            th {{ cursor: pointer; transition: color 0.15s; }}
            th:hover {{ color: var(--emerald) !important; }}
            .no-scrollbar::-webkit-scrollbar {{ display: none; }}
            .overflow-y-auto {{ overflow-y: auto; }}
            .overflow-x-hidden {{ overflow-x: hidden; }}
            tr:hover {{ background: rgba(255,255,255,0.03); }}
            
            /* Tiles (3x5 Grid für 15-Faktor-Matrix) */
            .tiles-grid {{ display: grid; grid-template-columns: repeat(5, 10px); grid-auto-rows: 10px; gap: 6px; justify-items: center; align-items: center; margin-top: 8px; }}
            .tile {{ width: 10px; height: 10px; border-radius: 2px; cursor: help; }}
            .tile.green {{ background: #10b981; }}
            .tile.red {{ background: #ef4444; }}
            .tile.gray {{ background: #374151; }}
            
            /* Monospace für numerische Spalten */
            .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, "Roboto Mono", "Courier New", monospace; }}
            
            /* Table layout */
            table {{ table-layout: fixed; width: 100%; border-collapse: collapse; }}
            th, td {{ padding: 8px 10px; max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
            @media(min-width:1024px) {{ th, td {{ padding: 10px 12px; }} }}

            /* Radar Tooltip */
            .radar-tooltip {{ 
                background: rgba(0,0,0,0.95) !important; 
                border: 2px solid rgba(16,185,129,0.4); 
                box-shadow: 0 0 30px rgba(16,185,129,0.2) !important; 
                z-index: 99999 !important;
            }}
            .radar-container {{
                width: 240px;
                height: 240px;
                position: relative;
                margin: 8px auto 12px;
                border: 1px solid rgba(255,255,255,0.1);
                border-radius: 8px;
                background: rgba(0,0,0,0.3);
            }}
            
            /* Info overlay */
            #infoOverlay {{
                position: fixed;
                top: 50%;
                left: 50%;
                transform: translate(-50%, -50%);
                width: 92%;
                max-width: 1100px;
                max-height: 85vh; /* Begrenzt die Höhe auf 85% des Bildschirms */
                overflow-y: auto; /* Aktiviert das Scrollen innerhalb des Fensters */
                z-index: 100;
                box-shadow: 0 10px 40px rgba(0,0,0,0.6);
                border-radius: 12px;
                display: none;
                -webkit-overflow-scrolling: touch; /* Flüssiges Scrollen für Handys */
                background: var(--info-bg); /* Sicherstellen, dass Hintergrund steht */
            }}
            #infoOverlay.open {{ display: block; }}
            .info-overlay-inner {{ background: var(--info-bg); color: var(--info-text); padding: 18px; border: 1px solid var(--info-accent); border-radius: 12px; }}
            .info-panels {{ display: grid; grid-template-columns: 1fr; gap: 12px; }}
            @media(min-width:768px) {{ .info-panels {{ grid-template-columns: repeat(3, 1fr); }}}}
            .info-panel {{ background: transparent; padding: 12px; border-radius: 8px; }}
            .info-panel h3 {{ margin: 0 0 6px 0; font-weight: 800; color: #cbd5e1; }}
            .info-panel p {{ color: #cbd5e1; line-height: 1.5; margin-bottom: 8px; }}
            .info-pill {{ background: var(--info-accent); color: #f1f5f9; padding: 8px 12px; border-radius: 999px; font-weight: 700; cursor: pointer; box-shadow: 0 4px 12px rgba(0,0,0,0.3); transition: all 0.2s; }}
            .info-pill:hover {{ background: #475569; box-shadow: 0 6px 16px rgba(0,0,0,0.4); }}
            .close-btn {{ position: absolute; right: 14px; top: 14px; background: transparent; border: none; color: #cbd5e1; font-weight: 800; cursor: pointer; font-size: 16px; }}
            .radar-fallback {{
                font-size: 10px;
                color: #94a3b8;
                font-family: monospace;
                line-height: 1.6;
                padding: 8px;
                background: rgba(0,0,0,0.3);
                border-radius: 6px;
                border: 1px solid rgba(255,255,255,0.1);
                display: none;
            }}
            
            /* Hover-Cards für Asset und Score */
            .card-hover {{
                visibility: hidden;
                opacity: 0;
                transition: opacity 0.2s, visibility 0.2s;
                position: absolute;
                z-index: 9999;
                pointer-events: none;
            }}
            .cell-with-card {{
                position: relative;
            }}
            .cell-with-card:hover .card-hover {{
                visibility: visible;
                opacity: 1;
                pointer-events: auto;
            }}
            .detail-card {{
                background: var(--info-bg);
                border: 2px solid var(--info-accent);
                border-radius: 12px;
                padding: 14px;
                color: var(--info-text);
                font-size: 11px;
                line-height: 1.6;
                box-shadow: 0 12px 32px rgba(0,0,0,0.8);
                white-space: normal;
                max-width: 300px;
                word-wrap: break-word;
            }}
            
            /* Info-Icon Styling */
            .info-icon {{
                display: inline-flex;
                align-items: center;
                justify-content: center;
                width: 24px;
                height: 24px;
                background: #334155;
                border: 1px solid #475569;
                border-radius: 50%;
                color: #cbd5e1;
                font-weight: bold;
                cursor: help;
                transition: all 0.2s;
                font-size: 12px;
            }}
            .info-icon:hover {{
                background: #475569;
                border-color: #64748b;
                color: #f1f5f9;
                box-shadow: 0 0 8px rgba(6, 182, 212, 0.3);
            }}
            
            /* Globales Tooltip Element */
            #global-tooltip {{
                position: absolute;
                z-index: 9999;
                display: none;
                background: var(--info-bg);
                border: 2px solid var(--info-accent);
                border-radius: 12px;
                padding: 14px;
                color: var(--info-text);
                font-size: 11px;
                line-height: 1.6;
                box-shadow: 0 12px 32px rgba(0,0,0,0.8);
                white-space: normal;
                max-width: 400px;
                word-wrap: break-word;
                pointer-events: none;
            }}
            #global-tooltip.visible {{
                display: block;
                pointer-events: auto;
            }}
        </style>
    </head>
    <body class="p-4 md:p-8">
        <div class="max-w-7xl mx-auto">
            <!-- Header -->
            <div class="flex flex-col md:flex-row justify-between items-center mb-6 gap-4">
                <h1 class="text-4xl font-black text-white tracking-tighter uppercase order-first flex items-center gap-4">
                    <i class="fa-solid fa-chart-line text-3xl" style="color:#06b6d4; text-shadow:0 0 18px rgba(6,182,212,0.45)"></i>
                    <span>Trading-Zentrale <span class="text-emerald-500">Ultimate</span></span>
                </h1>
                <div class="text-right text-sm text-slate-400">
                    <p class="text-xs text-slate-500 font-mono tracking-widest uppercase">Live Terminal v5.0</p>
                    <p class="text-sm text-slate-300 italic">{timestamp}</p>
                </div>
            </div>

            <!-- Disclaimer -->
            <div style="background:#0f0f1e; border:3px solid #ff8c00; border-radius:12px; padding:16px; margin-bottom:24px; box-shadow: 0 0 20px rgba(255,140,0,0.3);">
                <p style="color:#ff8c00; font-size:16px; font-weight:900; margin:0; text-transform:uppercase; letter-spacing:1px;">⚠️ Private Nutzung — Keine Anlageberatung</p>
                <p style="color:#cbd5e1; font-size:13px; margin:8px 0 0 0; line-height:1.6;">Experimentelles System ohne Gewährleistung. Diese Analysen stellen <strong>KEINE ANLAGEEMPFEHLUNG</strong> dar.</p>
            </div>

            <!-- Filter (Sticky) -->
            <div class="mb-4 sticky-header">
                <p class="text-xs font-bold text-slate-300 mb-2">Nach Sektor filtern:</p>
                <div style="display:flex; flex-wrap:wrap; gap:10px; align-items:center; padding-bottom:6px;">
                    {filter_buttons_html}
                </div>
            </div>

            <!-- Suche + Info-Pills -->
            <div class="flex flex-col md:flex-row items-stretch md:items-center justify-start gap-2 mb-6">
                <input type="text" id="searchInput" onkeyup="filterTable()" placeholder="Ticker oder Name..." 
                       class="glass rounded-xl px-5 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-emerald-500 flex-1 md:flex-none md:w-64">
                <div class="flex flex-wrap gap-2">
                    <button style="background:#1e293b; color:#f1f5f9; padding:8px 12px; border-radius:8px; font-weight:700; cursor:pointer; box-shadow:0 4px 12px rgba(0,0,0,0.3); transition:all 0.2s; border:none; white-space:nowrap;" onmouseover="this.style.background='#334155'" onmouseout="this.style.background='#1e293b'" onclick="openInfo()">📋 System <span style="font-size:10px; color:#94a3b8; margin-left:4px;">(i)</span></button>
                    <button style="background:#1e293b; color:#f1f5f9; padding:8px 12px; border-radius:8px; font-weight:700; cursor:pointer; box-shadow:0 4px 12px rgba(0,0,0,0.3); transition:all 0.2s; border:none; white-space:nowrap;" onmouseover="this.style.background='#334155'" onmouseout="this.style.background='#1e293b'" onclick="openInfo()">⚙️ Logik <span style="font-size:10px; color:#94a3b8; margin-left:4px;">(i)</span></button>
                    <button style="background:#1e293b; color:#f1f5f9; padding:8px 12px; border-radius:8px; font-weight:700; cursor:pointer; box-shadow:0 4px 12px rgba(0,0,0,0.3); transition:all 0.2s; border:none; white-space:nowrap;" onmouseover="this.style.background='#334155'" onmouseout="this.style.background='#1e293b'" onclick="openInfo()">🎯 Kacheln <span style="font-size:10px; color:#94a3b8; margin-left:4px;">(i)</span></button>
                </div>
            </div>

            <!-- Floating Info Overlay -->
            <div id="infoOverlay" role="dialog" aria-hidden="true">
                <div class="info-overlay-inner">
                    <button class="close-btn" aria-label="Schließen" onclick="closeInfo()">✕</button>
                    <div class="info-panels">
                        <div class="info-panel">
                            <h3>📋 Mein System</h3>
                            <p><strong>Ich basiere auf einer 15-Faktor-Matrix:</strong></p>
                            <p>Ich kombiniere <strong>Elliott-Wellen-Analyse</strong> mit <strong>fundamentalen Daten</strong> (ROE, Marge, KGV, Verschuldung) und <strong>Monte-Carlo-Simulation</strong>. Jeder Faktor wird als Kachel bewertet: Grün = erfüllt, Rot = nicht erfüllt, Grau = neutral/undefiniert.</p>
                            <p><strong>Meine Kernlogik:</strong> Ich filtere auf Werte, die sowohl technisch als auch fundamental stark sind. Mein Elliott-Setup muss bestätigt sein (BUY-Signal + Kurs unter Target). Mein Score wird durch ROE, Marge, CRV und Monte-Carlo geprägt.</p>
                            <p><strong>Mein Ziel:</strong> Ich suche Konvergenz-Setups mit hohem Risiko-Reward (CRV ≥ 2.0) und multiplen bestätigenden Signalen.</p>
                        </div>
                        <div class="info-panel">
                            <h3>⚙️ Meine Bewertungslogik</h3>
                            <p><strong>Wie ich Kandidaten bewerte:</strong></p>
                            <p><strong>1. Meine fundamentale Basis (K1–K5):</strong> Ich prüfe KGV (sektor-abhängig), Gewinnmarge (&gt;5% = Qualität), ROE (&gt;15% = stark), Verschuldung (D/E &lt;0.5 = sauber) und Dividenden-Qualität.</p>
                            <p><strong>2. Mein Wachstumspotenzial (K6–K10):</strong> Monte-Carlo &gt;70%, Upside zum Target, Wachstum YoY (&gt;10%), Analysten-Rating und Sektoren-Bonusse.</p>
                            <p><strong>3. Mein Timing + CRV (K11–K15):</strong> Elliott-Setup (gültiges BUY, Kurs 2% unter Target), Target-Entfernung, CRV-Basis (≥2.0) und CRV-Top (≥3.0) für maximale Rewards.</p>
                        </div>
                        <div class="info-panel">
                            <h3>🎯 Meine 15-Kachel-Matrix</h3>
                            <div class="grid grid-cols-1 md:grid-cols-3 gap-3 text-sm">
                                <div>
                                    <p class="font-bold" style="color:#cbd5e1;">🔍 Substanz</p>
                                    <ul class="text-sm" style="color:#cbd5e1;">
                                        <li><strong>K1: KGV</strong> – sektor-abhängig</li>
                                        <li><strong>K2: Marge</strong> – &gt;5% oder &gt;20%</li>
                                        <li><strong>K3: ROE</strong> – &gt;15% zeigt Kraft</li>
                                        <li><strong>K4: Verschuldung</strong> – D/E &lt;0.5</li>
                                        <li><strong>K5: Dividenden</strong> – nur mit ROE&gt;10%</li>
                                    </ul>
                                </div>
                                <div>
                                    <p class="font-bold" style="color:#cbd5e1;">📈 Potenzial</p>
                                    <ul class="text-sm" style="color:#cbd5e1;">
                                        <li><strong>K6: Monte Carlo</strong> – &gt;70%</li>
                                        <li><strong>K7: Upside</strong> – zum Target</li>
                                        <li><strong>K8: Wachstum</strong> – &gt;10% YoY</li>
                                        <li><strong>K9: Analysten</strong> – Strong Buy/Buy</li>
                                        <li><strong>K10: Sektor</strong> – Krypto/Energie/KI</li>
                                    </ul>
                                </div>
                                <div>
                                    <p class="font-bold" style="color:#cbd5e1;">⏱️ Timing</p>
                                    <ul class="text-sm" style="color:#cbd5e1;">
                                        <li><strong>K11: Elliott</strong> – BUY, &lt;2% zur Target</li>
                                        <li><strong>K12: Distanz</strong> – min 2% bis Target</li>
                                        <li><strong>K13: CRV-Basis</strong> – ≥2.0</li>
                                        <li><strong>K14: CRV-Top</strong> – ≥3.0</li>
                                        <li><strong>K15: Konfluenz</strong> – mehrere Signale</li>
                                    </ul>
                                </div>
                            </div>
                            <p class="text-xs" style="color:#94a3b8; margin-top:12px;">Farben: Grün = erfüllt · Rot = nicht erfüllt · Grau = neutral</p>
                        </div>
                    </div>
                </div>
            </div>
            <script>
                function openInfo() {{
                    var o = document.getElementById('infoOverlay');
                    if (!o) return;
                    o.classList.add('open');
                    o.setAttribute('aria-hidden', 'false');
                    // Nur den Hintergrund sperren, wenn wir NICHT auf dem Handy sind
                    if (window.innerWidth > 768) {{
                        document.body.style.overflow = 'hidden';
                    }}
                }}
                function closeInfo() {{
                    var o = document.getElementById('infoOverlay');
                    if (!o) return;
                    o.classList.remove('open');
                    o.setAttribute('aria-hidden', 'true');
                    document.body.style.overflow = '';
                }}
                document.addEventListener('keydown', function(e){{ if (e.key === 'Escape') closeInfo(); }});
            </script>

            <!-- Tabelle -->
            <div class="glass rounded-3xl shadow-2xl border-none flex flex-col">
                <div class="table-container overflow-x-auto rounded-3xl" style="width: 100%; max-width: 100vw;">
                    <table class="w-full text-left border-collapse" id="mainTable">
                        <thead class="bg-slate-900/95 border-b border-slate-700 sticky top-0 z-30 backdrop-blur-md">
                            <tr class="text-[10px] text-slate-400 uppercase tracking-widest font-bold">
                                <th class="px-6 py-5" onclick="sortTable(0)">Asset <i class="fa-solid fa-sort ml-1 opacity-30"></i></th>
                                <th class="px-3 py-5 text-center" style="width:40px;">Info</th>
                                <th class="px-6 py-5" onclick="sortTable(1)">Sektor <i class="fa-solid fa-sort ml-1 opacity-30"></i></th>
                                <th class="px-6 py-5 text-right" onclick="sortTable(2)">ROE <i class="fa-solid fa-sort ml-1 opacity-30"></i></th>
                                <th class="px-6 py-5 text-right" onclick="sortTable(3)">Debt/Eq <i class="fa-solid fa-sort ml-1 opacity-30"></i></th>
                                <th class="px-6 py-5 text-right" onclick="sortTable(4)">Div % <i class="fa-solid fa-sort ml-1 opacity-30"></i></th>
                                <th class="px-6 py-5 text-right font-black" onclick="sortTable(5)">Kurs <i class="fa-solid fa-sort ml-1 opacity-30"></i></th>
                                <th class="px-6 py-5 text-center font-bold text-emerald-400" onclick="sortTable(6)">Empfehlung</th>
                                <th class="px-6 py-5 text-right" onclick="sortTable(7)">CRV <i class="fa-solid fa-sort ml-1 opacity-30"></i></th>
                                <th class="px-6 py-5 text-right" onclick="sortTable(8)">Zyklus <i class="fa-solid fa-sort ml-1 opacity-30"></i></th>
                                <th class="px-6 py-5 text-right" onclick="sortTable(9)">Score <i class="fa-solid fa-sort ml-1 opacity-30"></i></th>
                            </tr>
                        </thead>
                        <tbody class="divide-y divide-slate-800">
                        
    """
    
    for idx, row in df.iterrows():
        score = row.get('Score', 0)
        crv = float(row.get('CRV', 0.0))
        ticker = str(row.get('Ticker', '')).strip()
        yh = row.get('Yahoo')
        link_symbol = (str(yh).strip() if yh is not None and not (isinstance(yh, float) and pd.isna(yh)) and str(yh).strip() else '') or ticker

        raw_currency = row.get('Währung', 'USD')
        currency_map = {'USD': '$', 'EUR': '€', 'CHF': 'CHF', 'GBp': 'p', 'CAD': 'C$', 'NOK': 'kr'}
        display_currency = currency_map.get(raw_currency, raw_currency)

        emp = get_empfehlung(row, display_currency)
        sektor_key = row.get('Sektor_Key', 'andere')
        sektor_name = row.get('Sektor', 'Andere')
        
        # Radar-Daten
        radar_json = row.get('Radar Vector', '')
        if not radar_json:
            radar_json = json.dumps([0, 0, 0, 0, 0])
        
        radar_vector_str = radar_json.replace('"', '\\"')
        
        # Fallback-Text
        fallback_text = "–"
        try:
            rv = json.loads(radar_json)
            if len(rv) == 5:
                fallback_text = f"W:{rv[0]:.0f} R:{rv[1]:.0f} S:{rv[2]:.0f} T:{rv[3]:.0f} B:{rv[4]:.0f}"
        except:
            pass

        row_color = sector_colors.get(sektor_key, '#475569')

        perf = float(row.get('Perf %', 0.0))
        perf_icon = "↑" if perf > 0 else "↓" if perf < 0 else "→"
        perf_color = "text-emerald-400 font-bold" if perf > 0 else "text-rose-500 font-bold" if perf < 0 else "text-slate-500"
        crv_color = "text-emerald-400 font-bold" if crv >= 2.0 else ("text-rose-500 font-bold" if crv < 1.0 and crv > 0 else "text-slate-400")

        # === 15-KACHEL-MATRIX IN PYTHON GENERIEREN ===
        def parse_num(val):
            """Hilfsfunktion zum Parsen von Zahlenwerten"""
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return None
            try:
                return float(val)
            except:
                return None
        
        pe_val = parse_num(row.get('PE', 0))
        margin_val = parse_num(row.get('Margin %', 0))
        roe_val = parse_num(row.get('ROE %', 0))
        debt_val = parse_num(row.get('Debt/Equity', 2))
        div_val = parse_num(row.get('Div. Rendite %', 0))
        mc_val = parse_num(row.get('MC-Chance', 0))
        growth_val = parse_num(row.get('Growth %', 0))
        crv_val = parse_num(row.get('CRV', 0))
        signal = str(row.get('Elliott-Signal', 'Warten')).upper()
        target = parse_num(row.get('Elliott-Ausstieg', 0))
        current_price_val = parse_num(row.get('Akt. Kurs', 0))
        
        # Variablen für data-Attribute
        mc_chance_val = row.get('MC-Chance', 0)
        ell_target = row.get('Elliott-Ausstieg', 0)
        ell_entry = row.get('Elliott-Entry', 0)
        rec_val = row.get('Elliott-Signal', 'Warten')
        
        # Tile-Logik: 15 Kacheln pro Faktor
        tiles = []
        
        # K1: KGV-Check (sektor-abhängig)
        if pe_val is None:
            tiles.append(0)  # Gray
        elif sektor_key in ['ki_chips', 'automation', 'medien']:
            tiles.append(1 if pe_val <= 45 else (-1 if pe_val > 80 else 0))
        elif sektor_key in ['metalle', 'gold_silber']:
            tiles.append(1 if pe_val <= 20 else (-1 if pe_val > 35 else 0))
        else:
            tiles.append(1 if pe_val <= 28 else (-1 if pe_val > 80 else 0))
        
        # K2: Marge (>5% = erfüllt)
        if margin_val is None:
            tiles.append(0)
        else:
            tiles.append(1 if margin_val > 5 else -1)
        
        # K3: ROE (>15% = erfüllt)
        if roe_val is None:
            tiles.append(0)
        else:
            tiles.append(1 if roe_val > 15 else (-1 if roe_val < 5 else 0))
        
        # K4: Verschuldung D/E (<0.5 = erfüllt)
        if debt_val is None:
            tiles.append(0)
        else:
            tiles.append(1 if debt_val < 0.5 else (-1 if debt_val > 2.0 else 0))
        
        # K5: Dividenden (>1.5% mit ROE>10 = erfüllt)
        if div_val is None or roe_val is None:
            tiles.append(0)
        else:
            tiles.append(1 if (div_val > 1.5 and roe_val > 10) else -1)
        
        # K6: Monte Carlo (>70% = erfüllt)
        if mc_val is None:
            tiles.append(0)
        else:
            tiles.append(1 if mc_val > 70 else (-1 if mc_val < 30 else 0))
        
        # K7: Upside (Target vs Current)
        if target is None or current_price_val is None or current_price_val <= 0:
            tiles.append(0)
        else:
            upside = ((target - current_price_val) / current_price_val) * 100
            tiles.append(1 if upside > 0 else -1)
        
        # K8: Wachstum (>10% = erfüllt)
        if growth_val is None:
            tiles.append(0)
        else:
            tiles.append(1 if growth_val > 10 else -1)
        
        # K9: Analysten (BUY/Strong Buy = erfüllt)
        emp_badge = str(row.get('Empfehlung', '')).lower()
        tiles.append(1 if ('buy' in emp_badge or 'stark' in emp_badge) else (-1 if 'beob' in emp_badge or 'hold' in emp_badge else 0))
        
        # K10: Sektor-Bonus (Krypto, Energie, KI = erfüllt)
        bonus_sectors = ['krypto_core', 'energie', 'ki_chips', 'automation']
        tiles.append(1 if sektor_key in bonus_sectors else 0)
        
        # K11: Elliott BUY Signal (BUY + Kurs <2% unter Target)
        if signal == 'BUY' and target and current_price_val and current_price_val > 0:
            if current_price_val < (target * 0.98):
                tiles.append(1)
            else:
                tiles.append(0)
        else:
            tiles.append(0)
        
        # K12: Target-Distanz (>=2% = erfüllt)
        if target and current_price_val and current_price_val > 0:
            dist = ((target - current_price_val) / current_price_val) * 100
            tiles.append(1 if dist >= 2 else -1)
        else:
            tiles.append(0)
        
        # K13: CRV-Basis (>=2.0 = erfüllt)
        if crv_val is None:
            tiles.append(0)
        else:
            tiles.append(1 if crv_val >= 2.0 else (-1 if crv_val < 1.0 else 0))
        
        # K14: CRV-Top (>=3.0 = erfüllt)
        if crv_val is None:
            tiles.append(0)
        else:
            tiles.append(1 if crv_val >= 3.0 else -1)
        
        # K15: Konfluenz (>=5 grüne Tiles = erfüllt)
        green_count = sum(1 for t in tiles if t == 1)
        tiles.append(1 if green_count >= 5 else 0)
        
        # Generiere Tiles-HTML
        tile_labels = ['KGV-Check', 'Marge', 'ROE', 'Verschuldung', 'Dividenden', 'Monte Carlo', 'Upside', 'Wachstum', 'Analysten', 'Sektor-Bonus', 'Elliott', 'Target-Distanz', 'CRV-Basis', 'CRV-Top', 'Konfluenz']
        tiles_html = '<div class="tiles-grid" style="display: grid; grid-template-columns: repeat(5, 10px); gap: 6px; margin-top: 8px;">'
        for idx, tile_val in enumerate(tiles):
            if tile_val == 1:
                color = "#10b981"  # Green
            elif tile_val == -1:
                color = "#ef4444"  # Red
            else:
                color = "#374151"  # Gray
            label = tile_labels[idx] if idx < len(tile_labels) else f"K{idx+1}"
            tiles_html += f'<div class="tile" style="width: 10px; height: 10px; background: {color}; border-radius: 2px; cursor: help;" title="{label}"></div>'
        tiles_html += '</div>'

        # Score-Tooltip - ENTFERNT (schwarzer Kasten)
        score_tooltip = ""

        # Asset-Details Card mit allen Infos + 15-Faktoren
        tile_status_list = []
        for idx, (label, tile_val) in enumerate(zip(tile_labels, tiles)):
            status_icon = '✓' if tile_val == 1 else '✗' if tile_val == -1 else '–'
            status_color = '#10b981' if tile_val == 1 else '#ef4444' if tile_val == -1 else '#94a3b8'
            tile_status_list.append(f'<div style="font-size:10px; margin:2px 0;"><span style="color:{status_color}">{status_icon}</span> <strong>K{idx+1}</strong>: {label}</div>')
        
        tile_status_html = '\n'.join(tile_status_list)
        
        # Info-Daten als JSON für das data-info Attribut
        info_data = {
            'name': row['Name'],
            'ticker': ticker,
            'sektor': sektor_name,
            'kurs': row.get('Akt. Kurs', '–'),
            'currency': display_currency,
            'score': score,
            'factors': tile_status_list
        }
        # JSON mit ensure_ascii=False für Unicode-Support, dann HTML-escape für Attribute-Kontext
        info_json = json.dumps(info_data, ensure_ascii=False)
        info_json_escaped = info_json.replace("'", "&apos;").replace('"', "&quot;")

        html_template += f"""
                <tr class="hover:bg-white/[0.02] transition group sektor-row" data-sektor="{row['Sektor_Key']}" data-radar='{radar_vector_str}' data-sektor-key='{sektor_key}' data-mc='{mc_chance_val}' data-target='{ell_target}' data-entry='{ell_entry}' data-margin='{margin_val}' data-growth='{growth_val}' data-pe='{pe_val}' data-rec='{rec_val}' style="border-left:4px solid {row_color};">
                        <td class="px-6 py-5 relative">
                            <div class="flex flex-col relative z-20">
                                <a href="https://finance.yahoo.com/quote/{link_symbol}" target="_blank" rel="noopener noreferrer" class="font-bold text-slate-100 hover:text-emerald-400 transition cursor-pointer block">{row['Name']}</a>
                                <span class="text-[10px] font-mono text-slate-500 uppercase">{ticker}</span>
                            </div>
                        </td>
                        <td class="px-3 py-5 text-center">
                            <span class="info-icon cursor-pointer text-slate-400 hover:text-amber-400 transition-colors" 
                                  data-info='{info_json_escaped}' title="Faktor-Details">ⓘ</span>
                        </td>
                        <td class="px-6 py-5 text-xs text-slate-400">{row['Sektor']}</td>
                        <td class="px-6 py-5 text-right font-mono text-sm">{row.get('ROE %', '–')}</td>
                        <td class="px-6 py-5 text-right font-mono text-sm">{row.get('Debt/Equity', '–')}</td>
                        <td class="px-6 py-5 text-right font-mono text-sm">{row.get('Div. Rendite %', '–')}</td>
                        <td class="px-6 py-5 text-right font-mono">
                            <div class="flex flex-col items-end gap-0.5">
                                <span class="text-sm font-bold text-slate-200">{row.get('Akt. Kurs', 0)} {display_currency}</span>
                                <span class="{perf_color} text-[10px]">{perf_icon} {abs(perf):.1f}%</span>
                            </div>
                        </td>
                        <td class="px-6 py-5 text-center">
                            <span class="{emp['badge_class']} px-2 py-1 rounded-lg text-[9px] font-bold uppercase tracking-tighter">{emp['badge']}</span>
                        </td>
                        <td class="px-6 py-5 text-right font-mono text-sm {crv_color}">{crv:.2f}</td>
                        <td class="px-6 py-5 text-right font-mono text-sm text-blue-400">{row.get('Zyklus %', 50.0):.0f}</td>
                        <td class="px-6 py-5 text-right relative has-tooltip cursor-help cell-with-card">
                            <span class="text-lg font-black text-white">{score}</span>
                            <div class="w-8 h-1 bg-slate-700 rounded-full ml-auto mt-1 overflow-hidden">
                                <div class="bg-emerald-500 h-full" style="width: {(float(score)/145)*100}%"></div>
                            </div>
                            {tiles_html}
                        </td>
                    </tr>
        """

    html_template += f"""
                        </tbody>
                    </table>
                </div>
            </div>
            
            <!-- Global Tooltip Element (einzeln für alle Info-Icons) -->
            <div id="global-tooltip" class="detail-card" style="display: none; position: absolute; z-index: 10000; background: #1e293b; border: 1px solid #334155; border-radius: 6px; padding: 12px; box-shadow: 0 10px 30px rgba(0,0,0,0.5); pointer-events: auto; color: #cbd5e1; font-size: 11px; line-height: 1.6; max-width: 380px; max-height: 70vh; overflow-y: auto;"></div>

            <script>
            const BENCHMARK_GLOBAL = {json.dumps(overall_avg)};
            const BENCHMARK_BY_SECTOR = {json.dumps(sector_avgs)};
            const radarCharts = {{}};
            let apexChartsLoaded = false;

            // Global Tooltip State
            let stickyTooltip = null;
            
            // Hilfsfunktion: Tooltip aktualisieren und positionieren
            function updateTooltip(icon, data) {{
                const tooltip = document.getElementById('global-tooltip');
                
                // Baue Tooltip-HTML aus JSON-Daten
                let factorHtml = '';
                if (data.factors && Array.isArray(data.factors)) {{
                    factorHtml = data.factors.join('');
                }}
                
                tooltip.innerHTML = `
                    <p style="font-weight: bold; border-bottom: 1px solid rgba(255,255,255,0.1); padding-bottom: 6px; margin-bottom: 6px; color: #f1f5f9;">
                        📋 ${{data.name}} (${{data.ticker}})
                    </p>
                    <p style="margin: 3px 0;"><strong>Sektor:</strong> ${{data.sektor}}</p>
                    <p style="margin: 3px 0;"><strong>Kurs:</strong> ${{data.kurs}} ${{data.currency}}</p>
                    <p style="margin: 3px 0;"><strong>Score:</strong> ${{data.score}}</p>
                    <p style="font-weight: bold; border-top: 1px solid rgba(255,255,255,0.1); padding-top: 6px; margin-top: 6px;">15-Faktoren:</p>
                    <div style="margin-top: 3px; font-size: 10px;">
                        ${{factorHtml}}
                    </div>
                `;
                
                // Intelligente Positionierung
                const rect = icon.getBoundingClientRect();
                const tooltipHeight = tooltip.offsetHeight || 300; // Fallback wenn noch nicht gemessen
                const viewportHeight = window.innerHeight;
                const spaceBelow = viewportHeight - rect.bottom;
                const spaceAbove = rect.top;
                
                tooltip.style.left = (rect.left + window.scrollX) + 'px';
                
                // Wenn unten wenig Platz: öffne oben
                if (spaceBelow < tooltipHeight + 16 && spaceAbove > tooltipHeight + 16) {{
                    tooltip.style.top = (rect.top + window.scrollY - tooltipHeight - 8) + 'px';
                }} else {{
                    tooltip.style.top = (rect.bottom + window.scrollY + 8) + 'px';
                }}
                
                tooltip.style.display = 'block';
            }}
            
            // Click-Handler: Toggle Sticky Mode
            document.addEventListener('click', function(e) {{
                if (e.target.classList.contains('info-icon')) {{
                    try {{
                        const infoJson = e.target.getAttribute('data-info');
                        if (!infoJson) return;
                        
                        const data = JSON.parse(infoJson);
                        
                        // Toggle sticky state
                        if (stickyTooltip === e.target) {{
                            stickyTooltip = null; // Unfreeze
                            document.getElementById('global-tooltip').style.display = 'none';
                        }} else {{
                            stickyTooltip = e.target; // Freeze at this icon
                            updateTooltip(e.target, data);
                            e.target.style.opacity = '1';
                            e.target.style.color = '#fbbf24'; // Highlight bei sticky
                        }}
                    }} catch (err) {{
                        console.error('Sticky Toggle Error:', err);
                    }}
                }}
            }});
            
            // Hover-Handler: Update nur wenn NICHT sticky
            document.addEventListener('mouseover', function(e) {{
                if (e.target.classList.contains('info-icon') && stickyTooltip === null) {{
                    try {{
                        const infoJson = e.target.getAttribute('data-info');
                        if (!infoJson) return;
                        
                        const data = JSON.parse(infoJson);
                        updateTooltip(e.target, data);
                    }} catch (err) {{
                        console.error('Tooltip Error:', err);
                    }}
                }}
            }});
            
            // Verstecke Tooltip beim Mauszeiger weg (nur wenn NICHT sticky)
            document.addEventListener('mouseout', function(e) {{
                if (e.target.classList.contains('info-icon') && stickyTooltip === null) {{
                    const tooltip = document.getElementById('global-tooltip');
                    setTimeout(() => {{
                        if (!tooltip.matches(':hover')) {{
                            tooltip.style.display = 'none';
                        }}
                    }}, 100);
                }}
            }});
            
            // Verstecke Tooltip wenn Maus tooltip verlässt und nicht sticky
            document.addEventListener('mouseout', function(e) {{
                const tooltip = document.getElementById('global-tooltip');
                if (tooltip && e.target === tooltip && stickyTooltip === null) {{
                    setTimeout(() => {{
                        tooltip.style.display = 'none';
                    }}, 100);
                }}
            }});
            
            // ESC-Taste zum Entsperren
            document.addEventListener('keydown', function(e) {{
                if (e.key === 'Escape' && stickyTooltip !== null) {{
                    stickyTooltip.style.color = ''; // Reset Farbe
                    stickyTooltip = null;
                    document.getElementById('global-tooltip').style.display = 'none';
                }}
            }});

            window.addEventListener('load', () => {{
                if (typeof ApexCharts !== 'undefined') {{
                    apexChartsLoaded = true;
                    console.info('✓ ApexCharts ready');
                }} else {{
                    console.warn('⚠ ApexCharts nicht geladen');
                }}
            }});

            function showFallback(radar_id, fallback_id) {{
                const radarDiv = document.getElementById(radar_id);
                const fallbackDiv = document.getElementById(fallback_id);
                if (radarDiv) radarDiv.style.display = 'none';
                if (fallbackDiv) fallbackDiv.style.display = 'block';
            }}

            function drawRadar(radar_id, fallback_id) {{
                const radarDiv = document.getElementById(radar_id);
                const fallbackDiv = document.getElementById(fallback_id);
                if (!radarDiv) return;

                if (!apexChartsLoaded || typeof ApexCharts === 'undefined') {{
                    showFallback(radar_id, fallback_id);
                    return;
                }}

                const tr = radarDiv.closest('tr');
                if (!tr) {{
                    showFallback(radar_id, fallback_id);
                    return;
                }}

                const radarJsonStr = tr.getAttribute('data-radar');
                const sectorKey = tr.getAttribute('data-sektor-key');

                if (!radarJsonStr) {{
                    showFallback(radar_id, fallback_id);
                    return;
                }}

                let radarVector;
                try {{
                    radarVector = JSON.parse(radarJsonStr);
                }} catch (e) {{
                    console.error('JSON-Parse Fehler:', e);
                    showFallback(radar_id, fallback_id);
                    return;
                }}

                if (!Array.isArray(radarVector) || radarVector.length !== 5) {{
                    showFallback(radar_id, fallback_id);
                    return;
                }}

                const benchmark = (sectorKey && BENCHMARK_BY_SECTOR[sectorKey]) 
                    ? BENCHMARK_BY_SECTOR[sectorKey] 
                    : BENCHMARK_GLOBAL;

                if (radarCharts[radar_id]) {{
                    radarCharts[radar_id].destroy();
                }}

                try {{
                    radarCharts[radar_id] = new ApexCharts(radarDiv, {{
                        series: [
                            {{ name: 'Asset', data: radarVector }},
                            {{ name: 'Benchmark', data: benchmark || [0, 0, 0, 0, 0] }}
                        ],
                        chart: {{
                            type: 'radar',
                            height: 240,
                            toolbar: {{ show: false }},
                            animations: {{ enabled: true }}
                        }},
                        xaxis: {{
                            categories: ['Wachstum', 'Rentabilität', 'Sicherheit', 'Technik', 'Bewertung']
                        }},
                        yaxis: {{
                            min: 0,
                            max: 100,
                            tickAmount: 4
                        }},
                        colors: ['#ffd700', '#6b7280'],
                        stroke: {{
                            width: [2.5, 1.5]
                        }},
                        fill: {{
                            opacity: [0.25, 0.1]
                        }},
                        markers: {{
                            size: [4, 2],
                            colors: ['#ffd700', '#6b7280']
                        }},
                        legend: {{
                            show: true,
                            fontSize: 9,
                            position: 'bottom'
                        }},
                        tooltip: {{
                            enabled: true,
                            theme: 'dark'
                        }}
                    }});
                    radarCharts[radar_id].render();
                    
                    if (fallbackDiv) fallbackDiv.style.display = 'none';
                }} catch (e) {{
                    console.error('ApexCharts Fehler:', e);
                    showFallback(radar_id, fallback_id);
                }}
            }}

            function filterTable() {{
                let input = document.getElementById('searchInput').value.toUpperCase();
                let rows = document.querySelectorAll('.sektor-row');
                rows.forEach(row => {{
                    row.style.display = row.textContent.toUpperCase().includes(input) ? "" : "none";
                }});
            }}

            function filterSektor(sektor) {{
                let rows = document.querySelectorAll('.sektor-row');
                rows.forEach(row => {{
                    if (sektor === 'Alle') row.style.display = "";
                    else row.style.display = row.getAttribute('data-sektor') === sektor ? "" : "none";
                }});
            }}

            function sortTable(n) {{
                const table = document.getElementById("mainTable");
                const tbody = table.tBodies[0];
                const rows = Array.from(tbody.rows);
                const currentDir = table.getAttribute("data-sort-dir") || "desc";
                const dir = currentDir === "asc" ? "desc" : "asc";
                table.setAttribute("data-sort-dir", dir);

                rows.sort((a, b) => {{
                    let x = a.cells[n].innerText.trim();
                    let y = b.cells[n].innerText.trim();

                    if (n === 2 || n === 3 || n === 4 || n === 5 || n === 7 || n === 8 || n === 9) {{
                        const xNum = parseFloat(x.replace(/[^0-9,.-]/g, '').replace(',', '.')) || 0;
                        const yNum = parseFloat(y.replace(/[^0-9,.-]/g, '').replace(',', '.')) || 0;
                        return dir === "asc" ? xNum - yNum : yNum - xNum;
                    }}

                    return dir === "asc"
                        ? x.localeCompare(y, 'de', {{ sensitivity: 'base' }})
                        : y.localeCompare(x, 'de', {{ sensitivity: 'base' }});
                }});

                rows.forEach(row => tbody.appendChild(row));
            }}
            </script>
        </div>
    </body>
    </html>
    """

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_template)
    print(f"✅ Dashboard erstellt: {output_path}")

if __name__ == "__main__":
    generate_dashboard()

