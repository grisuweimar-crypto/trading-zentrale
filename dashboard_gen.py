import pandas as pd
import datetime
import re

# Dein Sektor-Konzept (Watchlist Master 2026): feste Säulen mit Emojis, keine Duplikate.
# Reihenfolge = Reihenfolge der Filter-Buttons.
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
    """Sektor-String für Abgleich: Klein, ohne Emojis."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s = str(s).strip().upper()
    s = re.sub(r"[^\w\s&/-]", "", s)  # Emojis/Sonderzeichen raus
    return s.replace(" ", "")

def normalize_sektor(row):
    """
    Ordnet einen Watchlist-Eintrag (Sektor, Name, Ticker) einer festen Säule zu.
    Rückgabe: (key, display_name) für Filter und Tabelle.
    """
    sektor_raw = row.get("Sektor", "")
    name = str(row.get("Name", "") or "").upper()
    ticker = str(row.get("Ticker", "") or "").upper()
    s = _norm(sektor_raw)

    # Krypto: Core (BTC, ETH) vs Satelliten (SOL, ADA, DOGE, XRP, Coinbase etc.)
    if "BITCOIN" in name or "BTC" in ticker or (s and "KRYPTO" in s and "BITCOIN" in name):
        return "krypto_core", DISPLAY_BY_KEY["krypto_core"]
    if "ETHEREUM" in name or "ETH" in ticker:
        return "krypto_core", DISPLAY_BY_KEY["krypto_core"]
    if any(x in s for x in ["KRYPTO", "Krypto"]) or any(x in ticker for x in ["SOL", "ADA", "DOGE", "XRP", "COIN"]):
        return "krypto_sat", DISPLAY_BY_KEY["krypto_sat"]

    # Infra & Versorger (z. B. Korea Electric Power)
    if "KOREA" in name or "ELECTRIC" in name and "POWER" in name or "KEP" in ticker:
        return "infra", DISPLAY_BY_KEY["infra"]
    if "VERSORGER" in s or "INFRA" in s:
        return "infra", DISPLAY_BY_KEY["infra"]

    # Experimente & High Risk (Fluence, Largo, Quantumscape, Reliance, Solvay etc.)
    if "EXPERIMENTE" in s or "Experimente" in str(sektor_raw):
        return "experimente", DISPLAY_BY_KEY["experimente"]
    if "FLUENCE" in name or "LARGO" in name or "QUANTUMSCAPE" in name or "SOLVAY" in name:
        return "experimente", DISPLAY_BY_KEY["experimente"]
    if "INDUSTRIE" in s and "AUTO" in s:
        return "experimente", DISPLAY_BY_KEY["experimente"]

    # Säule 1: Automation & Robotik (Hardware, Robotik)
    if "HARDWARE" in s or "ROBOTIK" in s or "AUTOMATION" in s or "COGNEX" in name or "FANUC" in name or "YASKAWA" in name or "AUTOSTORE" in name or "ABB" in name or "TERADYNE" in name or "KEYENCE" in name or "ROCKWELL" in name or "UBTECH" in name:
        return "automation", DISPLAY_BY_KEY["automation"]

    # Säule 2: KI, Chips & Cloud (Gehirn, Tech, E-Commerce wie Amazon)
    if "GEHIRN" in s or "TECH" in s or "SOFTWARE" in s or "E-Commerce" in str(sektor_raw) or "ECOM" in s:
        return "ki_chips", DISPLAY_BY_KEY["ki_chips"]
    if "ASML" in name or "ALPHABET" in name or "AMAZON" in name or "INFINEON" in name or "SAP" in name or "MICROSOFT" in name or "NVidia" in name or "ORACLE" in name or "PALANTIR" in name or "SNOWFLAKE" in name or "TAIWAN SEMICON" in name or "TENCENT" in name or "BAIDU" in name or "ON SEMICON" in name or "MICRON" in name or "NXP" in name:
        return "ki_chips", DISPLAY_BY_KEY["ki_chips"]

    # Säule 3: Energie & Speicher (ohne Versorger)
    if "ENERGIE" in s or "ENERGY" in s:
        return "energie", DISPLAY_BY_KEY["energie"]
    if "CAMECO" in name or "EXON" in name or "EXXON" in name or "FIRST SOLAR" in name or "CATL" in name or "OCCIDENTAL" in name or "CHEVRON" in name or "SAMSUNG SDI" in name or "SIEMENS ENERGY" in name or "PETROCHINA" in name or "YELLOW CAKE" in name or "ALBEMARLE" in name:
        return "energie", DISPLAY_BY_KEY["energie"]

    # Säule 4: Metalle & Rohstoffe (Fundament, Recycling, Mining ohne Edelmetalle)
    if "FUNDAMENT" in s or "RECYCLING" in s or "ROHSTOFFE" in s:
        return "metalle", DISPLAY_BY_KEY["metalle"]
    if "FREEPORT" in name or "AURUBIS" in name or "CONSTELLIUM" in name or "VALE" in name or "POSCO" in name or "ZIJIN" in name or "UMICORE" in name:
        return "metalle", DISPLAY_BY_KEY["metalle"]
    if "MINING" in s and "EDELMETALLE" not in s:
        return "metalle", DISPLAY_BY_KEY["metalle"]

    # Gold & Silber (Edelmetalle)
    if "EDELMETALLE" in s or "GOLD" in s or "SILBER" in s or "SILVER" in name or "GOLD" in name or "AGNICO" in name or "ALAMOS" in name or "HECLA" in name or "FIRST MAJESTIC" in name or "BARRICK" in name or "NEWMONT" in name or "PAN AM" in name or "FRESNILLO" in name:
        return "gold_silber", DISPLAY_BY_KEY["gold_silber"]

    # Konsum & Marken
    if "KONSUM" in s or "LIFESTYLE" in s or "CAMPBELL" in name or "LUCKIN" in name or "LVMH" in name or "NESTLE" in name or "NIKE" in name or "PERNOD" in name or "MINISO" in name or "UNDER ARMOUR" in name or "COCA" in name or "PEPSI" in name or "PROCTER" in name or "PHILIP MORRIS" in name or "INGREDION" in name:
        return "konsum", DISPLAY_BY_KEY["konsum"]

    # Finanzen & Zahlungsverkehr
    if "FINANZEN" in s or "FINTECH" in s or "FINANCE" in s:
        return "finanzen", DISPLAY_BY_KEY["finanzen"]
    if "ALLIANZ" in name or "DEUTSCHE BANK" in name or "BLOCK" in name or "COINBASE" in name or "MASTERCARD" in name or "PAYPAL" in name or "BLACKROCK" in name or "NERDWALLET" in name:
        return "finanzen", DISPLAY_BY_KEY["finanzen"]

    # Gesundheit & Biotech
    if "PHARMA" in s or "GESUNDHEIT" in s or "HEALTH" in s:
        return "gesundheit", DISPLAY_BY_KEY["gesundheit"]
    if "BAYER" in name or "FRESEN" in name or "CENTENE" in name or "INCYTE" in name or "ORGANON" in name or "PFIZER" in name or "UNITEDHEALTH" in name or "NOVO" in name or "INTUITIVE" in name or "STRYKER" in name or "JOHNSON" in name or "ABBOTT" in name:
        return "gesundheit", DISPLAY_BY_KEY["gesundheit"]

    # Medien & Digitales
    if "MEDIEN" in s or "DIGITALES" in s or "DIGITAL" in s:
        return "medien", DISPLAY_BY_KEY["medien"]
    if "NETFLIX" in name or "SPOTIFY" in name or "LUMEN" in name or "TAKKT" in name or "ZETA" in name:
        return "medien", DISPLAY_BY_KEY["medien"]

    if not s or s in ("0", "NAN"):
        return "andere", DISPLAY_BY_KEY["andere"]
    return "andere", DISPLAY_BY_KEY["andere"]

def generate_dashboard(csv_path='watchlist.csv', output_path='index.html'):
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"❌ Fehler: {e}")
        return

    # Sektoren auf feste Säulen mappen (keine Duplikate, einheitliche Anzeige)
    out = df.apply(normalize_sektor, axis=1)
    df["Sektor_Key"] = [o[0] for o in out]
    df["Sektor"] = [o[1] for o in out]

    def get_empfehlung(row, display_currency="€"):
        """
        Eine klare Empfehlung: Was tun + welcher Einstieg (theoretisch).
        »Einstieg ab X« = Kauf beim Bruch dieses Niveaus, nicht zum aktuellen Kurs.
        »nur bei Korrektur« = nur wenn Kurs schon über Einstieg liegt (nicht nachkaufen am Hoch).
        """
        score = float(row.get('Score', 0))
        signal = str(row.get('Elliott-Signal', '')).upper()
        cycle = float(row.get('Zyklus %', 50))
        e_entry = row.get('Elliott-Einstieg', 0)
        e_entry = float(e_entry) if e_entry else 0
        current_price = float(row.get('Akt. Kurs', 0) or row.get('Akt. Kurs [€]', 0) or 0)
        entry_line = f"Einstieg ab {e_entry:.2f} {display_currency}" if e_entry > 0 else "–"

        if signal != "BUY":
            return {
                "badge": "Kein Setup",
                "badge_class": "bg-slate-700 text-slate-400 border border-slate-600",
                "line2": "Kein Elliott-Setup",
                "entry_line": "–",
            }

        # BUY: Einstieg nur beim Bruch von e_entry
        if cycle <= 20 and score >= 95:
            badge, badge_class = "Starkes Setup", "bg-emerald-500/20 text-emerald-400 border border-emerald-500/40"
        elif cycle <= 40 and score >= 75:
            badge, badge_class = "Setup ok", "bg-amber-500/20 text-amber-400 border border-amber-500/40"
        elif cycle > 60:
            badge, badge_class = "Vorsicht: Zyklus hoch", "bg-rose-500/20 text-rose-400 border border-rose-500/40"
            # "nur bei Korrektur" nur, wenn Kurs schon über Einstieg liegt (nicht nachkaufen). Liegt Kurs darunter, reicht "Einstieg ab X".
            if e_entry > 0 and current_price >= e_entry:
                entry_line = f"Einstieg ab {e_entry:.2f} {display_currency} (nur bei Korrektur)"
            elif e_entry > 0:
                entry_line = f"Einstieg ab {e_entry:.2f} {display_currency}"
            else:
                entry_line = "Zyklus hoch – nur bei Korrektur"
        elif score >= 60:
            badge, badge_class = "Beobachten", "bg-slate-500/20 text-slate-300 border border-slate-500/40"
        else:
            badge, badge_class = "Beobachten", "bg-slate-500/20 text-slate-300 border border-slate-500/40"

        return {
            "badge": badge,
            "badge_class": badge_class,
            "line2": entry_line,
            "entry_line": entry_line,
        }

    # Filter-Buttons: feste Reihenfolge (DISPLAY_SEKTOREN), nur vorhandene Keys, Filter nach Key (keine Duplikate)
    present_keys = set(df["Sektor_Key"].unique())
    filter_buttons_html = '<button onclick="filterSektor(\'Alle\')" class="px-4 py-2 rounded-full glass text-xs font-bold hover:bg-emerald-600 transition whitespace-nowrap flex-shrink-0">Alle</button>\n'
    for key, label in DISPLAY_SEKTOREN:
        if key in present_keys:
            # Key für Filter nutzen (escaping: Key enthält keine Anführungszeichen)
            filter_buttons_html += f'                    <button onclick="filterSektor(\'{key}\')" class="px-4 py-2 rounded-full glass text-xs font-bold hover:bg-emerald-600 transition whitespace-nowrap flex-shrink-0">{label}</button>\n'
    # Kurzhinweis für die Empfehlung
    hinweis_html = """
    <p class="text-xs text-slate-500 italic">
        <strong class="text-slate-400">»Einstieg ab X«</strong> = Kauf beim Bruch dieses Niveaus (Elliott), nicht zwingend zum aktuellen Kurs.
    </p>
    """

    df = df.sort_values(by='Score', ascending=False)
    timestamp = datetime.datetime.now().strftime('%d.%m.%Y um %H:%M:%S')
    
    
    html_template = f"""
    <!DOCTYPE html>
    <html lang="de">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Trading-Zentrale Ultimate</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
        <style>
            body {{ background: #020617; color: #f1f5f9; font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; }}
            .glass {{ background: rgba(15, 23, 42, 0.8); backdrop-filter: blur(12px); border: 1px solid rgba(255,255,255,0.1); }}
            .tooltip {{ visibility: hidden; opacity: 0; transition: opacity 0.3s; position: absolute; z-index: 50; }}
            .has-tooltip:hover .tooltip, .has-tooltip:active .tooltip {{ visibility: visible; opacity: 1; }}
            th {{ cursor: pointer; transition: color 0.2s; }}
            th:hover {{ color: #10b981 !important; }}
            .no-scrollbar::-webkit-scrollbar {{ display: none; }}
        </style>
    </head>
    <body class="p-4 md:p-8">
        <div class="max-w-7xl mx-auto">
            
            <div class="flex flex-col md:flex-row justify-between items-center mb-10 gap-4">
                <h1 class="text-4xl font-black text-white tracking-tighter uppercase">
                    Trading<span class="text-emerald-500 underline decoration-2">Zentrale</span>
                </h1>
                <div class="text-right">
                    <p class="text-xs text-slate-500 font-mono tracking-widest uppercase">Live Terminal v4.0</p>
                    <p class="text-sm text-slate-300 italic">{timestamp}</p>
                </div>
            </div>

            <div class="flex flex-wrap items-center justify-between gap-4 mb-8">
                <div class="w-full overflow-x-auto pb-2 flex gap-2 flex-nowrap min-h-[2.75rem]" style="scrollbar-width: thin;">
                    {filter_buttons_html}
                </div>
                
                <div class="flex flex-wrap items-center gap-6">
                    <details class="mb-8 group">
                        <summary class="list-none cursor-pointer glass inline-block px-4 py-2 rounded-lg text-[10px] font-bold text-slate-500 hover:text-emerald-400 transition uppercase tracking-widest">
                            <i class="fa-solid fa-circle-info mr-2"></i> So bewerten wir
                        </summary>
                        <div class="mt-4 glass rounded-2xl p-6 border-l-4 border-emerald-500 w-full overflow-hidden">
                            <h3 class="text-lg font-black mb-3 text-emerald-400">So bewerten wir</h3>
                            <ul class="text-sm text-slate-400 space-y-2 list-disc list-inside">
                                <li><strong class="text-slate-300">Empfehlung:</strong> Elliott-Setup (BUY = Welle-2-Korrektur) + Score + Zyklus. »Starkes Setup« = viele Faktoren sprechen für Einstieg – aber erst <strong>beim Bruch des angezeigten Niveaus</strong>, nicht zum aktuellen Kurs.</li>
                                <li><strong class="text-slate-300">Einstieg (theoretisch):</strong> Preis, ab dem das Elliott-Setup bestätigt ist (Bruch des Wellen-Hochs). Darunter = warten.</li>
                                <li><strong class="text-slate-300">Score:</strong> Technik (Elliott, Monte-Carlo) + Fundamental (KGV, Marge, Analysten) + CRV. Hoher Score = gutes Setup, kein direkter Kaufbefehl.</li>
                                <li><strong class="text-slate-300">Zyklus %:</strong> 0–20 = zyklisches Tief (gutes Timing), 80–100 = zyklisches Hoch (Vorsicht – oft kein Einstieg am Hoch).</li>
                                <li><strong class="text-slate-300">CRV:</strong> Chance-Risiko-Verhältnis (Ziel vs. 10%-Stop). &gt;2 = gut, &lt;1 = schlecht.</li>
                            </ul>
                        </div> </details> <div class="flex flex-wrap items-center justify-between gap-4 mt-6">
                        {hinweis_html}
                        <input type="text" id="searchInput" onkeyup="filterTable()" placeholder="Ticker oder Name..." 
                               class="glass rounded-xl px-5 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-emerald-500 w-full md:w-64">
                    </div>
                </div>
            </div>

            <div class="glass rounded-3xl shadow-2xl border-none flex flex-col" style="max-height: 75vh;">
                <div class="overflow-y-auto overflow-x-auto no-scrollbar rounded-3xl">
                    <table class="w-full text-left border-collapse" id="mainTable">
                        <thead class="bg-slate-900/95 border-b border-slate-700 sticky top-0 z-30 backdrop-blur-md">
                            <tr class="text-[10px] text-slate-400 uppercase tracking-widest font-bold">
                                <th class="px-6 py-5" onclick="sortTable(0)">Asset <i class="fa-solid fa-sort ml-1 opacity-30"></i></th>
                                <th class="px-6 py-5" onclick="sortTable(1)">Sektor <i class="fa-solid fa-sort ml-1 opacity-30"></i></th>
                                <th class="px-6 py-5 text-right font-black" onclick="sortTable(2)">Kurs · Einstieg <i class="fa-solid fa-sort ml-1 opacity-30"></i></th>
                                <th class="px-6 py-5 text-center font-bold text-emerald-400" onclick="sortTable(3)">Empfehlung</th>
                                <th class="px-6 py-5 text-right" onclick="sortTable(4)">CRV <i class="fa-solid fa-sort ml-1 opacity-30"></i></th>
                                <th class="px-6 py-5 text-right" onclick="sortTable(5)">Zyklus % <i class="fa-solid fa-sort ml-1 opacity-30"></i></th>
                                <th class="px-6 py-5 text-right" onclick="sortTable(6)">Score <i class="fa-solid fa-sort ml-1 opacity-30"></i></th>
                            </tr>
                        </thead>
                        <tbody class="divide-y divide-slate-800">
    """

    for _, row in df.iterrows():
        # --- 1. DATEN-VORBEREITUNG ---
        score = row.get('Score', 0)
        crv = float(row.get('CRV', 0.0))
        mc_chance = float(row.get('MC-Chance', 0))
        e_entry = row.get('Elliott-Einstieg', 0)
        e_exit = row.get('Elliott-Ausstieg', 0)
        e_entry_f = float(e_entry) if e_entry else 0
        ticker = str(row.get('Ticker', '')).strip()
        # Yahoo-Link: gespeichertes Yahoo-Symbol nutzen, dann landet man direkt auf der richtigen Aktie (kein ISIN)
        yh = row.get('Yahoo')
        link_symbol = (str(yh).strip() if yh is not None and not (isinstance(yh, float) and pd.isna(yh)) and str(yh).strip() else '') or ticker

        raw_currency = row.get('Währung', 'USD')
        currency_map = {'USD': '$', 'EUR': '€', 'CHF': 'CHF', 'GBp': 'p', 'CAD': 'C$', 'NOK': 'kr'}
        display_currency = currency_map.get(raw_currency, raw_currency)

        emp = get_empfehlung(row, display_currency)
        is_buy = row['Elliott-Signal'] == "BUY"

        # Punkte für Score-Tooltip
        p_elliott = 20 if is_buy else 0
        p_stats = round((mc_chance / 100) * 20, 1)
        p_crv = 0
        if is_buy:
            if crv >= 3.0: p_crv = 15
            elif crv >= 2.0: p_crv = 10
            elif crv < 1.0 and crv > 0: p_crv = -20
        p_konfluenz = 15 if (mc_chance > 70 and is_buy) else 0
        sektor_clean = str(row.get('Sektor', '')).upper()
        is_krypto_asset = 'KRYPTO' in sektor_clean or any(x in ticker for x in ['BTC', 'ETH', 'SOL', 'XRP', 'ADA', 'DOGE'])
        p_krypto = 15 if is_krypto_asset else 0
        p_fund_total = round(max(0, float(score) - p_elliott - p_stats - p_crv - p_konfluenz - p_krypto), 1)

        perf = float(row.get('Perf %', 0.0))
        perf_icon = "↑" if perf > 0 else "↓" if perf < 0 else "→"
        perf_color = "text-emerald-400 font-bold" if perf > 0 else "text-rose-500 font-bold" if perf < 0 else "text-slate-500"
        crv_color = "text-emerald-400 font-bold" if crv >= 2.0 else ("text-rose-500 font-bold" if crv < 1.0 and crv > 0 else "text-slate-400")

        # Kurs-Spalte: Akt. Kurs + theoretischer Einstieg
        einstieg_zeile = f"Einstieg ab {e_entry_f:.2f} {display_currency}" if e_entry_f > 0 else "–"

        # Asset-Tooltip: Akt. Kurs vs Einstieg (theoretisch)
        asset_tooltip = f"""
            <div class='tooltip glass p-4 rounded-xl text-[11px] w-56 shadow-2xl -mt-12 left-0 pointer-events-none'>
                <p class='font-bold text-emerald-400 mb-1 border-b border-white/10 pb-1'>{row['Name']}</p>
                <div class='flex justify-between mt-2'><span>Akt. Kurs:</span><span class='text-slate-300 font-mono'>{row.get('Akt. Kurs', 0)} {display_currency}</span></div>
                <div class='flex justify-between'><span>Einstieg (theoretisch):</span><span class='text-amber-400 font-mono'>{"ab " + str(round(e_entry_f, 2)) + " " + display_currency if e_entry_f > 0 else "–"}</span></div>
                <div class='flex justify-between'><span>Kursziel:</span><span class='text-emerald-400 font-mono font-bold'>{e_exit} {display_currency}</span></div>
                <div class='flex justify-between mt-1'><span>Wahrsch. 30d:</span><span class='text-blue-400 font-bold'>{mc_chance}%</span></div>
                <div class='mt-2 pt-1 border-t border-white/10 italic text-slate-500 text-[9px] text-center'>Klick für Yahoo</div>
            </div>
        """

        score_tooltip = f"""
            <div class='tooltip glass p-4 rounded-xl text-[11px] w-52 shadow-2xl -mt-40 -ml-24 pointer-events-none'>
                <p class='font-bold border-b border-white/10 mb-2 pb-1 text-emerald-400 text-center uppercase tracking-widest'>Punkte-Check</p>
                <div class='flex justify-between'><span>Technik:</span><span class='font-mono {"text-emerald-400" if p_elliott > 0 else "text-slate-500"}'>{p_elliott}/20</span></div>
                <div class='flex justify-between'><span>Statistik:</span><span class='font-mono text-blue-400'>{p_stats}/20</span></div>
                <div class='flex justify-between'><span>Fundamental:</span><span class='font-mono text-slate-300'>{p_fund_total}/50</span></div>
                <div class='border-t border-white/10 mt-2 pt-2 space-y-1'>
                    <div class='flex justify-between'><span>CRV:</span><span class='font-mono {"text-emerald-400" if p_crv > 0 else ("text-rose-500" if p_crv < 0 else "text-slate-600")}'>{"+" if p_crv > 0 else ""}{p_crv}</span></div>
                    <div class='flex justify-between'><span>Konfluenz:</span><span class='font-mono {"text-amber-400" if p_konfluenz > 0 else "text-slate-600"}'>+{p_konfluenz}</span></div>
                    <div class='flex justify-between'><span>Krypto:</span><span class='font-mono {"text-blue-400" if p_krypto > 0 else "text-slate-600"}'>+{p_krypto}</span></div>
                </div>
            </div>
        """

        html_template += f"""
                    <tr class="hover:bg-white/[0.02] transition group sektor-row" data-sektor="{row['Sektor_Key']}">
                        <td class="px-6 py-5 relative has-tooltip cursor-default">
                            {asset_tooltip}
                            <div class="flex flex-col relative z-20">
                                <a href="https://finance.yahoo.com/quote/{link_symbol}" target="_blank" rel="noopener noreferrer"
                                   class="font-bold text-slate-100 hover:text-emerald-400 transition cursor-pointer block">{row['Name']}</a>
                                <span class="text-[10px] font-mono text-slate-500 uppercase">{ticker}</span>
                            </div>
                        </td>
                        <td class="px-6 py-5 text-xs text-slate-400 font-medium italic">{row['Sektor']}</td>
                        <td class="px-6 py-5 text-right font-mono">
                            <div class="flex flex-col items-end gap-0.5">
                                <span class="text-sm font-bold text-slate-200">{row.get('Akt. Kurs', 0)} {display_currency}</span>
                                <span class="text-[10px] text-slate-500">{einstieg_zeile}</span>
                                <span class="{perf_color} text-[10px]">{perf_icon} {abs(perf)}%</span>
                            </div>
                        </td>
                        <td class="px-6 py-5 text-center">
                            <div class="flex flex-col gap-1 items-center">
                                <span class="{emp['badge_class']} px-2 py-1 rounded-lg text-[9px] font-bold uppercase tracking-tighter">{emp['badge']}</span>
                                <span class="text-[10px] text-slate-500">{emp['line2']}</span>
                            </div>
                        </td>
                        <td class="px-6 py-5 text-right font-mono {crv_color}">{crv}</td>
                        <td class="px-6 py-5 text-right font-mono text-blue-400">{row.get('Zyklus %', 50.0)}</td>
                        <td class="px-6 py-5 text-right relative has-tooltip cursor-help">
                            {score_tooltip}
                            <span class="text-lg font-black text-white">{score}</span>
                            <div class="w-8 h-1 bg-slate-700 rounded-full ml-auto mt-1 overflow-hidden">
                                <div class="bg-emerald-500 h-full" style="width: {(float(score)/145)*100}%"></div>
                            </div>
                        </td>
                    </tr>
        """

    html_template += """
                        </tbody>
                    </table>
                </div>
            </div>
            
            <script>
            function filterTable() {
                let input = document.getElementById('searchInput').value.toUpperCase();
                let rows = document.querySelectorAll('.sektor-row');
                rows.forEach(row => {
                    row.style.display = row.textContent.toUpperCase().includes(input) ? "" : "none";
                });
            }

            function filterSektor(sektor) {
                let rows = document.querySelectorAll('.sektor-row');
                rows.forEach(row => {
                    if (sektor === 'Alle') row.style.display = "";
                    else row.style.display = row.getAttribute('data-sektor') === sektor ? "" : "none";
                });
            }

            function sortTable(n) {
                const table = document.getElementById("mainTable");
                const tbody = table.tBodies[0];
                const rows = Array.from(tbody.rows);

                // Sortierrichtung togglen
                const currentDir = table.getAttribute("data-sort-dir") || "desc";
                const dir = currentDir === "asc" ? "desc" : "asc";
                table.setAttribute("data-sort-dir", dir);

                rows.sort((a, b) => {
                    let x = a.cells[n].innerText.trim();
                    let y = b.cells[n].innerText.trim();

                    // 🔢 NUMERISCHE SPALTEN: CRV (4), Zyklus (5), Score (6)
                    if (n === 4 || n === 5 || n === 6) {
                        const xNum = parseFloat(x.replace(/[^0-9,.-]/g, '').replace(',', '.')) || 0;
                        const yNum = parseFloat(y.replace(/[^0-9,.-]/g, '').replace(',', '.')) || 0;
                        return dir === "asc" ? xNum - yNum : yNum - xNum;
                    }

                    // 🔤 TEXT-SPALTEN: Asset, Sektor, Kurs, Signal
                    return dir === "asc"
                        ? x.localeCompare(y, 'de', { sensitivity: 'base' })
                        : y.localeCompare(x, 'de', { sensitivity: 'base' });
                });

                rows.forEach(row => tbody.appendChild(row));
            }

            </script>
        </div>
    </body>
    </html>
    """
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_template)

if __name__ == "__main__":
    generate_dashboard()