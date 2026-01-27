import pandas as pd
import datetime

def generate_dashboard(csv_path='watchlist.csv', output_path='index.html'):
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"❌ Fehler: {e}")
        return

    # --- NUR DIESE LOGIK WURDE GEÄNDERT ---
    # Sektor-Logik: Erst DB, dann Failsafe
    def get_sektor(row):
        s = row.get('Sektor', '')
        if pd.isna(s) or str(s).strip() in ['', '0', 'nan']:
            return 'Andere 🌐'
        return str(s).strip()

    df['Sektor'] = df.apply(get_sektor, axis=1)
    
    # Automatische Button-Generierung basierend auf vorhandenen Sektoren
    unique_sektoren = sorted(df['Sektor'].unique())
    filter_buttons_html = '<button onclick="filterSektor(\'Alle\')" class="px-4 py-2 rounded-full glass text-xs font-bold hover:bg-emerald-600 transition">Alle</button>\n'
    for s in unique_sektoren:
        if s != 'Alle': # "Alle" haben wir schon manuell
            filter_buttons_html += f'                    <button onclick="filterSektor(\'{s}\')" class="px-4 py-2 rounded-full glass text-xs font-bold hover:bg-emerald-600 transition">{s}</button>\n'
    # ---------------------------------------

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
                <div class="flex gap-2 overflow-x-auto pb-2 no-scrollbar">
                    {filter_buttons_html}
                </div>
                <input type="text" id="searchInput" onkeyup="filterTable()" placeholder="Ticker oder Name..." 
                       class="glass rounded-xl px-5 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-emerald-500 w-full md:w-64">
            </div>

            <div class="glass rounded-3xl overflow-hidden shadow-2xl border-none">
                <div class="overflow-x-auto">
                    <table class="w-full text-left" id="mainTable">
                        <thead class="bg-slate-900/80 border-b border-slate-700">
                            <tr class="text-[10px] text-slate-400 uppercase tracking-widest font-bold">
                                <th class="px-6 py-5" onclick="sortTable(0)">Asset <i class="fa-solid fa-sort ml-1 opacity-30"></i></th>
                                <th class="px-6 py-5" onclick="sortTable(1)">Sektor <i class="fa-solid fa-sort ml-1 opacity-30"></i></th>
                                <th class="px-6 py-5 text-right font-black" onclick="sortTable(2)">Kurs & Trend <i class="fa-solid fa-sort ml-1 opacity-30"></i></th>
                                <th class="px-6 py-5 text-center" onclick="sortTable(3)">Signal <i class="fa-solid fa-sort ml-1 opacity-30"></i></th>
                                <th class="px-6 py-5 text-right cursor-pointer hover:text-white" onclick="sortTable(4)">Score <i class="fa-solid fa-sort ml-1 opacity-30"></i></th>
                            </tr>
                        </thead>
                        <tbody class="divide-y divide-slate-800">
    """

    for _, row in df.iterrows():
        is_buy = row['Elliott-Signal'] == "BUY"
        sig_class = "bg-emerald-500/10 text-emerald-400 border border-emerald-500/30" if is_buy else "bg-slate-800 text-slate-500"
        score = row['Score']
        upside = row.get('Upside', 0)
        pe = row.get('PE', 0)
        
        # 24h Performance Logik
        perf = float(row.get('Perf %', 0.0))
        perf_icon = "↑" if perf > 0 else "↓" if perf < 0 else "→"
        perf_color = "text-emerald-400 font-bold" if perf > 0 else "text-rose-500 font-bold" if perf < 0 else "text-slate-500"

        # Infobox für Asset (Deep Dive)
        asset_tooltip = f"""
            <div class='tooltip glass p-4 rounded-xl text-[11px] w-56 shadow-2xl -mt-12 left-0'>
                <p class='font-bold text-blue-400 mb-1 border-b border-white/10 pb-1'>{row['Name']}</p>
                <div class='flex justify-between mt-2'><span>Upside:</span><span class='text-green-400 font-bold'>{upside}%</span></div>
                <div class='flex justify-between'><span>KGV (PE):</span><span class='text-amber-400 font-bold'>{pe if pe > 0 else '-'}</span></div>
                <div class='mt-2 pt-1 border-t border-white/10 italic text-slate-500 text-[9px]'>Klick für Yahoo Finance</div>
            </div>
        """

        # Infobox für Score (Aufschlüsselung)
        score_tooltip = f"""
            <div class='tooltip glass p-4 rounded-xl text-[11px] w-48 shadow-2xl -mt-20 -ml-20'>
                <p class='font-bold border-b border-white/10 mb-2 pb-1 text-emerald-400 text-center uppercase tracking-tighter'>Score-Faktoren</p>
                <div class='flex justify-between'><span>Elliott Wave:</span><span class='font-mono'>+20</span></div>
                <div class='flex justify-between'><span>Fundamental:</span><span class='font-mono'>+25</span></div>
                <div class='flex justify-between'><span>Statistik:</span><span class='font-mono'>+{int(row.get('MC-Chance',0)/5)}</span></div>
                <div class='mt-2 pt-1 border-t border-white/10 font-black flex justify-between uppercase'>
                    <span>Gesamt:</span><span>{score}</span>
                </div>
            </div>
        """

        html_template += f"""
                    <tr class="hover:bg-white/[0.02] transition group sektor-row" data-sektor="{row['Sektor']}">
                        <td class="px-6 py-5 relative has-tooltip cursor-help">
                            {asset_tooltip}
                            <div class="flex flex-col">
                                <a href="https://finance.yahoo.com/quote/{row['Ticker']}" target="_blank" class="font-bold text-slate-100 group-hover:text-emerald-400 transition cursor-pointer">{row['Name']}</a>
                                <span class="text-[10px] font-mono text-slate-500 uppercase">{row['Ticker']}</span>
                            </div>
                        </td>
                        <td class="px-6 py-5 text-xs text-slate-400 font-medium">
                            {row['Sektor']}
                        </td>
                        <td class="px-6 py-5 text-right font-mono">
                            <div class="flex flex-col items-end">
                                <span class="text-sm font-bold text-slate-200">{row['Akt. Kurs [€]']} €</span>
                                <span class="{perf_color} text-[10px]">{perf_icon} {abs(perf)}%</span>
                            </div>
                        </td>
                        <td class="px-6 py-5 text-center">
                            <span class="{sig_class} px-3 py-1 rounded-full text-[9px] font-black uppercase tracking-tighter">
                                {row['Elliott-Signal']}
                            </span>
                        </td>
                        <td class="px-6 py-5 text-right relative has-tooltip cursor-help">
                            {score_tooltip}
                            <span class="text-lg font-black text-white">{score}</span>
                            <div class="w-8 h-1 bg-slate-700 rounded-full ml-auto mt-1 overflow-hidden">
                                <div class="bg-emerald-500 h-full" style="width: {(score/120)*100}%"></div>
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
                let table = document.getElementById("mainTable");
                let rows = Array.from(table.rows).slice(1);
                let dir = table.getAttribute("data-sort-dir") === "asc" ? "desc" : "asc";
                table.setAttribute("data-sort-dir", dir);

                rows.sort((a, b) => {
                    let x = a.cells[n].innerText.replace(' €', '').replace('%', '').replace('↑', '').replace('↓', '').replace('→', '').trim();
                    let y = b.cells[n].innerText.replace(' €', '').replace('%', '').replace('↑', '').replace('↓', '').replace('→', '').trim();
                    
                    let xNum = parseFloat(x);
                    let yNum = parseFloat(y);
                    
                    if (!isNaN(xNum) && !isNaN(yNum)) {
                        return dir === "asc" ? xNum - yNum : yNum - xNum;
                    }
                    return dir === "asc" ? x.localeCompare(y) : y.localeCompare(x);
                });
                rows.forEach(row => table.tBodies[0].appendChild(row));
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