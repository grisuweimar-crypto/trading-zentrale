import pandas as pd
import datetime

def generate_dashboard(csv_path='watchlist.csv', output_path='index.html'):
    df = pd.read_csv(csv_path)
    
    # Sortieren: Beste Scores nach oben
    df = df.sort_values(by='Score', ascending=False)

    html_template = f"""
    <!DOCTYPE html>
    <html lang="de">
    <head>
        <meta charset="UTF-8">
        <title>Trading Dashboard - {datetime.datetime.now().strftime('%d.%m.%Y')}</title>
        <script src="https://cdn.tailwindcss.com"></script>
    </head>
    <body class="bg-gray-900 text-gray-100 p-8">
        <h1 class="text-3xl font-bold mb-6 text-blue-400">🚀 Trading Zentrale 2026</h1>
        <div class="overflow-x-auto bg-gray-800 rounded-lg shadow-xl">
            <table class="min-w-full table-auto">
                <thead>
                    <tr class="bg-gray-700 text-left">
                        <th class="px-4 py-3">Ticker</th>
                        <th class="px-4 py-3">Name</th>
                        <th class="px-4 py-3">Kurs</th>
                        <th class="px-4 py-3">Signal</th>
                        <th class="px-4 py-3">Score</th>
                        <th class="px-4 py-3">MC-Chance</th>
                    </tr>
                </thead>
                <tbody>
    """

    for _, row in df.iterrows():
        # Farbe für Signale
        sig_color = "text-green-400 font-bold" if row['Elliott-Signal'] == "BUY" else "text-yellow-400"
        # Farbe für Score
        score_color = "bg-green-600" if row['Score'] > 75 else "bg-blue-600" if row['Score'] > 50 else "bg-gray-600"

        html_template += f"""
                    <tr class="border-b border-gray-700 hover:bg-gray-750 transition">
                        <td class="px-4 py-3 font-mono text-sm">{row['Ticker']}</td>
                        <td class="px-4 py-3">{row['Name']}</td>
                        <td class="px-4 py-3 font-bold">{row['Akt. Kurs [€]']} €</td>
                        <td class="px-4 py-3 {sig_color}">{row['Elliott-Signal']}</td>
                        <td class="px-4 py-3">
                            <span class="{score_color} px-2 py-1 rounded text-xs">{row['Score']}</span>
                        </td>
                        <td class="px-4 py-3 text-gray-400">{row.get('MC-Chance', 0)} %</td>
                    </tr>
        """

    html_template += """
                </tbody>
            </table>
        </div>
        <p class="mt-4 text-gray-500 text-sm italic">Letzter Scan: """ + datetime.datetime.now().strftime('%H:%M:%S') + """</p>
    </body>
    </html>
    """

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_template)
    print(f"✅ Dashboard erstellt: {output_path}")

if __name__ == "__main__":
    generate_dashboard()