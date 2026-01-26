import requests
import config

def send_summary(top_stocks, total_value=0):
    token = config.TELEGRAM_TOKEN
    chat_id = config.TELEGRAM_CHAT_ID
    
    # Header mit Depotwert
    msg = f"🏦 **DEPOT-STATUS**\n"
    msg += f"Gesamtwert: **{total_value:,.2f} €**\n"
    msg += "------------------------------------------\n\n"
    
    msg += "🔥 **TOP SCANNER SIGNALE:**\n"
    for i, (idx, row) in enumerate(top_stocks.head(3).iterrows()):
        emoji = ["🥇", "🥈", "🥉"][i]
        msg += f"{emoji} **{row['Name']}**\n"
        msg += f"   Score: `{int(row['Score'])}` | Kurs: {row['Akt. Kurs [€]']:.2f}€\n\n"
    
    msg += "🏁 _Cloud-Dashboard ist aktuell!_"

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        requests.post(url, data={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"})
    except Exception as e:
        print(f"❌ Telegram-Sende-Fehler: {e}")