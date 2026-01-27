import os
import requests

# Secrets vom Hub laden
TOKEN = os.getenv("TELEGRAM_TOKEN", "DEIN_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "DEINE_CHAT_ID")

def send_signal(ticker, elliott_data, score, name="Unbekannt"):
    if TOKEN == "DEIN_BOT_TOKEN":
        return

    try:
        # Hier nutzen wir jetzt den Namen statt nur den Ticker/ISIN
        message = (
            f"🚀 *NEUES SIGNAL für {name}*\n"
            f"🔍 Ticker/ISIN: `{ticker}`\n\n"
            f"📊 Score: {score}/120\n"
            f"📈 Signal: {elliott_data.get('signal', 'Warten')}\n"
            f"🎯 Ziel: {elliott_data.get('target', 0)} €\n"
        )
        
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
        
        requests.post(url, json=payload)
    except Exception as e:
        print(f"❌ Telegram Fehler: {e}")