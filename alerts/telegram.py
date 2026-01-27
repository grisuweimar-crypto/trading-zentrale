import os
import requests

# Zuerst versuchen, die Secrets vom Hub (Umgebungsvariablen) zu laden
# Wenn nicht vorhanden, Fallback auf die Platzhalter
TOKEN = os.getenv("TELEGRAM_TOKEN", "DEIN_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "DEINE_CHAT_ID")

def send_signal(ticker, elliott_data, score):
    # Sicherheitscheck: Wenn noch die Platzhalter drin sind, abbrechen
    if TOKEN == "DEIN_BOT_TOKEN" or CHAT_ID == "DEINE_CHAT_ID":
        print(f"⚠️ Telegram für {ticker} übersprungen: Secrets nicht geladen!")
        return

    try:
        message = (
            f"🚀 *NEUES SIGNAL: {ticker}*\n\n"
            f"📊 Score: {score}/120\n"
            f"📈 Signal: {elliott_data.get('signal', 'Warten')}\n"
            f"🎯 Ziel: {elliott_data.get('target', 0)} €\n"
        )
        
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        payload = {
            "chat_id": CHAT_ID,
            "text": message,
            "parse_mode": "Markdown"
        }
        
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            print(f"📲 Telegram Nachricht für {ticker} gesendet!")
        else:
            # Das ist die Stelle, die dir den 404 wirft
            print(f"⚠️ Telegram API Fehler: {response.text}")
            
    except Exception as e:
        print(f"❌ Telegram Fehler bei {ticker}: {e}")