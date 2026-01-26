import requests

# Deine Telegram-Daten (Trag hier deine echten Daten ein!)
TOKEN = "DEIN_BOT_TOKEN"
CHAT_ID = "DEINE_CHAT_ID"

def send_signal(ticker, elliott_data, score):
    """
    Verschickt ein Signal an deinen Telegram-Bot.
    """
    try:
        message = (
            f"🚀 *NEUES SIGNAL: {ticker}*\n\n"
            f"📊 Score: {score}/120\n"
            f"📈 Signal: {elliott_data.get('signal', 'Warten')}\n"
            f"🎯 Ziel: {elliott_data.get('target', 0)} €\n"
            f"💰 Einstieg: {elliott_data.get('entry', 0)} €\n"
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
            print(f"⚠️ Telegram Fehler: {response.text}")
            
    except Exception as e:
        print(f"❌ Telegram Fehler bei {ticker}: {e}")