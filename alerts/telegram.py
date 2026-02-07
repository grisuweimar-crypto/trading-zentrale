import os
import requests

# Zieht die Daten aus den GitHub Secrets
TOKEN = os.getenv("TELEGRAM_TOKEN", "DEIN_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "DEINE_CHAT_ID")

def _currency_symbol(code: str) -> str:
    """Währungs-Code in Anzeige-Symbol (z. B. für Telegram)."""
    m = {"USD": "$", "EUR": "€", "CHF": "CHF", "GBp": "p", "CAD": "C$", "NOK": "kr"}
    return m.get(str(code).upper(), code)


def send_signal(ticker, elliott_data, score, name="Unbekannt", currency="USD"):
    if TOKEN == "DEIN_BOT_TOKEN":
        return

    sym = _currency_symbol(currency)
    try:
        message = (
            f"🚀 *NEUES SIGNAL für {name}*\n"
            f"🔍 Ticker/ISIN: `{ticker}`\n\n"
            f"📊 Score: {score}/120\n"
            f"📈 Signal: {elliott_data.get('signal', 'Warten')}\n"
            f"🎯 Ziel: {elliott_data.get('target', 0)} {sym}\n"
            f"💰 Einstieg: {elliott_data.get('entry', 0)} {sym}\n"
        )
        
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
        
        response = requests.post(url, json=payload)
        if response.status_code != 200:
            print(f"⚠️ Telegram Fehler: {response.text}")
    except Exception as e:
        print(f"❌ Telegram Exception: {e}")