import os
import requests


def _is_enabled() -> bool:
    """Gate Telegram sending. Default OFF.

    Enable explicitly with TELEGRAM_ENABLED=1/true/yes.
    """
    v = os.getenv("TELEGRAM_ENABLED", "0").strip().lower()
    return v in {"1", "true", "yes", "y", "on"}


# Ziehe Daten aus Environment Variables (kompatibel zu Legacy-Namen)
TOKEN = (
    os.getenv("TELEGRAM_BOT_TOKEN")
    or os.getenv("TELEGRAM_TOKEN")
    or os.getenv("TELEGRAM_BOT_API_TOKEN")
    or "DEIN_BOT_TOKEN"
)
CHAT_ID = (
    os.getenv("TELEGRAM_CHAT_ID")
    or os.getenv("TELEGRAM_CHATID")
    or os.getenv("TELEGRAM_CHANNEL_ID")
    or "DEINE_CHAT_ID"
)


def _currency_symbol(code: str) -> str:
    """Währungs-Code in Anzeige-Symbol (z. B. für Telegram)."""
    m = {"USD": "$", "EUR": "€", "CHF": "CHF", "GBp": "p", "CAD": "C$", "NOK": "kr"}
    return m.get(str(code).upper(), code)


def _configured() -> bool:
    return TOKEN != "DEIN_BOT_TOKEN" and CHAT_ID != "DEINE_CHAT_ID"


def send_signal(ticker, elliott_data, score, name="Unbekannt", currency="USD"):
    """Sende ein kompaktes Signal an Telegram.

    Hinweis: Telegram ist optional und standardmäßig deaktiviert.
    """
    if not _is_enabled() or not _configured():
        return False

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

        response = requests.post(url, json=payload, timeout=12)
        if response.status_code != 200:
            print(f"⚠️ Telegram Fehler: {response.text}")
            return False
        return True
    except Exception as e:
        print(f"❌ Telegram Exception: {e}")
        return False


def send_message(message: str) -> bool:
    """Allgemeine Nachricht senden (optional)."""
    if not _is_enabled() or not _configured():
        return False

    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}

        response = requests.post(url, json=payload, timeout=12)
        if response.status_code != 200:
            print(f"⚠️ Telegram Fehler: {response.text}")
            return False
        return True
    except Exception as e:
        print(f"❌ Telegram Exception: {e}")
        return False
