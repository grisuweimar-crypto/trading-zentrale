import requests
import config

def send_summary(df_top):
    """Erhält die Top-Aktien als DataFrame und versendet sie."""
    if df_top.empty:
        print("⚠️ Telegram: Keine Daten zum Versenden vorhanden.")
        return

    # Nachrichtenkopf
    message = "🚀 **Trading Scanner V27 - Top Signale**\n"
    message += "Währung: **EURO (€)**\n\n"

    # Zeilenweise die Aktien aus dem DataFrame auslesen
    for _, row in df_top.iterrows():
        name = row.get('Name', 'Unbekannt')
        score = row.get('Score', 0)
        kurs = row.get('Akt. Kurs [€]', 0)
        
        message += f"🔹 **{name}**\n"
        message += f"   Score: {score:.1f}\n"
        message += f"   Kurs: {kurs:.2f} €\n\n"

    # Versand über die Telegram API
    url = f"https://api.telegram.org/bot{config.TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": config.TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }

    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            print("✅ Telegram: Nachricht erfolgreich gesendet.")
        else:
            print(f"❌ Telegram-Fehler: {response.text}")
    except Exception as e:
        print(f"❌ Kritischer Fehler beim Telegram-Versand: {e}")