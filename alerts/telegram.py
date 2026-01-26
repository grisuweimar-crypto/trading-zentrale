import requests
import config

def send_summary(top_stocks, message_header="🚀 SCANNER TOP CHANCEN"):
    """Sendet eine formatierte Liste der besten Aktien an Telegram."""
    if not top_stocks:
        return

    msg = f"*{message_header}*\n\n"
    for _, stock in top_stocks.iterrows():
        msg += f"✅ *{stock['Name']}*\n"
        msg += f"   Score: {stock['Score']:.1f} | Chance: {stock['MC_Chance']:.1f}%\n"
        msg += f"   Signal: {stock.get('Elliott_Signal', 'N/A')}\n\n"

    url = f"https://api.telegram.org/bot{config.TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": config.TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "Markdown"
    }
    
    try:
        requests.post(url, data=payload, timeout=10)
    except Exception as e:
        print(f"❌ Telegram Fehler: {e}")