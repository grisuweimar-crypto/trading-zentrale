import requests
import config

url = f"https://api.telegram.org/bot{config.TELEGRAM_TOKEN}/sendMessage"
data = {"chat_id": config.TELEGRAM_CHAT_ID, "text": "🤖 Test-Nachricht vom Scanner!"}
response = requests.post(url, data=data)
print(response.json())