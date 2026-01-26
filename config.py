import os
import json
from dotenv import load_dotenv

# Lädt lokal die .env Datei (falls vorhanden)
load_dotenv()

def get_secret(key, default=None):
    """Holt Daten aus Umgebungsvariablen (GitHub Secrets oder .env)"""
    return os.getenv(key, default)

# 1. Telegram Daten
TELEGRAM_TOKEN = get_secret("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = get_secret("TELEGRAM_CHAT_ID")

# 2. Google Credentials
# GitHub Secrets speichert das JSON oft als einen langen String
creds_raw = get_secret("GOOGLE_SHEET_CREDENTIALS")

if creds_raw:
    try:
        # Versuche, den String in ein echtes Python-Dictionary umzuwandeln
        GOOGLE_SHEETS_JSON = json.loads(creds_raw)
    except Exception as e:
        print(f"⚠️ Fehler beim Laden der Google-Credentials: {e}")
        GOOGLE_SHEETS_JSON = {}
else:
    GOOGLE_SHEETS_JSON = {}

# Falls du lokal noch direkt testen willst, kannst du hier prüfen:
if not TELEGRAM_TOKEN:
    print("❌ TELEGRAM_TOKEN nicht gefunden! Prüfe deine Secrets/Umgebungsvariablen.")