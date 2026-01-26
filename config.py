import os
import json
from dotenv import load_dotenv

load_dotenv()

# Wir nehmen das Secret "GOOGLE_CREDENTIALS" von GitHub
creds_raw = os.getenv("GOOGLE_CREDENTIALS")

if creds_raw:
    try:
        # Wir speichern es intern unter einem einheitlichen Namen
        GOOGLE_SHEETS_JSON = json.loads(creds_raw)
    except:
        GOOGLE_SHEETS_JSON = {}
else:
    GOOGLE_SHEETS_JSON = {}

# 1. Telegram Daten
TELEGRAM_TOKEN = get_secret("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = get_secret("TELEGRAM_CHAT_ID")

# 2. Google Credentials
# GitHub Secrets speichert das JSON oft als einen langen String
creds_raw = get_secret("GOOGLE_CREDENTIALS")

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