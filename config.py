import os
import json
from dotenv import load_dotenv

# Lädt lokal die .env Datei (für deinen Test am PC)
load_dotenv()

def get_secret(key, default=None):
    """Sicherheits-Funktion: Holt Daten von GitHub oder aus der .env"""
    return os.getenv(key, default)

# --- 1. Google Credentials ---
# GitHub nutzt dein Secret "GOOGLE_CREDENTIALS"
creds_raw = get_secret("GOOGLE_CREDENTIALS")

if creds_raw:
    try:
        GOOGLE_SHEETS_JSON = json.loads(creds_raw)
    except Exception:
        GOOGLE_SHEETS_JSON = {}
else:
    GOOGLE_SHEETS_JSON = {}

# --- 2. Telegram Daten ---
TELEGRAM_TOKEN = get_secret("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = get_secret("TELEGRAM_CHAT_ID")

# Name deines Google Sheets (muss exakt so heißen wie online)
GOOGLE_SHEET_NAME = "Trading_Zentrale_Cloud"