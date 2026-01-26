"""
TRADING SCANNER V27 - GitHub Actions Edition
"""

# MAIN
from cloud.repository import TradingRepository

repo = TradingRepository()

df_wl = repo.load_watchlist()
df_pf = repo.load_portfolio()

print("✅ Watchlist:", len(df_wl))
print("✅ Portfolio:", len(df_pf))

# CONFIG
import os
import json

def require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"❌ ENV-VARIABLE FEHLT: {name}")
    return val

# === GOOGLE ===
GOOGLE_SHEET_NAME = require_env("GOOGLE_SHEET_NAME")
GOOGLE_KEY = json.loads(require_env("GOOGLE_KEY"))

# === TELEGRAM ===
TELEGRAM_TOKEN = require_env("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = require_env("TELEGRAM_CHAT_ID")

# === SCANNER ===
MC_DAYS = int(os.getenv("MC_DAYS", "30"))
MC_SIMULATIONS = int(os.getenv("MC_SIMULATIONS", "500"))

#CLOUD AUTH
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from config import GOOGLE_KEY

def get_gspread_client():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        GOOGLE_KEY,
        scope
    )
    return gspread.authorize(creds)

#CLOUD REPOSITORY
import pandas as pd
from cloud.auth import get_gspread_client
from config import GOOGLE_SHEET_NAME

class TradingRepository:

    def __init__(self):
        self.client = get_gspread_client()
        self.sheet = self.client.open(GOOGLE_SHEET_NAME)

    def load_watchlist(self) -> pd.DataFrame:
        ws = self.sheet.worksheet("Watchlist")
        data = ws.get_all_records()
        if not data:
            raise RuntimeError("❌ Watchlist ist leer")
        return pd.DataFrame(data)

    def load_portfolio(self) -> pd.DataFrame:
        ws = self.sheet.worksheet("Portfolio")
        data = ws.get_all_records()
        if not data:
            raise RuntimeError("❌ Portfolio ist leer")
        return pd.DataFrame(data)

    def save_watchlist(self, df: pd.DataFrame):
        ws = self.sheet.worksheet("Watchlist")
        ws.clear()
        ws.update(
            [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
        )

    def save_portfolio(self, df: pd.DataFrame):
        ws = self.sheet.worksheet("Portfolio")
        ws.clear()
        ws.update(
            [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
        )

#ALERTS TELEGRAM
import requests
from config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

def send(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    r = requests.post(url, data=payload, timeout=10)
    r.raise_for_status()

