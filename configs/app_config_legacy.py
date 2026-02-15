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

# --- 3. Quality & Control Config ---
# Winsorizing
WINSORIZE_ENABLED = True
WINSORIZE_Q_LOW = 0.01
WINSORIZE_Q_HIGH = 0.99
WINSORIZE_COLS = [
    'Growth %', 'ROE %', 'Margin %', 'Debt/Equity', 
    'Volatility', 'MaxDrawdown', 'RS3M', 'Trend200',
    'DollarVolume', 'AvgVolume'
]

# Confidence Score
CONFIDENCE_WEIGHTS = {
    'coverage': 0.25,
    'confluence': 0.25,
    'risk_clean': 0.20,
    'regime_align': 0.20,
    'liquidity': 0.10
}

CONFIDENCE_CORE_FACTORS = [
    'growth', 'roe', 'margin', 'debt_ratio', 
    'volatility', 'rs3m', 'trend200'
]

CONFIDENCE_OPPORTUNITY_FACTORS = [
    'growth', 'roe', 'margin', 'rs3m', 'trend200'
]

CONFIDENCE_RISK_FACTORS = [
    'volatility', 'drawdown', 'debt_ratio'
]

CONFIDENCE_THRESHOLDS = {
    'HIGH': 75,
    'MED': 50
}

# Calibration Light
CALIBRATION_ENABLED = True
CALIBRATION_SNAPSHOT_PATH = "data/snapshots/score_history.csv"
CALIBRATION_FORWARD_DAYS = 20