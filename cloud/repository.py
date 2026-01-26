import pandas as pd
from cloud.auth import get_gspread_client
from config import GOOGLE_SHEET_NAME
from datetime import datetime

class TradingRepository:
    def __init__(self):
        self.client = get_gspread_client()
        self.sheet = self.client.open(GOOGLE_SHEET_NAME)

    def load_watchlist(self) -> pd.DataFrame:
        ws = self.sheet.worksheet("Watchlist")
        return pd.DataFrame(ws.get_all_records())

    def save_watchlist(self, df: pd.DataFrame):
        ws = self.sheet.worksheet("Watchlist")
        ws.clear()
        ws.update([df.columns.tolist()] + df.fillna("").astype(str).values.tolist())

    def save_history(self, total_value):
        try:
            ws = self.sheet.worksheet("Historie")
            zeitstempel = datetime.now().strftime("%d.%m.%Y %H:%M")
            ws.append_row([zeitstempel, float(total_value)])
        except Exception as e:
            print(f"❌ Historie-Fehler: {e}")

    def load_import_aktien(self) -> pd.DataFrame:
        try:
            ws = self.sheet.worksheet("Import_Aktien")
            return pd.DataFrame(ws.get_all_records())
        except: return pd.DataFrame()

    def load_import_krypto(self) -> pd.DataFrame:
        try:
            ws = self.sheet.worksheet("Import_Krypto")
            return pd.DataFrame(ws.get_all_records())
        except: return pd.DataFrame()