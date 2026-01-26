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
