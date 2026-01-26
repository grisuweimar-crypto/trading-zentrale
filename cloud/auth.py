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
