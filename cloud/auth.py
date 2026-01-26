import gspread
from oauth2client.service_account import ServiceAccountCredentials
import config # Wir importieren die ganze Config

def get_gspread_client():
    # Wir greifen auf die Variable zu, die wir gerade in config.py angelegt haben
    creds_dict = config.GOOGLE_SHEETS_JSON
    
    if not creds_dict:
        raise ValueError("❌ Fehler: GOOGLE_SHEETS_JSON ist leer. Check deine Secrets!")

    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client