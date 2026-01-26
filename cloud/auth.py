import os
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def get_gspread_client():
    # Wir nehmen NUR dein Secret
    creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    
    if not creds_json:
        raise ValueError("❌ Fehler: Secret 'GOOGLE_CREDENTIALS' nicht gefunden!")

    try:
        info = json.loads(creds_json)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            info, 
            ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        )
        return gspread.authorize(creds)
    except Exception as e:
        raise ValueError(f"❌ Fehler beim Login mit GOOGLE_CREDENTIALS: {e}")