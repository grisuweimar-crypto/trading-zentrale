import os
import json
import gspread

def get_gspread_client():
    # Nimmt das JSON-Secret aus der Umgebungsvariable `GOOGLE_CREDENTIALS`
    creds_json = os.environ.get('GOOGLE_CREDENTIALS')

    if not creds_json:
        raise ValueError("❌ Fehler: Secret 'GOOGLE_CREDENTIALS' nicht gefunden!")

    try:
        info = json.loads(creds_json)
        # Moderner gspread-Helper: erstellt Client direkt aus dict
        return gspread.service_account_from_dict(info)
    except Exception as e:
        raise ValueError(f"❌ Fehler beim Login mit GOOGLE_CREDENTIALS: {e}")