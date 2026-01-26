import os
import json
from google.oauth2 import service_account

def get_credentials():
    # WIR SUCHEN JETZT NUR NOCH NACH DEINEM NAMEN
    creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    
    if not creds_json:
        # Fehlermeldung angepasst auf deinen Secret-Namen
        raise ValueError("❌ Fehler: Das Secret 'GOOGLE_CREDENTIALS' ist leer oder nicht gesetzt!")

    try:
        # Versuche die Anmeldedaten zu laden
        info = json.loads(creds_json)
        return service_account.Credentials.from_service_account_info(
            info, 
            scopes=['https://www.googleapis.com/auth/spreadsheets', 
                    'https://www.googleapis.com/auth/drive']
        )
    except Exception as e:
        raise ValueError(f"❌ Fehler beim Parsen der GOOGLE_CREDENTIALS: {e}")