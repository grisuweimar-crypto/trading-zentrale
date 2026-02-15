import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

class TradingRepository:
    def __init__(self):
        # Wir definieren nur noch den Namen der lokalen Datei
        self.filename = 'watchlist.csv'

    def load_watchlist(self):
        # Schaut nach, ob die CSV da ist, sonst erstellt sie eine leere
        if os.path.exists(self.filename):
            logger.info(f"Lade lokale Daten aus {self.filename}")
            # Lade CSV ohne erzwungene dtypes, um Konflikte zu vermeiden
            df = pd.read_csv(self.filename, dtype=str, na_filter=True)
            # Konvertiere Spalten zu besseren Typen wo sinnvoll
            numeric_cols = ['Akt. Kurs','Perf %','Score','CRV','MC-Chance',
                           'Zyklus %','ROE %','Debt/Equity','Div. Rendite %','FCF','Enterprise Value','Revenue',
                           'FCF Yield %','Growth %','Margin %','Rule of 40','Current Ratio','Institutional Ownership %']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            # Standardisiere/füge neue Spalten hinzu, falls ältere CSVs sie nicht enthalten
            canonical = [
                'Ticker','Name','Yahoo','Akt. Kurs','Akt. Kurs [€]','Währung','Perf %','Score','CRV',
                'Elliott-Signal','Elliott-Einstieg','Elliott-Ausstieg','MC-Chance','Zyklus %','Zyklus-Status',
                'ROE %','Debt/Equity','Div. Rendite %','FCF','Enterprise Value','Revenue','FCF Yield %',
                'Growth %','Margin %','Rule of 40','Current Ratio','Institutional Ownership %','Radar Vector'
            ]
            for col in canonical:
                if col not in df.columns:
                    if col in numeric_cols:
                        df[col] = pd.NA
                    else:
                        df[col] = ''
            return df
        else:
            logger.warning(f"watchlist.csv nicht gefunden, erstelle neues Grundgerüst.")
            # Leeres DataFrame mit standardisierten Spalten
            cols = ['Ticker', 'Name', 'Yahoo', 'Akt. Kurs', 'Währung', 'Perf %', 'Score', 'CRV', 'Elliott-Signal', 'Elliott-Einstieg', 'Elliott-Ausstieg', 'MC-Chance', 'Zyklus %', 'Zyklus-Status', 'ROE %', 'Debt/Equity', 'Div. Rendite %']
            return pd.DataFrame(columns=cols)

    def save_watchlist(self, df):
        # Speichert alles lokal. KEIN Google-Login nötig!
        # Vor dem Speichern: Spalten in standardisierter Reihenfolge schreiben (falls vorhanden)
        canonical = [
            'Ticker','Name','Yahoo','Akt. Kurs','Währung','Perf %','Score','CRV',
            'Elliott-Signal','Elliott-Einstieg','Elliott-Ausstieg','MC-Chance','Zyklus %','Zyklus-Status',
            'ROE %','Debt/Equity','Div. Rendite %','FCF','Enterprise Value','Revenue','FCF Yield %',
            'Growth %','Margin %','Rule of 40','Current Ratio','Institutional Ownership %','Radar Vector','PE',
            'ConfidenceScore','ConfidenceLabel','ConfidenceBreakdown'
        ]
        cols_to_write = [c for c in canonical if c in df.columns] + [c for c in df.columns if c not in canonical]
        df.to_csv(self.filename, index=False, columns=cols_to_write)
        logger.info(f"Erfolgreich lokal gespeichert in {self.filename}")