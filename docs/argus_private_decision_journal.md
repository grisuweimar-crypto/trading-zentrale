# ARGUS privates Entscheidungsjournal — Forschungsgrenzen

Die drei Erkenntnisebenen sind getrennte Artefakte:

1. **Transaktions-Ledger (retrospektive Fakten):** ZERO-Buchungen mit Datum, Cashbetrag, ISIN und Anzahl, unabhängig davon, ob damals ARGUS existierte.
2. **Retrospektive ARGUS-Diagnose:** heutige versionierte Logik bewertet ältere Konstellationen. `historically_available=false`, `validation_eligible=false`. Nicht als damals veröffentlichte Prognose zählen.
3. **Prospektives Entscheidungsjournal:** VOR den Outcomes festgehaltener Snapshot, Evidenz-Verfügbarkeit, ARGUS-Version, Zustandsbewertung. Nutzereingriffe und Ergebnisse sind separate Ereignisse mit eindeutigen Referenzen.

## Integrität / Trennung

- Journaleinträge sind nur außerhalb des öffentlichen Git-Repositories erlaubt; Datei enthält privatpotenzielle Symbole/Beweggründe.
- Jeder Eintrag besitzt Inhalts-Hash; Kette verknüpft Vorgängerhash. Doppelte Ereignisse sind idempotent.
- Änderungen alter Einträge sind nicht vorgesehen; eventuelle Korrekturen müssen durch neue explizite Einträge erfolgen.
- Hashkette bietet Manipulationserkennung gegenüber unveränderten Ankern, aber keine externe Zeitbeglaubigung. Vor einer echten prospektiven Studie ist unabhängige zeitnahe Archivierung bzw. Timestamp-Provenienz erforderlich.
- `evidence_available_at` muss vor oder gleich `recorded_at` liegen; für historische Scannerdaten zusätzlich tatsächliche PIT-Verfügbarkeit prüfen.
- Outcomes benötigen einen vorbestehenden Ereignisbezug. Outcomes allein dürfen nicht Validierungsreife behaupten.
- Fehlende Daten bleiben fehlend. Nutzerbeweggründe nicht aus Handelsbewegungen erfinden.
- Aktuell keine automatische Erfassung beim Scannerlauf, kein Watch-Journal-Leseadapter, keine Prognosevalidierung; das sind gesonderte Integrationsschritte.

## Forschungsdesign vor Auswertung

Präregistrierung: Horizont (z.B. 5/20/60 Handelstage), benchmark-relative Ergebnisse, Gebühren, FX und Originalwährung, Regime, Stichprobengröße, verschachtelte Daten und mehrfaches Testen. Nur prospektive, rechtzeitig protokollierte Beobachtungen zählen in die primäre Auswertung. Retrospektive Diagnosen dienen Hypothesenbildung, nicht Bestätigung.
