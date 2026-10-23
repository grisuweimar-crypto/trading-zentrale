# ARGUS – M2 autonome Kurszonen v0.1 (Research, nicht produktiv)

Stand: 23.10.2026. Gilt für den M2-Prototyp, nicht als abgeschlossene M2/M3/M4/M5-Validierung.

## Ziel und Trennung

ARGUS erzeugt ohne Analystenvideo und ohne fremd gesetzte Zielmarken Preisbänder aus historisch beobachteten, vollständig abgeschlossenen OHLCV-Tageskerzen. Der Code verwendet die vorhandenen kausalen Elliott-vNext-6A-Pivots lediglich als bestätigte Swing-Punkte. **Er behauptet keine Elliott-Zählung.** Scanner-Score, Ranking, Watch und Entscheidungen bleiben unverändert.

- Z1 horizontal: jüngste unabhängige Swing-Lows und Swing-Highs aus drei fixierten kausalen Zeitskalen. Pro Richtung/Grad maximal vier unterschiedliche Preisbänder aus jüngeren 160 beobachteten Sessions.
- Z2 Retracement: 0.382/0.500/0.618/0.786 aus bestätigtem A→B Swing, für bullische und bärische Richtungen. Ursprung A wird als gesondertes strukturelles Invalidierungsband ausgegeben.
- Z3 Zielprojektionen: 1.000/1.272/1.618 aus bestätigtem A→B→C (C muss die Strecke teilweise korrigieren, ohne A zu durchbrechen). Ohne dieses bestätigte Muster bleibt diese Zone leer.
- Die Zonenbreite beträgt auf jeder Seite max(0.30 × ATR14, 0.15 % des Kursniveaus); die Vorabregel wird erst nach einer neuen Versionsnummer verändert. Technische Relevanz und empirische Erfolgswahrscheinlichkeit sind noch **nicht** bestimmt (null/not_validated).
- Keine automatische Zonenkonfluenz-Doppelzählung, kein methodischer Sieger, keine Entry-Fill-, CRV- oder Renditebehauptung. Mehrzeitrahmen-Cluster und Szenario-Trigger folgen als M3.

## CLI für fünf Werte

Python/pytest/pandas/numpy müssen verfügbar sein (Projekt-Dependencies). Beispiel für einen **historischen Research-Replay**:

~~~bash
PYTHONPATH=src python scripts/run_argus_zones_m2.py \
  --input artifacts/market_data/yahoo_ohlcv.csv \
  --manifest configs/argus_zones_m2_assets_v1.json \
  --asof 2026-10-22 \
  --bar-end-utc 2026-10-22T20:00:00Z \
  --price-basis auto \
  --output-dir _argus_zones_m2_preview
~~~

Das Listing-Manifest enthält AMD, INTC, ARM, SMH, NOW samt expliziten Kurswährungen und Handelsplätzen; vor einem produktiven Einsatz sind die Börsenidentitäten separat gegen Primärquellen zu prüfen. Der gemeinsame UTC-Barabschluss ist ausschließlich für den dokumentierten US-Handelstag 22.10.2026 vorgesehen. Er darf nicht stillschweigend auf fremde Börsen, frühere Winterzeit-Stichtage oder allgemeine tägliche Zeitpläne kopiert werden.

Das System gibt reale Zonen nur aus, wenn alle fünf Serien den Vollständigkeits-/Kursbasis-Gate bestehen. Bei fehlenden Tageskerzen, OHLC-Fehlern, nicht konsistenten Adjustierungen oder Währungsabweichung stoppt es sichtbar und gibt **keine** künstlichen Zonen aus. Bei unqualifizierten Swings darf das Ergebnis no_qualifying_zone ausgeben.

Im Forschungsworkflow wird der fünfteilige Preview in einem temporären CI-Verzeichnis erstellt und als befristetes Artefakt bereitgestellt; es erfolgt weder eine Änderung der produktiven historischen Scanner-Outputs noch eine Freigabe an M5. Ein historischer Replay darf nicht als zum damaligen Handelstag veröffentlichte Prognose ausgegeben werden. Das veröffentlichte known_at ist bewusst die **tatsächliche Berechnungsveröffentlichung**, nicht der frühere Pivot-Zeitpunkt; dieser steht separat in jedem Driver.

## Punkt-in-Zeit-Grenzen

Die Analyse filtert die Rohzeilen vor Berechnung und Daten-Hash auf asof. Spätere Kerzen dürfen ein identisches Ergebnis nicht verändern. Pivot ist erst nach seiner confirmed_time verwendbar. Historische Adjusted-OHLC wird aus adj_close/close transformiert, soweit jede Session einen gültigen Faktor hat; bei teilweise fehlenden Adjusted-Closes scheitert die Berechnung. Die Provider-Revisions- und Corporate-Action-**PIT-Qualität historischer Adjustierungen ist damit noch nicht bewiesen**; deshalb keine M4-Walk-forward-Freigabe.

Ein Input mit raw_unadjusted (nur ohne adjustierte Schlusskurse oder bei ausdrücklich gewählter Rohbasis) wird data_quality=degraded markiert. Für produktive Vergleiche von Split-Kandidaten ist eine gesonderte Kapitalmaßnahmenprüfung unverzichtbar. Es gibt kein stilles Zurückfallen von teilweise adjustierten Kursen auf Rohkurse.

## Abnahme

- [x] Eigenständiger Z1/Z2/Z3-Rechenkern mit wiederverwendeter 6A-Swing-Erkennung implementiert.
- [x] Reproduzierbares maschinenlesbares JSON-Schema mit expliziten Drivern, Ankern, Bandkanten und not_validated.
- [x] Fünfer-Forschungsmanifest und CLI, ohne bestehende Scannerentscheidungen zu verändern.
- [ ] GitHub-CI-Tests der neuen Engine: noch zu verifizieren.
- [ ] Die **fünf tatsächlichen** aktuellen historischen Preis-Zonenexports, nicht nur synthetische Beispiele, sind gegen das Archiv auszugeben und auf Datenfehler zu prüfen.
- [ ] M3: deterministische Cluster ohne korrelierte Doppelzählung, mehrstufige bedingte Szenarien.
- [ ] Daten-/Börsenkalender-PIT-/Corporate-Action-Abnahme; danach kontrollierte M5-Quelle statt Replay.

## Direkte Ankopplung an M5

Die JSON-Dokumente enthalten die Pflichtidentität nach dem privaten M5-Schema (origin=argus_zone_engine, asset_id, exchange, currency, timeframe, asof, bar_end_utc, published_at, known_at, data_hash, drivers, zones, scenarios, evidence_status). Das Schema ist zunächst ein **beabsichtigter Datenvertrag**, kein Beweis einer produktiven automatischen M5-Erfassung.

Nächster Schritt: CI-Tests und echte fünf Berichte nachweisen, dann M3-Konfluenz und Ergebnis-/Kostenkontrakte ergänzen. Erst später gesicherte unveränderliche Vorhersagen in den privaten M5-Shadow-Ledger übertragen.
