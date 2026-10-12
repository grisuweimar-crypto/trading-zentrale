# ZERO → Private ARGUS W10 Watch: Betrieb

Der öffentliche GitHub-Runner verarbeitet niemals private Depot-/Kontoexporte. Die Watch wird auf einem vertrauenswürdigen privaten Host mit lokal bereitgestelltem Checkout und einem Verzeichnis außerhalb des Repositories ausgeführt.

## Private Eingaben

- Aktien-CSV und Krypto-CSV desselben ZERO-Exports
- ZERO-Kontoumsätze mit den historischen Buchungen
- private JSON-Datei `{ "broker_ISIN": "EXACT_SCANNER_SYMBOL" }` (jede aktuelle ISIN einmal)
- Zeitstempel des Depotexports `--as-of 2026-10-12T11:15:00+02:00`, sofern tatsächlich zutreffend

Nicht exakt abgebildete Papiere erhalten nur nach expliziter privater Prüfung einen eindeutigen Marker `UNTRACKED:<ISIN>`. Dadurch bleiben sie in der Watch sichtbar, erhalten aber den Zustand `symbol_not_in_daily_research` und dürfen keine aus anderen Aktien übernommenen Aktionen erhalten.

## Private Ausführung

```bash
python scripts/run_zero_private_argus.py \
  --stocks /privat/ZERO-aktienpos.csv \
  --crypto /privat/ZERO-kryptopos.csv \
  --transactions /privat/ZERO-kontoumsaetze.csv \
  --symbol-map /privat/zero_scanner_map.json \
  --as-of '2026-10-12T11:15:00+02:00' \
  --private-dir /privat/argus-run
```

Voraussetzungen: erfolgreich versiegelter W10-Stand, `daily_research.json` passend zum laufenden Decision-Stand, aktuelle Evidence-Runtime. **Ein Positionsstand nach dem Decision-Snapshot ist nicht zulässig.** Beim nächsten publizierten Scanner-Snapshot kann der Brokerexport zeitlich passend eingehängt werden. Keine Auswertung älterer Decision-Bundles anhand später bekannt gewordener Positionen.

## Ausgaben

- `positions.json`: private Bestände und Kaufpreise
- `ledger.json`: datierte Transaktionen, Dividendensummen und Bestandsabgleich
- `watch.json`: Private 7H-Watch, soweit der versiegelte W10-Bestand ausreicht
- `watch_diagnostics.json`: Prüfbericht (keine vollständigen Depotdaten)

Der Job verweigert eine Ausführung bei Bestandsabweichungen, nicht verknüpften ISINs oder ungültigen W10-Identitäten. Fehlende Scanner-Titel bleiben explizit unavailable.

## Prospektive Forschung

Das append-only-Journal ist getrennt implementiert (`scripts/record_argus_private_decision.py`). Für ein automatisches Capturing bei jedem frischen Scannerlauf ist ein privater, dauerhafter Ausführungs- und Speicherort erforderlich; GitHub Actions ist in diesem Repository **nicht** als Broker-Datenspeicher zu verwenden. Retrospektiv erzeugte Bewertungen niemals als Echtzeit-Prognose stempeln.

Private CSV, Mapping-Dateien, Watch-Ausgaben oder Journale dürfen nicht in öffentliche Pull Requests, Artefakte oder Logs gelangen.
