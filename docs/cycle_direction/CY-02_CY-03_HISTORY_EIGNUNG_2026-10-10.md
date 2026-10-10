# CYCLE-DIR – CY-02/CY-03 und vorhandene History: überprüfte Eignung (10.10.2026)

**Masterplan:** `CYCLE-DIR-2026-10-09-v1` · **Repository-Basis:** `main=2fd666793a08c41ab75ef3639d3a63aca1681288` · **PR:** [#286](https://github.com/grisuweimar-crypto/trading-zentrale/pull/286)  
**Durchführung:** tatsächlicher GitHub Actions [Run #38050138703 / Job #114207433882](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/38050138703/job/114207433882) · **Status:** **Bestands- und Datenqualitätsprüfung durchgeführt**, **CY-03/CY-06 empirische Verwendung nicht freigegeben**.

## 1. Tatsächlich vollständig geprüfte Dateien

Die vier veröffentlichten CSV-Dateien wurden im GitHub-Runner vollständig, speicherschonend (CSV-Streaming) und ohne Dateischreibzugriff ausgewertet. Die SHA-256-Werte wurden dabei **über die tatsächlichen Datei-Bytes neu berechnet**, nicht bloß aus einem alten Manifest kopiert.

| Quelle | Größe (Bytes) | echte Zeilen | Scannertage / Datumsscheiben | belegter Date-Datenbereich | gezählte Asset-Identitäten |
| --- | ---: | ---: | ---: | --- | ---: |
| `artifacts/research/history_analysis.csv` | 12.621.428 | 42.394 | 587 | 17.09.2024–10.10.2026 | 304 |
| `artifacts/research/history_recent.csv` | 10.200.540 | 24.150 | 120 | 12.06.–10.10.2026 | 217 |
| `artifacts/snapshots/score_history.csv` | 12.231.564 | 41.390 | 221 | 10.02.–10.10.2026 | 304 |
| `artifacts/research/price_backfill.csv` | 19.296.193 | 110.496 | 753 | 17.09.2024–09.10.2026 | 215 |

Prüfsummen zur Reproduzierbarkeit:

- `history_analysis.csv`: `0e3ccd36851c5137e397b72c6ff2144f54eb2f6b8954b7ff16096249f84b412a`
- `history_recent.csv`: `c6625275a66e114079cfdaa4ecfcce59e371d69a0f046409558ac138dd1c5c2e`
- `score_history.csv`: `49983edad8cb6e1ead92fed7a584a5826ae34ec12ae1453ea982267b032b2bd7`
- `price_backfill.csv`: `8172c273b14f498c341d5edc15d207de646bad1e320de272081255f80503a212`

**Achtung:** Dass die CSV eine alte `date` trägt, besagt noch nichts darüber, wann dieser Wert vom Provider verfügbar war. `history_analysis`, `history_recent` und `score_history` überlappen beträchtlich: die dort jeweils enthaltenen **8.032** numerischen Cycle-Zeilen sind **keine 24.096 voneinander unabhängigen Signale**. `price_backfill` enthält Preiswerte, **keine** gemessenen Cycle-Signale.

## 2. Cycle-Qualität und Quellen-/PIT-Eignung

Die drei überlappenden Scanner-History-Dateien haben je **8.032 numerische Cycle-Werte**:

| Kennzahl | Je betroffene History-Datei |
| --- | ---: |
| Zykluszahl numerisch | 8.032 |
| Davon echte Zeichenfolge `0` bzw. numerischer Nullwert – Ursprung nicht zwingend belegbar | 2.610 |
| Davon numerischer Wert 50 – Ursprung nicht zwingend belegbar | 70 |
| Numerisch, aber ohne vollständige geprüfte Qualitäts-/Cycle-Source-Kombination | 7.213 |
| `cycle_quality=VALID` gespeichert | 954 |
| `cycle_source=YAHOO_PIT_CYCLE_V1` gespeichert | 848 |

**819 syntaktische Kandidaten** mit zeitpunktbezogenen Feldern (`snapshot_id`, Formelfassung, SHA-256, `VALID` etc.) in `history_recent.csv` und `score_history.csv`. `history_analysis.csv` hat keine `snapshot_id`-Spalte und deshalb **0** vollständig syntaktische Kandidaten nach diesem strengen Schema. **Zusätzlicher tatsächlicher Zeitverteilungs-Check aus Run #38050138703, bei Nachlauf erneut bestätigt:** Alle **819** Kandidaten liegen ausschließlich auf dem **10.10.2026**, keine einzige davon auf einem früheren Datum! `history_recent.csv` hat dafür **1** verschiedene Snapshot-ID, `score_history.csv` insgesamt **4** IDs **am selben Datum**; wiederholte Runs desselben Tages zählen nicht als vergangene unabhängige 1/5/10-Observationstage. `history_analysis.csv` enthält keine `snapshot_id`. Die 819 Kandidaten wurden nicht gegen externes Provider-Datum/amtliche Bar-Verfügbarkeit oder unabhängige Börsenherkunft zertifiziert; sie erhalten **keine** automatische Freigabe. **Die Anzahl früher datierter, zugleich mit Formel/SHA/ID belegter Kandidaten aus diesen Dateien beträgt 0.** Die Abweichung `819` zu `848` bezeichnet unterschiedliche Prüfdefinitionen; keines der Zahlen ist eine gezählte Research-Zulassung.

Außerdem enthielten die Legacy-Zeilen häufig weder beobachtungszeitliche Bar-Publikationsmarker noch eindeutig aktuelle Formeldokumentation. Die bereits bekannte [CY-01-Null-Imputationsmaske](CY-01_DATENQUALITAET.md) bleibt gültig; es erfolgt **keine** pauschale Reparatur auf Basis eines heutigen Yahoo-Downloads.

Für Preisreihen enthält `price_backfill.csv` **110.496** OHLC-/Adjusted-/Volume-Zeilen und ein `retrieved_at`-Feld (109.492 Zeilen mit erfasstem Zeitfeld); dies ist ein **Abrufmarker**, kein Beweis, dass alle Werte bereits zum damaligen `date` bzw. einem alten Scanner-Entscheidungscutoff vorlagen. Es können historische Bar-Targets und Returns sein, sofern deren eigene Datenqualitäts-/Listing-/Adjustierungs-Policy separat bestätigt wird; sie dürfen aber nicht als historisch beobachtete Cycle-Zeilen deklariert werden.

## 3. CY-03-Manifestaudit: unabhängig nachberechnete Hashes

Die echten drei Ledgerdateien `artifacts/cycle_history/observations.csv`, `eligibility.csv` und `coverage.csv` stimmten **bytegenau** mit ihren im `manifest.json` ausgewiesenen SHA-256-Werten überein. Die Maske zählt:

- **1** tatsächlich archivierten CY-03-Snapshot: `f409c312-0349-4b29-8dab-3ecdbd9463b3`, vom **10.10.2026**.
- **215** Asset-Zeilen, davon **209** rechen-/formelgültig und **6** ausgeschlossen.
- Für `lag_1obs`, `lag_5obs` und `lag_10obs` jeweils **209 `NO_PRIOR` und 6 `INVALID_CURRENT`**, also **0** vergleichbare Ketten.
- **215/215** Zeilen tragen `research_status=BLOCKED_EXTERNAL_VERIFICATION_269`, davon **0** Researchzulassungen.
- Ein gehashter publizierter Preisbar-Eingang pro Snapshot ist eine **replayfähige interne** Quelle. Er garantiert **nicht** die unabhängige historische Yahoo-Publikationszeit/Originalwährung.

Die automatische Logik prüft die v1-Gates **fail-closed**, scheitert bei veränderten Bestandsdateien sowie künstlich hochgesetztem `research_eligible` und schreibt keine bestehenden Research-/History-Bytes um.

## 4. CY-02-Fortschritt: Börsenkalender

[PR #285](https://github.com/grisuweimar-crypto/trading-zentrale/pull/285) wurde mit grüner CI in `main` gemergt (`2fd666793a08c41ab75ef3639d3a63aca1681288`): Die dokumentierten JPX-Cash-Equity-Schließtage 21.–23.09.2026 und der KRX-Chuseok-Zeitraum 24./25.09.2026 sind nun versioniert und streng an Börsen-Suffix, Originalwährung und genau belegte Werktage gebunden; alle anderen Lücken bleiben gesperrt. Die sechs historischen Ausschlüsse des 10.10. wurden **nicht** umgeschrieben und deren neuer Status ist erst nach einem neuen echten produktiven Scannerlauf nachzuweisen.

[Issue #269](https://github.com/grisuweimar-crypto/trading-zentrale/issues/269) bleibt für **independent Original-Listing-/Währung-/providerseitige Bar-Publikationszeit** offen. Der kalenderrechtliche Teil ist nicht gleichbedeutend mit einem beweiskräftigen historischen Kursfeed. Keine Erhöhung des `research_eligible`-Flags durch den Kalender-Patch.

## 5. Fachlicher Befund, Nichtfreigabe und präziser Wiederaufnahmeplan

**Ja, die vorhandene History ist umfangreich genug für weitere Daten- und Methodenforschung; nein, sie ist nicht automatisch eine 2024/2025-PIT-belegte Cy-02-Zyklus-Signalhistorie.**

1. **CY-02:** Offiziell geprüfte Börsenkalender-Policy ist umgesetzt. Nächster echter Scanner-Publish muss sich anhand aktueller Qualitätsverteilung und exakt replayter unveränderter Bars bewähren. Danach Originalwährungs-/Listing-/Provider-Barzeit-Vertragsnachweis für konkrete repräsentative und problematische Ticker separat sichern und signiert/versioniert kontrollieren.
2. **CY-03:** Mindestens zwei reale, verschiedene veröffentlichte Snapshot-Tage für den ersten `1obs`-Vergleich, sechs für `5obs`, elf für `10obs`. Zusätzlich müssen identische Asset-/Währungs-/Formel-/Kalender- und Bar-Identitäten, Hash-Ketten und externe Quellenfreigaben tatsächlich nachgewiesen sein. Scannerbeobachtungen sind **keine** Handelstage. Die zeitliche Einteilung ist keine automatische statistische Eignung.
3. **Hochgeladene bzw. bereits archivierte History:** Im lokalen Repo vorhandene Versionen sind hier vollständig gescannt. Bevor ein älterer separat hochgeladener Datensatz neu gebraucht wird, ist die SHA-/inhaltliche Gleichheit zu prüfen; **kein erneuter Upload erforderlich** für die hier erfüllte Bestandsaufnahme.
4. **CY-06/empirisch:** Syntaktische Candidates erst nach unabhängiger Quellenbewertung mit dem tatsächlich versiegelten CY-05 `FROZEN_PRE_RUN`-L1-Manifest verwenden. Gleiche Asset-/Zeit-/Regime-Coverage A–D, negative Kandidaten, Effektiv-N, FDR, Return-Integrität, Überschneidungen und echte zukünftige Outcomes getrennt prüfen. Eine besonders hohe Trefferquote aus Legacy-Cycle-Werten ohne PIT-Herkunft wäre kein wissenschaftlicher Nachweis.

**Status:** Bestandsprüfung **FERTIG_TECHNISCH**; CY-02 ist für die eng umrissene Kalenderänderung technisch abgenommen, **nicht** generell quellen-/PIT-freigegeben. CY-03 **FERTIG_TECHNISCH_PRODUKTIV / NICHT_FREIGEGEBEN**. CY-06 **BLOCKIERT**, kein statistischer Test durchgeführt. Kein Monitor eingerichtet, wie vom Nutzer gewünscht.
