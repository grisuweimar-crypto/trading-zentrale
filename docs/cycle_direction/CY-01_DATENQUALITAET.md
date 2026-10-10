# CYCLE-DIR — CY-01 Datenqualität / Korrekturstand

**Plan:** CYCLE-DIR-2026-10-09-v1 · **Vorgänger:** CY-00 über PR #259 auf main gemergt.  
**Branch:** `fix/cycle-dir-cy01-data-quality-20261009`  
**Fachliche Grenze:** keine Neuberechnung des Oszillators (CY-02), kein nachträgliches Rewrite der bestehenden Scanner-/Research-Archive (CY-03), keine Änderung an Score/Risk/Decision, keine Modell- oder Signalfreigabe.

## Defekt und konkrete Abhilfe

`src/scanner/app/build_watchlist.py` wandelte fehlende/nichtnumerische `Zyklus %` per `.fillna(0.0)` in echte `cycle=0` um. Die parallel vorhandene Rohspalte `cycle` enthielt am geprüften 08.10.2026 selbst 156 Nullen und durfte daher kein Row-wise-Fallback sein.

Nun wird die Originalspalte `Zyklus %` **spaltenweit vorgezogen**. Fehlende Werte werden `NaN` und der Wert bekommt `cycle_quality=MISSING_SOURCE`; ungültige, nicht endliche bzw. außerhalb 0..100 liegende Werte bekommen `INVALID_VALUE`. Explizit aufgezeichnete numerische 0 und 100 bleiben erhalten; 50 wird nicht mehr als technischer Missing-Fallback erzeugt. Extern vorgegebene `STALE`- und `INSUFFICIENT_HISTORY`-Flags blockieren die Ausgabe auch bei numerischem Quellwert.

Die Felder `cycle_quality` und `cycle_source` fließen in `watchlist_full.csv`, `score_history.csv`, `latest_scanner.csv` sowie `history_recent.csv`/`history_analysis.csv` für **neu erfasste** Beobachtungen. Sie sind keine validierten historischen Rekonstruktionen. Ein neuer, kleiner `artifacts/reports/cycle_quality.csv` dokumentiert den Zustand jeder aktuellen Zeile.

Die tägliche Research-Brücke respektiert ein explizit leeres `cycle` und verbietet einen nicht mehr gültigen Rückfall auf `Zyklus %`. Die Weboberfläche zeigt fehlende Werte `—` statt `0%`; Detail und Tabelle erhalten die gleiche Semantik. Der serverseitige Fallback gab bei NaN bereits leer aus und wurde unverändert gelassen.

## Datenquelle und Geltungsgrenzen

**`cycle_quality=VALID` besagt in CY-01 nur, dass ein *aufgezeichneter* Quellwert numerisch und im gültigen Bereich 0..100 liegt, nicht, dass er am Scanner-Tag frisch aus Bars berechnet wurde.** `cycle_source=LEGACY_ZYKLUS_PCT` bezeichnet die Ursprungs-*Spalte*, nicht den Beweis einer historischen Formel. Die zeitpunktbezogene Formel-/Bar-/Source-Provenance ist weiterhin **UNVERIFIZIERT**; sie wird erst durch CY-02 kontrolliert geliefert. Ebenso wird ein von Legacy mechanisch eingesetztes 50 ohne Ursprungsbeweis nicht automatisch zu einem nachgewiesenen echten Zykluswert.

Wegen potenziell kontaminierter historischer 0-Werte wurde ein **nicht destruktives** Snapshot-spezifisches Ausschlussartefakt angelegt:

`artifacts/research/cycle_quality/exclusion_mask_2026-10-08.csv`

- Snapshot: `32739832-2481-491f-b131-f7413c07f6b2`, Stand 08.10.2026
- 80 **MISSING_SOURCE_IMPUTED_ZERO**, darunter 75 Aktien und fünf Kryptowährungen mit eigener `CRYPTO:...`-Asset-ID
- drei **EXPLICIT_ZERO_UNVERIFIED**, die weder als falsch noch als echt bewiesen sind
- 83 Einträge insgesamt, feste Zuordnung zur historischen Snapshot-ID, zum alten Quell-/Yahoo-Symbol und zum damaligen veröffentlichten Nullwert
- Das Artefakt ist eine **Quarantäneempfehlung** für diesen einen Snapshot und wird von den alten L2-/L3-Engines aktuell **nicht automatisch konsumiert**. Die vollständige historienübergreifende maskengestützte Feature-Eignung bleibt CY-03; alte Artefakte werden nicht still verändert.

## Test- und Gate-Vorgehen

Die neue CI `.github/workflows/cycle_dir_cy01.yml` enthält Syntax- und Regressionsprüfungen für CY-01, History, Research und bestehende Pattern-Discovery-Module L2/L3. Tests decken Blank→NA, Nichtzahl/Range→INVALID, gemessene 0/100/50, getrennte Roh-/Kanonikspalte, explizites STALE-/INSUFFICIENT_HISTORY-Veto, leeres Feld in History und UI-Darstellung ab.

Abnahme erst nach **grünem CI-Lauf**, Prüfung weiterer abhängig getesteter Workflows und Code-Review der qualitativen Semantik. Die erstmalige erfolgreiche Codeprüfung bedeutet noch nicht, dass zeitliche Validität, Musterwirkung oder eine Kauf-/Verkaufsempfehlung wissenschaftlich bewiesen sind.

## Nächste Schritte

1. PR-CI auf tatsächliche Testresultate prüfen; auftretende Fehler in diesem Branch korrigieren.
2. Stellen mit `cycle`-/`Zyklus %`-Fallbacks nochmals auf Datenwiederbelebung prüfen.
3. Beim ersten neuen vollständigen Scannerlauf die neu publizierten `cycle_quality`-Counts gegen `artifacts/reports/cycle_quality.csv` und Research-Snapshot prüfen.
4. Relevante Score-/Risk-/Decision-Verhaltensgleichheit via Regression nachweisen.
5. Danach CY-01 formal schließen. **CY-02** soll eine echte, versionierte Berechnung einschließlich per-Bar-As-of-Provenance liefern; **CY-03** muss die historische Quarantäne in die Pattern-Research-Eignung integrieren.

## 10.10.2026 — CY-01 Abnahmeprotokoll / unabhängige QM-Triage

**Geprüfter CY-01-Kandidat:** `c5aa666db3e4eba1d056cec81177afb6864c161d`. Die CY-01-CI **cycle-quality** ist erfolgreich (78 Tests, Syntax-Kompilierung). BA-QM8 Scanner E2E, QM-B Historical Taxonomy, QM-B Observed Membership und Pattern Discovery L10–L13 waren auf diesem PR-Head ebenfalls grün.

**Separater globaler Blocker:** Die vollständige Return Integrity Recheck auf demselben CY-01-Head meldete `3 failed, 1607 passed`, Lauf [#38027504183](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/38027504183). Nicht als grün verschweigen. Betroffen: Elliott Stage4 Sufficient Stats (1e-17-Mean-Abweichung), QM10 frozen-vs-live Snapshot Assertion und Magma frozen-vs-live rank Assertion.

**Kontrollierter Basistest:** Temporäre PR [#265](https://github.com/grisuweimar-crypto/trading-zentrale/pull/265) verwendete im eigenen Workflow exakt den unveränderten `main`-SHA `5eb1ff9a2962c30b563222328d2f24c986471bec`; Lauf [#38028559730](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/38028559730) zeigte `2 failed, 1 passed`:
- QM10: identischer Fehler auch auf unverändertem main (Proof bindet 06.10., live Metadata 09.10.).
- Magma: identischer Fehler auch auf unverändertem main (live rank 214 vs frozen 212).
- Elliott: besteht isoliert auf main; die Ursache der letzten Ziffer im Full-Suite-Kontext ist **nicht** abschließend isoliert und darf nicht als definitiv unabhängig bewiesen bezeichnet werden.

**Governance:** Die drei Cross-QM-Befunde bleiben als eigenständiges [QM-Issue #264](https://github.com/grisuweimar-crypto/trading-zentrale/issues/264) offen. Sie dürfen nicht durch das Umschreiben historischer Verträge, das Loosen von Promotion-Gates oder stilles Verwerfen der Regressionstests beseitigt werden. Ein eventueller CY-01-Merge kann deshalb ausschließlich als **bewusste, eng begrenzte Modulabnahme mit offen dokumentierter globaler Regression** gelten, niemals als Behauptung eines grünen gesamthaften Repository-Gates.

**CY-01-Umfang:** Missingness-/Range-Validierung, Fallback-Verbot, nullable Research/History/UI, QM-Status, Snapshot-spezifische Nullwert-Quarantäne; keine neue Formel, keine rückwirkende Ergänzung alter Daten, kein produktives Scoring-/Decision-Upgrade. Das alte Zyklusfeld beweist keine aktuelle Bar-Provenance und wird erst in CY-02 wieder frisch berechnet; die historienweite Quarantäne gehört CY-03.

