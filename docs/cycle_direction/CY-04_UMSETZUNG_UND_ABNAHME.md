# CYCLE-DIR CY-04 — Deskriptive Zyklusrichtung, Dashboard und technische Abnahme

**Plan:** `CYCLE-DIR-2026-10-09-v1` · **Paket:** ausschließlich CY-04  
**Ausgangs-main:** `bf252d38d122e3273c4f317fcff1e1e0cdcecba4` · **Branch:** `feat/cycle-dir-cy04-descriptive-ui-20261010` · **PR:** [#284](https://github.com/grisuweimar-crypto/trading-zentrale/pull/284)  
**Datum:** 10.10.2026, Europe/Berlin

## 1. Abgrenzung und Architektur

Die CY-04-Masterplanvorgaben: 1/5/10 vorausgehende **Scannerbeobachtungen** statt Handelstage; nur verlässlich vergleichbare PIT-Snapshots, kein 0-/50-Fallback; getrennte deskriptive Zustände `UP`, `DOWN`, `UNCHANGED`, `UNAVAILABLE` ohne L3-`NO_DIRECTIONAL_CHANGE`-Semantikänderung. Kompakte Tabelle: Zykluswert + Pfeil/Δ5obs; Detailansicht/Tooltip: 1/5/10obs, Qualitätsgrundlage und Quellenzeiten.

Umsetzung:
- `src/scanner/ui/cycle_direction.py`: vollständig **read-only**. `project_eligible_directions` nimmt nur gesondert als `ELIGIBLE` freigegebene aktuelle Zeilen mit einer gültigen Kette. `load_direction_for_ui` lässt sich ausschließlich durch den nachprüfenden CY-03-Adapter freigeben: `inspect_cycle_archive` führt dessen Integritätsprüfungen aus; das aktuelle v1-Schema liefert explizit `research_released=False`, also **keine reale Richtung**. Die künftige Freigabe verlangt einen eigenen fachlichen CY-03-Vertrags-/Schemareview, keine Flag-Manipulation.
- Aktuelle Daten werden mit `asset_id`, kanonischem Cycle, `VALID`-Qualität, Formelversion, Originalwährung, Preis-/Listing-Symbol und SHA-256 der Cycle-Preisbar gegengeprüft. Bei fehlender/inkohärenter Identität werden alle Deltas und Pfeile explizit `UNAVAILABLE`.
- `src/scanner/ui/generator.py`: neue rein präsentative Felder `cy04_delta_{1,5,10}obs`, `cy04_direction_{1,5,10}obs`, `cy04_quality`, `cy04_as_of`, `cy04_last_bar` nur im auszugebenden UI-Datensatz; keine Änderung an Score-/Risk-/Decision-/Watchlist-CSV, kein Backend-Scoring.
- In der Haupttabelle steht **Zyklus / Δ5**. Ein Pfeil erscheint ausschließlich für echte Δ5-Basis, einschließlich `→ 0.0 Pkt.` als **echte unveränderte Beobachtung**. Kein Datenpunkt dagegen bleibt als `—` ohne Pfeil. Im Detail und Tooltip werden alle drei Lags mit Gründen, Zeitbezug und Qualitätszustand erläutert. Die Zyklusspalte ist auch auf kleinen Displays sichtbar, die Tabelle bleibt horizontal scrollbar.

**Fachlicher Hinweis im UI:** `Scannerbeobachtungen ≠ Handelstage; Zykluspunkte ≠ Prozent-Rendite; Anstieg ≠ Kursprognose.`

## 2. Reale Produktivdaten und harte Freigabegrenze

Das `artifacts/cycle_history/manifest.json` auf dem Ausgangs-`main` verzeichnet genau den Snapshot `f409c312-0349-4b29-8dab-3ecdbd9463b3`, `as_of=2026-10-10`, Run `github-38040088358-1`, **215** archivierte Assetzeilen, **209** `PROVISIONAL_REPLAYED`, **6** ausgeschlossen, `lag_1/5/10=0/0/0` und `research_eligible=0`.

**Sperre:** Issue [#269](https://github.com/grisuweimar-crypto/trading-zentrale/issues/269) (Börsen-/Bar-/Währungs- und historische PIT-Herkunft) offen. Der Cy-03-v1-Adapter erkennt technisch hashverifizierte Beobachtungen, **aber keine zugelassene Release-Version**. Die aktuellen 209 gültigen Rechnungen dürfen daher nicht stillschweigend in Forschungs- oder Richtungsnachweise umgewandelt werden. Das ist eine absichtliche **fail-closed**-Anzeige, kein UI-Defekt.

Das aktuelle 215er-Archiv darf weder bei diesem Paket verändert noch rückwirkend umdeklariert werden. Auch die vorhandenen alten `history_analysis.csv`-Zykluswerte bleiben für CY-04 ausgeschlossen.

## 3. Tests, Abnahme und Risiken

**Eigener technischer Prüflauf:** `.github/workflows/cycle_dir_cy04.yml` (PR-Trigger). Der Workflow prüft `python -m compileall -q src/scanner/ui src/scanner/reports/cycle_history.py`, dann
`python -m pytest -q tests/test_cycle_direction_cy04.py tests/test_cycle_history_cy03.py tests/test_cycle_quality_cy01.py tests/test_research_views.py`, rendert das UI mit dem **realen veröffentlichten** `watchlist_full.csv` in ein **temporäres** Verzeichnis und führt `node --check` auf dem erzeugten Dashboard-JavaScript aus. Kein Commit an `artifacts/`.

Die Regressionen decken ab: echte Cycle-`0` versus Missingness, `UP`, `DOWN`, `UNCHANGED`, die 1/5/10-Observation-Grenzen, fehlende Vorgänger, Quarantänestatus, STALE, unterschiedliche Listings/Währungen/Formeln/Preis-SHA, neueste Snapshot-Identität, superseded same-day Snapshots, unveränderte Score-/dScore-Tabellenspalten und **keinen** produktiven Pfeil ohne Release.

**Formale Ergebnisse, Job-/Run-ID, Anzahl Tests und Merge-SHA** werden erst nach tatsächlich beendeten GitHub-Checks eingetragen. Grün für UI bedeutet **nicht** fachlich freigegebene Richtung und keinen empirischen Trading-Mehrwert.

## 4. Stop-Gates und Wiedereinstieg

1. **Technischer CY-04-Abschluss:** UI-Patch plus Test-/Smoke-Regression auf `main` mit explizitem Release-Stopp; danach `FERTIG_TECHNISCH`.
2. **Fachlicher CY-04-Abschluss:** CY-03 erhält unabhängig geprüfte, source-/PIT-/identitätskonforme und **mehrtägige** Historie mit tatsächlich zulässigem 1/5/10-Observation-Lag und einer versionierten Forschungs-/UI-Freigabe. Danach muss ein echtes, gematchtes Asset einen der Pfeile/Δ5 in der live publizierten UI zeigen; hierfür **neue Abnahme**, nicht fiktiver Test.
3. Kein automatischer Übergang nach CY-06/Elliott, kein Scoring-/Decision-/Portfolio-Eingriff.
4. Veralteter Dokumentations-Draft [#279](https://github.com/grisuweimar-crypto/trading-zentrale/pull/279) wird nach technischer CY-04-Abnahme als überholt markiert; seine Stop-Gate-Befunde bleiben fachlich respektiert.

## 5. CYCLE-DIR-Übergabe

- **Paket:** CY-04; **Fachziel:** Richtung + Δ5obs im Scanner-Dashboard, nur bei vollständigem Nachweis
- **Technisch:** Implementierung im separaten PR #284; Merge-/CI-Abnahme siehe nachgetragene Ergebnisse
- **Fachlich:** derzeit **NICHT_FREIGEGEBEN** (CY-03 v1 ohne externe Quellen-/PIT-Freigabe, keine Ketten)
- **Empirisch:** nicht Gegenstand des UI-Pakets; **kein Signalnachweis**
- **Datenbasis:** Snapshot `f409c312-0349-4b29-8dab-3ecdbd9463b3`, `as_of=2026-10-10`, 215 Assetzeilen / 0/0/0 Lags
- **Nicht geändert:** produktive Score-/History-/Decision-/Portfolio-Module, alte Snapshots, Legacy-Cycle
- **Nächster fachlicher Arbeitsschritt:** zunächst CY-02/#269 und CY-03-Quelle/PIT/Historienabnahme, anschließend reales UI-Delta-Replay und getrennte empirische History-Eignungsprüfung für CY-06.
