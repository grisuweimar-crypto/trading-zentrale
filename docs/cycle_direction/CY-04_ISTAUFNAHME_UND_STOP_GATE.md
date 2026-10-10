# CYCLE-DIR CY-04 — Istaufnahme, Stop-Gate und Uebergabe (10.10.2026)

**Plan:** `CYCLE-DIR-2026-10-09-v1`, ausschliesslich CY-04.
**Basis-`main` / Branch:** `a5a28d073acb3e16b2e42d3699b90443616ed2c2` / `docs/cycle-dir-cy04-readiness-20261010`.
**Auditzeit:** 10.10.2026 ca. 11:25 MESZ (Europe/Berlin).
**Entscheidung:** `BLOCKIERT` / `NICHT_FREIGEGEBEN`; **keine** CY-04-UI-Implementierung, kein produktiver Richtungs-Pfeil, keine Behauptung eines technischen oder fachlichen CY-04-Abschlusses.

## 1. Abhaengigkeitspruefung gegen den echten `main`

- `docs/cycle_direction/STATUS.md`: CY-00 `FERTIG_FACHLICH`; CY-01 `FERTIG_TECHNISCH`; CY-02 intern/rechnerisch `FERTIG_TECHNISCH`, externe Original-Listing/Quote-Currency/Bar-PIT-Herkunft **nicht freigegeben** (`#269`); CY-03 erste append-only Produktion `FERTIG_TECHNISCH_PRODUKTIV`, fachlich und researchseitig **nicht freigegeben**.
- CY-03 publiziert durch [Scanner #38040088358](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/38040088358); Basis-Publikationsrevision `7ed4597c487a6e0fca8edd30eba78d5ac6f617b7`; CY-03 CI `#38039377480`: 116 Tests PASS. **Diese Tests sind keine CY-04-Tests.**
- `docs/cycle_direction/CY-03_HISTORISIERUNG_UND_EIGNUNG.md` Abschnitt 9 untersagt den stillen CY-04/CY-05-Uebergang. [#269](https://github.com/grisuweimar-crypto/trading-zentrale/issues/269) ist weiterhin als Quell-/Session-PIT-Gate offen; unabhängige externe Nachweise fehlen.

## 2. Aktuelle reale Datenbasis (read-only aus `main` geprueft)

| Nachweis | Istwert |
|---|---|
| Manifest-Schema | `cycle_observations_cy03_v1` |
| `latest_snapshot_id` | `f409c312-0349-4b29-8dab-3ecdbd9463b3` |
| `run_id` | `github-38040088358-1` |
| `as_of` | `2026-10-10` |
| Snapshot-/Ledger-Zaehler | **1 Snapshot, 215 Asset-Zeilen** |
| `availability` in `observations.csv` | 209 `PROVISIONAL_REPLAYED`, 6 `EXCLUDED_INSUFFICIENT_HISTORY` |
| `eligibility.csv` Lag 1/5/10 | je 209 `NO_PRIOR`, 6 `INVALID_CURRENT`; **0 technische Vergleichsketten** |
| Research-Status | alle 215 `BLOCKED_EXTERNAL_VERIFICATION_269`, `research_eligible=0` |
| `coverage.csv` | 215 Asset/Monat-Zeilen, kein positiver Lag |
| `history_metadata.json` | gleiche `snapshot_id`; `latest_run_complete=true`, `validation.status=ok` |

**Kanonische Dateikette:** `artifacts/cycle_history/{manifest.json,observations.csv,eligibility.csv,coverage.csv,bars/<snapshot_id>.csv.gz}`. Nur diese neue, versionierte CY-03-Kette ist fuer CY-04-Lags zulaessiger Ursprung; `score_history.csv`, `history_analysis.csv` und `history_recent.csv` koennen kontaminierte historische Legacy-Cycles enthalten und duerfen **nicht** als Ersatzvorgaenger dienen.

**Manifest-Datenfingerprints (SHA256, am Audittag aus `main` gelesen):**

- `observations.csv`: `b094acb37e42b183b3a502c6a8bef73baa4a1cc745c5735e`
- `eligibility.csv`: `7113bea0bdc6b0275765a61445a35313c0c1e5fd3bc02550af502402ac8e9c70`
- `coverage.csv`: `6c08cc34d94dfbf62ea98fb797a88bd1196844b883433ccff40cba1771c97d2e`
- Archivierte Bars: `0a7674651b35cf02ecac581398fd0d6ac5204fc0f435e3f5764533117eea8007`

Die SHA256-Werte sind **Manifestreferenzen**, nicht in dieser CY-04-Arbeit nochmals lokal durch eine komplette Byte-Hashpruefung berechnet. Der produktive CY-03-Publikationsbeleg dokumentiert deren urspruenglichen Integritaetsnachweis.

## 3. Exakte CY-04-Aenderungsstellen (Ist, noch nicht geaendert)

| Pfad/Stelle | Heute | Spaeteres CY-04-Minimalpaket |
|---|---|---|
| `src/scanner/reports/cycle_history.py`: `lag_mask`, `verify_existing_history` | definiert same-day canonical, 1/5/10-Lag-Eignung, Identitaets-/Gap-/Quell-/Integritaetssperren | Reuse statt zweite Lag-Semantik; unabhaengige Freigabepruefung erforderlich |
| `artifacts/cycle_history/observations.csv` | Zahl und hochwertige Provenance pro beobachtetem Snapshot | Delta aus genau `cycle(t)-cycle(t-nobs)` erst bei nachweisbar gueltiger Kette |
| `artifacts/cycle_history/eligibility.csv` | Statusmaske `NO_PRIOR`/`INVALID_CURRENT`/`PROVISIONAL_CHAIN` | **Kein Pfeil bei Status ungleich freigegeben**; `PROVISIONAL_CHAIN` allein reicht nicht bei offener #269 |
| `artifacts/research/history_metadata.json` | heutige Snapshot-/Run-Identitaet | Gleichheit der Basis `snapshot_id`, `run_id`, `as_of` vor Ausgabe pruefen |
| `src/scanner/ui/generator.py`: `DEFAULT_COLUMNS` | `cycle`, `cycle_quality`, `cycle_source`, `cycle_status`; `asset_id` nicht in Defaultspalten | sichere eindeutige Asset-Zuordnung vor `by_asset`-Lookup, keine Symbol-Aliasheuristik |
| `src/scanner/ui/generator.py`: `build_ui` | laedt History-Delta-, Segment- und Reality-JSON, aber **keinen** CY-03-Cycle-Direction-View | read-only, validierter Cycle-Direction-View nur bei Gate-Freigabe |
| `src/scanner/ui/generator.py`: `cyclePct`, `formatCycle` | `cycle_quality!=VALID` wird verdeckt; Haupttabelle zeigt gerundete `NN%` ohne Delta | numerischer Cycle + Richtung + `Δ5obs` mit 1 Dezimalstelle, separat gated; kein Null-/50-Ersatz |
| `src/scanner/ui/generator.py`: Table `data-k=cycle`, Tabellenrender, `openDrawer`, Fallback-Tbody | bestehende Sortierung, mobile `hide-sm`, Drawer-Qualitaet, rein numerische Fallback-Zelle | Tabellenlayout und Sortierung erhalten; Δ1/Δ10 + Quell-/Statusdetails im Drawer/Tooltip, Fallback ohne Fake-Pfeile |

Die L3-Research-Kategorie `NO_DIRECTIONAL_CHANGE` bleibt unveraendert; `FLAT`/`unveraendert` ist ausschliesslich eine UI-Bezeichnung fuer **exakt** `delta=0` aus einer gueltigen Kette.

## 4. Geplanter freigabefaehiger Ausgabevertrag (nicht aktiv)

- Basis: pro Asset dieselbe nachgewiesene Listing-/Preis-/Waehrungs-/Formel-Identitaet, gleiche Basis-Snapshotserie, spaetester Tageslauf, streng monotone Quellenzeit. Keine FX-Konversion und keine historischen Rewrites.
- `delta_1obs = cycle(t)-cycle(t-1)`, entsprechend 5/10; **Skalenpunkte**, keine Prozent-Rendite.
- `UP` nur bei Delta >0, `DOWN` nur <0, `UNVERAENDERT` nur bei belegtem Delta=0. Kein `UP` wegen Null-Imputation; echte 0/50/100 bleiben als Werte erlaubt.
- Primaer kompakt: `37,4 ↑ +12,6 (5 Beob.)`; bei belegter Null: `37,4 → 0,0 (5 Beob.)`; bei fehlender 5er Basis `37,4 —`; bei fehlendem aktuellem Wert `—`.
- Delta 1/10, `cycle_quality`, Formelversion, Quelle, letzter Bar/`as_of`, Lag-Ausschlussgrund im Drawer/Tooltip; nur bei Platz und nach bestandenen Tests.
- Tooltip: **Scannerbeobachtungen sind keine Handelstage; Zykluspunkte sind keine Renditeprozente; steigender Cycle ist keine Kursprognose.**
- Keine Aenderung an Score, Selection/Timing/Risk/Confidence/Elliott, Universal Stance, Decision, Portfolio Action/Execution oder Pattern-L2/L3.

## 5. CY-04-Abnahmematrix am 10.10.2026

| Masterplan CY-04 | Status | Befund |
|---|---|---|
| CY-03 als zulaessige Vergleichskette | **NICHT ERFUELLT** | 0/0/0 Lags; #269 offen |
| Delta 1/5/10 aus echten Beobachtungen | **NICHT ERFUELLT** | keine Vorgaengersnapshots |
| Belegter UP/DOWN/UNVERAENDERT-Pfeil sichtbar | **NICHT ERFUELLT** | darf nicht erfunden werden |
| Fehlende Basis/Stale erzeugen keine Pfeile | **PASS (Ist-Ausgangszustand)** | keine neuen Richtungs-Pfeile; noch kein neuer CY-04-Regressionsbeleg |
| Kompakte mobile UI, Sortierung, History/Decision Regression | **NICHT GETESTET** | keine Codeaenderung; benoetigt spaeter neuen PR-CI-Lauf |
| Keine unbelegten Handels-/Research-Signale | **PASS (Scope)** | keine Aktivierung, keine Produktionsaenderung |
| Dokumentierter Stop-/Wiedereinstiegspunkt | **DOKUMENTIERT** | dieses Dokument und separates Statusregister |

**Neue CY-04-Tests:** keine implementiert und keine ausgefuehrt. Historische CY-03-`116 passed` werden **nicht** als CY-04-Ergebnis umdeklariert. Die read-only-Auszaehlung der drei aktuellen CSV-/Manifest-Dateien erfolgte ueber GitHub-Dateizugriff; keine lokale End-to-End/UI-Mobil- oder Playwright-Pruefung.

**Erforderliche CY-04-Negativtests nach Gatefreigabe:** fehlender Vorgaenger, fehlendes aktuelles Cycle, falsche/unechte Null oder 50, Stale, falscher Asset-/Listing-/Currency-/Formel-Join, >5-Kalendertage-Aktiengap/1-Tag-Kryptogap, doppelte Tageslaeufe, geaenderte Archive/Hashes, fremder Snapshot, unzureichende 5/10obs-Historie und unvollstaendige/asynchrone Publikation. Dazu UP/DOWN/echte Null-Aenderung, Genauigkeit der Dezimalstellen, Tooltip/Drawer, Sortierung, Mobilansicht und unveraenderte Score-/Decision-Ausgaben.

## 6. Explizites Stop-Gate / Wiedereinstieg

1. **CY02-B01 #269 extern qualifizieren und fachlich freigeben**: Listing-/Quote-Waehrung, Provider-Publikationszeit / Exchange-Kalender und As-of-Bargrenze dokumentieren. `WATCHLIST_DECLARED_ONLY` / `SESSION_DATE_CUTOFF_ONLY` sind keine unabhaengige Quelle.
2. **CY-03 fachlich pruefen**: mehrere echt veroeffentlichte, hashverifizierte, nach Asset/Listing/Formel vergleichbare Scannertage; zumindest 2/6/11 qualifizierte Beobachtungen fuer 1/5/10 Lags, mit positiver numerischer Coverage, Replay/Idempotenz und unveraenderten Originalarchiven. Mehrfachlaeufe werden nur einmal je Scan-Tag gezaehlt.
3. **Erst dann eigener CY-04-Feature-PR** auf dann aktuellem `main`: getrennten direction-View + UI-Renderer mit striktem Quality-/Snapshot-Gate, synthetic Goldentests und echten read-only Live-Testbelegen bauen.
4. **Abnahme getrennt:** nach CI/Visual-/Regressionsnachweis `FERTIG_TECHNISCH`, nach echten belegten Pfeilen und korrektem Fehlverhalten `FERTIG_FACHLICH`. Keine empirische Handelsvalidierung daraus ableiten.

## 7. CYCLE-DIR Uebergabe (Abschluss dieser Bestandsaufnahme, nicht des Features)

- **Paket:** CY-04 — Deskriptive Zyklusrichtung und kompakte UI.
- **Status technisch / fachlich / empirisch:** Feature `NICHT IMPLEMENTIERT` / `BLOCKIERT` / `NICHT VALIDIERT`.
- **Aenderungen:** ausschliesslich CY-04-Readiness-Dokumentation und `docs/cycle_direction/STATUS.md` auf separatem Branch. Kein `main`-Schreibzugriff, keine alten Snapshot-Bytes aendern.
- **Getestet:** read-only Vergleich von Manifest, Ledger, Lag-Maske, Coverage und aktuellem `history_metadata.json`; 1 Snapshot und 0/0/0 Lags nachgezaehlt. `pytest` / Browser-Tests fuer CY-04 **nicht ausgefuehrt**.
- **Datenbasis:** Snapshot `f409c312-0349-4b29-8dab-3ecdbd9463b3`, `as_of=2026-10-10`; Fingerprints siehe Abschnitt 2.
- **Blocker:** Issue #269, fehlende mehrtaegige 1/5/10-Beobachtungsketten; CY-03 Fachgate.
- **Nicht geaendert:** ganze Produktionspipeline und Scoring/Decision/Portfolio-/Research-Contracts.
- **Exakt naechster Schritt:** nach #269 und ersten qualifizierten mehrtaegigen Ledgern CY-03-Gate erneut auditieren, dann CY-04-Implementierungsbranch/Tests beginnen.
