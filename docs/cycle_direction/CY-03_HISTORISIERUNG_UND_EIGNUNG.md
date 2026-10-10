# CYCLE-DIR CY-03 — Historisierung, Identität, PIT und Eignungsmaske

**Plan:** CYCLE-DIR-2026-10-09-v1 · **Datum:** 10.10.2026 (Europe/Berlin)  
**Paket:** ausschließlich CY-03 · **Branch:** `feat/cycle-dir-cy03-pit-history-20261010`  
**Basis-SHA:** `3a4061f48d02e1ca35d180bf8148ff907d7bb609` (main bei Branchanlage; das Projekt hat automatische gleichzeitige Artefakt-Commits).  
**Status bei Erstimplementierung:** `IN_ARBEIT / NICHT_FREIGEGEBEN`; Tests, CI und Live-Append sind einzeln nachzuweisen. **Kein FERTIG_FACHLICH ohne gültige Ketten und externes CY-02-Gate.**

## 1. Referenzstand und fachliche Eingangssperre

- CY-00: PR #259. CY-01: PR #260 (+ UI-Fix #267) auf main. CY-02: PR #266 + Nachbesserung #268 auf main.
- Letzter **verifiziert veröffentlichter** aktueller Scannerstand vor diesem Paket: Autopilot [#38033664492](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/38033664492), `as_of=2026-10-10`, `snapshot_id=94a23ea7-9a1b-4bf8-9ef0-d97065d3eed2`, `run_id=github-38033664492-1`. 215 Assets; **193 VALID**, 13 MISSING_SOURCE, 6 INSUFFICIENT_HISTORY, 2 STALE, 1 INVALID_VALUE (Summe ausgeschlossene 22).
- Aktuelle Research-Views transportieren `cycle`, `cycle_quality`, `cycle_formula_version`, `cycle_price_sha256` usw.; `score_history.csv` führt aktuelle Scannerbeobachtungen mit. `history_analysis.csv` enthält zusätzlich ältere Legacy-Beobachtungen und ist **kein** geeigneter ungefilterter Cycle-Lag-Datensatz.
- **Harter externer Gate:** [CY02-B01 #269](https://github.com/grisuweimar-crypto/trading-zentrale/issues/269) offen. `WATCHLIST_DECLARED_ONLY` und `SESSION_DATE_CUTOFF_ONLY` sind keine extern signierten Listing-/Exchange-/Publikationsbelege. Daher `research_eligible=0` und `research_status=BLOCKED_EXTERNAL_VERIFICATION_269` trotz erfolgreicher technischer Replay-Evidence.
- Allgemeiner Return-Integrity-Blocker [#264](https://github.com/grisuweimar-crypto/trading-zentrale/issues/264) bleibt ein getrenntes QM-Thema.

## 2. Ist-Wirkungskette und minimale Änderung

`run_daily` → `watchlist_full.csv` mit CY-02-Qualität, Versions- und Preisfeldern → `generate_research_views.py` (gültige aktuelle Views/Metadaten/History) → `audit_cycle_current.py` (exaktes Replay der 60 Preisbars) → Price Backfill → Daily Research → **neu: `scripts/record_cycle_history.py`** → übrige vorhandene Decision-/History-Delta-/UI-Module → Quality Gate/Push.

Die neue Funktion `scanner.reports.cycle_history.record` liest `history_metadata.json`, `latest_scanner.csv`, die tatsächlich publizierte `watchlist_full.csv`, `cycle_quality.csv` und `cycle_input_bars.csv.gz`. Sie kontrolliert Metadaten-Hashes, `run_id`, `snapshot_id`, `as_of`, die unveränderte Watchlist und ruft den etablierten CY-02-60-Bar-Replay-Auditor erneut auf.

**Neue Artefakte (keine rückwirkende Änderung alter Archive):**

| Pfad | Funktion |
|---|---|
| `artifacts/cycle_history/observations.csv` | append-only, eine Zeile je neuer **wirklich beobachteter** Snapshot-ID/Asset-ID |
| `artifacts/cycle_history/bars/{snapshot_id}.csv.gz` | unveränderliche Kopie der 60-Bar-Eingangsbelege des jeweiligen Scans |
| `artifacts/cycle_history/eligibility.csv` | regenerierbare Qualitäts- und Lag-Eignungsmaske für 1/5/10 Beobachtungen |
| `artifacts/cycle_history/coverage.csv` | aggregierte Coverage je Asset und Monat sowie je Lag und expliziter Ausschlussgrund |
| `artifacts/cycle_history/manifest.json` | Source-/Ledger-/Masken-/Coverage-SHA256 und Qualitäts-/Gate-Zählwerte |

`observations.csv` wird bei gleicher Snapshot-ID ausschließlich byteidentisch replayt; widersprüchliche Wiederveröffentlichung wird abgelehnt. Alte CSV-/Snapshot-Originalbytes werden **nicht geändert**. Die Maske und Coverage sind ableitbare Research-Views, keine nachträglich erfundenen alten Scannerbeobachtungen.

## 3. Identität, Zeit und PIT-Policy

- Zeilenidentität: `(snapshot_id, asset_id)`; Listing/Symbol und separater Yahoo-`price_symbol`; vom Scanner **deklarierte** Original-`currency`, `price_basis`, `price_source`, `source`, `formula` und Quell-Bar-Fingerprint. `market_timezone=UNVERIFIED` statt geratener Börsen-Zeitzone.
- Zeit: `as_of` = Scan-Datum, `generated_at` = UTC-View-Publikation, `cycle_as_of` = UTC-Preis-Cutoff-Scanzeit, `computed_at` = UTC-Oszillatorlauf; `last_bar` = Yahoo-Sessiondatum. Gültig nur: last_bar **vor** UTC-Datum von cycle_as_of; `cycle_as_of <= computed_at <= generated_at`. Echte Börsen-Schluss-/Provider-Verfügbarkeit bleibt **ungeprüft**.
- Nur neu mit `YAHOO_PIT_CYCLE_V1` und `cycle_detrended_sma20_range40_v1` erzeugte aktuelle, vollständig durch CY-02 replayte `VALID`-Zyklen tragen `PROVISIONAL_REPLAYED`. Echte 0, 50 und 100 sind möglich und werden nicht anhand ihres Zahlenwerts ausgeschlossen.
- Quellfehler, leere/Legacy-Zyklen, unbewiesene frühere Nullen/Fünfziger und Staleness sind **kein** belegter Vorgänger. Es erfolgt ausdrücklich **keine** Rekonstruktion/`RECONSTRUCTED_RESEARCH` und kein historisches Umstempeln.
- Wiederholte Tagesläufe: alle bleiben im Ledger. Für Lag/Coverage zählt je Datum nur der Snapshot mit dem spätesten `generated_at`, ältere Versionen heißen `SUPERSEDED_SAME_DAY`; sie werden weder gelöscht noch als zusätzlicher Lag gezählt.
- Ein Asset-Neuzugang hat `NO_PRIOR`, Austritt und späterer Wiedereintritt ohne lückenlose Snapshot-Kette `UNIVERSE_OR_OBSERVATION_GAP`. Bei wechselndem Listing/Yahoo-Symbol, Originalwährung, Formel, Bar-Basis oder Quellnachweis: `IDENTITY_OR_FORMULA_CHANGED`; keine Aliaskonversion, keine FX-Kalkulation.
- Ein `1/5/10obs`-Lag bezeichnet vorangehende vergleichbare **Scannertage**, keine Handelstage. Konservative Gaps von >5 Kalendertagen für Aktien bzw. >1 Tag für Kryptos blockieren die Kette. Nicht-monotone As-of-/Last-Bar-Belege blockieren ebenfalls. Der Unterschied zwischen `PROVISIONAL_CHAIN` und unabhängig geprüfter Forschungszulässigkeit bleibt sichtbar.
- Jede `PROVISIONAL_CHAIN` ist **nur** eine technische Vergleichsoption. Für die Pattern Discovery gilt unabhängig davon `BLOCKED_EXTERNAL_VERIFICATION_269`. CY-03 ändert keine L2/L3-Suche und stellt keine Prognose oder Delta-UI bereit.

## 4. Qualitätskontrolle und Abnahmekriterien

| Kriterium Masterplan | Prüfnachweis / Gate |
|---|---|
| Exakter HEAD, getrennte Branch-Arbeit | Basis-SHA oben; PR gegen jeweils neuestes main |
| Neues Cycle in aktuellen Views/Historie | bereits durch CY-02 in `latest_scanner` und `score_history`; zusätzliche eindeutig getrennte CY-03-Ledger |
| Original-Snapshots unverändert | keine Writes nach `score_history.csv`, `history_analysis.csv`, `history_recent.csv`, `latest_scanner.csv`; CI-Dry-Run read-only |
| Formelfassung, Listing, Currency, PIT, Sourcehash | jede Zeile bindet Versions-/Identitätsdaten und immutable SHA-/60-Bar-Evidence; externe Listing-/Barzeit bleibt **UNVERIFIZIERT** |
| Replay deterministisch | lokale Unit-/Negativtests für Identität und 1/5/10; CI-Dry-Run gegen publizierten Originalscan |
| Doppellauf/Gaps/Versionswechsel/Alias/Fehlwert | explizite Masken-Status und Negativtests |
| Coverage nach Asset, Monat, 1/5/10 | `coverage.csv` mit technisch verfügbaren Ketten und explizitem Ausschluss-/Blockstatus |
| Scoring/Decision unverändert | keine Anpassung der Score-/Decision-Quellen; Autopilot um reine Historisierung vor deren Auswertung erweitert |
| Fachlicher und empirischer Status | Gate #269 offen; keine Forschungs-/Tradingfreigabe, keine CY-04/05-Promotion |

**Tests (vor Freigabe tatsächliche Ergebnisse nachtragen):**
```bash
python -m pytest -q tests/test_cycle_history_cy03.py tests/test_cycle_audit_cy02.py tests/test_cycle_oscillator_cy02.py tests/test_cycle_quality_cy01.py tests/test_research_views.py tests/test_daily_research.py tests/test_history_delta_completion.py tests/pattern_discovery/test_l2_feature_library.py tests/pattern_discovery/test_l3_search_engine.py
python scripts/record_cycle_history.py --dry-run
```

**Wichtig zur Coverage:** Am Start vor dem ersten neuen CY-03-Produktivscan gibt es **noch keine beobachtete, lang genug bestehende CY-03-10obs-Kette**. Ein manueller/CI-Dry-Run darf das ältere Live-Snapshot vom 10.10.2026 auswerten, aber nicht ohne abschließenden Publish als neue Append-Historie ausgeben. Ohne mindestens 2/6/11 qualifizierte Beobachtungen pro durchgehend identischem Asset sind 1/5/10obs-Ketten aus dieser neuen Historie nicht vorhanden. Kalender-/Fehlwertfilter können zusätzliche Beobachtungen ausschließen.

## 5. Abschluss und nächste Übergabe

**Technischer Erststand:** Implementierung vorbereitet; eine PR und CI müssen den Kandidaten unabhängig prüfen. Nach erfolgreichem Publish die echten `manifest.json`-/`coverage.csv`-Zählungen, Replay-Hashes, alten Dateihashes und neue `main`-SHA dokumentieren. **Noch kein fachlicher Abschluss** solange #269 offen bzw. keine qualifizierten vollständigen Ketten bestehen.

**Explizit nicht geändert:** productive Scoring, Timing, Risk, Confidence, Elliott, Universal Stance, Decision, Portfolio Action, Execution, L1–L14-Statistik und Pattern-Promotion, bestehende historische Original-Snapshots.

**Exakt nächster Gate-Schritt:** PR-CI und Read-only Live-Snapshot-Audit; danach gemäß #269 unabhängig Listing/Währung/Börsenzeit bestätigen; erst nach Freigabe und mindestens einer tatsächlich erfolgreich publizierten neuen CY-03-Beobachtung technischen Betriebsabschluss erwägen, vollständige Lag-Ketten erst bei tatsächlicher historischer Coverage.
