# CYCLE-DIR CY-02 — aktuelle Zyklusberechnung und Frischebeweis

**Plan-ID:** CYCLE-DIR-2026-10-09-v1 · **Paket:** ausschließlich CY-02  
**Basis:** main e77eb4082d04e9269a0fb5120259ecc9bb76109d, 10.10.2026  
**Branch:** feat/cycle-dir-cy02-pit-oscillator-20261010  
**Status:** **FERTIG_TECHNISCH (PR-Kandidat) / NICHT_FREIGEGEBEN (fachlich)**. 94 Tests grün auf `7d71805c81d0`; echte Preis-/Währungs-/Listing- und Publikationsprüfung nach Merge noch ausstehend. Keine empirische Freigabe.

## 1. Bestandsaufnahme und Abhängigkeiten

CY-00 (PR #259) hat anhand der Rohdaten vom 08.10.2026 den Pfad von `Zyklus %` über `build_watchlist.py`, `cycle`, die aktuellen Research-Views, History und Pattern-L2/L3 dokumentiert. CY-01 (PR #260; main HEAD e77eb408) beseitigte die Imputation von 80 Nullen und korrigierte Quality/Source und Anzeige. Die 80 imputierten Nullen und drei unbewiesenen expliziten Nullen vom 08.10.2026 liegen in einer separaten Quarantänemaske. CY-03 muss die historische Research-Eignung weiter absichern.

**Quellenbeweis bisher:** `src/scanner/data/enrich/yahoo_prices.py` führte im produktiven `run_daily`-Pfad Yahoo-`1d`/`auto_adjust=True` für Preise, Trend200, RS3M und Risiko aus. `Zyklus %` wurde damit **nicht** neu berechnet. Die alte `legacy/market/cycle.py`-Funktion (SMA20/detrended/40er Fenster, historische 50-Fallbacks) ist **nicht** als aktiver Quellcode für bestehende Werte nachgewiesen. Frühere numerisch gültige Quellenwerte bleiben unbestätigte **LEGACY_BEOBACHTUNGEN** und werden nicht nachträglich als neue Formel etikettiert.

## 2. Neue, eigenständige Spezifikation

| Eigenschaft | Deterministischer CY-02-Vertrag |
|---|---|
| Formelversion | `cycle_detrended_sma20_range40_v1` |
| Quellenkennung | `YAHOO_PIT_CYCLE_V1` |
| Preisquelle | `yfinance.download`, YahooSymbol/handelbare Original-Notierung |
| Frequenz/Basis | 1 Tagesbar pro Session, `auto_adjust=True`, `Close`, **keine Währungsumrechnung** |
| Formel | `d[i] = Close[i] - SMA20[i]`; letzter Wert innerhalb Min/Max der **letzten 40 d[i]** auf 0–100 skaliert |
| Mindesthistorie | **60** abgeschlossene Tagesbars (letzte 60, ohne Imputation); damit alle letzten 40 d[i] definiert |
| Konstantenfall | `INVALID_VALUE` / kein numerischer Wert (nicht 50) |
| Ausfälle | `STALE`, `MISSING_SOURCE`, `INSUFFICIENT_HISTORY`, `INVALID_VALUE`, numerisch nur bei `VALID` |
| Clamping/Rundung | [0,100], auf vier Nachkommastellen; echte 0/50/100 möglich |
| Bar-Cutoff | Bar-**Sessiondatum strikt kleiner als UTC-Kalendertag des Scans**; heutige/in Zukunft datierte Bars werden verworfen |
| Frische Aktien | letzter verwendeter Bar höchstens drei zurückliegende Werktage (Mo–Fr), Wochenende berücksichtigt; unbekannter Feiertagskalender bleibt Einschränkung |
| Frische Kryptos | letzte vollständig abgelaufene UTC-Tagesbar **gestern**; keine Wochenendpause |
| Datenlücken | Gap zwischen zwei verwendeten Bars größer als fünf Kalendertage (Aktien) oder ein Tag (Krypto) → blockiert |
| Split-/Diskontinuitätskontrolle | `auto_adjust=True`, zusätzlich 4x-/0,25x-Sprung als Sperre mit Reviewgrund; kein vollständiger Corporate-Actions-PIT-Beweis |

Die neue Berechnung kann aus numerischer Sicht ähnlich wie die Legacy-Formel sein. **Sie ist nicht historisch oder semantisch identisch nachgewiesen.** Für CY-03 dürfen alte und neue Werte nicht ohne Formel-/Quellenprüfung zu Deltas verbunden werden.

## 3. Herkunft pro Wert und Ausgabe

Die Berechnung liefert `Zyklus %` und Qualitätsfelder `cycle_quality`, `cycle_source`, `cycle_quality_reason`, dazu:

- `cycle_formula_version`, `cycle_price_source`, `cycle_price_basis`, `cycle_price_symbol`, `cycle_currency`
- `cycle_last_bar` (Yahoo-Tages-**Sessiondatum**), `cycle_as_of` (UTC-Scanzeit), `cycle_computed_at` (UTC)
- `cycle_price_sha256` (SHA256 von Formel, Basis, YahooSymbol, angegebener Originalwährung und **genau 60 verwendeten datierten Schlusskursen**), `cycle_eligible_bars`.

Neue Daten werden im aktuellen Watchlist-Datenfluss weitergereicht und im neu erweiterten **Current-/Research-View-Vertrag** berücksichtigt; der aktuelle `artifacts/reports/cycle_quality.csv` bleibt ein Prüfartefakt, dessen Spalten ebenfalls erweitert werden. `normalize_cycle_source` erhält die **neue** Quellkennung; aus einer numerisch gültigen Legacy-Spalte wird dadurch nicht ohne Berechnung ein angeblich aktueller Wert.

**Ausfallverhalten:** Bei ausgeschaltetem Datenabruf, fehlenden Provider-Daten oder Ausnahme wird der alte Cycle **nicht** als frisch weitergereicht. Die restlichen alten Preis-Features bleiben gemäß bestehendem Enrichment-Verhalten unberührt; die Zyklus-Evidence wird ausdrücklich blockiert.

## 4. Grenzen und verbleibende Prüfungen

1. Yahoo-Tagesindizes bezeichnen Sessions, keine signierten tatsächlichen Börsenschluss- oder Publikationszeitpunkte. Der UTC-Vortags-Cutoff ist **bewusst konservativ**, kann jüngste bereits abgeschlossene europäische Sessions bis zum nächsten UTC-Tag auslassen. Keine heutige Bar wird bloß wegen Download-Verfügbarkeit als vollständig angenommen.
2. Der Yahoo-Batch liefert keine durchgängig unabhängig bestätigte Handelsplatz-/Quote-Währung im verwendeten Frame. Die originale `Currency`/Symbol-Angabe wird mitgeführt, nicht extern verifiziert. Alias-Duplikate werden durch bestehende Canonical-/Dedup-Logik verwaltet; eine unabhängige Listing-/Currency-Provider-Verifikation ist **UNVERIFIZIERT** und darf nicht als Qualitätsbeweis ausgegeben werden.
3. Split-/Dividend-adjustierte aktuelle Yahoo-Preise sind nicht zwangsläufig historische PIT-Preise, die damals schon in dieser Adjustment-Version verfügbar waren. **Keine rückwirkende Rekonstruktion, keine alten Snapshot-Rewrites**.
4. Ein grün getesteter Algorithmus ist keine profitable Prognose; wissenschaftliche Muster- und Forward-Validierung finden erst in CY-05 bis CY-08 statt. Score, Opportunity, Risk, Confidence, Elliott, Selection, Timing, Decision, Portfolio-Action und Execution werden inhaltlich nicht geändert.
5. Vor fachlichem Abschluss erforderlich: erfolgreiche CY-02-PR-CI, Review der Feld-/Quelle-Semantik, ein **echter vollständig publizierter Scannerlauf nach Integration** mit numerischen Qualitäts-Counts, SHA-/Bar-/as_of-Nachweis, Regression auf Score/Decision und Nachweis, dass kein Live-Wert als historisch gültige Beobachtung fehlklassifiziert wird. Baseline-Bug #264 bleibt eigenständig und darf nicht als mit CY-02 erledigt bezeichnet werden.

## 5. Tests und Freigabeprotokoll

Neue Testdatei: `tests/test_cycle_oscillator_cy02.py`. Feste Goldenkursreihe (Erwartung 7,2258), doppelte Eingaben, konstante Reihe, zu wenige Bars, NaN/negativ/unendlich, Gap und Duplicate, Splitverdacht, Provider-Ausfall, UTC-Bargrenze, Aktien/7-Tage-Krypto, Ausfall- und Währungskennzeichnung, Yahoo-Pipeline-Integration, Research-Feldfortpflanzung.

GitHub-CI: `.github/workflows/cycle_dir_cy02.yml`, inklusive CY-01 und bestehender History-/Research-/L2-/L3-Regressionssuiten.

**Befunde nach PR-CI und Live-Prüfung ergänzen:** konkrete Run-ID, Commit-SHA, Testanzahlen, Probeasset, Cyclequalität/Lastbar/Hash, Ausnahmen, Vergleich der unveränderten Score-/Decision-Outputs.

## 6. Status / nächstes Paket

Bis die Nachweise unter Abschnitt 5 vollständig vorliegen, **kein `FERTIG_FACHLICH`** und kein Merge allein wegen Code oder plausibler Formula. CY-03 darf erst auf der nachgewiesenen Version aufbauen. Das Repository-Paket enthält bewusst weder Historiereparatur noch Delta/UI, L2/L3-Muster-Promotion oder Scoring-Neubewertung.

## 7. Abnahmebericht 10.10.2026 (MESZ)

**Ausgangs-HEAD:** `e77eb4082d04e9269a0fb5120259ecc9bb76109d` (nach CY-01). **Arbeitsbranch:** `feat/cycle-dir-cy02-pit-oscillator-20261010`. **PR:** [#266](https://github.com/grisuweimar-crypto/trading-zentrale/pull/266) (Draft, kein Merge in main).

**Änderungen:** Neue Berechnungsquelle, Einbau Yahoo-Enrichment und Fail-closed-Ersatz, CY-01-Quellpräzedenz erweitert, neue Provenance in aktuelle Research-Views, CI + Unit-Tests. Erster Kandidat `cbeef70a2f70`; nach echtem Ersttest wurde die bisherige int64-`Zyklus %`-Spalte gegen Dezimalwerte abgesichert (`7d71805c81d0`).

**Belegte Tests:**
- Erstlauf [#38029547769](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/38029547769): 93 bestanden, **ein Fehler** durch int64-Quellspalte; nicht als grün umetikettiert.
- Korrekturlauf [#38029618787](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/38029618787): **94 passed**, 0 failed, Python-Compile erfolgreich. CY-02 + CY-01, History/Research, Pattern-L2/L3.
- Unabhängige PR-CIs auf `7d71805c81d0`: [CY-01](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/38029618802) erfolgreich, [BA-QM8](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/38029618799) erfolgreich, [QM-B Historical Taxonomy](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/38029618809) erfolgreich, [QM-B Observed Membership](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/38029618810) erfolgreich.
- Globale [Return Integrity Recheck](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/38029618877): eigener späterer Status zu prüfen; die bekannten Baseline-Fehler aus [Issue #264](https://github.com/grisuweimar-crypto/trading-zentrale/issues/264) bleiben ein separates QM-Thema. Kein globaler Full-Suite-PASS behauptet.

**Datenbasis der Goldentests:** synthetische 90 Tagesbars mit festem Schlusskursvektor `100 + 0.1*i + 3*sin(i*0.27)`; as_of `2026-10-10T07:00:00+00:00`; letzter zulässiger Bar `2026-10-09`; v1-Zyklus `7.2258`; SHA256-Replay geprüft. **Kein echtes Live-Snapshot, keine gehandelte Rendite, kein empirischer Alpha-Nachweis.**

**Abnahmetrennung:** Die deterministische Berechnung und die CY-02-relevante Testkette sind **technisch geprüft**. Fachliche Freigabe **noch nicht erteilt**: Das derzeitige Yahoo-Batch belegt den Börsen-/Währungsstatus nicht unabhängig, verwendet Sessiondatei statt verifiziertem Bar-Schlusszeitpunkt; ein produktiver Post-Merge-Runtimebeleg mit Asset-/Qualitätszählung fehlt.

**Nicht geändert:** altes `latest_scanner.csv`, `score_history.csv`, `history_recent.csv`, `history_analysis.csv`, alte Exclusion-Maske, Scoring-Engine und Decision/Portfolio/Execution; keine L2/L3-Atomisierung oder UI-Richtung.

**Blocker:** CY02-B01: Unabhängige Provider-Handelsplatz-/Währungs-/Sessionbestätigung noch nicht eingeführt; CY02-B02: noch kein echter neuer publizierter Scannerlauf auf dem freizugebenden Stand; globale QM-Baseline #264 separater Blocker für QM-Promotion, nicht zwangsläufig für den CY-02-Code.

**Nächster exakter Schritt:** PR #266 fachlich auf Quellen-/Close-Zeitpunkt und Listing prüfen; ggf. fehlende unabhängige Verifikation ergänzt testen. Anschließend PR erst nach grünem CI mergen und **danach** einen echten Autopilot-Scannerlauf gegen die veröffentlichten Felder (`cycle_quality`, `cycle_formula_version`, `cycle_price_sha256`, `cycle_last_bar`, `cycle_as_of`) auditiert vergleichen. Erst dann Status `FERTIG_FACHLICH` erwägen; CY-03 beginnt danach.

## 9. Produktionsnahes Fail-closed Gate

Ein eigener Standardbibliothek-basierter **CY-02-Current-Audit** (`scripts/audit_cycle_current.py`) validiert das tatsächliche `watchlist_full.csv` und gleicht es zeilenweise mit `artifacts/reports/cycle_quality.csv` ab, bevor die tägliche Research-/UI-Veröffentlichung startet. `VALID` erfordert einen numerischen Wert 0–100, die neue Versions-/Quellenkennung, deklarierte Währung mit explizitem `WATCHLIST_DECLARED_ONLY`, Session-Zeitlabel mit `SESSION_DATE_CUTOFF_ONLY`, valide As-of-/Lastbar-Ordnung sowie 64-stelligen SHA256-Fingerprint. Jeder fehlende/ungültige/stale/insufficient-Wert muss leer bleiben und **darf keinen Kurs-Hash enthalten**. Für widersprüchliche Status-/Wert-Paare, fehlende Pflichtfelder oder Qualitätsreport-Abweichungen schlägt der Scannerlauf vor der Publikation fehl.

Die Validierung **belegt nicht** unabhängig, dass Yahoo die Bars genau zu diesem Zeitpunkt final publiziert hatte oder die erklärte Währung vom Anbieter signiert wurde. Es bleibt ein konservativer Parser-/Reproduzierbarkeits-Nachweis und ein Fail-closed-Publikations-Gate, **kein** PIT-Freibrief für historische Kursrekonstruktion. Vollständige externe Provider-/Listing-Verifikation bleibt offen und wird nicht als stillschweigende erfolgreiche Abnahme ausgegeben.

## 10. Prüffähiger 60-Bar-Quellenbeleg (Nachschärfung der Masterplan-Abnahme)

Der Masterplan verlangt ausdrücklich, dass **die konkrete Preisreihe**, nicht nur ein Hash, für jeden neu berechneten Wert kontrollierbar bleibt. Deshalb wird mit jedem neuen Scannerlauf `artifacts/reports/cycle_input_bars.csv.gz` geschrieben (bei Provider-Ausfall nur Header, also keine Wiederverwendung älterer Eingaben). Je einzigartigem `YahooSymbol + deklarierter Originalwährung + cycle_price_sha256` werden genau **60 tägliche Sessiondaten und adjustierte Close-Zahlen** im Format `%.17g` gespeichert, außerdem `as_of` und Formelversion. Mehrfache unveränderte Quellkennungen werden dedupliziert.

Die Liste wird direkt aus dem **selben yfinance-Frame** aufgezeichnet, der den Cycle-Wert erzeugte. Keine erneute Kursabfrage mit möglicherweise geänderten adjustierten Preisen wird als rückwirkender Beweis verkauft. Die Sidecar-Daten werden vor dem Scoring geschrieben, berühren aber die Score-Daten und bestehenden Snapshots nicht.

Das aktualisierte Publikations-Gate `scripts/audit_cycle_current.py` liest die komprimierte Belegliste, prüft die Einträge je `VALID`-Wert und ruft die versionierte v1-Funktion erneut mit **genau denselben 60 Werten und dem ursprünglichen `as_of`** auf. Es verlangt identische vierstellige Cycle-Ausgabe, SHA256 und letzten Bar. Manipulierte Bars, fehlende Fenster, falsche Version oder nicht referenzierte Belege sperren die Veröffentlichung. Zusätzliche Regression: ein festes 90-Bar-Beispiel wird auf 60 tatsächlich verwendete Bars reduziert und vor/nach Änderung eines einzigen gespeicherten Schlusskurses überprüft.

**Wissenschaftliche Grenze:** Diese Belege machen die tatsächliche *innerhalb des Scannerlaufs verwendete Preisreihe* replizierbar. Sie machen aus Yahoo-Daten weder eine unabhängige Börsenquellenbestätigung noch ein historisches, vor damaligem Handelsabschluss veröffentlichtes PIT-Archiv. Für den ersten produktiven Scan ist deshalb ein manueller Status-/Quote-/Frische-Review zusätzlich zu den automatischen Gates vorgesehen. CY-03 historisiert anschließend getrennt und präventiv, ohne alte Ausgabewerte zu überschreiben.

## 11. Wiederaufnahme am 10.10.2026 nach dem CY-01-Autopilot-Retry

- **Tatsächlich publizierter Scannerlauf:** Autopilot `38030550084`, Attempt 2, `run_id=github-38030550084-2`, Snapshot `4b53a93b-fb9f-4704-b56b-1a714f36e428`, 215 Assets; davon 135 `VALID` und 80 `MISSING_SOURCE`. Dieser Lauf gehört zum alten CY-01-Legacy-Code und beweist **nicht** die neue CY-02-Formel.
- **Prüfstand v1:** [CY-02-Workflow #38032854774](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/38032854774), **107 passed** auf Branch-HEAD `05718f9496da54f65b37c7832a16fb2282f93303`. [BA-QM8 #38032854771](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/38032854771) PASS; CY-01 und QM-B-Membership/Taxonomy PASS. Der größere Return-Integrity-/QM-Prozess ist nicht als globaler PASS zu behandeln (separat #264).
- **Preisfolge und Ausgabevertrag:** Ein aktueller Wert ist nur dann `VALID`, wenn mindestens 60 plausible, konservativ vor dem As-of datierte Tagesbars, eine deklarierte Währung und eindeutige Formel-/Preis-/Symbolkennung vorhanden sind. Eine fehlende Währung oder ein Alias-Konflikt führt zum erklärten Fehlen. Keine künstlichen Nullwerte.
- **Score/Decision-Grenze:** Inspektion von `src/scanner/domain/scoring_engine/engine.py`: Die aktuelle Score-Berechnung nutzt `Growth`, `ROE`, `Margin`, `MC-Chance`, `Elliott`, `Trend200`, `RS3M` u. a., *nicht* `Zyklus %` als gewichteten Score-Faktor. Die neue `Zyklus %`-Spalte wird nur durch die Enrichment-Schicht geschrieben. Ein empirischer Profit-/Alpha-Nachweis wird daraus nicht abgeleitet.
- **Noch offener Betriebsnachweis:** CY-02-PR in `main` integrieren, danach echten Scannerlauf auf dem neuen Code durchführen und *erst nach tatsächlichem Publish* Formel-/Qualitätsverteilung, 60-Bar-Replay, unveränderte alte Snapshots, Währungsherkunft und Metadaten auditieren. Externe Börsen-Verifikation bleibt selbst bei grünem Scanner-Gate unverifiziert.
