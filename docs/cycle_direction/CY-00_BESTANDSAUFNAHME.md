# CYCLE-DIR — CY-00 Bestandsaufnahme

**Audit-ID:** CYCLE-DIR / CY-00 / 2026-10-09  
**Plan:** CYCLE-DIR-2026-10-09-v1, Abschnitt 5 / CY-00  
**Repository:** grisuweimar-crypto/trading-zentrale  
**Geprüfter main-HEAD:** 8cd09063f204c7eb27a3fe95d3ddc5b1a4e89233 (2026-10-09 11:26:08 UTC)  
**Auditzeitpunkt:** 2026-10-09, circa 11:35 UTC  
**Produktiv-Snapshot:** 2026-10-08, Snapshot 32739832-2481-491f-b131-f7413c07f6b2, Run github-37822530040-1  
**Produktiver Scanner-Code zum Lauf:** 609e01a191bb8e3c51e8c63dd337f3cfe6e0ea8f (aus scanner_input_provenance.json; **nicht** mit Audit-HEAD verwechseln)  
**Status:** Bestandsaufnahme dokumentiert, keine Code-, Daten- oder Contract-Änderungen. CY-01 darf separat beginnen, sobald dieser Bericht übernommen/reviewt ist.  
**Begrifflichkeiten:** UNVERIFIZIERT = Herkunft/Frische/Gültigkeit nicht belegt; FEHLEND = Quelldatei enthält kein numerisches Messergebnis; EXPLIZITE_NULL = in der Rohquelle numerisch eingetragen, Gültigkeit noch nicht bewiesen.

## 1. Prüfauftrag und Grenzen

Ziel von CY-00 ist, den produktiven Datenpfad des Zyklus, seinen Ursprung, die historischen Ablagen und alle abhängigen Forschungs-/UI-Verträge nachvollziehbar zu erfassen. Es wurde ausschließlich read-only gearbeitet, außer dass dieses Dokument auf einem separaten Dokumentationsbranch angelegt wurde. Es fand keine Neu-Berechnung, historische Korrektur, Score-Änderung, Musterentdeckung oder Trading-Evaluation statt.

Geprüft wurden Quellcode, produktive CSVs und mit dem Snapshot veröffentlichte Metadaten. Die vollständigen mehrmegabytegroßen historischen CSVs waren über den verwendeten Dateizugriff nicht zum zeilenweisen Lesen verfügbar. Deshalb sind *historische Zero-/Stale-Quoten und individuelle historische Herkunft* ausdrücklich NICHT verifiziert. Die vorhandenen Metadaten werden nur als Metadatenbefund wiedergegeben.

## 2. Tatsächliche Produktionskette

1. **Trigger:** .github/workflows/run_scanner.yml → python -m scanner.app.run_daily. Im Workflow ist SCANNER_FETCH_YAHOO=1 gesetzt. Der Runner bereitet die Research-Publikation vor, führt den Scanner aus, erzeugt Research-Views, lädt nachgelagert Preis-Historie und publiziert Ergebnisse.
2. **Input:** src/scanner/app/run_daily.py ruft build_watchlist_outputs() aus src/scanner/app/build_watchlist.py auf. Als Ausgangsmaterial dient die beständige artifacts/watchlist/watchlist.csv; sie wird mit data/inputs/universe_master.csv synchronisiert.
3. **Markt-Enrichment:** src/scanner/data/enrich/yahoo_prices.py ruft Yahoo-Tagesbars ab. _compute_features aktualisiert unter anderem Kurs, Performance, Trend200, Volatilität, Drawdown, Liquidität und RS3M. **Keine Berechnung / Aktualisierung von Zyklus % in der geprüften aktiven Funktion.** MarketDate wird auf das Tagesdatum des Laufs gesetzt; das beweist NICHT, dass ein vorhandener Zyklus am selben Tag neu gemessen wurde, oder dass jede verwendete Kurskerze bereits zum individuellen Börsenschluss verfügbar war.
4. **Eingangsfelder:** src/scanner/data/schema/canonical.py kennt cycle ← Zyklus % und cycle_status ← Zyklus-Status; falls eine cycle-Spalte schon existiert, überspringt canonicalize_df den erneuten Aufbau dieses Feldes. Der produktive Builder überschreibt nachfolgend cycle ausdrücklich aus Zyklus %.
5. **Defektstelle:** src/scanner/app/build_watchlist.py, ca. Zeilen 500–507: Umwandlung Zyklus % zu numerischem cycle mittels pd.to_numeric(..., errors="coerce").fillna(0.0); alternativ vorhandenes cycle ebenso; wenn beide fehlen, cycle=0.0. Dadurch werden fehlende/nichtnumerische Informationen **als Zyklustief 0 exportiert**. Das ist ein Datenqualitätsfehler und CY-01-Arbeit.
6. **Score:** src/scanner/app/score_step.py → src/scanner/domain/scoring_engine/engine.py. Im geprüften produktiven Engine-Pfad wird cycle_pct zwar im CSV-Mapper als Extra eingelesen, aber nicht als unmittelbar gewichteter Faktor der gezeigten Opportunity/Risk-Berechnung benutzt. Eine umfassende Side-Effect-Freiheit aller anderen Tools wird hier nicht behauptet; CY-01 muss vollständige Regressionen durchführen.
7. **Outputs:** artifacts/watchlist/watchlist_full_raw.csv dokumentiert die Rohdaten nach Markt-Enrichment, artifacts/watchlist/watchlist_full.csv enthält canonical/derived fields. src/scanner/reports/daily_research.py bildet cycle (Alias cycle, Zyklus %) auf den Publisher ab. src/scanner/reports/research_views.py veröffentlicht artifacts/research/latest_scanner.csv und führt die zeitgestempelte Beobachtungsarchivierung.
8. **History:** src/scanner/reports/history_delta.py schreibt bereits berechnetes cycle in artifacts/snapshots/score_history.csv; dort wird laut Funktion upsert_daily_snapshot bei Wiederholung nach Datum+Symbol upsertet. research_views.py baut separat history_analysis.csv und history_recent.csv als Research-Views/Archivprojektionen. Die Semantik **lokales Datum+Symbol-Upsert** ist nicht mit einem garantierten append-only Eventlog für Mehrfachläufe gleichzusetzen; das ist für CY-03 zu klären.
9. **Pattern Discovery:** configs/pattern_discovery/feature_library_v1.json registriert scanner.cycle. L2 erlaubt raw, delta_observations, change_direction mit 1/5/10 früheren Beobachtungen und threshold_crossing bei 25/50/75. L3 kann UP/DOWN aus Differenz ableiten, führt unverändert gegenwärtig als NO_DIRECTIONAL_CHANGE ohne FLAT-Atom; continuous_delta_atomization=false und kontinuierliches Niveau ist nicht als separates Level-Band-Atom registriert.
10. **UI:** src/scanner/ui/generator.py zeigt cycle in der Haupttabelle und im Asset-Drawer; beide verwenden derzeit fallback (cyclePct(r) ?? 0).toFixed(0) und können somit selbst bei später fehlendem Wert erneut 0% vortäuschen. Die reine HTML-Tabellenfunktion enthält ebenfalls Formatierung von cycle. CY-01 muss Downstream-Anzeigen prüfen.

## 3. Nachweisbare Quelldaten: Stand 2026-10-08

Die Zahlen wurden aus den konkreten CSV-Dateien am festgehaltenen Audit-HEAD ausgelesen; keine Prognosen und keine rückwirkenden Rekonstruktionen.

| Prüfobjekt | Zeilen / Kennzeichen | Befund |
|---|---|---|
| artifacts/watchlist/watchlist_full_raw.csv | 224 Rohzeilen | Zyklus % numerisch bei 144, leer bei 80; keine zusätzlich beobachteten nichtnumerischen Quellstrings |
| Alte Quellspalte Zyklus % | 144 Zahlen | davon drei **explizite** 0 (MU, UMI.BR, OGN) und zwei explizite 50 (GOT.V, CDNL); weder Null noch 50 beweisen ohne Lineage eine echte Messung |
| Parallele Rohspalte cycle | 224 Rohzeilen | 156 numerische Einträge, **alle 0**; 68 leer. Unter den 156 Nullen liegen **141 Widersprüche** zu nichtnulligen Zyklus-%-Werten, drei gleichlautende Quellnullen und zwölf Nullen bei fehlender Quelle |
| artifacts/research/latest_scanner.csv | 215 publizierte eindeutige Assets; as_of 2026-10-08 | **83 Werte cycle=0**, zwei cycle=50, weitere 130 numerische andere Werte |
| Nachweisbare Aktien-Nullimputation | 75 Titel | die passende Raw-Zeile hat leeres Zyklus %, der Research-Snapshot 0 |
| Explizite Raw-Nullen im Research | 3 Titel | MU, UMI.BR, OGN; eingetragen, aber Formel/Frische **UNVERIFIZIERT** |
| Kryptos | 5 Titel | Research-IDs CRYPTO:ADA, CRYPTO:BTC, CRYPTO:ETH, CRYPTO:SOL, CRYPTO:DOGE zeigen 0; Roh-Yahoo-IDs ADA-USD, BTC-USD, ETH-USD, SOL-EUR, DOGE-EUR haben leeres Zyklus %. Mapping gesondert behandeln |
| Rohdatenduplikate | 9 Zweiergruppen | 224 Rohzeilen entsprechen 215 verschiedenen Yahoo-Symbolen; dupliziert: 0700.HK, ON, FLNC, KEP, OXY, PKX, VALE, INCY, QS. Die Zyklus-%-Werte innerhalb dieser neun Paare waren gleich; Duplikate dennoch unabhängig testen |

**Wichtig zu den Quellnullen:** EXPLIZITE_NULL bedeutet nur, dass ein Feld eine Zahl 0 enthielt, nicht dass ein realer Oszillatorwert nachweisbar frisch oder gültig war. Ein CY-01-Fix darf weder alle Nullen pauschal löschen noch die parallel vorhandene, fast ausschließlich nullende Rohspalte cycle als sichere Alternative verwenden.

**Beispiel-Untergruppe der nachweislich imputierten Aktien:** SYM, 000660.KS, 005930.KS, 8035.T, 9888.HK, 9988.HK, AMAT, AMKR, ASX, KLAC. Die volle symbolgenaue Negativmaske sollte CY-01 anhand dieses exakt gepinnten Snapshots reproduzierbar erzeugen, nicht aus diesen Beispielnamen ableiten.

**Quellabgleich:** artifacts/watchlist/watchlist.csv und artifacts/watchlist/watchlist_full_raw.csv zeigen auf den verglichenen Zeilen dieselben Zyklus-%-Inhalte. Der produktive Snapshot transformiert die Krypto-Kennungen; deshalb dürfen Kryptos nicht allein durch String-Gleichheit von symbol mit YahooSymbol als nicht zuordenbar gelten.

## 4. Zeit, Herkunft, Kursbasis und Frische

**Nachweisbar:** artifacts/research/scanner_input_provenance.json dokumentiert zur publizierten Beobachtung as_of 2026-10-08 / Run github-37822530040-1 die Markt-Enrichment-Quelle, deren Provider-Frame-SHA256 86aab2c3ef32ff9a3436ed11ed89e1164b7b698a02cb1f48790914ee8ce25f60, sowie den Input-SHA256 46813922a748f38afce0e1f286f579645113a7467ede01e421bfcdddf6dbb9f5 für artifacts/watchlist/watchlist_full_raw.csv. Die Research-Metadaten weisen latest_run_complete=true aus. Die Yahoo-Enrichment-Summary meldet provider_frame_rows=366, 222 erfolgreich gefetchte und zwei fehlgeschlagene Werte (tickers_total=215 als dortige Tool-Zählung; die Summen sind ggf. um Benchmarks bzw. Row-Definitionen verschieden).

**Nicht nachweisbar pro cycle-Wert:** berechnete Formel/version, Erzeugungszeit, verwendete Preisquelle/Symbol-Börse/Währung, konkrete letzte Kursbar und ihr Schlusszeitpunkt, Datenfrische und Behandlung von Fehler-/Konstantenfällen. Die Provider-Frame-Provenance gilt für das heutige Enrichment; sie ist **kein Beleg für die Erzeugung der alten Zykluswerte**. As-of 2026-10-08 darf für alte Zykluswerte nicht als neuer Messzeitpunkt ausgegeben werden.

**Legacy-Referenz:** legacy/market/cycle.py berechnet einen trendbereinigten Close-vs-SMA(20)-Oszillator über 40 jüngere Detrended-Werte (bei period=20); historische Mindestanforderung len(df) >= 60. Auf zu wenig Daten, vielen NA oder konstanter Spannweite folgt dort pauschal 50. Die Methode wird im inaktiven src/scanner/app/main_legacy.py importiert und aufgerufen. Der produktive Workflow startet jedoch scanner.app.run_daily. **UNVERIFIZIERT**, ob die gespeicherten Quellwerte jemals genau mit dieser Formel berechnet wurden. Die 50er in GOT.V/CDNL sind deshalb nicht automatisch valide.

**Spezielle Uhrzeit-/Währungsrisiken:** US-/EU-/Asien-Schlusszeiten, Wochenenden/Kryptos und auto_adjust=True im Yahoo-Enrichment dürfen nicht mit einer unversionierten, eventuell anders erzeugten Zyklus-Preisreihe vermischt werden. Ein gemeinsames MarketDate ist kein Beleg einer gemeinsamen letzten Beobachtungsbar.

## 5. Historie und Forschungsfähigkeit

| Artefakt | Rolle und festgestellter Stand | Aussagegrenze |
|---|---|---|
| artifacts/snapshots/score_history.csv | History-Delta-Quelle mit Snapshot-Rows inklusive cycle | Vollständiger Inhalt nicht zeilenweise auditiert; alte Null-/50-Herkunft unbekannt |
| artifacts/research/history_analysis.csv | verifizierte Research-Archivsicht | Vollständige historische Cycle-Verteilung noch UNVERIFIZIERT |
| artifacts/research/history_recent.csv | kompakter jüngerer Research-Verlauf | Keine automatisch gültigen Deltas bei imputierten Werten |
| artifacts/research/history_research_metadata.json | am 2026-10-05 publiziert; 38.604 Historienzeilen, 300 Symbole, 1.188 gleiche Datum+Symbol-Kombinationen als zusätzliche Rohbeobachtungen | Metadatenbefund, keine selber nachgezählte aktuelle Datei; Duplikatsemantik inkl. Run-ID in CY-03 prüfen |
| artifacts/research/history_metadata.json | Snapshot-ID 32739832-2481-491f-b131-f7413c07f6b2; as_of 2026-10-08 | Bestätigt produktive Research-Publikation, aber keine Cycle-Frische |
| configs/pattern_discovery/feature_library_v1.json | Cycle Feature samt 1/5/10-direction und 25/50/75-cross | Richtungsfunktion schon vorhanden, Daten-Eignung nicht belegt |
| configs/pattern_discovery/l3_search_contract_v1.json | delta-atomization deaktiviert | Cycle-Level-Bands sind nicht bereits implementiert |

**Relevantes methodisches Risiko:** L2 prüft echte aufgezeichnete Beobachtungen und ihre formale Verfügbarkeit, erkennt aber eine upstream *als 0 materialisierte Missingness* ohne zusätzliche Herkunftsmaske nicht zuverlässig als fehlend. Die Forschung muss deshalb bei CY-01/CY-03 die Qualität an der Quelle und an der Lag-Bildung sichern. Für historische 0 und 50 darf keine rückwirkend behauptete valide Messung entstehen. Für 1/5/10obs muss die Definition vergleichbarer aufeinanderfolgender Scannerbeobachtungen einschließlich mehrfacher Tagesläufe festgelegt werden.

## 6. Betroffene Verträge, Gates und Tests (Inventarliste für Folgepakete)

| Pfad / Ebene | Gefährdung / CY-01-Prüfung |
|---|---|
| src/scanner/app/build_watchlist.py | Fallback 0 muss ohne Quell-Kompromiss verschwinden; vorhandene cycle-Rohspalte ist ebenfalls verdächtig |
| src/scanner/data/schema/canonical.py | Mapping-Präzedenz und zulässige nullable cycle-Werte; cycle_status darf nicht falsche Gewissheit signalisieren |
| src/scanner/data/enrich/yahoo_prices.py | aktualisiert keinen Zyklus; nur CY-02 wird die Berechnungs-/Frischearchitektur ändern |
| src/scanner/reports/daily_research.py | REQUIRED verlangt derzeit *Spalte cycle*, keine valide Messung pro Zeile; Alias-Wahl/Kohärenz/NA prüfen |
| src/scanner/reports/research_views.py | KNOWN_COLUMNS, Publication-Validator, schema evolution, latest/history, historischer Import |
| src/scanner/reports/history_delta.py | Snapshot-Schreiben, num_series, möglicher 0-Import, date+symbol-upsert |
| src/scanner/ui/generator.py | Haupttabelle und Asset-Drawer zeigen fehlendes cycle als 0%; Tabellenformatierung ebenfalls prüfen |
| scripts/generate_research_views.py + .github/workflows/run_scanner.yml | vollständiger täglicher Lauf darf bei Missingness nicht heimlich als Cycle-Messung gelten; relevante Tests müssen vor Publikation laufen |
| configs/pattern_discovery/feature_library_v1.json + src/scanner/research/pattern_discovery/feature_library.py | Feature bleibt versioniert; L2-PIT-Verfügbarkeit plus unveränderte Source-Nullen problematisch |
| configs/pattern_discovery/l3_search_contract_v1.json + src/scanner/research/pattern_discovery/search_engine.py | keine neue Level-Band- oder Delta-Bucket-Semantik ohne CY-05 |
| tests/test_history_delta_completion.py, tests/test_daily_research.py, tests/test_research_views.py, tests/test_recent_history.py, tests/pattern_discovery/test_l2_feature_library.py, tests/pattern_discovery/test_l3_search_engine.py | Bestands-Regression und neue Negativfälle (fehlend, 0, 50, 100, Crypto, doppelte Läufe) |
| artifacts/research/scanner_input_provenance.json, history_metadata.json, history_research_metadata.json | Quellenbelege, Metadatendefinitionen, Qualitäts-/Frischefelder nachbessern, keine Historie umschreiben |

## 7. Befundregister (für Folge-PRs)

- **CY00-F01 / KRITISCH:** fehlendes Zyklus % wird zu cycle=0 in build_watchlist.py. **CY-01 erforderlich.**
- **CY00-F02 / HOCH:** parallele raw-cycle-Spalte 156-mal 0 (141-mal Konflikt mit Zyklus %). Keine naive Alias-Fallback-Migration! **CY-01.**
- **CY00-F03 / HOCH:** fehlender Zyklus wird in UI per ??0 erneut zur 0%-Anzeige. **CY-01.**
- **CY00-F04 / HOCH:** aktive Preisaktualisierung berechnet keinen Zyklus und stempelt MarketDate ohne Cycle-Bar-Provenance. **CY-02.**
- **CY00-F05 / HOCH:** Formel und tatsächliche Quelle alter Zykluswerte unbewiesen; historische 50 kann Neutral-Fallback sein. **CY-02/CY-03.**
- **CY00-F06 / HOCH:** in archivierten Beobachtungen bereits materialisierte Nullwerte gefährden PIT-fähige Lag-Features. **CY-01/CY-03.**
- **CY00-F07 / MITTEL:** 5 Krypto-Aliasse, 9 doppelte Raw-Symbolgruppen, mehrere Märkte/Zeitzonen erfordern Identitäts- und Gap-Regeln. **CY-01/CY-03.**
- **CY00-F08 / MITTEL:** Research-Views und Historien-Workflow besitzen nicht automatisch dieselbe Granularität; History-Statistik zu Cycle-Nullen/Staleness noch nicht auditiert. **CY-03.**

**Keine Befunde gelten damit als behoben.** CY-00 ist eine Inventur, keine CAPA-Implementierung.

## 8. CY-00-Abnahme und Prüfprotokoll

- [x] Aktuellen main-HEAD und Auditbasis verifiziert.
- [x] Produktiven Aufrufpfad, Alias-Mapping, UI, Historisierung und L2/L3-Verträge nachverfolgt.
- [x] Quelldaten mit aktuellem veröffentlichtem Research-Snapshot abgeglichen, Fehl- und Nullwerte getrennt.
- [x] Krypto-Zuordnung und neun Raw-Duplikate gesondert ausgewiesen.
- [x] Legacy-Funktion ausdrücklich nicht als aktive produktive Berechnung behauptet.
- [x] Kurskerzen-, Formel- und Frischeinformationen entweder belegt oder als UNVERIFIZIERT gekennzeichnet.
- [x] Konkrete Änderungsorte, Regressionstests und CY-01-Findings dokumentiert.
- [ ] Vollständige große historische CSVs zeilenweise auf Null-/Frischemasken untersucht — **absichtlich nicht Teil der erreichten aktuellen Datenbasis; als CY-03-Befund offen**.
- [ ] Repository-PR ist gemergt — **erst nach gesonderter Prüfung**.
- [ ] Test-Suite ausgeführt — **nicht erfolgt**, denn CY-00 änderte keinen Runtime-Code; Quellprüfungen + CSV-Abgleiche sind kein pytest-Lauf.

**Technischer Befundstatus:** Inventur fertig; Dokumentations-Merge ausstehend. **Fachlicher Befundstatus:** dokumentiert, einschließlich offener empirischer/technischer Beweise. **Empirische Validierung:** nicht gestartet.

## 9. Exakt nächster Schritt: CY-01

1. Auf frisch geprüftem main in eigenem Branch beginnen; diesen CY-00-Bericht und CYCLE-DIR-Plan als Referenz binden.
2. Test-first: Leere Zyklus-%-Felder, explizite 0/50/100, ungültige Eingaben, widersprüchliche raw-cycle=0, Crypto-Aliasse, UI-NA, sowie Research-Publikation reproduzieren.
3. Finale Quellpräzedenz und nullable Semantik mit Qualitätssignalen spezifizieren: 0 nur bei belegter Eingabe, keine Verwendung des opportunistisch gefüllten raw-cycle=0.
4. In gesamtem Pipelinepfad die künstliche 0-Imputation und UI-Fallbacks beheben; keine neue Oszillatorformel in CY-01.
5. Bestehende Snapshots unverändert lassen; über historische Ausschlussmaske/Provenance für CY-03 entscheiden, keine stillen Datenänderungen.
6. Relevante Tests + Score-/Risk-/Decision-Regression laufen lassen und Bericht mit konkreten Befehlen/Nachweisen erstellen; keine positive Aussage ohne echten Test.

**Commit-Kandidat für CY-01:** fix(cycle): preserve missing values and provenance

## 10. Übergabe für neuen Chat

**Datum/Uhrzeit/Zeitzone:** 2026-10-09, ca. 11:35 UTC / 13:35 MESZ  
**Paket:** CY-00  
**Branch:** docs/cycle-dir-cy00-audit-20261009  
**Basis-SHA:** 8cd09063f204c7eb27a3fe95d3ddc5b1a4e89233  
**Aktueller HEAD:** beim Folgeschritt neu prüfen, niemals aus der Erinnerung übernehmen  
**Fachliches Ziel:** Produktion/History/Pattern-Datenfluss und Fehlerstellen erfassen  
**Status:** Inventur fachlich dokumentiert; keine Fehler behoben; keine empirische Prüfung  
**Änderungen:** nur docs/cycle_direction/CY-00_BESTANDSAUFNAHME.md; Commit/PR siehe GitHub-Dokumentationsbranch  
**Getestet:** read-only Repository-Analyse und deterministischer Roh-/Snapshot-CSV-Abgleich; kein pytest ausgeführt  
**Datenbasis:** 2026-10-08, Snapshot 32739832-2481-491f-b131-f7413c07f6b2, 215 Research-Assets, 224 Rohzeilen  
**Hauptbefunde:** 83 publizierte Nullwerte = 75 klar imputierte Aktien + 3 explizite Raw-Nullen ohne Frischebeweis + 5 Krypto-Aliasse mit leerer Rohquelle; raw-cycle-Spalte besteht aus 156 Nullen und 68 Leerwerten; keine aktive Zyklusberechnung  
**Offen:** CY00-F01 bis F08; vor allem Quellen-/Frische-Nachweis und künftige Datenqualitäts-Gates  
**Nicht geändert:** keine Produktionsdatei, kein Scoring, kein Decision, keine Archive, keine registrierten Feature-Versionen  
**Exakt nächster Schritt:** CY-01 auf separatem Branch, testgetriebene Missingness-Reparatur

**Referenz:** CYCLE-DIR-2026-10-09-v1; ursprünglicher Plan Scanner_vNext_CYCLE-DIR_Massnahmenplan_2026-10-09.md. Der vollständige Masterplan ist weiterhin die verbindliche Roadmap; dieser Bericht ist ausschließlich die CY-00-Inventur.
