# CYCLE-DIR – CY-06 Discovery-/Statistik-Eignungsbericht und Stop-Gate

**Plan:** `CYCLE-DIR-2026-10-09-v1` · **Paket:** CY-06 ausschließlich  
**Prüftag:** 2026-10-10 (Europe/Berlin)  
**Referenz:** `main` bei Bestandsaufnahme `5087b79aa1e57a3658d59d359374530b8e25a8e5` (Commit 2026-10-10 10:53:37 UTC)  
**Arbeitsbranch:** `research/cycle-dir-cy06-readiness-20261010`  
**Gesamtentscheidung CY-06:** **BLOCKIERT / NICHT_FREIGEGEBEN** für reale L3–L6-Forschung. Die **read-only Bestands- und Eignungsprüfung** ist durchgeführt. Die CY-06-Fachabnahme ist **nicht** erreicht.  
**Ergebnisstatus:** **NICHT AUSWERTBAR aufgrund fehlender research-geeigneter Zeitreihen**. Dies ist **kein** negatives Ergebnis eines statistischen Hypothesentests.

## 1. Paketgrenze und Abhängigkeiten

Der Masterplan CY-06 verlangt (1) Eignungsprüfung, (2) L3 Discovery auf einer vorregistrierten Discovery-Partition, (3) L4 Multiplicität/Effective-N/Abhängigkeiten/Unsicherheit, (4) gepaarte A/B/C/D-Vergleiche mit gleicher Coverage, (5) Coverage- und Bias-Audit und (6) nur bei L4-Eignung L5-Freeze und L6-Abhängigkeitsgraph.

CY-05 ist auf `main` durch [PR #280](https://github.com/grisuweimar-crypto/trading-zentrale/pull/280) gemergt (Merge `ae6ceda6a9182d450cf14d71e4db500403f4817c`). Die **opt-in** L2/L3-v2-Contracts und das vor Outcome-Inspektion fixierte Methodendesign existieren. Die dokumentierte CY-05-CI [#38046132023](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/38046132023) meldet **SUCCESS / 86 passed** für den damaligen Code-HEAD `ce8ca9a2d60759f73f13e14ee8d7a52b80085197`. Das bestätigt **technische** Tests, nicht die externe PIT-Zulassung eines historischen Samples.

**Harter Sperrgrund:** [Issue #269](https://github.com/grisuweimar-crypto/trading-zentrale/issues/269) ist am Prüftag offen. Die Archivzeilen weisen `WATCHLIST_DECLARED_ONLY`, `SESSION_DATE_CUTOFF_ONLY` und `market_timezone=UNVERIFIED` aus. Eine intern reproduzierbare 60-Bar-Rechnung ersetzt keinen unabhängigen Nachweis von Listing/Währung/zeitlicher Marktbar-Verfügbarkeit. **CY-03 ist daher nicht fachlich forschungsfreigegeben.**

Das CY-05-Methodendesign `configs/cycle_direction/cy05_preregistered_design_v1.json` setzt `execution_allowed=false`, `frozen_l1_run_manifest_created=false` und `empirical_results_computed=false`. Der echte Daten-Cutoff, Discovery/Holdout-Split, vollständige L1-Input-Hashes und ein auf realen freigegebenen Daten erzeugtes `FROZEN_PRE_RUN`-Manifest fehlen. **Kein CY-06-L3-Run vor diesen Nachweisen.**

## 2. Gelesene Quellen, Bindungen und quantitative Istlage

Alle folgenden Werte beziehen sich auf **den verifizierten Repository-Stand**, nicht auf erfundene Rückrechnungen. Die Dateihashes sind **die im Archivmanifest veröffentlichten SHA-256-Werte**, keine in dieser Sitzung erneut berechneten Binär-Hashes.

| Quelle | Verbindliche Angabe / SHA-256 laut Manifest |
| --- | --- |
| `artifacts/cycle_history/manifest.json` | schema `cycle_observations_cy03_v1`; `snapshots=1`; `observations=215`; `research_eligible=0` |
| `artifacts/cycle_history/observations.csv` | `b094acb37e42b183b3a502c6a8bef73baa4a1cc745e7e7fad6edc4b745c5735e` |
| `artifacts/cycle_history/eligibility.csv` | `7113bea0bdc6b0275765a61445a35313c0c1e5fd3bc02550af502402ac8e9c70` |
| `artifacts/cycle_history/coverage.csv` | `6c08cc34d94dfbf62ea98fb797a88bd1196844b883433ccff40cba1771c97d2e` |
| Separates CY-03-Bars-Archiv | Manifest `archive_bars_sha256=0a7674651b35cf02ecac581398fd0d6ac5204fc0f435e3f5764533117eea8007` |
| Quelle `watchlist_full.csv` | `018cb52025326bd3c2e995bd61ee482f129a473b9a4bff330add025f99e83008` |
| Aktuelle Identität | `snapshot_id=f409c312-0349-4b29-8dab-3ecdbd9463b3`; `run_id=github-38040088358-1`; `as_of=2026-10-10` |

**Istzählung durch read-only Einlesen der drei veröffentlichten CSV-Dateien:**

| Prüfdimension | Istbefund |
| --- | --- |
| Archivierte Beobachtungen / eindeutige Assets | 215 / 215 |
| Unterschiedliche Snapshot-IDs / `as_of`-Tage | 1 / 1 (`2026-10-10`) |
| `PROVISIONAL_REPLAYED`, `quality=VALID`, numerischer Cycle | 209 |
| Ausgeschlossen: `INSUFFICIENT_HISTORY / GAP_IN_DAILY_BARS` | 6: `000660.KS`, `005930.KS`, `6503.T`, `6506.T`, `6861.T`, `8035.T` |
| `CRYPTO:`-Asset-IDs | 6, alle technisch provisional, **keine** Researchzulassung |
| übrige Asset-IDs | 209, davon 203 provisional und 6 ausgeschlossen (nicht pauschal alle als Aktien klassifiziert) |
| `lag_1obs` | 0 gültig; 209 `NO_PRIOR`, 6 `INVALID_CURRENT` |
| `lag_5obs` | 0 gültig; 209 `NO_PRIOR`, 6 `INVALID_CURRENT` |
| `lag_10obs` | 0 gültig; 209 `NO_PRIOR`, 6 `INVALID_CURRENT` |
| `research_status` | 215 × `BLOCKED_EXTERNAL_VERIFICATION_269` |
| Für Forschung freigegebene Beobachtungen | **0** |
| Marktwährungs-Herkunft / Bar-Zeitbeweis | 215 × `WATCHLIST_DECLARED_ONLY` / 215 × `SESSION_DATE_CUTOFF_ONLY` |
| Marktzeitzone | 215 × `UNVERIFIED` |
| Berechnungsformel und Preisbasis im Archiv | 215 × `cycle_detrended_sma20_range40_v1`; 215 × `1d_auto_adjust_true_close` |

**Coverage:** Der Archivzeitraum umfasst nur den 10.10.2026. Es gibt deshalb weder zeitliche Stützregionen für 1/5/10obs noch eine echte Regimeabdeckung oder eine nach Assetklassen/Monaten belastbare Stichprobe. Es sind keine gültigen Paarvergleiche möglich. Die aktuelle Zugehörigkeit zum Scanner-Universe ist **keine** historische survivorship-freie Stichprobenbestätigung. Die 6 CRYPTO-IDs unterliegen eigener 7-Tage-/Session- und Gap-Semantik, nicht dem Standard einer Aktienbörse.

## 3. Eignungsprüfung alter History / Ausschlüsse

**Nicht als Cycle-Forschungssample freigegeben:**

- `artifacts/snapshots/score_history.csv`, `artifacts/research/history_analysis.csv`, `artifacts/research/history_recent.csv` sowie historische Legacy-`cycle`-Felder: kein durchgehend belegter Formel-/Provider-/Bar-Cutoff-/Währungs-Vertrag für alle damaligen Cycle-Beobachtungen. `history_analysis.csv` enthält auch Legacy-Reihen; historisch wiederholte Datum/Symbol-Kombinationen bedürfen einer Run-/PIT-Bindung. Numerisch vorhandene 0 oder 50 sind keine eigenständigen Herkunftsnachweise.
- Der dokumentierte Qualitätsvorfall des 08.10.2026 hatte **80 nachweislich als Null imputierte Missing-Quelle-Fälle**, darunter fünf Crypto-IDs (CY-01-Maske `artifacts/research/cycle_quality/exclusion_mask_2026-10-08.csv`). Dies ist ein historischer **Beispiel-/Quarantänebefund**, keine vollständige nachgezählte Alt-History-Statistik.
- Rückwirkend berechnete Cycle-Werte dürfen nur mit separater `RECONSTRUCTED_RESEARCH`-Kennung und unabhängigem damaligem Availability-Nachweis aufgenommen werden; nicht als echte zeitgenössische Scannerbeobachtung und nicht durch Nachdatierung in CY-03.
- Aktuelle 209 `PROVISIONAL_REPLAYED`-Werte sind **rechnerisch gültig, aber research-unzulässig**. Fehlende zulässige Vorgänger, externe Quote-/Listing-Verifikation und Multi-Day-Coverage können nicht durch heutige Kursdaten ergänzt werden.

**Asset-/Listing-/Zeit-/Regime-Eignung:** Nur der eine `as_of`-Tag ist vorhanden. Ein umfassender Bias-/Regime-/Currency-Stabilitätsvergleich ist daher **nicht schätzbar**; Asset-Alias, Market-Timing und Survivorship bleiben als erforderliche künftige Negativ-/Coverage-Prüfungen markiert. Währungsfelder sind zwar gefüllt, aber nur Watchlist-deklariert.

## 4. Vorregistrierter Vergleich A/B/C/D — geplant, nicht ausgeführt

Das CY-05-Design `configs/cycle_direction/cy05_preregistered_design_v1.json` bindet:

| Arm | Vorgegebene Forschungsbedingung | Iststatus |
| --- | --- | --- |
| A | Kontrollarm ohne Cycle-Level/-Richtung | **NICHT AUSGEFÜHRT** – keine gemeinsame freigegebene Cycle-Research-Kohorte |
| B | A + aktuelles `cycle_level_band` | **NICHT AUSGEFÜHRT** |
| C | B + `change_direction` auf 1/5/10 qualifizierten Scannerbeobachtungen | **NICHT AUSGEFÜHRT**, 0 erlaubte Lags |
| D | C + maximal eine weitere zugelassene Nicht-Cycle-Bedingung (höchstens 3 Atome) | **NICHT AUSGEFÜHRT** |

Fixierter **Primärkontrast:** C minus B; Zustand `LEVEL_LT_25` + `UP(5obs)`; Target `peer_excess_20t_gt_0`, primäre Metrik gepaarter inkrementeller relativer Alpha-Effekt **nach Kosten**. L3-Target-Horizonte 5/20/40/60 Handelssessions sind **nicht** 1/5/10 Scannerbeobachtungen. Designkosten: 20 bps Roundtrip, Sensitivität 10/50 bps, **Planannahmen statt gemessener Broker-Kosten**. Geplanter BH-FDR `q=0.05`; L1-Minimum Roh-N 40, 3 zeitliche Stützregionen, Effekt 0.01, Baseline-Lift 0.005 sowie Such-/Kandidatenbudgets gemäß Design. Diese Grenzwerte werden nicht nach Ergebnisansicht angepasst.

**Kein** `FROZEN_PRE_RUN`-Manifest mit realem Datensatz und Holdout ist vorhanden. Die vorregistrierte **Methodik** ersetzt den gebundenen **Run-Freeze** nicht. Deshalb wurde weder A noch B noch C noch D auf Outcome-Daten ausgeführt. Insbesondere darf Arm A allein auf einem anderen, möglicherweise viel größeren historischen Sample nicht gegen C verglichen werden.

## 5. L3–L6 Bestandsaufnahme und konkrete Stopps

| Modul / Code | Vorhandene Vertragsfunktion | CY-06-Ergebnis |
| --- | --- | --- |
| L3: `search_engine.py`, `l3_search_contract_cycle_v2.json` | Separate opt-in Niveau-/Richtungsatomisierung, max. 3 Atome, Kandidaten-Rejektionsprotokoll | **BLOCKED_BEFORE_L3**: kein research-eligible Input, kein real eingefrorener L1-Run. Nicht ausgeführt. |
| L4: `statistical_guard.py`, `l4_statistical_guard_v1.json` | Vollständige L3-Familie in Multiplicität, Effective-N-Proxy, zeitliche Supportregionen, symbolabhängige Konzentration, hashgesäte Moving-Block-Bootstrap-Intervalle, Baseline-Lift | **NOT_APPLICABLE** ohne echte L3-Kandidaten/Outcomes; keine p-/q-Werte, Konfidenzintervalle oder Schätzer. |
| L5: `candidate_registry.py`, `l5_candidate_registry_v1.json` | Freeze nur bei `ELIGIBLE_FOR_L5` mit L1/L3/L4-Hashes und QM-C-Übergabe | **NOT_APPLICABLE**: keine L4-freigegebenen CY-06-Kandidaten; kein PAT-Freeze. |
| L6: `dependency_graph.py`, `l6_dependency_graph_v1.json` | Hashgebundene L5-Knoten/Events, Duplicate/Nested/Related, strukturelle und empirische Abhängigkeit | **NOT_APPLICABLE**: keine CY-06-PAT-Objekte; kein Abhängigkeitsgraph. |

**Statistikstatus (nicht nullwertig schätzen):** Anzahl real getesteter CY-06-Hypothesen **0**; Kandidatenliste **leer, weil kein Test erlaubt war**, nicht weil alle Hypothesen falsch waren. Negativliste der **getesteten** Muster ebenfalls leer. Eligible Roh-N=0; ein statistisches Effective-N ist **nicht schätzbar** (nicht als gemessenes „0“ ausgeben). Keine Supportregionen, kein Baseline-Lift, keine C–B-Differenz, keine Kostenrendite, kein FDR-/Multiplicity-Test, keine Bootstrap-Intervalle, keine Effektstärke und keine Erfolgsquote. Keine Out-of-Sample-/Prospektivbehauptung.

## 6. Durchführung / nachgewiesene Prüfungen

**Tatsächlich in dieser Arbeitseinheit durchgeführt:** per GitHub-Repositoryverbindung read-only Abruf von `main`-HEAD, PR #280, Issue #269, Statusregister, CY-03-Archiv (Manifest, Observations, Eligibility, Coverage), CY-05-Design und L3/L4/L5/L6-Contractdateien; getrennte CSV-Zeilen-/Statusauszählung.

**14/14 PASS** für gezielte, **nicht-pytest-basierte**, schreibfreie Konsistenzassertionen: (1) Manifest-Beobachtungszahl, (2) Ein-Snapshot-Abgleich, (3) Snapshot-ID über Ledger/Eligibility, (4) eindeutige Asset-IDs, (5) provisional-Zähler, (6) Exclusion-Zähler, (7) Eligibility-Zeilenanzahl, (8) Coverage-Zeilenanzahl, (9) vollständiger Issue-269-Research-Block, (10) kein gültiger 1/5/10-Lag, (11) 0-Lags im Manifest, (12) fehlender echter L1-Freeze, (13) `execution_allowed=false` / L1-Gate, (14) keine empirischen Ergebnisse laut Design. Das sind **lokale logische Konsistenzprüfungen an gelesenen GitHub-Dateiinhalten**, keine Ausführung der Repository-Test-Suite und **keine unabhängige Byte-/Kursprovider-Prüfung**.

**Vorhandener separater CI-Nachweis:** CY-05 #38046132023 `SUCCESS`, 86 Tests auf dortigem Branch-Commit (nicht als CY-06-Durchlauf zählen).

**Nicht ausgeführt und Grund:** `pytest`/L3/L4/L5/L6 auf realen CY-06-Daten, L1-Freeze, jede Outcome-Suche, statistische Hypothesentests, PAT-Freeze und L6-Persistenz. Dafür fehlen erlaubte Forschungsinputs. Ein direkter externer Git-Clone aus dieser Arbeitsumgebung war nicht möglich (DNS-Zugriff auf github.com fehlgeschlagen); GitHub-Repositorydaten wurden über die autorisierte Verbindung geprüft. Diese Einschränkung erzeugt keine fiktiven Testbelege.

**Regression-/Versionsgrenze:** Dokumentation und Statuspflege verändern weder L2/L3-v1/v2-Contracts noch Cycle-Berechnung, bestehende Archivbytes, Scoring, Elliott, Risk/Confidence, Selection/Timing, Decision, Portfolio Action oder Execution. Es werden keine Ergebnisse in `artifacts/research/pattern_discovery/discovery_runs` und keine PAT-/L6-Register geschrieben.

## 7. Abnahmematrix, explizite Blocker und Wiederanlauf

| CY-06-Abnahmekriterium | Ergebnis |
| --- | --- |
| Eignungsbericht mit quantifizierten Ausschlüssen | **ERFÜLLT** für heute publizierte CY-03-Dateien; ältere Legacy-Reihen mangels Beweis ausgeschlossen |
| Data-quality-/PIT-/Research-Gate | **NICHT ERFÜLLT**: #269 offen; 0 research eligible |
| Vorregistrierter, auf aktuelle Daten gebundener L1-Run, sauberer Discovery-/Holdout-Split | **NICHT ERFÜLLT** |
| Echte qualifizierte 1/5/10obs-Historie | **NICHT ERFÜLLT**, 0/0/0 |
| L3 vollständige Kandidaten-/Negativliste aus erlaubtem Discovery-Run | **NICHT ANWENDBAR**, Run verboten |
| L4 N/Effective-N, Kosten, Multiplicität, Unsicherheit, gepaarter A–D-Vergleich | **NICHT ANWENDBAR**, keine Stichprobe |
| L5 nur qualifizierte Kandidaten einfrieren / L6-Abhängigkeiten | **NICHT ANWENDBAR**, keine L4-Freigabe |
| Keine Resultat-, History- oder Produktivsignal-Fälschung | **ERFÜLLT**, reiner Dokumentations- und Sperrpfad |

**Blocker CY06-B01:** [#269](https://github.com/grisuweimar-crypto/trading-zentrale/issues/269) durch unabhängige Listing-/Währungs-/Kalender-/Bar-As-of-Evidence und dedizierten, versionierten Release-Vertrag lösen; `PROVISIONAL_REPLAYED` allein darf nie `ELIGIBLE` werden.

**Blocker CY06-B02:** Neue echte, mehrfach publizierte CY-03-Beobachtungen wachsen lassen und append-only, Identität, Originalwährung, Kalender, as-of, Fingerprints, Replay und Lag-Coverage je Asset/Monat nachweisen. Für 5obs sind mindestens 6, für 10obs mindestens 11 echte zulässige Beobachtungen pro durchgehend identischem Asset nötig; zusätzliche Gaps/Regime- und Ziel-Reifeanforderungen können den Zeitraum verlängern.

**Blocker CY06-B03:** Vor **jeder** Outcome-Sichtung den realen Inputdatensatz, Snapshot-/Universe-/Contract-/Methodendesign-Hashes, Source-Cutoff, Discovery-/Holdout-Partition, Searchbudget, komplette Target-Familie, Kosten, Regelung zu überlappenden Sessions und die Mindestkriterien im gültigen `FROZEN_PRE_RUN`-L1-Manifest binden; Versions-/Replay-Prüfung unabhängig dokumentieren.

**Blocker CY06-B04:** Erst danach in **diesem** Paket auf identischem Asset-/Zeit-/Regime-Sample A/B/C/D getrennt recherchieren, **alle** Kandidaten einschließlich negatives/verworfener dokumentieren, L4 FDR + robuste Unsicherheit + Effective-N belegen und bei L4-Eignung L5/L6 ausführen.

**Wiederaufnahmebedingung:** CY06-B01 + B02 + B03 unabhängig belegt. Vorher kein produktiver oder vermeintlich empirischer CY-06-Run. Ist später eine zulässige Stichprobe weiterhin zu klein, weiter `BLOCKIERT / NICHT_FREIGEGEBEN` und Beobachtungen sammeln.

## 8. Formale CYCLE-DIR-Übergabe

- **Datum/Zeitzone:** 2026-10-10 · Europe/Berlin (Git-Referenzzeit 10:53:37 UTC).
- **Paket:** CY-06 – Eignung, Discovery, L4–L6 und Vergleichsstatistik.
- **Branch / Ausgangs-HEAD:** `research/cycle-dir-cy06-readiness-20261010` / `5087b79aa1e57a3658d59d359374530b8e25a8e5`; `main` nach Erstellung der Dokumentation nicht geändert.
- **Ziel:** Objektiv entscheiden, ob Cycle-Richtung gepaart gegen reine Cycle-Niveaubaseline eine zusätzliche Vorhersageinformation besitzt.
- **Technischer Status:** Bestands- und Sperrprüfung dokumentiert; **CY-06-Code-/Testlauf nicht durchgeführt**.
- **Fachlicher Status:** **BLOCKIERT**, kein vollständiger CY-06-Abschluss.
- **Empirischer Status:** **NICHT AUSWERTBAR**, keine Hypothesen getestet, kein Vorteil oder Nachteil nachgewiesen.
- **Geänderte Pfade:** dieses Dokument und `docs/cycle_direction/STATUS.md` auf separatem Branch; Referenzen auf späteren Commit/PR im Review ergänzen.
- **Getestet:** oben explizit dokumentierte 14 read-only Assertions; separate frühere CY-05-CI mit 86 Tests. Keine lokale pytest-/Runner-Ausführung und kein CI-Ergebnis für CY-06 behauptet.
- **Datenbasis:** 1 × CY-03 Snapshot `f409c312-0349-4b29-8dab-3ecdbd9463b3`; 215 Assets, 209 provisional, 6 ausgeschlossen, 0 Eligible, 0/0/0 qualifizierte Lags, SHA256-Referenzen oben.
- **Offene Blocker:** #269, CY06-B01–B04.
- **Bewusst unverändert:** frühere Archive, v1/v2-Forschungsverträge, Daten/Scoring/UI/Decision/Portfolio.
- **Exakt nächster Schritt:** CY06-B01/#269 extern belegen und CY-03-Mehrtag-Archiv sammeln; danach L1-Release-/Holdout-Freeze **vor** der ersten echten L3-Outcome-Auswertung.

**Abschlussentscheidung:** Nur die Bestandsaufnahme und die methodisch gebotene **Nicht-Auswertung** sind abgeschlossen. Das Arbeitspaket CY-06 selbst bleibt bis zur Abnahme seiner empirischen Arbeitsschritte **BLOCKIERT / NICHT_FREIGEGEBEN**. Das Statusregister darf es nicht als `FERTIG_FACHLICH` kennzeichnen.

## 9. Nachlauf am 10.10.2026 – Abhängigkeitsfehler und Quellenbeleg (nach Ausgangsprüfung)

- **CY-03 Krypto-Gap:** Bei Code-Review der realen Asset-IDs `CRYPTO:BTC` etc. wurde festgestellt, dass der bisherige `lag_mask()`-Schutz die 1-Kalendertag-Regel nur an `-USD/-EUR/-USDT/-BTC`-Suffixen erkennen konnte. Die tatsächlich archivierten sechs `CRYPTO:`-Assets fielen dadurch fälschlich unter die Aktien-5-Tage-Toleranz. [PR #282](https://github.com/grisuweimar-crypto/trading-zentrale/pull/282) führt eine minimal-invasive Korrektur und positive/negative Regressionen ein. **PR #282 ist nach erneutem CI-Lauf (CY-03, Cycle-Quality und QM-B erfolgreich) gemergt:** `main`-Commit `509e22cfa6253fcc5b0e800cc80e339491e9b530`. Das alte CY-03-Archiv bleibt unverändert; die geänderte konservative Gap-Semantik wirkt erst bei künftigen Masken-/Archivprüfungen.
- **Separates QM-B-Gate:** Der CY-03-PR-Lauf zeigte einen historischen Taxonomie-Scan-Fehler **nicht** aufgrund des Krypto-Codepatches, sondern wegen einer zuvor unreviewten `configs/pattern_discovery/feature_library_cycle_v2.json`. Nach eigenständiger Prüfung der zeitpunktgebundenen, nicht rückprojizierenden Feldsemantik wurde [PR #283](https://github.com/grisuweimar-crypto/trading-zentrale/pull/283) mit grünen Checks gemergt, `main`-SHA `a90abebac25c12348028929a79923aafd50110b5`. Das ist eine QM-B-Klassifikation, **keine** Forschungsfreigabe.
- **Issue #269 / koreanische Feiertagslücken:** Die offiziellen Feiertage 24.–26.09.2026 der [Bank of Korea](https://www.bok.or.kr/eng/main/contents.do?menuNo=400373) und die [KRX-KOSPI-Handelsregel](https://global.krx.co.kr/contents/GLB/06/0602/0602010201/GLB0602010201T1.jsp), wonach staatliche Feiertage keine Börsensessions sind, belegen die 24./25.09.-Lücken als kalenderkonsistent. [Dokumentiert als Issue-Kommentar](https://github.com/grisuweimar-crypto/trading-zentrale/issues/269). Das ersetzt **nicht** eine versionierte Kalender-Policy, Provider-Bar-PIT-Zertifizierung oder das offene externe Research-Gate.
- **Weiterhin unverändertes CY-06-Ergebnis:** Null zulässige Lags und null Research-Eligibility am einzigen publizierten CY-03-Snapshot; keine L3–L6-Analyse zulässig und keine Inkrementalität bewiesen.
