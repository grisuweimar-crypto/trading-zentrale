# CYCLE-DIR — Wissenschaftliche Nachweiskette und laufende Bereitschaftsprüfung

**Plan:** `CYCLE-DIR-2026-10-09-v1` · **Stand:** 10.10.2026 · **Geltung:** Querschnitts-Governance-Prüfung zu CY-02–CY-08, **keine neue Forschungsphase**.

## 1. Warum der bisherige Stopp notwendig ist

Die erste echte CY-03-Publikation umfasst einen archivierten Scanner-Snapshot vom 10.10.2026 mit 215 Assets (209 intern gültig, 6 ausgeschlossen), 0 Forschungsfreigaben und keinen 1/5/10-Observation-Lags. [Issue #269](https://github.com/grisuweimar-crypto/trading-zentrale/issues/269) erfordert unabhängige Quell-Listing-/Originalwährungs-/Börsensession-/Bar-Verfügbarkeitsbelege.

**Wesentliche Implementierungsgrenze:** `src/scanner/reports/cycle_history.py` erzeugt v1-`research_eligible=0` und `BLOCKED_EXTERNAL_VERIFICATION_ISSUE_269` **absichtlich dauerhaft**. `cycle_cy05_adapter.py` akzeptiert keinen eigenmächtig umgeschriebenen Manifeststatus. Weder 100 neue Snapshots noch ein geschlossenes GitHub-Issue können dies ohne **separaten wissenschaftlichen Release-Vertrag** freischalten.

Weitere bekannte Wartebedingungen: ein echter L1-`FROZEN_PRE_RUN` mit PIT-fixiertem Daten-/Holdout-Split, eine echte L3–L6-Auswertung, danach ausschließlich *post-L5-freeze* neue L7-Captures, vollständig gereifte 5/20/40/60-Session-L8-Zielpfade, pre-registrierte QM-C4-L9-Looks, L10-Rating, eine separate L12-Reviewerentscheidung und erst dann L13 Shadow.

## 2. Neu eingerichtete Kontrolle: `scripts/watch_cycle_science.py`

Der Watch prüft **read-only**:

| Stufe | Was er wirklich beweist | Was er nicht behauptet |
| --- | --- | --- |
| CY-03 Archivintegrität | Vorhandene CY-05-`inspect_cycle_archive`-Prüfung: ursprüngliche Ledgerzeilen, SHA-256 aller bisherigen Quellen-CSVs und 60-Bar-Gzip-Archive, nachrechenbare Lag-Maske/Coverage, Snapshot-/Run-Identität | Keine unabhängige Provider-PIT-Freigabe |
| Datensammlung | Anzahl unveränderlicher Snapshots und unterschiedlicher Scan-Daten, letzter archivierter Tag, echte `PROVISIONAL_CHAIN`-Zähler für 1/5/10obs, Erhalt der ersten Snapshot-ID | Kein Gleichsetzen einer Kurs-/Trendverbesserung mit Signalgüte; keine Gleichsetzung 5obs mit fünf Handelstagen |
| Frische | Alter der letzten publizierten Archivbeobachtung am Europe/Berlin-Kalendertag, Warnung bei mehr als 3 Kalendertagen | Kein exakt zugesicherter GitHub-Cron-Ausführungszeitpunkt, kein pauschaler Quellen-Bar-PIT-Beweis |
| CY-02 Provenance | Status von #269 **nur als Informationsfeld**, weiterhin `NOT_VERIFIED` | **Issue closed** ist weder signierter Quellbeleg noch Research-Zulassung |
| CY-05/CY-06 | L1- und L5-Artefaktpfade / Defizite; Verdachtsartefakte erfordern zusätzliche echte PIT- und Cycle-Bindungsprüfungen | Datei-Existenz ist **kein** echter Run-Freeze oder qualifizierter PAT |
| CY-07/CY-08 | Sichtbarkeit vorhandener generischer L7/L8/L9/L10/L12-Register und fehlender **exakt Cycle-PAT-gebundener** Freigabe | Generisches Lab-`READY_WITH_WORK`, synthetische Tests oder ungebundene Registry-Einträge sind kein CYCLE-DIR-Nachweis |

Die Watch liefert `BLOCKED` bei offenem Wissenschaftsgate und `INTEGRITY_FAILURE` bei beschädigtem Archiv, unzulässiger v1-Researchfreigabe, fehlendem Erst-Snapshot oder gebrochenem CY-05-Methodik-Vertrag. **BLOCKED ist bei gültigen, noch unreifen Daten ein erwartetes und technisch erfolgreiches Prüfergebnis; INTEGRITY_FAILURE ist ein tatsächlicher CI-Fehler.**

Keine automatische Änderung von `research_eligible`, kein `FROZEN_PRE_RUN`, keine Pattern Discovery, keine generierte `SUPPORTED`-Bestätigung, keine L12-Admission, kein Umbau des produktiven Scanners.

## 3. Automatisierung und Nachweise

`.github/workflows/cycle_dir_science_watch.yml`:

- **nach erfolgreichem Scanner-Autopilot** via `workflow_run` (Checkout des bereits veröffentlichten `main`),
- **einmal täglich** um planmäßig **22:50 UTC** (GitHub-Cron kann verzögert laufen, keine exakte SLA),
- jederzeit **manuell** per `workflow_dispatch`,
- Regression auf PRs für Watch, Tests und diesen Vertrag,
- GitHub-Issue-Zustand #269 abrufen; bei API-Ausfall nur `UNKNOWN`, **niemals** fälschlich freigeben,
- keine Repositorium-Schreibrechte (`contents: read`); veröffentlichte Audit-JSON als GitHub-Actions-Artefakt 30 Tage und laufbezogene `GITHUB_STEP_SUMMARY`,
- nur bei Integritäts-/Contractfehlern CI **rot**, bei blockierten Forschungsbedingungen **grün mit BLOCKED**.

Die Aktion **ersetzt weder** den produktiven täglichen Scanner noch dessen bestehendes Fehler-/Retriesystem. Wenn mehrere Tage keine Daten erscheinen, liefert die tägliche Kontrolle `STALE`/Warnhinweis; sie erzeugt keine künstlichen Snapshots.

## 4. Nachweisbarer Freigabepfad – was als Nächstes gebaut werden muss

| Reihenfolge | Wissenschaftliche Zulassung / Beleg | Manuelle Entscheidung |
| --- | --- | --- |
| **A: CY-02 (#269)** | Für jede betroffene Listing-/Kursquelle: Originalticker, Handelsplatz, Originalwährung und Quote-Einheit (GBp/ZAc), Provideridentität, Exchange-Zeitzone und Kalender, konkrete am jeweiligen Scanzeitpunkt **bereits verfügbare** adjustierte Tagesbars sowie verifizierbare Zeit-/Hash-Nachweise; Ausnahmen explizit ausgeschlossen | unabhängiger Beleg-/Governance-Review, nicht bloßes Issue-Schließen |
| **B: CY-03 verifizierter v2-Release** | Neuer **versionierter** Freigabe- und Leservertrag, der nur **einzeln geprüfte** asset-/snapshotgebundene Reihen aus dem unveränderten v1-Roharchiv ableitet; externer Beleg pro Datenteilmenge, unveränderliche Hashbindung, Listing-/Währungs-/Frische-Coverage, Lag-Qualität und Negativmaske. Keine rückwirkende Umdeklaration als ehemals verfügbar | gesondertes Review und CI zur exakten Quellbeleg-Kette |
| **C: CY-05 echter L1-Run-Freeze** | verifizierte freigegebene Forschungsreihe, Vorab-Cutoff, deterministisch versiegelte Input-Hashes, Discovery-/Holdout-Auswahl, QM-C-/L1-Datenfamilie, A/B/C/D und C–B-Kontrast sowie Kosten **vor** Einsicht in Outcomes registrieren; ausreichend unabhängige Informationen/Supportregionen | explizite L1-/Methodik-Freigabe; **kein** Auto-Discovery |
| **D: CY-06** | L3 Discovery, L4 FDR/Effective-N/Overlaps/Blockintervalle, gepaarter C–B-Nettokostenvergleich auf gleicher Coverage, L5 PAT-Freeze und L6-Abhängigkeiten **nur wenn qualifiziert** | auch negatives oder nicht auswertbares Ergebnis behalten; kein nachträgliches Schwellentuning |
| **E: CY-07** | nach dem echten PAT-Freeze neue independent time-stamped L7-Captures; 5/20/40/60-Session-L8-Outcomes reifen lassen; geplante L9-Looks incl. QM-C5, L10 A/B nur bei entsprechender Evidenz | wissenschaftliche Falsifikation, Unterstützung oder ungelöste Prüfung – kein synthetischer Ersatz |
| **F: CY-08** | L12 exakter PAT-/L6-/L9-/L10-/Reviewerentscheid; nur Shadow, kostenbewusste L13 gepaarte Ablation, L14 QM- und Decay-Monitoring | keine produktive Action; weitere Integration späteres eigenes Paket |

**Nicht vergessen:** Zeit allein schafft weder Unabhängigkeit noch ausreichendes N. Mehrere tägliche Snapshots desselben Datums zählen nur als **ein** kanonischer 1/5/10obs-Tag. Erst fünf *qualifizierte* Vorgänger ermöglichen einen deskriptiven 5obs-Lag; für die vorregistrierte C–B-Hypothese mit 20 Sessions und ihre prospektive Bestätigung gelten zusätzliche **eigene** Reife- und Mindest-N-/Stützregionsbedingungen. Negative und ausgefallene Samples bleiben als Ausschlüsse sichtbar.

## 5. Freigabegrenzen für die heutige Arbeit

**Als technisch erledigt gilt nur:** Die Überprüfung ist ausführbar und kann Archivwachstum und fehlende Belege nach jedem erfolgreichen Scan und täglich auditieren, ohne die Forschungs-Gates aufzuweichen. Sie macht bereits implementierte Schutzklauseln sichtbar.

**Nicht erledigt / nächste inhaltliche Priorität:** externe PIT-/Listing-Evidenz #269 und danach der **formal geprüfte CY-03-v2-Forschungsfreigabevertrag**. Ein Freigabe-Button durch bloßes Ändern von `research_eligible` wäre ein wissenschaftlicher Fehler. Bis dahin: `BLOCKED`, auch wenn technisch mehr Lag-Zeilen entstehen.

**Wissenschaftlich kein bestätigter positiver Effekt:** `NO_PROSPECTIVE_EVIDENCE`, `NOT_EVALUATED`, kein Alpha-, Prognose- oder Handelssignal.
