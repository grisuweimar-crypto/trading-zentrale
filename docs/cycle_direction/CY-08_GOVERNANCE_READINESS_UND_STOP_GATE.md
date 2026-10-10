# CYCLE-DIR — CY-08 L12/L13/L14 Governance-Istaufnahme, Readiness-Gate und Stop-Entscheid

**Plan-ID:** `CYCLE-DIR-2026-10-09-v1` · **Paket:** ausschließlich CY-08  
**Prüfdatum:** 10.10.2026 (Europe/Berlin)  
**Geprüfter `main`-HEAD:** `29b02e456fbb880bb074ceda0c287a993d6894e3`  
**Arbeitsbranch:** `research/cycle-dir-cy08-governance-readiness-20261010`  
**Entscheidung:** **BLOCKIERT / NICHT_FREIGEGEBEN** für realen Pattern-Review, echte L13-Ablation oder Betrieb einer CY-08-Cycle-Pattern-Admittance. **Read-only Governance-Inventur, automatisierter Vorprüfer und gezielte CI-Regression** sind die zulässige Teilleistung. **Kein empirisches Ergebnis.**

## 1. Verbindlicher Auftrag, vorhandene Architektur

Masterplan CY-08 verlangt:

1. **L12**: nur L9-`SUPPORTED`, L10 A/B, exakte L5-Identität (ID/Version/Spec-Hash), L6-Abhängigkeitsbehandlung und **separater expliziter Reviewerentscheid**;
2. Zulässige Modi ausschließlich `ANNOTATION_ONLY` (keine Richtungsautorität) und `SHADOW_CHALLENGER` (`SHADOW_ONLY`); keine produktive Handlungsautorität;
3. **L13**: gleiche Assets, Zeitpunkte, Horizonte, Targets und Baselines; gepaarte Ablation `EXISTING_TIMING` vs. `EXISTING_TIMING_PLUS_PROMOTED_PATTERNS`, einschließlich **Kosten** und Abhängigkeiten;
4. **L14**: geregelte, beobachtbare Operations: Qualitätsfehler, Coverage, Frische, Drift/Decay und nur *explizit beantragte*, neu präregistrierte außerordentliche Discovery;
5. Eine spätere Wirkung jenseits Shadow erfordert ein **eigenes zukünftiges Vorhaben**, nicht CY-08.

Die generischen L12/L13/L14-Implementierungen sind bereits vorhanden:

| Ebene | Vorhandene Quelle und Schutz | Beurteilung für CY-08 |
| --- | --- | --- |
| L12 | `promotion_gate.py`, `promotion_registry.py`, `l12_promotion_gate_v1.json`, `test_l12_promotion_gate.py` | Explizite hashgebundene Review-/Registry- und Rollback-Logik existiert; Rating allein und veraltete Bindungen reichen nicht; **keine Cycle-Zulassung** nachgewiesen |
| L13 | `challenger_integration.py`, `l13_decision_challenger_v1.json`, `test_l13_decision_challenger.py` | Nur aktuelle L12-`ADMITTED`-/`CURRENT`-Zulassung mit `SHADOW_ONLY`, same-snapshot/horizon und korrelationsbewusster Clusterung; **kein aktueller CYCLE-DIR-Challenger** |
| L14 | `continuous_operations.py`, `l14_continuous_operations_v1.json`, `test_l14_continuous_operations.py` | Allgemeiner, research-only Scheduler und QM-/Decay-Audit vorhanden; keine automatische Promotion oder unregistrierte Discovery |

**Ursprünglich identifizierte konkrete Lücke: generische L13-Kosten.** In der geprüften generischen L13-Ablation werden Directional Hit Rate Lift und Mean Aligned Outcome Lift mit Moving-Block-Unsicherheit ausgewertet; im Code-/v1-Vertrag existiert **keine eigene Netto-Kosten-Ablation**. Der generische L13-v1-Vertrag bleibt absichtlich unverändert; die CY-08-Pflicht „inkl. Kosten“ wird als **separater versionierter, kostenbewusster Shadow-Paarvergleich** nachgerüstet. Diese technische Umsetzung ist nicht mit einer echten, bereits durchgeführten CYCLE-DIR-Ablation zu verwechseln. Kostenlose Brutto-Outcomes dürfen nicht als *netto nach Transaktionskosten* bezeichnet werden. Das vorregistrierte CY-05-Methodendesign nennt **20 bps Roundtrip**, Sensitivität **10/50 bps** als Modellannahmen, nicht als real gemessene Orderkosten. Vor einer realen L13-Auswertung sind außerdem die Kostenanwendung auf beide tatsächlich unterschiedlichen Shadow-Arme, deren hypothetische Umschichtungen und identische PIT-/Coverage-Bindung explizit zu spezifizieren und versioniert zu testen. **Keine Schwellenwahl anhand künftiger Ergebnisse.**

## 2. Echte Datengrundlage und Hard-Gates am 10.10.2026

| Prüfung | Nachweis aus `main` | Konsequenz |
| --- | --- | --- |
| CY-02/CY-03 unabhängiger Publisher-/Quote-PIT-/Listing-Nachweis | [Issue #269](https://github.com/grisuweimar-crypto/trading-zentrale/issues/269) **OPEN**; `WATCHLIST_DECLARED_ONLY`, `SESSION_DATE_CUTOFF_ONLY`, Zeitzone unbestätigt | Research-Zulassung **gesperrt** |
| CY-03 | `artifacts/cycle_history/manifest.json`: **1** Snapshot; **215** Zeilen; **209** intern provisional replaybar; **6** ausgeschlossen; **0** research-eligible | Kein zulässiges Discovery- oder Prospektiv-Sample |
| CY-03 Lag-Kette | `provisional_lags` 1/5/10 = **0/0/0** | Keine geprüfte 5obs-Richtungskondition |
| CY-05 | `cy05_preregistered_design_v1.json`: `frozen_l1_run_manifest_created=false`, `empirical_results_computed=false`, `execution_allowed=false` | Kein echter L1-Freeze; kein erforschtes Pattern |
| CY-06 | `CYCLE_DIR_DISCOVERY_REPORT.md`: **BLOCKIERT**; kein qualifizierter L5-Freeze / L6-Graph | L12 kann keinen Cycle-PAT aufnehmen |
| CY-07 | [PR #287](https://github.com/grisuweimar-crypto/trading-zentrale/pull/287) zum Prospektiv-Stop-Gate **offen, nicht gemergt**; fachlich keine L7–L11-Cycle-Evidence | Kein L9-`SUPPORTED`, kein gültiges A/B-L10-Rating |
| Publizierte Pattern-Artefakte | `artifacts/research/pattern_discovery/` enthält nur `README.md` und `operations/` | Kein realer Pattern-Promotion-Review, keine gepaarte Cycle-Ablation |
| Generische L14-Publikation | `operations/latest.json` mit `READY_WITH_WORK`, 0 frozen Patterns, 0 Captures, 0 Matured Outcomes, 0 L9-Looks und `DUE_PREREGISTRATION` für **allgemeines initiales** Lab | **Keine** CYCLE-DIR-Forschungsfreigabe; L14-„ready“ bedeutet nicht CY-08-`ADMITTED` |

Die bei L12/L13 bereits existierenden synthetischen Testfixtures sind **kein reales prospektives CYCLE-DIR-Pattern**. Diese CY-08-Istaufnahme enthält keinen Handels-, Alpha- oder Performancenachweis.

## 3. Umgesetzte, sichere Teilleistungen im CY-08-Branch

**3.1 Nicht mutierender Vorprüfer**: `scripts/audit_cycle_cy08.py`

- Liest ausschließlich vorhandene Verträge `L12/L13/L14`, das CY-05-Design und das CY-03-Archivmanifest. **Keine** produktiven oder wissenschaftlichen Artefakte werden erzeugt oder verändert.
- Prüft die research-only/kein-Execution-Boundary; L12-`SUPPORTED`/A-B/Reviewer/Modi, L13-gegen-`ADMITTED`+`CURRENT`/Paarbildung, L14 ohne Auto-Promotion/Auto-Discovery.
- Dokumentiert als **aktuelle Blocker**: `CY05_L1_RUN_FREEZE_NOT_DOCUMENTED`, `CY03_NO_RESEARCH_ELIGIBLE_OBSERVATIONS`, `CY03_NO_5OBS_DIRECTION_HISTORY`, `L13_NET_COST_ABLATION_NOT_IMPLEMENTED` (durch den separaten Vertrag und das Netto-Modul **technisch behoben**; noch keine empirische Auswertung).
- Liefert `BLOCKED` oder höchstens `REVIEW_REQUIRED`. **Niemals** `ADMITTED`, `SUPPORTED`, produktive Freigabe oder Simulation einer echten L12/L13-Entscheidung. Selbst wenn einzelne Archivzahlen steigen, werden dadurch keine Zertifikate, Hashbindungen oder Reviewbeschlüsse ersetzt.
- Das Ergebnis ist eine **technische Vorprüfung**, **keine** umfassende L12-Eignungsverifikation. Insbesondere testet der Script nicht selbst die tatsächlichen L5-/L9-/L10-/L6-Evidenzketten; hierfür bleiben die bestehenden Module und menschlicher Review maßgeblich.

**3.2 Vier kontrollierte Negativ-/Sicherheits-Tests**: `tests/test_cy08_readiness.py`

- niemals Admittance, Shadow-Auswertung oder produktive Decision-Mutation;
- unsicherer L12-Contract scheitert geschlossen;
- künstlich erhöhte Archiv-Zahlen umgehen L1- und Kosten-Sperre nicht;
- ein allgemeiner L14-`READY_WITH_WORK`-Beleg ist keine Pattern-Zulassung.

**3.3 Eigener Workflow**: `.github/workflows/cycle_dir_cy08.yml`

- läuft auf PR-Dateiänderungen/auf `workflow_dispatch`, **nur mit `contents: read`**;
- Regression der generischen L12-/L13-/L14-Tests plus eigene Tests;
- realer *read-only* Vorprüfer auf dem PR-Checkout, mit maschinenprüfbaren No-Write-/No-Admission-Feldern;
- **kein** automatischer Promotion-/L13-/Discovery-/Execution-Lauf.

**Ergänzung: Technischer Kostenabschluss auf diesem Branch**\n\n- **Versionierte Kostenannahmen:** `configs/cycle_direction/cy08_net_cost_v1.json` fixiert **10/20/50 Basispunkte Roundtrip**, 20 Basispunkte als Primärfall und 50 Basispunkte als Stressfall. Diese Beträge sind modellierte Forschungsannahmen, **keine** beobachteten Transaktionskosten oder Garantien realisierbarer Leerverkäufe.\n- **Separates L13-Netto-Modul:** `src/scanner/research/pattern_discovery/cycle_cy08_net_ablation.py` vergleicht den eingefrorenen bestehenden Timing-Zustand mit dem konservativ **fusionierten** L13-Shadow-Zustand auf demselben verifizierten gereiften L8-Outcome. `conflicted` und `insufficient_evidence` führen zu **0 hypothetischer Exponierung**, nicht zur Richtungsentscheidung. Ein aktiver Signal-Arm bezahlt pro isolierter Beobachtung einen vollen modellierten Roundtrip; überlappende Forward-Fenster werden nicht als unabhängige Trades oder tatsächlicher Portfolioverlauf ausgegeben.\n- **Pairing/PIT:** Eindeutige Asset/Snapshot/Horizont/Target/Baseline-Schlüssel, aktuelle L13-Trace- und L8-Outcome-Hashvalidierung, Claim-Hashes, adjustierte Originalwährungs-Bars, exakt identische Referenz und maturierter Zielfenstertag; keine Binärklassifikationszahl als kontinuierliche Rendite. Mixed Scopes, Double Counts und inkonsistente Shadow-Fusion scheitern geschlossen.\n- **Statistik:** Zeitblock-Bootstrap gruppiert die gepaarten Events nach Beobachtungstag; Mindest-N=30, Richtungs-/Exponierungs-Differenz-N=10 und zwei Supportregionen (2× Zielhorizont als Proxy). Ein **NET_INCREMENTAL_VALUE_CANDIDATE** ist nur bei positivem mittlerem Vorteil und **positiver unterer 95%-Block-Bootstrap-Grenze sowohl im 20- als auch im 50-bps-Szenario** möglich; sonst `NO_NET_INCREMENTAL_VALUE` oder `INSUFFICIENT_SUPPORT`. Es bleibt **immer Forschung**, keine L12-Zulassung.\n- **Regression:** `tests/test_cy08_net_ablation.py` mit acht Negativ-/Kosten-/Bootstrap-/Paarprüfungen; auf Branch-CI zusammen mit L12–L14 und Readiness-Suite ausgeführt. Die simulierten Kostenbeispiele sind **keine gemessene empirische CYCLE-DIR-Evidenz**.\n\n**Wichtig:** Diese Zusatzprüfung ersetzt nicht die generische L13-Brutto-Diagnostik. Sie gibt auch keine Transaktionsdaten, tatsächliche Handelskosten oder offene Positionen vor und revalidiert nicht selbst eine aktuelle L12-Reviewerzulassung. Das bleibt vor **jedem tatsächlichen CY-08-Einsatz** separat zwingend. Ergebnisse müssen als Forschungs-/Modell-Netto-Proxies bezeichnet werden, nicht als ausgeführte Renditen.\n\n**Testgrenze:** Die neuen 4 Tests wurden lokal mit **synthetisch kontrollierten Contract-/Archivfixtures** ausgeführt: **4/4 PASS**; zusätzlich `py_compile` für Vorprüfer und Tests. Die vorhandenen GitHub-Repo-Regressionspakete und der echte Repo-Checkout werden durch den neuen Workflow geprüft; **CI-PASS erst nach entsprechendem GitHub-Nachweis behaupten**.

## 4. CY-08 Abnahmematrix und Wiedereinstieg

| Abnahme aus Masterplan | Status |
| --- | --- |
| Basis-HEAD, bestehende L12–L14-Verträge und L14-Receipt erfasst | **ERFÜLLT (Inventur)** |
| Research-only Grenzen und existierende Negativtests identifiziert | **ERFÜLLT (Inventur)** |
| Eigener fail-closed CY-08-Vorprüfer, Regression, getrennter CI-Workflow | **TECHNISCH VORBEREITET, Merge/CI gesondert prüfen** |
| Echte CY-07-Bestätigung + L12-Eignung (L5/L6/L9/L10) | **NICHT ERFÜLLT** |
| Ausdrückliche Pattern-/Version-/Hash-genaue Reviewerentscheidung | **NICHT ANWENDBAR** ohne zulässigen Kandidaten; **nicht vorweggenommen** |
| Nur `ANNOTATION_ONLY`/`SHADOW_CHALLENGER` zugelassen | **Vertrag vorhanden**, keine reale Cycle-Admittance |
| L13 gleiche Kohorte/Horizont und Abhängigkeitsgraph | **Generisch implementiert**, kein Cycle-Lauf |
| L13 Netto-Kosten, Coverage, robuste gepaarte Ablation | **FERTIG_TECHNISCH**: separater vorregistrierter 10/20/50-bps-Paarvergleich und Negativtests; **NICHT AUSGEFÜHRT** auf echten Cycle-L7/L8-Daten |
| L14 Qualitäts-/Coverage-/Freshness-/Decay-Audit für reale Cycle-Muster | **Allgemeine Infrastruktur**, **noch kein Cycle-Muster** im Betrieb |
| Kein automatisches Auto-Discovery/Produktions-Upgrade | **Durch bestehende research-only Verträge und Scope des Vorprüfers geschützt**; aktuelle PR-Änderungen ändern keine produktive Logik |
| Prüfbarer CY-08-Status `BLOCKED`/`DEFERRED`/`REJECTED`/zugelassener Modus | **Aktuell nur `BLOCKED`**; andere Status nur nach gültigen Reviews |

### Sperren, Reihenfolge und nächste Maßnahmen

1. **Externes Daten-/PIT-Gate [#269](https://github.com/grisuweimar-crypto/trading-zentrale/issues/269)** und echte mehrtägige CY-03-Forschungshistorie inkl. freigegebener 5obs-Lags herstellen (eigener Vorgänger-Scope).
2. **CY-06**: echter daten-/hashgebundener L1-Run, danach zulässige L3–L6-Auswertung und ein explizit geeigneter L5-Pattern-Kandidat; ein negatives Ergebnis wäre ebenfalls korrekt und kann zu **keiner Promotion** führen.
3. **CY-07**: Post-Freeze L7 Capture, vollständig gereifte L8 Outcomes, L9 QM-C-Bestätigung und L10 A/B Rating für **dieselbe Pattern-ID/-Version/-Spec-Hash**, mit gültigem L6-Kontext.
4. **CY-08 Netto-Kosten-Implementierung technisch abgeschlossen:** gesonderter vorregistrierter 10/20/50-bps-L13-Shadow-Paarvergleich mit eigener Regression. Bei erster echten Nutzung vorher L12-aktuelle Zulassung erneut prüfen und Kosten-/Shortbarkeit-Szenarien als Modell kenntlich machen. Nicht rückwirkend auf Legacy-Outcomes anwenden.
5. **Erst dann** separaten L12-Review eröffnen; nur nach Zulassung die passende L13-Shadow-Auswertung, L14 Cycle-spezifische Monitoring-/Decay-Audits und dokumentierte, ggf. negative Reviewentscheidung. Über mehr als Shadow entscheidet CY-08 nicht.

**Fachstatus: BLOCKIERT / NICHT_FREIGEGEBEN.** Ein grüner Governance-Check belegt **nur technische Schutzbedingungen**, keinen Mehrwert der Zyklusrichtung.

## 5. CYCLE-DIR-Übergabe

- **Datum/Zeitzone:** 10.10.2026, Europe/Berlin
- **Paket:** CY-08 – L12–L14 Readiness/Governance-Vorbereitung
- **Basis:** `main` `29b02e456fbb880bb074ceda0c287a993d6894e3`
- **Branch:** `research/cycle-dir-cy08-governance-readiness-20261010`
- **Technisch:** Read-only Preflight, Tests und PR-CI ergänzt; L12–L14 generisch bereits implementiert
- **Fachlich/empirisch:** **NICHT FERTIG / NICHT AUSGEWERTET**
- **Blocker:** #269, CY-03-Mehrtag/Research, CY-06-L5, CY-07 prospektives L9/L10 und ein gültiger L12-Reviewerentscheid. **Die CY-08 Netto-Kosten-Implementierung ist technisch erledigt**, reale gepaarte Outcome-Evidenz fehlt.
- **Absichtlich nicht geändert:** Scanner Score, Selection/Timing, Universal Stance, Decision, Portfolio Action, Depot Watch, Execution, bestehende Forschungssnapshots und L12–L14-Kern-Engine
- **Exakt nächster Schritt:** PR-CI inklusive acht Netto-Kostentests prüfen und erst bei grünem Scope-Merge technisch schließen. Reale Cycle-L12/L13-Ergebnisse bleiben bis #269/CY-03/CY-06/CY-07 gesperrt.
