# Trading-Zentrale — Operating Playbook

*Stand: 09.10.2026 · produktive Scanner-vNext-/Research-Architektur*

Dieses Handbuch beschreibt **vorhandene** Abläufe, Dateiquellen und Prüfungen im Repository. Frühere Anleitungen zu automatischem Rebalancing, festen Kaufquoten oder nicht mehr vorhandenen Skripten sind **keine** gültigen Betriebsanweisungen. Es werden weder Orders ausgeführt noch Forschungsbefunde automatisch zur Handelsfreigabe befördert.

## 1. Was der Scanner tatsächlich liefert

Der Scanner verfolgt Aktien und Kryptowährungen und veröffentlicht tägliche Research-Artefakte. Analyse, Interpretation, Risiko, Confidence und Portfolioentscheidung sind getrennte Ebenen. Ein hoher Scannerwert ist **kein** alleinstehendes Kauf- oder Nachkaufsignal.

- **Score: 0–100** (nicht 0–200): aktuelle Scanner-Kennzahl; historische Score-Skalen und Versionswechsel dürfen nicht nachträglich vereinheitlicht werden.
- **Opportunity, Risk und Confidence:** getrennt zu interpretierende Kennzahlen. Keine willkürlichen HIGH-/LOW-Grenzen oder garantierten Gewinnwahrscheinlichkeiten aus alten Playbooks ableiten.
- **RS3M, Trend200, Cycle und weitere Felder:** nur im Rahmen der jeweils dokumentierten Berechnung und gültigen Scanner-Version auswerten.
- **Originalwährungen und Point-in-Time:** keine stillschweigende Währungsumrechnung oder nachträgliche Ergänzung historischer Scannerzustände mit heutigen Daten.
- **Fehlende Daten:** explizit als fehlend führen; nicht mit plausibel wirkenden Kursen, Fundamentaldaten oder Scores auffüllen.

## 2. Verantwortliche Workflows

| Zweck | Produktiver Dateipfad |
| --- | --- |
| Scanner und gesicherte Tagespublikation | `.github/workflows/run_scanner.yml` |
| Decision-/W10-Integrationskette | `.github/workflows/decision_watch_pipeline.yml` |
| Öffentliche Watch-Runtime | `.github/workflows/decision_watch_runtime.yml` |
| Öffentliche Symbolansichten | `.github/workflows/decision_watch_symbol_views.yml` |
| Kontinuierliche QM-Kontrolle | `.github/workflows/ba_qm12_continuous_qm.yml` |
| QM-H-Findings, CAPA und Regressionen | `.github/workflows/qm_h_capa.yml` |

Der Scanner-Autopilot besitzt im Workflow nominelle **Europe/Berlin**-Zeitfenster 16:07, 17:07 sowie Nachholtermine 18:37 und 20:37 Uhr. GitHub-Actions-Ausführung und Veröffentlichung können zeitlich verzögert sein. `scripts/autorun_state.py` soll bereits korrekt veröffentlichte Tagesläufe erkennen; mehrfach konfigurierte Trigger sind **keine** Aufforderung zu Mehrfachpublikationen.

Ein manueller `workflow_dispatch` kann bei Bedarf einen Workflow auslösen; die regulären Guards, Snapshot-Kohärenz und bestehenden Produktionssperren gelten unverändert.

## 3. Verbindliche produktive Artefakte

Unter `artifacts/research/`:

| Datei | Bedeutung |
| --- | --- |
| `history_metadata.json` | Scanner-Publikationsidentität, Validierung und Datenabdeckung |
| `latest_scanner.csv` | Vollständige aktuelle Scannerbeobachtungen |
| `daily_research.json` | Kompakte Research-Auswertung zum veröffentlichten Snapshot |
| `probability_calibration_2.json` | Phase-2-Ergebnis mit expliziter Quell-Snapshot-Bindung |
| `decision_snapshot_w10.json` | W10-Abschluss mit `status=sealed` |
| `watch_runtime/manifest.json` | Identität, Shard-Zuordnung und Projektion der öffentlichen Watch-Runtime |
| `watch_runtime/public_long_reference.json` | Öffentliche Long-Referenz, ohne private Positionen |

**Frische-/Kohärenzregel:** `history_metadata.json`, `daily_research.json`, Phase 2, die versiegelte W10-Entscheidung und die Watch-Runtime müssen sich auf ihren jeweils vorgesehenen, validen Quell-Snapshot beziehen. Ein alter oder nicht kohärenter Stand wird **nicht** durch frühere Daten als „aktuell“ ausgegeben. `history_recent.csv` ist ein historischer Datenträger, aber kein Ersatz für den aktuellen Publikationsmarker.

## 4. Täglicher Betriebscheck

1. Im GitHub-Actions-Verlauf prüfen, ob **Scanner_vNext Autopilot** erfolgreich einen gültigen Tagesstand veröffentlicht hat. Bei Fehlstart oder `incomplete` ausdrücklich Ursache und letzte gültige Veröffentlichung unterscheiden.
2. `artifacts/research/history_metadata.json` prüfen: `snapshot_id`, `as_of`, `latest_run_complete`, `validation.status`, `validation.required_symbol_count`, `validation.symbol_count`, `validation.numeric_score_count` und `validation.policy.min_score_ratio`.
3. Für Score-Abdeckung kontrollieren, dass `numeric_score_count >= ceil(symbol_count * min_score_ratio)`. Die maßgebliche Mindestquote stammt **aus dem publizierten Snapshot**; BA-QM12 erzwingt diese Prüfung inzwischen selbst. Preisabdeckung davon getrennt ausweisen.
4. Nach erfolgreicher Kalibrierung und Decision-Kette `decision_snapshot_w10.json` (versiegelt) und `watch_runtime/manifest.json` (passende Identität und Projektion) prüfen. Bei verzögerten Upstream-Läufen nicht von einer bereits aktuellen Watch ausgehen.
5. Die Watch-Runtime verteilt Symbolhistorien auf Shards. **2.000.000 Bytes pro Shard** sind die Transportobergrenze; bei weniger als **20 % Reserve** besteht ein gesonderter QM-Prüfbedarf. Eine erfolgreiche Veröffentlichung ersetzt nicht die Kapazitätsprüfung.
6. Wertpapierdepot-Watch und Portfolioempfehlungen erst auf einem kohärenten Scanner-/Decision-Stand betrachten. Private Depotpositionen gehören **nicht** in öffentliche Runtime-Dateien. Entscheidungen und Orderausführung sind eigenständige Schritte.

## 5. Kontrollen und sichere Diagnosebefehle

Nur auf einem geeigneten Repository-Checkout und mit installierten Projektabhängigkeiten ausführen. Diese Beispiele sind Prüfungen, **keine Trading-Befehle**:

```bash
# Vorhandene Research-Publikation gegen ihren Vertrag prüfen
python scripts/generate_research_views.py --validate-only

# Bestehende Watchlist/Vertragsstruktur prüfen
python scripts/validate_contract.py

# Produktiven BA-QM12-Stand und dessen Regression prüfen
python -m pytest -q tests/test_ba_qm12_continuous_qm.py

# QM-H-Ledger und CAPA-Kontrollen prüfen
python -m pytest -q tests/test_qm_h_capa.py
```

Die früher im Playbook genannten Programme zum Health Report, zur „Calibration Light“, zum automatischen Rebalancing und zum Telegram-Test sind im aktuellen Repository **nicht** als ausführbare Skripte vorhanden. Ihre ehemaligen Beispielbefehle sind daher nicht mehr anzuwenden. Kein manueller Ersatz darf stillschweigend einen produktiven Scannerlauf, eine historische Quelle oder einen Handelsauftrag verändern.

## 6. Fehlerbehandlung und Forschungssperren

- **Scanner unvollständig / Daten fehlen:** den `history_metadata.json`-Status, GitHub-Job-Log und die Quell-Snapshot-ID prüfen. Kein Umschalten auf alte Daten unter neuer Identität.
- **Score-Abdeckung unter Mindestquote:** BA-QM12 muss fail-closed abbrechen; Ursachen in Daten und Validierung suchen, nicht die Schwelle rückwirkend passend machen.
- **W10/Watch nicht kohärent:** Upstream-Phase, W10-Seal und Runtime-Publikation getrennt prüfen. Die Watch darf nicht aus gemischten Snapshots aufgebaut werden.
- **Runtime-Shards nahe am 2-MB-Limit:** Kapazitätsstatus und Headroom prüfen; niemals einfach die Transportgrenze umgehen.
- **QM-/CAPA-Befund:** Findings im hashverketteten QM-H-Ledger mit separatem Nachweis von Umsetzung, Wirksamkeit und Abschluss behandeln. Bestehende Forschungs- und Promotion-Sperren werden nicht durch grüne Engineering-Tests aufgehoben.
- **Investitionsentscheidung:** Research-Empfehlungen und tatsächliche Depotaktionen voneinander trennen; keine unbelegte Trefferquote, Renditegarantie oder automatische Rebalancing-Regel aus historischen Playbooks übernehmen.

## 7. Änderungsdisziplin

Änderungen an Score- oder Decision-Logik, Risikodefinitionen, Quellen, historischen Daten, Schwellen und Portfolio-Aktionen benötigen eine jeweils eigenständige fachliche Prüfung und die vorgesehenen Point-in-Time-, Research- und QM-Gates. Ein Dokumentationsfix hat **keine** solche Freigabewirkung.

Dieses Playbook ist eine Betriebsorientierung für den aktuellen Repository-Zustand; die versionierten Workflows, Contracts, Artefakte und deren tatsächliche Prüfresultate bleiben maßgeblich.
