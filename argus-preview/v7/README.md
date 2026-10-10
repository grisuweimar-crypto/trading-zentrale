# ARGUS PANOPTES — V7 A/B (lesende Darstellung)

Stand: 2026-10-10. Die produktive Trading-Zentrale bleibt unverändert.

## Nutzeroberfläche

- `argus-preview/index.html`: gemeinsame ARGUS-Vorschau (V4–V6 erhalten), Menüpunkt `Forschungsstand`.
- `argus-preview/v7/research-lab.js`: V7-A — L14-Monitor, neun Stationen, Originalstatuscodes und deutschsprachige **Erklärtexte**.
- `argus-preview/v7/qm-monitor.js`: V7-B — QM-H-Befunde/CAPA-Ereignisse und separat der historische QM-J-Einzelbericht.
- `argus-preview/v7/index.html`: zusätzliche eigenständige Prüfansicht; keine zweite Forschungs-Engine.

## Autoritative Quellen und Abgrenzungen

1. `artifacts/research/history_metadata.json`: Scanner-Publikationsmarker und Snapshot-ID.
2. `artifacts/research/pattern_discovery/operations/latest.json` und verweisendes unveränderliches `cycles/{cycle_hash}.json`: L14-Betriebsstatus, neun originale Arbeitsstationen, registrierte Zähler und QM-C5/BA-QM12-Status. Keine eigenständig berechneten Forschungsratings.
3. `artifacts/research/qm/qm_h_capa_ledger.jsonl`: append-only QM-H-Befundereignisse. Die Oberfläche **rekonstruiert ausschließlich den zuletzt protokollierten Status je Finding** aus unveränderten `OPEN`/`FINDING_TRANSITION`-Ereignissen. Das ist eine Darstellung einer Ereignisliste, keine neue QM- oder Evidenzentscheidung. Event-Sequenz, Statuskette und Hash-Verweise werden strukturell geprüft; **keine SHA-256-Neuverifikation**.
4. `artifacts/research/qm/qm_j_decision_e2e_falsification.json`: separater historischer QM-J-Einzelbericht, nicht als aktueller Scannerstatus ausgegeben.
5. `docs/pattern_discovery/l14_continuous_operations.md` und `docs/qm_h_defect_near_miss_capa.md`: Bedeutung der im UI erläuterten Stati.

QM-H-CAPA-Abschluss (`CLOSED`) ist keine empirische Pattern-Bestätigung. L14-`PASS` betrifft den jeweiligen Kontrollschritt, nicht die Renditeprognose. QM-H und L14 können verschiedene Datenstände haben und werden nicht zu einem neuen Gesamtstatus zusammengeführt. Andere QM-A-, QM-B-, QM-C- oder Research-Blocker werden durch diese Ansichten nicht pauschal aufgehoben.

## Implementierungs- und Teststatus

- Keine Änderungen an Scannerberechnung, Pattern-Discovery-/QM-Ausführung, Scoring, Decision oder Handelsentscheidung.
- V7-A mit veröffentlichtem L14-Zyklus in simulierter Browserumgebung geprüft: neun Stationen, Originalstatus und deutschsprachige Erklärungen; inkonsistenter Zyklus wird abgewiesen.
- V7-B mit dem veröffentlichten QM-H-Ledger und QM-J-JSON in simulierter Browserumgebung geprüft: neun Findings, 61 Events (Stand 09.10.2026); korrumpierte Ereignisse werden fail-closed nicht angezeigt.
- Beide JavaScript-Dateien syntaktisch geprüft; die vorhandene V6-Oberfläche bis auf die neue Einbindung unverändert.
- **Echte Browser-Abnahme der V7-A/B-Erweiterung steht noch aus.** Frühere Browserbestätigung für V7-Grundversion ersetzt sie nicht.

## Noch offen (V7-C/D)

- Einzelmusterbibliothek nur dann auf echte veröffentlichte L5–L10-Register binden, wenn vollständige semantische Verträge und Register vorliegen.
- Responsive Prüfung und Fehlerfalltests auf Smartphone, ZenBook und Desktop; anschließend Nutzerabnahme.
