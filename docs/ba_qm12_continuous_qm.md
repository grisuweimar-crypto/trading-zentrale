# BA-QM12 – Konsolidierung & Produktiver QM-Betrieb

BA-QM12 macht Qualitätsmanagement zu einem dauerhaften Betriebsbestandteil von Scanner-vNext. Der Arbeitsschritt ist technisch abschließbar; der resultierende Continuous-QM-Betrieb bleibt danach aktiv.

## Dauerhaft gebundene Kontrollen

Der BA-QM12-Orchestrator bindet die im Masterplan geforderten Kontrollfamilien: QM-A Governance, QM-H CAPA, QM-I Lineage, relevante Negative Controls, Regressionstests, Coverage Monitoring, Drift Monitoring, Calibration Monitoring und Production Health.

Vorhandene Kontrolllogik wird nicht dupliziert. BA-QM12 prüft ihre fortbestehende Aktivität und führt ihre relevanten Regressionen in einem täglichen Continuous-QM-Lauf zusammen.

## Aktuelles Snapshot-Monitoring

Coverage wird aus der kanonischen history_metadata.json gelesen. Calibration muss an denselben aktuellen Snapshot gebunden sein. Der öffentliche Watch-Runtime muss denselben Snapshot verwenden, darf keine fehlenden aktuellen Pakete enthalten und keine privaten Positionsdaten persistieren.

Drift wird zunächst deskriptiv überwacht. Ohne vorregistrierten Schwellenwert erzeugt BA-QM12 bewusst keinen nachträglich erfundenen PASS/FAIL-Grenzwert. Ein späterer Drift-Schwellenwert muss als eigene, vorab definierte QM-Regel eingeführt und validiert werden.

## Pflichtkette für neue Module

Neue Module müssen künftig diese Reihenfolge explizit durchlaufen:

Design → QM Contract → Test → Research → Validation → Promotion → Production → Continuous QM

Engineering Completion ist keine empirische Validation. Validation ist keine Promotion. Promotion ist nicht automatisch Production. Production ohne Continuous QM ist nicht zulässig.

## Bestehende Sperren

BA-QM12 darf bestehende Evidenzsperren nicht freigeben. Insbesondere bleibt das Phase-1A-Lag-1-Finding PROMOTION_BLOCKED. W8 bleibt nicht promotion-eligible, bis seine separat definierte prospective-unspent Evidenzanforderung erfüllt wurde.

## Betrieb

.github/workflows/ba_qm12_continuous_qm.yml läuft bei relevanten Änderungen, manuell und täglich. Ein Fehler ist ein QM-Signal und kein Anlass, die Prüfung zu umgehen oder fehlende Evidenz als neutral zu behandeln.


## Masterplan-Abschluss-Gate

BA-QM12 unterscheidet ausdrücklich zwischen abgeschlossener QM-Bauphase und
vollständig erfülltem Masterplan-Endzustand. Ein vollständiger Abschluss darf
nur behauptet werden, wenn der ausführbare Residual-Monitor keine blockierenden
planinternen Restpunkte mehr meldet.

Der Monitor bindet W6 Elliott-Lineage, Phase-1A Lag-1 CAPA-Wirksamkeit, W8,
BA-QM2 externe historische Integrität, BA-QM6/7 empirische Validierung und die
Decision-Layer-Promotion. Phase 8 External Evidence bleibt als separater
quarantänisierter Pfad sichtbar und wird nicht allein wegen fehlender Promotion
zum Blocker des kanonischen Decision-Pfads.

Für neue prospektive Elliott-6H-Captures ist W6 explizit über
Raw Source -> Feature -> 6H -> W6 Context -> Portfolio Action nachvollziehbar.
Stage-4/6G-Validierung und ein eigener Unsicherheitszustand sind Teil der
Lineage. Alte/manuelle Quellen ohne exakte Provenienz bleiben fail-closed
lineage-incomplete; ihre Herkunft wird nicht geraten.

Die durch PR #187 reparierte Trennung zwischen historischem BA-QM10/11-
Abschlussbeleg und aktueller Live-Runtime bleibt davon unberührt. BA-QM12 prüft
den aktuellen Snapshot, die Runtime-Identität und die aktuelle Runtime-Coverage
weiterhin dynamisch.
