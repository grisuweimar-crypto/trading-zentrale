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
