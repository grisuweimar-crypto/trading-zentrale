# Wertpapierdepot Watch — Prospektive Startgrenze

Die kanonische Depot-Watch-Orchestrierung wurde mit Merge-Commit
`dbb0ed42213b5e7a958ca76fd996a919795998a5` am
`2026-09-30T08:47:58+00:00` produktiv in `main` aufgenommen.

Für die neue append-only Phase-7A-Evidence-Historie gilt daher:

- `prospective_not_before = 2026-09-30T08:47:58+00:00`
- Scanner-Snapshots mit `generated_at` vor dieser Grenze dürfen nicht nachträglich in `decision_evidence_7a.jsonl` aufgenommen werden.
- Es gibt keinen rückwirkenden Bootstrap aus dem Scannerstand vom 29.09.2026 oder älter.
- Der erste zulässige Eintrag muss aus einem tatsächlich nach der Deployment-Grenze erzeugten autoritativen Scanner-Snapshot stammen.
- Fehlt ein solcher Snapshot, bleibt die Watch für den neuen Decision-Pfad fail-closed; Score, R-Code oder andere Scannerfelder dürfen das nicht ersetzen.

Der Gate liegt vor dem Aufbau der 7A-Pakete. Dadurch wird nicht erst beim Archivieren, sondern bereits vor jeder vermeintlich prospektiven Rekonstruktion eines alten Snapshots abgebrochen.

Diese Grenze ist ein Integritäts-/Provenienzschutz. Sie ist keine empirische Promotion von Phase 7, Hysterese, Portfolio Action, Elliott oder Phase 8.
