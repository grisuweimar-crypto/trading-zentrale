# QM8 CAPA – W10-/Decision-Integrationsübergabe, 10.10.2026

## Fehler und nachgewiesene Ursache

- Der produktive unabhängige [QM8-Audit #38038351379](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/38038351379) scheiterte mit 6 × `w10_manifest_snapshot_mismatch`. Zu dem Zeitpunkt zeigte die aktuelle `daily_research.json` auf Snapshot `f875112c-cbdc-451d-b35c-1b3dea0ddc3f`, das persistierte W10-Manifest noch auf `94a23ea7-9a1b-4bf8-9ef0-d97065d3eed2`. Keine Quelle belegt daraus falsche Cycle-Kurswerte.
- Der unmittelbar auslösende [Decision Watch Integration Pipeline #38038337386](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/38038337386) hatte `readiness:success`, aber **`integrate:skipped`**. GitHub meldete den Gesamtworkflow trotzdem `success`. Die bisherige QM8-Workflow-Regel prüfte nur das **Gesamtworkflow-Conclusion**, nicht den tatsächlichen Publishing-Job. Dadurch wurde ein älteres, korrekt versiegeltes W10 fälschlich als *aktueller* Integrationsabschluss vorausgesetzt.
- Beim nachfolgend wirklich abgeschlossenen [Decision Integration #38038772742](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/38038772742) war `integrate:success`; W10 wurde für das aktuelle Research-Snapshot neu publiziert. Der erneute [QM8-Audit #38039156418](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/38039156418) war **SUCCESS**, einschließlich der unveränderten strengen Snapshot-/Hash-Prüfung.

## Korrektur und Kontrollgrenze

Der Workflow `.github/workflows/ba_qm8_scanner_e2e_audit.yml` bekommt jetzt einen expliziten **upstream-integration-Gate-Job**. Bei `workflow_run` der `Decision Watch Integration Pipeline` wird über die offiziell lesbare GitHub-Jobs-API die Ausführung von **genau einem** Job `integrate` geprüft:

- `integrate:completed/success` → volle unabhängige QM8-Suite ausführen; **keine** Lockerung der `W10 snapshot_id == daily_research snapshot_id`-Bedingung;
- `integrate:completed/skipped` → **DEFERRED**, nicht als technische aktuelle Snapshot-Abnahme deklarieren; strukturell spätere echte Integration bzw. manueller Audittermin notwendig;
- fehlende/doppelte Jobs, laufende Jobzustände, echte Fehler, fehlerhafte API-Antworten → fail-closed statt vermuteter Erfolg.

Bei Pull Requests und manuellen `workflow_dispatch`-Aufrufen wird die QM8-Suite weiterhin **vollständig** unabhängig ausgeführt. Die Bedingung `always()` verhindert, dass die für diese Ereignisse naturgemäß übersprungene Upstream-Prüfung die eigentliche CI aus Versehen überspringt. Kein Einfluss auf Scanner-Daten, Decision-Ausgaben, W10-Versiegelung, Portfolio oder Order-Logik.

**Abnahme:** Positive und negative synthetische Job-Fälle im Unit-Test, statischer Workflow-Triggertest, bestehende vollständige QM8-Real-Artifact-Regressionsprüfung und anschließende kontrollierte Betriebsbeobachtung. Keine Umdeutung eines tatsächlich übersprungenen Audits zu `PASS`.
