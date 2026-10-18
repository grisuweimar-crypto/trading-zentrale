# W10/W11-Abnahme für abgeschlossene Tageskerzen – Stand 17.10.2026

## Verifizierte produktive W10-W11-Linie (ohne Umstellung)
- In `main` sind `artifacts/research/daily_research.json`, `artifacts/research/decision_snapshot_w10.json` und `artifacts/research/watch_runtime/manifest.json` zum selben Snapshot `6575ff0f-33b4-4fd9-8fdd-1d40c9dbefdd`, as_of 2026-10-17 vorhanden.
- W10 `status=sealed`, `sealed_at=2026-10-17T17:23:10.540678+00:00`.
- Das Watch-Runtime-Manifest enthält 220 Instrumente und eine konsistente Snapshot-ID.
- Echter read-only W11-Lauf mit aktuellem veröffentlichten W10 und **rein synthetischem** 1-Stück-Testdepot: [GitHub Actions 39057129976](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/39057129976), **success**.
- `run_depot_watch_e2e_w11.py`: `status=passed`, `evidence_transport=compact_watch_runtime`, `private_outputs_inside_repository=false`.
- Ausgegeben/archiviert wurde nur ein 337-Byte-Rezepteartefakt ohne Depotzeilen; Testposition und private Watch-Ausgabe lagen ausschließlich im GitHub Runner-Temp-Verzeichnis.
- Damit ist die bestehende W10 → W11-Pipeline lesend getestet. **Nicht** nachgewiesen ist, dass der produktive `main`-Scanner bereits die neue abgeschlossene Tageskerzenbasis verwendet. PR #326 bleibt isoliert.

## Neue Closed-Bar-Snapshot-Linie (Forschung, isoliert)
- `scripts/verify_closed_bar_w10_w11_shadow.py` führt auf demselben Runner nach dem geprüften Closed-Bar-Scan die **unveränderten** Phase2/3/4-Berechnungen, originale W10-Manifest/Phase5-Git-Provenienz, Phase6 `not_supplied`, W10 `freeze`/final-7A/`seal`, Watch-Runtime-Aufbau und echte W11-Abnahme mit synthetischem Buch aus.
- Kein `git push`, keine private Position im Git-Repository, kein Austausch der ursprünglichen historischen PIT-Signale gegen Ex-post-Rekonstruktionen.
- CI: `.github/workflows/closed_bar_scanner_integration.yml`, `permissions: contents: read`.
- Entscheidend ist die separate, neue Snapshot-ID im Shadow-W10 und der W11-Beleg; ein alter W10 mit anderem Snapshot darf **nicht** als Abnahme gelten.
- CI-Läufe: [39056212429](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/39056212429) (erste Iteration); [39056358232](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/39056358232) (korrigierter W11-Statuscontract). **Bei Erstellung dieses Dokuments noch keine finale W10/W11-Shadow-Abnahme verifiziert.**

## Freigabegrenze
Die Produktivumschaltung der Preisquelle und die W10-Veröffentlichung auf `main` dürfen erst nach einem erfolgreich versiegelten neuen Closed-Bar-Snapshot, einer erfolgreichen W11-Shadow-Abnahme und einer gesonderten Review/Promotion erfolgen. Ein erfolgreicher *bestehender* W10-W11-Test ersetzt nicht diese Bedingung.

## Nachträglich verifizierter Abschlussstand (aktuell)

- **Vollständiger Closed-Bar-Scanner → W10-Seal → W11-Abnahme im isolierten Branch**: [39056358232](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/39056358232) abgeschlossen **success** (2026-10-17 18:38 UTC). Kein Produktions-Push. Scanner-Snapshot `67dafeda-f0e8-4686-9d3b-231f57554d56`, 220/220 Scannerwerte (219 Score>0, 1 Score 0, 0 Fehler), W10 `sealed_same_snapshot`, 220 Pakete, Phase6 `not_supplied`, W11 `passed` mit ausschließlich synthetischem Ein-Stück-Testdepot. Originale W10-/W11-Skripte ausgeführt, nicht bloß simuliert.
- **Produktiver Workflow-Ablauf im isolierten Test inkl. CY02/CY03**: [39061041674](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/39061041674) abgeschlossen **success** (2026-10-17 19:10 UTC). Neuer Snapshot `304dcc10-1c53-40cf-b822-7cf5457606a7`, Universe 220; kompletter Scanner ohne Score-Fehler; CY02 `passed`, CY03 `captured`, Daily Research/7A Snapshot-Identität identisch. Archivprojektion hat **keinen vorgetäuschten frischen Yahoo-Provider-Receipt**; `main` nicht verändert.
- **Produktionsnahe Regressionstests**: [39060852803](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/39060852803) **success**; [39060856046](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/39060856046) Closed-Day-Tests **success**. Der alte Provider-Receipt-Test wurde aktualisiert: Bei direktem Yahoo-Fetch erforderlich, bei Cache-Projektion gerade nicht.
- **Produktionsvorbereitung auf PR-Branch**: `.github/workflows/run_scanner.yml` prefetches jetzt vor dem Scoring mit `scripts/prefetch_closed_bar_inputs.py`, setzt `SCANNER_CLOSED_DAILY_ONLY=1` und `SCANNER_PRICE_SOURCE=archive`, unterbindet einen zweiten Fetch nach Scoring und unterbindet synthetische Providerzeit-Belege; ursprünglicher Zeitplan bleibt gleich.
- **Wichtig:** Vollständige Freigabe umfasst zusätzlich Prüfung des Diffs gegen fortgeschrittenes `main`, mögliche CI-/Qualitätsregeln des PR und bewusste Promotion. Diese bestätigten Research-Runs führen **keine** Änderung auf `main` durch; historische rückwirkende Forschungsmessungen werden damit nicht zu ursprünglichen PIT-Scores.
