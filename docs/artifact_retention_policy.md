# Scanner_vNext – Artifact retention policy

Stand: 2026-10-11. **Schutzregel, keine automatische Löschfreigabe.**

## Ziel

Produktive Veröffentlichungen und historisch point-in-time korrekte Research- und QM-Belege dürfen durch Aufräumen nicht beschädigt werden. Es wird **keine Git-Historie umgeschrieben**, kein historischer Scannerstand aus heutigen Daten rekonstruiert und kein automatischer Retention-Job eingeführt. Eine Datei wird nicht allein aufgrund ihres Alters, Namens, ihrer Größe oder eines identischen Inhalts gelöscht.

## Aufbewahrungsklassen

| Klasse | Beispiele | Regel |
| --- | --- | --- |
| retain_source_or_audit | artifacts/snapshots/score_history.csv; artifacts/research/history_analysis.csv; artifacts/research/price_backfill.csv; artifacts/market_data/yahoo_ohlcv.csv; artifacts/research/decision_evidence_7a.jsonl.gz; artifacts/research/qm/; artifacts/cycle_history/ | Originale und versiegelte Nachweise schützen. Ausschließlich separate, belegte Migration. |
| retain_current_publication | artifacts/research/latest_scanner.csv; daily_research.json; history_metadata.json; current_decision_packets_7a.json; decision_snapshot_w10.json; watch_runtime/; artifacts/ui/ | Auf main belassen, solange tägliche Leser, Snapshot-Gates und externe URLs die Pfade benötigen. |
| derived_review_required | artifacts/research/history_recent.csv; artifacts/snapshots/score_history_recent.csv; artifacts/research/history_research.csv; artifacts/research/monthly/ | Können abgeleitet sein, sind **nicht** freigegeben. Bytegleiches historisches Rebuild, Referenzen und Archivnutzen erst prüfen. |
| possible_copy_review_required | artifacts/watchlist/watchlist (1).csv sowie nummerierte Kopien | Nur nach Referenz- und Herkunftsprüfung aus aktuellem Baum entfernen. Ursprünglichen Git-Blob für Wiederherstellung dokumentieren. |
| unclassified_review_required | alle übrigen Artefakte | Unbekannte Dateien nicht löschen; Erzeuger und Verbraucher zuerst ermitteln. |

Unterschiedliche Dateipfade können dieselben Bytes besitzen und trotzdem unterschiedliche Verträge erfüllen: watchlist.csv ist Eingabe/DB, watchlist_full_raw.csv ein Provenienz-/Scoring-Export. Gleiches gilt für Presets und versiegelte QM-Belege. Git dedupliziert identische Inhalts-Blobs bereits; doppelter logischer Pfad ist nicht gleich physisch doppelte Repositorygröße.

## Vor jeder späteren Löschung

1. Pfad und aktuellen Git-Blob-SHA, Erzeuger und direkte/dynamische Leser aus Code, Workflows, Tests, UI und externen Raw-URLs dokumentieren.
2. Klassifizieren; bei Unsicherheit unverändert aufbewahren. Historische Rückverfolgbarkeit mit originalem Kurs-/Scannerzeitpunkt, SHA und Snapshot-ID sichern.
3. Wenn Rekonstruierbarkeit behauptet wird: aus **denselben damaligen Inputs** byte- bzw. hashgleich wiederherstellen. Ein Rebuild mit aktuellen Daten genügt nicht.
4. Wiederherstellungs-Commit/Blob festhalten. Separate PR verwenden; produktives main zunächst unangetastet lassen.
5. CI, betroffene Generator-/Consumer-Tests und Scanner → Research → Decision-W10 → watch_runtime Snapshot-/Hash-Kette prüfen; bei Fehlern PR nicht mergen.
6. Tatsächliche Git-Speicherersparnis von eingesparten Worktree-Bytes unterscheiden. Normales Löschen entfernt keine alten Git-Versionen.

## Erzeugung und Vorbeugung

Der produktive Workflow run_scanner.yml verwendet weiterhin eine breite Staging-Regel (git add artifacts/). Eine enge Allowlist würde ohne vollständige Erzeuger-/Leserprüfung potenziell Live-Daten, CI-/Watch-Gates und Prospective Research abschneiden. Sie wird **in dieser Phase nicht geändert**.

Die neue .gitignore sperrt nur unbeabsichtigte nummerierte Watchlist-Kopien sowie übliche Editor-/Backup-Reste. Sie wirkt auf bislang ungetrackte Dateien; bereits verfolgte Dateien müssen separat aus dem Index genommen werden. Forschungsarchive, aktuelle Runtime-Shards und andere bekannte produktive Outputs werden nicht ignoriert.

Vor Einführung jeder neuen Artefaktart sind Quelle, Klasse, Generator, Leser, Veröffentlichungsfrequenz, Frische-/Snapshot-Kontrakt und Aufbewahrung festzuhalten. Für Git-Wachstum später zuerst die wiederholte Veröffentlichung großer Decision- und Watch-Runtime-Daten analysieren. **Keine automatische Rotation**, solange PIT-, QM-, Elliott- oder Cycle-Belege betroffen sind.

## Prüfbefehl (nur lesend)

~~~sh
python scripts/audit_repository_artifacts.py
python scripts/audit_repository_artifacts.py --json
python -m pytest -q tests/test_audit_repository_artifacts.py
~~~

Die Auswertung zeigt Dateigrößen, Klassen und Git-Blob-Dopplungen; sie löscht nichts, verändert keine Daten und stellt keine Löschfreigabe dar.

## Geplanter kleiner Cleanup: nummerierte Watchlist-Kopie

artifacts/watchlist/watchlist (1).csv, letzte beobachtete Änderung vom 01.03.2026, ursprünglich im Commit 3ec36b2ee871. Letzter Inhalt im alten main: Git-Blob 08ce14e25b098b92153a9eb354a39c382cca17df, 118.767 Bytes, 152 Datenzeilen. Historische Yahoo-Kennungen wie BTC-EUR, ETH-EUR, ADA-EUR, SLVR.V, DV.V und BRVALEACNOR0 unterscheiden sich teilweise von der heutigen Liste; ihre Werte werden **nicht** in aktuelle Symbole umgedeutet.

Die Datei soll nur aus dem aktuellen Arbeitsbaum entfernt werden. Die Originalfassung bleibt unter https://github.com/grisuweimar-crypto/trading-zentrale/commit/3ec36b2ee871 und über den Blob-SHA aus Git wiederherstellbar. In untersuchten produktiven Watchlist-Build-, Research- und Workflow-Einstiegen wurde kein Leser des Backup-Pfads gefunden; eine lückenlose Laufzeit-/Externverbraucherprüfung über alle denkbaren Aufrufe ist damit nicht behauptet. Bei CI-/Verbraucherfehlern wird die Löschung zurückgenommen.

## Spätere separate Pakete

- GitHub-Workflow-Inventur und Required-Checks; keine pauschale Stilllegung der 137 Workflow-Dateien.
- Monthly Research: Byte-Reproduzierbarkeit und Archivierungsvertrag prüfen, bevor Ausgaben verschoben werden.
- Große Decision-/Runtime-Veröffentlichungen: Verbraucherverträge und Kosten einer anderen historischen Ablage nachweisen, ohne die W10-/Research-Kette zu brechen.
