# Wertpapierdepot Watch — kanonische Orchestrierung

## Ziel

Die Wertpapierdepot Watch ist die private Endanwendung des Scanner-vNext-Systems. Sie darf keine zweite Scanner- oder Handelslogik besitzen. Die kanonische Kette lautet:

1. autoritativer Scannerlauf / `latest_scanner.csv`
2. `daily_research.json` mit identischer `snapshot_id`
3. aktuelle typisierte Phase-7A-Evidence
4. append-only `decision_evidence_7a.jsonl`
5. Phase 7D Universal Stance
6. Phase 7E zeitliche Zustands-/Hysteresehistorie
7. Phase 7F Portfolio Action mit privatem Positionssnapshot
8. Phase 7G Reliability & Explainability
9. Phase 7H Wertpapierdepot Watch

Die Watch selbst recomputet keine dieser vorgelagerten Entscheidungen.

## Öffentlicher und privater Teil

### Öffentlicher Teil

Nach einem erfolgreichen Scannerlauf erzeugt

`python scripts/build_current_decision_evidence_7a.py --archive`

für jedes Symbol des exakten aktuellen Scanner-Snapshots ein validiertes 7A-Paket und archiviert es idempotent in

`artifacts/research/decision_evidence_7a.jsonl`.

Zusätzlich wird der aktuelle Paket-Satz nach

`artifacts/research/current_decision_packets_7a.json`

geschrieben.

Diese Dateien enthalten keine Depotpositionen.

### Privater Teil

Die Watch wird mit

```bash
python scripts/run_depot_watch_orchestrated.py \
  --positions /privat/position_book.json
```

ausgeführt.

Ohne `--output` wird kein privater Watch-Stand in das Repository geschrieben. Das Positionsbuch bleibt ein expliziter Runtime-Input.

## Zeit- und Snapshot-Bindung

`daily_research_v1` führt derzeit zwei verschiedene Zeitinformationen:

- `as_of`: Markt-/Scannertag, zum Beispiel `2026-09-29`
- `generated_at`: exakter Erzeugungszeitpunkt des autoritativen Snapshots

Phase 7A benötigt eine exakte Zeit, weil `available_from` niemals nach dem Paketzeitpunkt liegen darf. Die Orchestrierung verwendet deshalb für die Decision-Kette eine defensive Kopie des Daily-Snapshots mit

- unveränderter `snapshot_id`
- `scanner_as_of_date = daily_research.as_of`
- Decision-`as_of = daily_research.generated_at`

Das öffentliche `daily_research.json` wird dabei nicht verändert. Es wird keine künstliche Mitternachtszeit erzeugt.

## Aktuell automatisch erzeugte Evidence

### Selection

Der aktuelle Scannerzustand wird PIT-gerecht als Selection-Kontext übernommen. Verwendet werden nur tatsächlich vorhandene Felder wie Score, Phase-1A-kompatibles Score-Perzentil, Quality-Band und R-Code.

**Wichtig:** Aus Score, Rank, R-Code oder Quality-Band wird keine positive oder negative Richtung erfunden. Selection bleibt ohne explizite Richtung, solange keine separat eingefrorene empirische Direction-Mapping-Regel existiert.

### Timing

Die eingefrorenen Phase-1B-Muster aus `timing_patterns_1b_frozen.json` werden auf die aktuelle Beobachtung mit denselben PIT-Feature-Atomen angewendet. Nur ein tatsächlicher Match erzeugt einen Directional Claim. Die Richtung stammt ausschließlich aus `discovery_direction` des eingefrorenen Musters.

Timing bleibt `research_only` und `directional_but_immature`; die Orchestrierung befördert diese Claims nicht automatisch zu produktiver Evidence.

### Noch nicht automatisch rekonstruiert

Die Orchestrierung erzeugt absichtlich keine Ersatzclaims für:

- Phase 2 Probability
- Phase 3 Risk
- Phase 4/5 Confidence-vNext
- Phase 6 Elliott-vNext
- Phase 8 External Evidence

Fehlende typisierte Adapter bleiben als fehlende Evidence sichtbar. Scannerfelder mit ähnlichen Namen werden nicht als Ersatz verwendet.

## Phase 8

Phase-8-Evidence wird durch diese Orchestrierung nicht automatisch aktiviert. Solange kein separat genehmigter Production-Integration-Change existiert, bleibt der aktuelle Phase-7-Pfad autoritativ. Ein späterer Phase-8-Adapter muss seine Promotion-/Provenance-Verträge erfüllen und darf nicht direkt zur Portfolio Action oder zu Orders springen.

## Hysterese / 7E

7E wird aus der realen append-only 7A-Historie eines Symbols aufgebaut. Die Orchestrierung dupliziert niemals den aktuellen Snapshot, um eine Bestätigung zu erzeugen.

Dadurch gilt:

- ein erster directional Snapshot → `bootstrap_pending`
- erst eine zulässige weitere Beobachtung auf einem anderen Kalendertag und mit anderer Snapshot-ID kann die eingefrorene Minimal-Repeat-Regel bestätigen
- Konflikt oder unzureichende Evidence bleibt blockierend

Die Regel selbst bleibt research-only und empirisch unbestätigt, wie im bestehenden 7E-Vertrag festgelegt.

## Fehlerverhalten

- fehlt der aktuelle 7A-Packet für eine Position, bleibt die Watch für dieses Symbol `decision_bundle_missing`
- Scanner-Score/R-Code ersetzen den fehlenden Decision-Pfad nicht
- ein Snapshot-/Zeitkonflikt führt nicht zu einer stillen Date-only-Verknüpfung
- private Positionen werden nur in 7F eingebracht
- Orders, Positionsgrößen und Zielgewichte bleiben außerhalb der Watch

## Noch bestehende empirische Grenze

Diese Änderung orchestriert die bereits definierten Systemverträge. Sie promotet keine Phase-7-Regel und behauptet keine empirische Fertigstellung. Insbesondere sind die 7E-Hysterese und 7F-Portfolio-Action weiterhin prospective/research-only, bis ihre jeweiligen Promotion-Gates erfüllt sind.
