# Elliott vNext – Stage 5 Incremental Cross-System Validation

Status: Research-only. Stage 4 ist technisch vollständig abgeschlossen; Stage 5 misst nun den inkrementellen Informationswert der bereits eingefrorenen Scanner↔Elliott-Semantik.

## Ziel

Stage 5 beantwortet die in 6E/6G offen gelassene Frage, ob Elliott im Zusammenspiel mit vorhandenen Scannerzuständen zusätzliche Information liefert. Es werden keine Elliott-Regeln, Scanner-Schwellen oder Decision-Layer-Regeln neu optimiert.

## Datenfluss

`Stage-4 PIT Replay → 6D Elliott Routes → 6E Scanner Transitions → 6G Forward Outcomes → Stage-5 Incremental Comparison`

Die 20 bereits abgeschlossenen Stage-4-Replay-Chunks werden wiederverwendet. Es findet kein neuer historischer Elliott-Recount statt.

## Event-Time Scanner Evidence

Für jeden kausalen Elliott-Event werden nur die vorab in 6E definierten Scanner-Transitions geprüft.

Für den inkrementellen Outcome-Test sind ausschließlich Transitions aus dem Fenster `-20 … 0` Scanner-Sessions zulässig. Positive Offsets `+1 … +20` bleiben reine Lead/Lag-Diagnostik und dürfen nicht in den Event-Time-Informationszustand zurückfließen.

## Baseline

Ein Event mit einer Transition wird nicht gegen einen beliebigen Marktzeitraum verglichen, sondern nur gegen Elliott-Events derselben Klasse ohne diese Transition:

- gleiche Evidenzpartition,
- gleicher Forward-Horizont,
- gleiche Elliott-Review-Orientierung,
- gleiche Wellenphase,
- gleicher Wellengrad,
- gleiche Elliott-Richtung.

Damit wird gefragt: liefert die bereits definierte Scanner-Transition innerhalb desselben Elliott-Kontexts zusätzliche Trennschärfe?

## Outcomes

Verwendet werden die eingefrorenen 6G-Outcomes:

- directional signed forward return,
- directional review correctness,
- 5/10/20/40/60 Sessions,
- Performance nur auf Adjusted-Price-Basis,
- kein Raw-Close-Fallback.

## Unsicherheit

Stage 5 benutzt denselben Circular-Moving-Observation-Date-Block-Bootstrap wie 6G:

- Blocklänge = 2 × Horizont,
- Datumskluster bleiben zusammen,
- mindestens zwei Support-Regionen,
- 1000 Bootstrap-Replikationen im kanonischen Lauf.

Die globale Aggregation arbeitet mit exakten tagesweisen sufficient statistics (Summe + Anzahl). Ein Regressionstest vergleicht diese Berechnung gegen die rohe 6G-Implementierung.

## Confirmation / Redundancy / Rescue / Conflict

Diese Relationstypen werden nur ausgewertet, wenn echte historische, versionierte as-of Model-Claims vorliegen. Aktuelle Probability-/Risk-/Confidence-Befunde werden niemals rückwirkend erzeugt.

Wenn solche Claims für einen historischen Elliott-Event fehlen, bleibt der Arm `not_testable`. Fehlende Claims sind weder Zustimmung noch Konflikt.

## Evidenzgrenze

- `available_from <= 2026-09-25`: `legacy_development_descriptive_only`
- `available_from > 2026-09-25`: `prospective_unspent`

Legacy-Ergebnisse dürfen Effektgrößen und Fehlerbilder beschreiben, aber keine Promotion begründen.

## Nicht erlaubt

Stage 5:
- verändert keine Elliott-Kernlogik,
- optimiert keine Scanner-Schwelle,
- nutzt keine Post-Event-Scannerinformation als Event-Time-Evidenz,
- erzeugt keine Universal Stance,
- verändert keine Portfolio Action,
- erzeugt keine Order,
- promotet nichts automatisch.

## Technische Abnahme

Stage 5 ist technisch abgeschlossen, wenn:

1. alle 20 Stage-4-Replay-Chunks kausal verarbeitet sind,
2. Scanner-Transitions ausschließlich aus dem eingefrorenen 6E-Katalog stammen,
3. Event-Time und positive Lead/Lag-Offsets strikt getrennt sind,
4. Forward-Outcomes der 6G-Regel folgen,
5. Same-Class-Baselines verwendet werden,
6. der Block-Bootstrap gegenüber der Rohberechnung äquivalent getestet ist,
7. fehlende historische Model-Claims fail-closed bleiben,
8. das kanonische Artefakt `artifacts/research/elliott_vnext_stage5_incremental.json` auf `main` veröffentlicht ist,
9. `empirical_promotion_status` weiterhin `NOT_PROMOTED` bleibt.
