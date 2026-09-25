# Modul 6E – Scanner ↔ Elliott Cross-System Research

Status: Research-only. Keine produktive Scanner-, Depot-Watch- oder Handelslogik.

## Ziel

6E baut die reproduzierbare Messschicht zwischen dem bestehenden Scanner-Research und Elliott vNext.

Die Kette lautet:

`6A kausale Pivots → 6B Szenarien → 6C Fibonacci-Geometrie → 6D Review-Routing → 6E Cross-System Research`

6E untersucht:

- Redundancy,
- Confirmation,
- Elliott Rescue,
- Scanner Rescue,
- Conflict,
- Lead/Lag,
- stage-specific incremental value,
- swing-routing value.

Dabei ist eine wichtige Grenze eingefroren: 6E kann Überlappung, zeitliche Reihenfolge und Konflikte quantifizieren. Einen **inkrementellen prognostischen Nutzen** darf es noch nicht behaupten. Forward-Returns, Peer-Alpha, Drawdown-Outcomes und robuste Unsicherheit gehören in 6G.

## 1. Point-in-Time Elliott ist zwingend

Historische 6E-Ereignisse dürfen ausschließlich aus bereits beobachteten 6D-Routen/Snapshots mit einem kausalen `available_from` stammen.

Nicht zulässig ist:

1. heute die gesamte historische Kursreihe anzusehen,
2. daraus mit heutigem Wissen einen Elliott-Count zu erzeugen,
3. diesen Count rückwirkend so zu behandeln, als wäre er damals bekannt gewesen.

Fehlt ein historischer PIT-Elliott-Stream, lautet das Ergebnis **fehlende Evidenz / nicht testbar**. Es wird nichts rekonstruiert oder imputiert.

Das entspricht der bereits in Phase 1B eingefrorenen Regel, historische Elliott-Zustände nur dann zu verwenden, wenn sie damals tatsächlich gespeichert bzw. kausal erzeugt wurden.

## 2. Scanner-Seite

6E verwendet die gleiche historische Scanner-Semantik wie Phase 1A/1B:

- Same-day reruns: letzter publizierter Zustand gewinnt deterministisch.
- Score ist ein Qualitäts-/Regime-Deskriptor, kein BUY/SELL-Signal.
- R0–R5 ist eine Qualitätsklasse, kein BUY/SELL-Signal.
- Fehlende historische Felder bleiben fehlend.
- Keine heutigen Werte werden rückwirkend ergänzt.

Für die Event-Matrix werden die vorhandenen PIT-Felder gelesen:

- Score,
- Opportunity,
- Risk,
- RS3M,
- Trend200,
- Cycle,
- gespeichertes R-Code, soweit vorhanden.

Für kontinuierliche Größen werden nur vorab definierte 1-/5-/10-Beobachtungs-Deltas berechnet. Lead/Lag verwendet keine nachträglich optimierten Schwellen.

## 3. ±20-Session Eventfenster

Jedes kausale Elliott-Ereignis erhält ein Fenster von mindestens

`-20 … 0 … +20`

beobachteten Scanner-Sessions desselben Symbols.

Vorzeichenkonvention:

- negativer Offset: Scanner-Transition trat **vor** Elliott auf,
- 0: gleiche Scanner-Session,
- positiver Offset: Elliott trat **vor** der Scanner-Transition auf.

Positive Offsets sind ausdrücklich `post_event_analysis_only`. Sie dürfen nicht in den Informationszustand am Elliott-Ereignistag zurückfließen.

Wenn `available_from` zwischen zwei Scanner-Sessions liegt, wird das Ereignis an die erste Scanner-Beobachtung **am oder nach** `available_from` gekoppelt. Damit kann eine vorherige Scanner-Beobachtung keine noch nicht verfügbare Elliott-Information besitzen.

## 4. Vorab definierte Lead/Lag-Transitions

6E misst unter anderem die zeitliche Lage von:

- Score turn up/down,
- Opportunity turn up/down,
- Risk turn up/down,
- RS3M turn up/down,
- Trend200 turn up/down,
- Cycle turn up/down,
- Trend200 zero crossing,
- Cycle crossing 25/50/75,
- R-Code upgrade/downgrade.

Ein `turn` ist lediglich ein Vorzeichenwechsel des 1-Beobachtungs-Deltas. Das ist eine beschreibende Transition, kein neu entdecktes Trading-Muster.

Diese Matrix erlaubt später insbesondere die im Research-Plan vorgesehenen Fragen:

- dreht Score/RS3M vor oder nach einem möglichen W2-Ende?
- verschlechtert sich Momentum vor oder nach W3-Zielnähe?
- entstehen Reversal-/RS-Transitions um W4-Zonen?
- verschlechtern sich Selection/Momentum um mögliche W5-Abschlüsse?

## 5. Frozen Model Claims

Für Timing, Probability, Risk, Confidence bzw. andere bereits existierende Modelle verwendet 6E einen expliziten Claim-Vertrag.

Ein Claim braucht mindestens:

- `symbol`,
- `model`,
- `claim_id`,
- `model_version`,
- `available_from`,
- `stance`,
- `maturity`.

Zulässige Stances:

- `supportive`,
- `cautionary`,
- `neutral`,
- `unknown`.

Zulässige Reifezustände:

- `mature`,
- `directional_but_immature`,
- `insufficient_evidence`,
- `unavailable`.

Nur Claims mit `available_from <= Elliott available_from` sind am Ereignistag sichtbar. Ein heutiger Phase-2-/3-/4-/5-Befund darf nicht in einen älteren Tag zurückprojiziert werden.

Insbesondere gilt:

- Risk `cautionary` kann eine Downside-Warnung repräsentieren.
- Fehlende/immature Risk-Evidenz ist `unknown`, nicht automatisch Zustimmung.
- Ein niedriger Risk-Wert wird nicht automatisch als Alpha-Support interpretiert; Phase 3 hat gerade Protection und Alpha getrennt.
- Confidence muss als versionierter as-of Claim vorliegen; Legacy Confidence wird nicht ungeprüft als zuverlässiges Vertrauenssignal verwendet.

## 6. Beziehungslabels

Die Labels sind **Research-Kategorien, keine Performanceurteile**.

### Confirmation candidate

Ein reifer Scanner-Claim und die Elliott-Review-Orientierung sind am Ereignisrand kompatibel.

### Redundancy candidate

Ein kompatibler reifer Scanner-Claim existierte bereits vor dem Elliott-Ereignis. Das ist ein Kandidat für Redundanz; erst 6G kann feststellen, ob Elliott trotzdem zusätzlichen Informationswert hatte.

### Elliott rescue candidate

Elliott liefert eine directional Review-Orientierung, während der entsprechende Scanner-Claim fehlt oder unreif ist.

### Scanner rescue candidate

Elliott ist neutral (`hold_review`), während ein reifer Scanner-Claim directional ist.

### Conflict

Ein reifer Scanner-Claim und ein directional Elliott-Review weisen in inkompatible Review-Richtungen.

Konflikte werden nicht aufgelöst. Das bleibt Aufgabe späterer Forschung und letztlich der globalen Decision-Schicht.

## 7. Keine erneute Optimierung alter Phasen

6E verändert nicht:

- Phase-1A Selection,
- Phase-1B Timing-Muster,
- Phase-2 Probability Calibration,
- Phase-3 Risk,
- Phase-4/5 Confidence,
- Scanner Score,
- Opportunity/Risk-Gewichte,
- R0–R5,
- produktive Depot-Watch.

6E sucht auch keine neuen Scanner-Schwellen anhand der späteren Kursentwicklung.

## 8. Discovery / Validation / Holdout

Die alten Grenzen bleiben als Provenienzmarken sichtbar:

- Stable start: 2026-04-15,
- frühere Discovery-Referenz: bis 2026-07-31,
- frühere Validation: ab 2026-08-01.

Der Phase-1B-/Phase-2-Holdout ab 2026-08-01 ist **bereits verbraucht**. 6E darf ihn zur beschreibenden Cross-System-Überlappung verwenden, aber nicht zur Auswahl oder Optimierung von 6E-Regeln.

Für einen späteren inkrementellen Nutzenbeleg ist neue unverbrauchte bzw. prospektive Evidenz erforderlich.

## 9. Was 6E bereits quantifizieren darf

- Anzahl kausaler Elliott-Ereignisse,
- Coverage nach Symbol, Wellengrad, Wellenphase und Trigger,
- Scanner-Eventfenster-Coverage,
- Häufigkeit vorab definierter Scanner-Transitions,
- Lead/Lag-Verteilung,
- Confirmation-/Redundancy-/Rescue-/Conflict-Häufigkeiten für explizite as-of Model Claims.

## 10. Was 6E noch nicht behaupten darf

Nicht in 6E:

- Elliott erhöht den erwarteten Return um X %,
- Elliott verbessert die Trefferquote um Y pp,
- Confirmation schlägt Scanner-only,
- Elliott Rescue ist profitabel,
- Teilverkauf + Rückkauf schlägt Hold,
- W5-Protection verbessert Nettoergebnis,
- ein Cross-System-Label ist eine Kauf-/Verkaufsentscheidung.

Diese Aussagen benötigen 6G mit PIT-Forward-Outcomes und der bereits etablierten Abhängigkeitsbehandlung.

## 11. Übergabe an 6G

6G erhält aus 6E eine eingefrorene, reproduzierbare Event-/Join-Semantik. Dort werden anschließend getrennt nach Wellenphase, Richtung und Wellengrad geprüft:

- 5/10/20/40/60T Forward-Outcomes soweit reif,
- Peer-Alpha bei passender Benchmark,
- Confirmation vs Scanner-only / Elliott-only,
- Rescue und Conflict,
- stage-specific incremental value,
- Swing-Routing vs Hold-/Buy-and-Hold-Baselines,
- Transaktionskosten,
- robuste Unsicherheit mit der projektweit etablierten Circular-Moving-Observation-Date-Block-Methode und effektivem Block `2 × Horizont`.

## Abnahmekriterien 6E

6E ist technisch abgenommen, wenn:

1. nur kausal beobachtete 6D-Ereignisse verwendet werden,
2. ein fehlender Elliott-History-Stream fail-closed bleibt,
3. Scanner-Missingness nicht imputiert wird,
4. Same-day reruns deterministisch behandelt werden,
5. mindestens ±20 Sessions abgebildet werden können,
6. Post-Event-Daten explizit von Event-Time-Evidenz getrennt bleiben,
7. as-of Model Claims keine Zukunftsinformation in ein Ereignis tragen,
8. Conflict/Unknown erhalten bleiben,
9. der verbrauchte Holdout nicht zur Regelauswahl genutzt wird,
10. keinerlei Trade- oder Orderentscheidung ausgegeben wird.
