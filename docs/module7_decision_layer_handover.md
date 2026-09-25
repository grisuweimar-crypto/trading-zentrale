# Projektübergabe – Trading-Zentrale / Scanner-vNext

## Phase 7: Interpretation / Decision Layer

Status: Foundation vorbereitet. Eigentliche Implementierung erst nach vollständigem Abschluss von Phase 6 Elliott vNext.

Repository: `grisuweimar-crypto/trading-zentrale`

Foundation-Branch: `phase7-decision-layer-foundations`

## 1. Einordnung in die Roadmap

Diese Phase war ursprünglich als Phase 6 Interpretation / Decision Layer geplant. Durch den späteren Einschub von Elliott vNext wurde sie zu Phase 7.

Reihenfolge:
- Phase 5: Adaptive Learning Overlay – abgeschlossen
- Phase 6: Elliott vNext / Fibonacci – in Arbeit
- Phase 7: Interpretation / Decision Layer – hier vorbereitet
- Phase 8: zusätzliche externe Faktoren – später

## 2. Verbindliche Projektprinzipien

- strikt empirisch arbeiten
- keine erfundenen Kurse, Zustände, Ergebnisse oder Quellen
- Point-in-Time strikt
- fehlende Evidenz ausdrücklich als fehlend behandeln
- Selection, Timing, Probability, Risk, Confidence und Elliott semantisch getrennt halten
- Discovery / Validation / Holdout trennen
- Holdout nie zur Policy-Auswahl verwenden
- überlappende Forward-Windows nicht als unabhängige Beobachtungen behandeln
- Portfolio nicht für Universal-Stance-Training verwenden
- frühere Phasen in Phase 7 nicht rückwirkend neu optimieren

## 3. Semantik der Upstream-Module

### Selection
Richtung / relative Attraktivität im Universe.

### Timing
Richtung / Zeitpunkt innerhalb desselben Titels.

### Probability
Kalibrierung von Selection-/Timing-Evidenz. Kein unabhängiger zusätzlicher Vote.

### Risk
Downside-/Fehlerrisiko. Kein Return-/Kauf-Vote.

### Confidence
Belastbarkeit der vorhandenen Aussage. Niedrige Confidence ist nicht bearish.

### Adaptive Learning
Darf nur nach den Phase-5-Promotion-Regeln Reliability, Conflict Penalties, Horizon Mappings o. ä. beeinflussen. Keine stillen Upstream-Änderungen.

### Elliott vNext
Struktur-/Stage-Sensor. Liefert Primary/Alternative Counts, Invalidation, W2/W3/W4/W5-Kontext, Ziel-/Korrekturzonen und Swing-Review-Kontext. Kein autonomer Trade-Befehl.

## 4. Phase-7-Kernarchitektur

### Universal Stance
Portfolio-unabhängig, für jeden Universe-Titel:
- BUY
- HOLD
- SELL
- INSUFFICIENT_EVIDENCE

`INSUFFICIENT_EVIDENCE` ist fail-closed und darf nicht als neutrales HOLD behandelt werden.

### Portfolio Action Overlay
Erst danach reale Position berücksichtigen:
- OPEN
- ADD
- HOLD
- PARTIAL_REDUCE
- EXIT
- NO_ACTION
- INSUFFICIENT_EVIDENCE

Portfolio kann eine Aktion begrenzen, aber nicht rückwirkend den Universal Stance umetikettieren.

## 5. Kein Super-Score

Phase 7 startet ausdrücklich nicht mit einer Formel wie:
`0.3*Selection + 0.2*Timing + ...`

Verboten:
- manuell erfundene Gewichte
- Probability doppelt zählen
- niedrige Confidence als bearish
- Risk als Kaufsignal
- Elliott-Zielzone allein als Kauf-/Verkaufssignal
- Portfolio im Universal-Stance-Training

Policies müssen Discovery/Validation-basiert, interpretierbar und OOS geprüft sein.

## 6. Conflict / Confirmation

Mindestens folgende Zustände untersuchen:
- CONFIRMED
- MIXED
- CONFLICT
- STRUCTURAL_WARNING
- TIMING_WARNING
- INSUFFICIENT_EVIDENCE

Beispiele:
- starke Selection + positives Timing + passende W2/W4-Struktur kann Confirmation sein
- starke Selection + W5 completion risk ist ein Konflikt, nicht automatisch SELL
- schwache Selection + attraktive W2-Geometrie ist Rescue-Kandidat, nicht automatisch BUY

## 7. Zustandswechsel

Pro Titel:
- previous_stance
- current_stance
- transition
- transition_reasons
- days_in_current_stance

Hysterese/Cooldown nur empirisch, nicht manuell erfinden.

## 8. Swing Management

Phase 7 darf Elliott-Swing-Kontext in Portfolioaktionen übersetzen, aber nur zusammen mit den übrigen Modulen.

Zu prüfen:
- W2/W4 + positive Bestätigung -> OPEN / ADD / Re-Add
- W3 exhaustion + weitere Ermüdung -> PARTIAL_REDUCE
- W5 completion risk + negative Bestätigung -> größere REDUCE/EXIT-Entscheidung

Jeder aktive Swingpfad muss gegen einfaches Halten bzw. No-Swing verglichen werden, inklusive Gebühren, Slippage und Re-Entry-Kosten.

## 9. Decision Reliability

Eigene Größe, getrennt von Phase-4-Confidence.

Mögliche Inputs:
- Data Quality
- Upstream Confidence
- Evidenzabdeckung
- Conflict/Confirmation
- historische Sample Size der Policy-Zelle
- Walk-forward-Stabilität

Decision Reliability ist keine Richtungsaussage.

## 10. Phase 8 bleibt draußen

Zusätzliche externe Faktoren wie Sentiment, EPS-Revisionen, Short Interest oder weitere externe PIT-Daten gehören in Phase 8.

Phase 7 Core muss ohne diese Daten funktionieren.

Nicht historisierte Live-News dürfen nicht heimlich in die historische Policy-Kalibrierung gelangen.

## 11. Geplante Teilphasen

- 7A Input Contract & Coverage
- 7B Frozen Baseline & Decision Research Dataset
- 7C Conflict / Confirmation Research
- 7D Universal Stance Policy
- 7E Transition / Hysteresis
- 7F Portfolio Action Overlay & Swing Management
- 7G Decision Reliability & Explainability
- 7H Depot-Watch Integration
- 7I Final Validation & Promotion

## 12. Vorbereitete Foundation-Dateien

- `docs/decision_layer_7_foundations.md`
- `docs/decision_layer_7_research_plan.md`
- `docs/module7_decision_layer_handover.md`
- `configs/decision_layer_7_contract_v1.json`
- `configs/decision_layer_7_output_schema_v1.json`
- `tests/test_decision_layer_7_foundations.py`

## 13. Startauftrag nach Phase 6

1. Phase 6 vollständig abschließen und deren finalen Output-Vertrag einfrieren.
2. `phase7-decision-layer-foundations` gegen den dann aktuellen `main` vergleichen.
3. Foundations auf einen frischen Phase-7-Arbeitsbranch übernehmen/rebasen.
4. 7A beginnen und finale Upstream-Verträge der Phasen 1–6 einlesen.
5. Frozen pre-Phase-7 Baseline erzeugen.
6. Erst danach Decision Research Dataset und Policy-Forschung starten.
7. Keine externen Phase-8-Daten vorziehen.

Ziel: Das gesamte System soll aus getrennten, empirisch geprüften Sensoren eine verständliche und reproduzierbare Handlungsaussage erzeugen, ohne die wissenschaftliche Trennung der Module wieder aufzugeben.
