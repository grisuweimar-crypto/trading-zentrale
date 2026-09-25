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
Downside-/Fehlerrisiko. Kein Return-/Kauf-Vote. Hohe Downside darf eine Aktion begrenzen, aber nicht allein ein positives Richtungsurteil in SELL invertieren.

### Confidence
Belastbarkeit der vorhandenen Aussage. Niedrige Confidence ist nicht bearish.

### Adaptive Learning
Darf nur nach den Phase-5-Promotion-Regeln Reliability, Conflict Penalties, Horizon Mappings o. ä. beeinflussen. Keine stillen Upstream-Änderungen.

### Elliott vNext
Struktur-/Stage-Sensor. Liefert Primary/Alternative Counts, Invalidation, W2/W3/W4/W5-Kontext, Ziel-/Korrekturzonen und Swing-Review-Kontext. Kein autonomer Trade-Befehl.

## 4. Universal Stance und HOLD-Semantik

Portfolio-unabhängig, für jeden Universe-Titel:
- BUY
- HOLD
- SELL
- INSUFFICIENT_EVIDENCE

Wenn `HOLD`, muss intern ein Detailzustand geführt werden:
- `HOLD_CONSTRUCTIVE`: grundsätzlich positive/tragfähige Lage, aber aktuell keine neue Aktion.
- `HOLD_NEUTRAL`: keine klare Richtungs- oder Handlungsevidenz.
- `HOLD_UNRESOLVED`: relevante Sensoren widersprechen sich; Beobachtung statt Aktion.

Wichtig:
`HOLD_PORTFOLIO_CONSTRAINED` ist als Universal-Stance-Detail ausdrücklich verboten. Wenn der Titel universell BUY bleibt, das Depot aber voll ist, lautet die Kombination z. B.:
- Universal Stance: BUY
- Portfolio Action: NO_ACTION
- Portfolio Reason: MAX_POSITION_CONCENTRATION

Damit bleibt die Titelbewertung unverfälscht.

## 5. Portfolio Action Overlay

Erst nach Universal Stance reale Position berücksichtigen:
- OPEN
- ADD
- HOLD
- PARTIAL_REDUCE
- EXIT
- NO_ACTION
- INSUFFICIENT_EVIDENCE

Portfolio kann eine Aktion begrenzen, aber nicht rückwirkend den Universal Stance umetikettieren.

## 6. INSUFFICIENT_EVIDENCE

Fail-closed und niemals als neutrales HOLD behandeln.

Reason Codes:
- `INSUFFICIENT_DATA`
- `INSUFFICIENT_MODEL_COVERAGE`
- `INSUFFICIENT_CONSENSUS`
- `INSUFFICIENT_VALIDATION`
- `OUTSIDE_VALIDATED_DOMAIN`
- `STALE_OR_INCOMPATIBLE_INPUT`
- `INPUT_CONTRACT_VIOLATION`

Vertragliche harte Fälle wie inkompatible Inputs dürfen deterministisch fail-closed sein. Empirische Schwellen wie notwendige Historientiefe oder Coverage werden nicht vorab erfunden.

## 7. Kein Super-Score

Phase 7 startet ausdrücklich nicht mit einer Formel wie:
`0.3*Selection + 0.2*Timing + ...`

Verboten:
- manuell erfundene Gewichte
- Probability doppelt zählen
- niedrige Confidence als bearish
- Risk als Kaufsignal oder alleinige Richtungsinversion
- Elliott-Zielzone allein als Kauf-/Verkaufssignal
- Portfolio im Universal-Stance-Training

Policies müssen Discovery/Validation-basiert, interpretierbar und OOS geprüft sein.

## 8. Conflict / Confirmation Taxonomy

Mindestens unterscheiden:
- `SELECTION_TIMING_CONFLICT`
- `DIRECTION_STRUCTURE_CONFLICT`
- `PRIMARY_ALTERNATIVE_STRUCTURE_CONFLICT`
- `MARKET_CONTEXT_DIVERGENCE`
- `RELATIVE_STRENGTH_DIVERGENCE`
- `RISK_CONSTRAINT_NOT_DIRECTIONAL_CONFLICT`
- `RELIABILITY_OR_COVERAGE_CONFLICT`
- `DATA_OR_VERSION_CONFLICT`

Conflict/Confirmation wird vor der Stance Policy untersucht. Risk-Constraints dürfen nicht mit Directional Conflict vermischt werden.

## 9. Decision Reliability

Eigener strukturierter Vertrag, getrennt von Phase-4-Confidence.

Output mindestens:
- `level`: HIGH / MEDIUM / LOW / INSUFFICIENT
- `coverage`
- `data_quality`
- `module_agreement`
- `conflict_severity`
- `walk_forward_support`
- `reason_codes`

Ein interner Research-Score 0–1 ist später zulässig, aber nur empirisch kalibriert und niemals als objektive Wahrheit oder Richtungsaussage.

## 10. Zustandswechsel / Hysterese

Pro Titel:
- previous_stance
- current_stance
- transition
- transition_reasons
- days_in_current_stance

Drei Mechanismen getrennt testen:
- `confirmation_window`
- `evidence_margin`
- `exception_override`

N und Margin werden nicht vorab festgelegt.

Harte Contract-/Daten-/Struktur-/Risk-Invalidierungen dürfen Hysterese sofort übersteuern. Hysterese darf niemals einen klar invalidierten Zustand künstlich fortschreiben.

## 11. Swing Management

Phase 7 darf Elliott-Swing-Kontext in Portfolioaktionen übersetzen, aber nur zusammen mit den übrigen Modulen.

Zu prüfen:
- W2/W4 + positive Bestätigung -> OPEN / ADD / Re-Add
- W3 exhaustion + weitere Ermüdung -> PARTIAL_REDUCE
- W5 completion risk + negative Bestätigung -> größere REDUCE/EXIT-Entscheidung

Jeder aktive Swingpfad muss gegen einfaches Halten bzw. No-Swing verglichen werden.

Kosten mindestens:
- Gebühren
- Spread soweit material
- Slippage soweit material
- Re-Entry-Kosten
- verpasste Rebounds
- Opportunity Cost geringeren Exposures in starken Trends
- Steuer-/Realisierungseffekte nur soweit belastbar modellierbar

## 12. Kanonischer Decision-State-Katalog

Datei:
`configs/decision_state_catalog_v1.json`

Foundation enthält 24 Fälle.

Zwei Falltypen:
- `contract_deterministic`: Semantik muss unabhängig von späterer Policy gelten.
- `research_pending_policy`: beschreibt Forschungsfrage/Kandidatenoutput, aber friert noch keine endgültige Entscheidung ein.

Der Katalog wird parametrisch getestet und muss bei neuen Stance-/Reason-/Conflict-Zuständen mitgeändert werden.

## 13. Phase 8 bleibt draußen

Zusätzliche externe Faktoren wie Sentiment, EPS-Revisionen, Short Interest oder weitere externe PIT-Daten gehören in Phase 8.

Phase 7 Core muss ohne diese Daten funktionieren.

Nicht historisierte Live-News dürfen nicht heimlich in die historische Policy-Kalibrierung gelangen.

## 14. Geplante Teilphasen

- 7A Input Contract, Coverage & State Semantics
- 7B Frozen Baseline & Decision Research Dataset
- 7C Conflict / Confirmation Research
- 7D Universal Stance Policy
- 7E Transition / Hysteresis
- 7F Portfolio Action Overlay & Swing Management
- 7G Decision Reliability & Explainability
- 7H Depot-Watch Integration
- 7I Final Validation & Promotion

## 15. Vorbereitete Foundation-Dateien

- `docs/decision_layer_7_foundations.md`
- `docs/decision_layer_7_research_plan.md`
- `docs/module7_decision_layer_handover.md`
- `configs/decision_layer_7_contract_v1.json`
- `configs/decision_layer_7_output_schema_v1.json`
- `configs/decision_state_catalog_v1.json`
- `tests/test_decision_layer_7_foundations.py`

## 16. Startauftrag nach Phase 6

1. Phase 6 vollständig abschließen und deren finalen Output-Vertrag einfrieren.
2. `phase7-decision-layer-foundations` gegen den dann aktuellen `main` vergleichen.
3. Foundations auf einen frischen Phase-7-Arbeitsbranch übernehmen/rebasen.
4. 7A beginnen und finale Upstream-Verträge der Phasen 1–6 einlesen.
5. HOLD-/NO_ACTION-/INSUFFICIENT-Semantik und State Catalog als erste Contract-Tests ausführen.
6. Frozen pre-Phase-7 Baseline erzeugen.
7. Erst danach Decision Research Dataset und Policy-Forschung starten.
8. Keine externen Phase-8-Daten vorziehen.

Ziel: Das gesamte System soll aus getrennten, empirisch geprüften Sensoren eine verständliche und reproduzierbare Handlungsaussage erzeugen, ohne die wissenschaftliche Trennung der Module wieder aufzugeben.
