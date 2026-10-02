# Projektübergabe – Trading-Zentrale / Scanner-vNext

## Phase 7: Interpretation / Decision Layer

Status: Foundation vorbereitet. Eigentliche Implementierung erst nach vollständigem Abschluss von Phase 6 Elliott vNext.

Repository: `grisuweimar-crypto/trading-zentrale`

Foundation-Branch: `phase7-decision-layer-foundations`

## Einordnung

Diese Phase war ursprünglich als Phase 6 Interpretation / Decision Layer geplant. Durch den späteren Einschub von Elliott vNext wurde sie zu Phase 7.

Roadmap:
- Phase 5: Adaptive Learning Overlay – abgeschlossen
- Phase 6: Elliott vNext / Fibonacci – in Arbeit
- Phase 7: Interpretation / Decision Layer – hier vorbereitet
- Phase 8: zusätzliche externe Faktoren – später

## Verbindliche Semantik

Selection = relative Attraktivität/Richtung.
Timing = Zeitpunkt/Richtung innerhalb des Titels.
Probability = Kalibrierung von Selection/Timing, kein zusätzlicher Vote.
Risk = Downside-/Fehlerrisiko, kein Richtungs-Vote und keine alleinige BUY->SELL-Inversion.
Confidence = Zuverlässigkeit, nicht bullisch/bearish.
Adaptive Learning = nur nach Phase-5-Promotion-Regeln.
Elliott vNext = Struktur-/Stage-/Swing-Kontext, kein autonomer Trade-Befehl.

## Universal Stance

Portfolio-unabhängig:
- BUY
- HOLD
- SELL
- INSUFFICIENT_EVIDENCE

Bei HOLD ist ein Detailzustand Pflicht:
- HOLD_CONSTRUCTIVE
- HOLD_NEUTRAL
- HOLD_UNRESOLVED

Portfolio-Constraints dürfen niemals als Universal-HOLD codiert werden.

## Portfolio Action Overlay

Erst nach Universal Stance:
- OPEN
- ADD
- HOLD
- PARTIAL_REDUCE
- EXIT
- NO_ACTION
- INSUFFICIENT_EVIDENCE

Beispiel:
Universal BUY + MAX_POSITION_CONCENTRATION -> Portfolio NO_ACTION, Universal bleibt BUY.

## INSUFFICIENT_EVIDENCE

Fail-closed, niemals neutrales HOLD.

Reason Codes:
- INSUFFICIENT_DATA
- INSUFFICIENT_MODEL_COVERAGE
- INSUFFICIENT_CONSENSUS
- INSUFFICIENT_VALIDATION
- OUTSIDE_VALIDATED_DOMAIN
- STALE_OR_INCOMPATIBLE_INPUT
- INPUT_CONTRACT_VIOLATION

## Conflict / Confirmation

Vor der Stance Policy typisieren:
- SELECTION_TIMING_CONFLICT
- DIRECTION_STRUCTURE_CONFLICT
- PRIMARY_ALTERNATIVE_STRUCTURE_CONFLICT
- MARKET_CONTEXT_DIVERGENCE
- RELATIVE_STRENGTH_DIVERGENCE
- RISK_CONSTRAINT_NOT_DIRECTIONAL_CONFLICT
- RELIABILITY_OR_COVERAGE_CONFLICT
- DATA_OR_VERSION_CONFLICT

Risk-Constraint ist nicht automatisch Directional Conflict.

## Decision Reliability

Getrennt von Phase-4-Confidence.

Pflichtfelder:
- level: HIGH / MEDIUM / LOW / INSUFFICIENT
- coverage
- data_quality
- module_agreement
- conflict_severity
- walk_forward_support
- reason_codes

Optionaler interner Research-Score 0–1 nur nach empirischer Kalibrierung; niemals objektive Wahrheit und niemals Richtungssignal.

## Hysterese

Getrennt testen:
- confirmation_window
- evidence_margin
- exception_override

N und Margin nicht vorab festlegen.

Harte Input-/Daten-/Struktur-/Risk-Invalidierungen dürfen Hysterese sofort übersteuern.

## Swing Management

Elliott darf nur Review-Kontext liefern.

Zu testen:
- W2/W4 + Bestätigung -> OPEN/ADD/Re-Add
- W3 exhaustion + Ermüdung -> PARTIAL_REDUCE
- W5 completion risk + negative Bestätigung -> größere Reduce/Exit-Prüfung

Kein mechanisches W2=BUY / W5=SELL.

Swing gegen No-Swing inklusive Gebühren, Spread/Slippage soweit material, Re-Entry, verpassten Rebounds und Exposure-Opportunity-Cost testen. Steuer-/Realisierungseffekte nur soweit belastbar modellierbar.

## Decision-State-Katalog

`configs/decision_state_catalog_v1.json`

Enthält 24 kanonische Fälle.

- `contract_deterministic`: semantische Regeln, die schon vor Policy-Research gelten.
- `research_pending_policy`: Forschungsfälle, deren finales BUY/HOLD/SELL noch nicht vorweggenommen wird.

Katalog wird parametrisch getestet.

## Teilphasen

- 7A Input Contract, Coverage & State Semantics
- 7B Frozen Baseline & Decision Research Dataset
- 7C Conflict / Confirmation Research
- 7D Universal Stance Policy
- 7E Transition / Hysteresis
- 7F Portfolio Action Overlay & Swing Management
- 7G Decision Reliability & Explainability
- 7H Depot-Watch Integration
- 7I Final Validation & Promotion

## Vorbereitete Dateien

- `docs/decision_layer_7_foundations.md`
- `docs/decision_layer_7_research_plan.md`
- `docs/module7_decision_layer_handover.md`
- `configs/decision_layer_7_contract_v1.json`
- `configs/decision_layer_7_output_schema_v1.json`
- `configs/decision_state_catalog_v1.json`
- `tests/test_decision_layer_7_foundations.py`

## Startauftrag nach Phase 6

1. Phase 6 vollständig abschließen und finalen Output-Vertrag einfrieren.
2. Foundation-Branch gegen aktuellen `main` vergleichen.
3. Foundations auf frischen Phase-7-Arbeitsbranch übernehmen/rebasen.
4. 7A starten und finale Upstream-Verträge einlesen.
5. HOLD-/NO_ACTION-/INSUFFICIENT-Semantik und State Catalog zuerst als Contract-Tests ausführen.
6. Frozen pre-Phase-7 Baseline erzeugen.
7. Danach Decision Research Dataset und Conflict/Confirmation Research.
8. Keine Phase-8-Daten vorziehen.

Ziel: Das gesamte System erzeugt aus getrennten, empirisch geprüften Sensoren eine verständliche, reproduzierbare und depotkontextuell umsetzbare Handlungsaussage, ohne die wissenschaftliche Trennung der Module aufzugeben.
