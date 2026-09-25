# Projektübergabe – Trading-Zentrale / Scanner-vNext

## Phase 8: External Evidence Layer

Untertitel: Fundamentals, Revisions, Positioning, Events & Macro Context

Status: Foundation vorbereitet. Eigentliche Implementierung nach Abschluss von Phase 7.

Repository: `grisuweimar-crypto/trading-zentrale`

Foundation-Branch: `phase8-external-evidence-foundations`

## Leitfrage

Welche außerhalb der bisherigen Preis-/Scannerarchitektur liegenden Informationen liefern Point-in-Time einen eigenständigen, reproduzierbaren und Out-of-Sample bestätigten Zusatznutzen gegenüber dem eingefrorenen Phase-7-Core?

## Verbindliche Grenzen

- Externe Evidenz bleibt eigene Evidence Family.
- Selection, Timing, Probability, Risk, Confidence und Elliott werden nicht umdefiniert.
- UNKNOWN ist nicht neutral.
- Kein aktueller Konsens, Restatement oder Macro-Wert wird rückprojiziert.
- Jede Quelle benötigt PIT-/Coverage-/Lizenz-/Vintage-Provenance.
- Jede Familie wird einzeln gegen den Phase-7-Core getestet.
- Kein Family-Stacking vor erfolgreichem Single-Family-Research.
- Keine Promotion aus Discovery allein.
- Kein Holdout-Tuning.
- Keine direkte BUY/HOLD/SELL- oder Orderentscheidung aus externer Evidenz.

## Evidenzfamilien

1. Fundamentals / Earnings Change
2. Analyst Estimates / Revisions
3. Positioning / Crowding
4. Structured Events / News
5. Macro / External Exposure

## Datenstatus

- KNOWN
- UNKNOWN
- NOT_APPLICABLE
- STALE
- LICENSED_OUT
- LOW_COVERAGE
- CONFLICTING_SOURCES

## Promotion Gates

- PIT
- Coverage
- Incremental Value
- Stability
- Robustness
- Cost
- Multiple Testing

Alle Gates müssen für eine produktive Integration bestanden sein.

## Teilphasen

- 8A External Source & PIT Contract
- 8B Revisions Single-Family Pilot
- 8C Fundamentals & Structured Corporate Events
- 8D Positioning / Crowding
- 8E Structured Events & News
- 8F Macro & Exposure Context
- 8G Incremental Evidence Research
- 8H Cross-Factor Interaction
- 8I Decision Layer Extension

## Startauftrag

1. Phase 7 vollständig abschließen und Phase-7-Core einfrieren.
2. Foundation-Branch gegen den dann aktuellen `main` prüfen.
3. Foundations auf frischen Phase-8-Arbeitsbranch übernehmen.
4. 8A beginnen: Quelleninventar und vollständigen PIT-Vertrag real prüfen.
5. Noch keine Datenquelle aufgrund theoretischer Attraktivität integrieren.
6. Revisions als ersten Single-Family-Piloten priorisieren, falls 8A eine saubere und wirtschaftlich vertretbare Historie findet.
7. Andernfalls Fundamentals/Structured Corporate Events vorziehen.
8. Jede Familie gegen Phase 7 allein testen.
9. Cross-Factor-Research erst nach Einzelpromotion.
10. Decision Layer erst in 8I erweitern.

## Vorbereitete Dateien

- `docs/external_evidence_8_foundations.md`
- `docs/external_evidence_8_research_plan.md`
- `docs/module8_external_evidence_handover.md`
- `configs/external_evidence_8_contract_v1.json`
- `configs/external_source_registry_schema_v1.json`
- `configs/external_conflict_matrix_v1.json`
- `tests/test_external_evidence_8_foundations.py`
