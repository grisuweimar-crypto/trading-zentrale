# Phase 8 – External Evidence Layer

Untertitel: Fundamentals, Revisions, Positioning, Events & Macro Context

Status: Foundation-only. Phase 7A–7I ist technisch abgeschlossen und der Phase-7I-Validierungsvertrag wurde am 2026-09-25 eingefroren. Phase 8 bleibt bis zum gesonderten Startauftrag ohne produktive Integration.

## 1. Leitfrage

Phase 8 untersucht ausschließlich, ob externe Informationen außerhalb der bisherigen Preis-/Scannerarchitektur Point-in-Time einen eigenständigen, reproduzierbaren und Out-of-Sample bestätigten Zusatznutzen gegenüber dem eingefrorenen Phase-7-Core liefern.

Phase 8 ist ausdrücklich kein Feature-Creep-Projekt und erzeugt keinen neuen Super-Score.

Verbindliche Baseline ist der eingefrorene Phase-7-Core unter `configs/decision_validation_promotion_v1.json`:
- Freeze: `2026-09-25`
- `prospective_unspent` ab: `2026-09-26`

Phase 8 darf die Phase-7-Prospektivevidenz nicht durch rückwirkende Regeländerungen entwerten.

## 2. Evidenzfamilien

1. Fundamentals / Earnings Change
2. Analyst Estimates / Revisions
3. Positioning / Crowding
4. Structured Events / News
5. Macro / External Exposure

Revisions werden als erste Single-Family-Priorität behandelt, sofern 8A eine belastbare historische Datenquelle findet.

## 3. External Evidence bleibt eigene Familie

Externe Evidenz darf Selection, Timing, Probability, Risk, Confidence oder Elliott nicht rückwirkend umdefinieren.

Phase 8 erzeugt einen separaten Block `external_evidence`, der später kontrolliert in den Decision Layer eingespeist wird.

## 4. Vollständiger PIT-Vertrag

Jede externe Beobachtung muss – soweit für die Datenfamilie anwendbar – folgende Provenance-Felder führen:
- `event_time`
- `published_at`
- `ingested_at`
- `valid_from`
- `revision_id`
- `source`
- `license`
- `restatement_policy`
- `vintage`

Familien-spezifische PIT-Regeln:
- Fundamentals: preliminary/final/restated trennen; keine heutige Restatement-Version rückprojizieren.
- Revisions: historische Konsens-Snapshots bzw. Vintages; aktueller Konsens niemals rückprojizieren.
- Short Interest: Settlement Date und Publication Date getrennt führen; nutzbar erst ab Veröffentlichung.
- Macro: Release-Vintage und spätere Revisionen getrennt halten.
- Events/News: First Public Release und Source Hierarchy dokumentieren.

Wenn eine Quelle diese Kausalität nicht garantieren kann, ist sie nicht Phase-8-Core-fähig.

## 5. Statusmodell

Externes Evidence-Statusmodell:
- `KNOWN`
- `UNKNOWN`
- `NOT_APPLICABLE`
- `STALE`
- `LICENSED_OUT`
- `LOW_COVERAGE`
- `CONFLICTING_SOURCES`

`UNKNOWN`, `LOW_COVERAGE`, `STALE`, `LICENSED_OUT` und `CONFLICTING_SOURCES` dürfen niemals still zu neutraler Evidenz werden.

Zusatzfelder:
- `coverage`
- `freshness_days`
- `provenance_quality`
- `validated_domain`
- `source_count`
- `reason_codes`

Ein numerisches Quellen-Confidence-Feld darf nur als interne Datenqualitätsmetrik verwendet werden und ist kein Richtungs-Vote.

## 6. Coverage, Universe und Domain Gating

Jede Familie muss explizit dokumentieren:
- geografische Abdeckung,
- Börsen-/Instrumentabdeckung,
- Historientiefe,
- Frequenz,
- Verzögerung,
- Lizenzgrenzen,
- validierte Domänen.

Zusätzlich ist für Research ein As-of-Universe-/Coverage-Ledger erforderlich. Es muss mindestens historische Zugehörigkeit, Aufnahme-/Ausschlussgrund, Listing-/Delisting-Status und Datenverfügbarkeit am jeweiligen Zeitpunkt nachvollziehbar machen. Aktuelle Universe-Mitgliedschaft darf nicht rückwirkend als historische Mitgliedschaft verwendet werden.

Eine Familie darf außerhalb ihrer validierten Domäne nicht als bekannte neutrale Evidenz dargestellt werden.

## 7. Promotion Gates vor Integration

Jede externe Familie muss vor Integration in Phase 7 mindestens folgende Gates bestehen:

### PIT Gate
Keine mögliche Look-ahead-Nutzung; historische Vintages rekonstruierbar.

### Coverage Gate
Ausreichende Abdeckung in einer klar definierten Domäne; Missingness dokumentiert.

### Incremental Gate
Messbarer zusätzlicher Out-of-Sample-Nutzen gegenüber `Phase7 Core` allein.

### Stability Gate
Effekt nicht ausschließlich auf einen engen Zeitraum oder ein einziges Marktregime konzentriert.

### Robustness Gate
Moderate Datenlücken oder realistische Qualitätsverschlechterungen zerstören den Effekt nicht vollständig.

### Cost Gate
Datenbeschaffungs- und gegebenenfalls handelsinduzierter Zusatzaufwand stehen im Verhältnis zum Netto-Mehrwert.

### Multiple-Testing Gate
Hypothesen vorab registriert; FDR-/Multiplicity-Kontrolle bzw. andere geeignete Schutzmechanismen verwendet; keine unkontrollierte Feature-Suche.

Kein bestandener Gate = keine Integration.

## 8. Research Governance / Evidence Consumption

Für Phase 8 gilt zusätzlich:
- Hypothesen werden nach Familie registriert und als exploratory oder confirmatory gekennzeichnet.
- Confirmatory Regeln werden vor Betrachtung der zugehörigen zukünftigen Outcomes eingefroren.
- Menschliche Outcome-Inspektionen, die zu Designänderungen führen, werden protokolliert.
- Evidenz, auf deren Outcome-Basis Regeln oder Schwellen verändert wurden, wird als `spent_for_design` behandelt und darf nicht erneut als unabhängige Bestätigung gelten.
- Reine präventive QA-Änderungen ohne Betrachtung zukünftiger Outcomes können den `unspent`-Status erhalten, sofern dies dokumentiert ist.

## 9. Konfliktmatrix

Phase 8 modelliert externe Evidenz zunächst relativ zum Phase-7-Core, nicht als direkten Trade-Befehl.

Kanonische Relation:
- Core POSITIVE + External POSITIVE -> `CONFIRMING`
- Core POSITIVE + External NEGATIVE -> `CONFLICTING`
- Core POSITIVE + External UNKNOWN -> `UNKNOWN`
- Core NEGATIVE + External POSITIVE -> `CONFLICTING`
- Core NEGATIVE + External NEGATIVE -> `CONFIRMING`
- Core unresolved/insufficient + External known -> `EXTERNAL_ONLY`
- External mixed across families -> `MIXED_EXTERNAL`
- External coverage insufficient -> `INSUFFICIENT_EXTERNAL`

Diese Relation ist eine Beschreibung, keine bereits validierte Decision-Policy.

## 10. Decision Reliability Extension

Phase 8 darf die bestehende Phase-7-Reliability nicht überschreiben. Sie liefert zusätzliche strukturierte Reliability-Dimensionen:
- `core_evidence`
- `external_evidence`
- `external_coverage`
- `provenance_quality`
- `external_family_agreement`
- `core_external_conflict_severity`
- `walk_forward_support_external`
- `reason_codes`

Eine spätere kombinierte Decision Reliability darf nur in Phase 8I nach erfolgreicher Validierung erweitert werden.

## 11. Forschungshierarchie

1. Source/PIT feasibility
2. Single-family incremental test
3. Stability/robustness/domain checks
4. Promotion Gate
5. Erst danach Cross-Factor Interaction
6. Erst danach Decision-Layer-Extension

Nie mehrere neue Familien gleichzeitig einführen und anschließend versuchen, den Mehrwert rückwirkend zuzuordnen.

## 12. Priorisierung

Operative Reihenfolge:
1. 8A PIT- und Source Contract
2. Revisions Single-Family Pilot
3. Fundamentals + structured corporate events
4. Positioning/Crowding
5. Macro Exposure
6. breite News-/Event-Erweiterungen erst nach erfolgreicher strukturierter Basis

## 13. Phase-7-Grenze

Phase 7 bleibt eingefrorene Baseline.

Phase 8 darf nicht:
- Selection/Timing neu trainieren,
- Probability/Confidence umdefinieren,
- Risk semantisch verändern,
- Elliott als Directional Vote umdeuten,
- Portfolio-Action direkt überschreiben,
- Orders erzeugen.

Phase-8-Evidenz wird erst nach Promotion als eigener Evidence-Family-Block in den Decision Layer integriert.
