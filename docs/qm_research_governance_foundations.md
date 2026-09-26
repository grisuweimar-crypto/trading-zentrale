# QM / Research Governance Foundations

Status: Foundation-only. Keine produktive Integration. Start der eigentlichen QM-Umsetzung erst nach Abschluss von Phase 8 und erneuter Synchronisierung mit dem dann aktuellen `main`.

## Zweck

Dieser QM-Track ist keine neue Signalphase und keine nachträgliche Ergebnisoptimierung. Er dient als methodisches Qualitätsmanagement für die bereits aufgebaute Scanner-vNext-Architektur.

Die Grundlage sind:
- interne Architektur- und Codeprüfung,
- externe Zweitprüfungen durch Perplexity und DeepSeek,
- erkannte Unsicherheiten zu Multiplikität, Abhängigkeiten, Survivorship, Probability-Kalibrierung, menschlichem Evidenzverbrauch und Elliott-Zusatznutzen.

## Zentrale Trennlinie

### Preventive QA Change
Eine Änderung entsteht aus Architektur-, Code-, Provenance- oder Methodenprüfung, ohne zukünftige Outcomes der betroffenen Hypothese zur Regelanpassung zu verwenden.

Sie darf den Status zukünftiger Evidenz grundsätzlich erhalten, muss aber protokolliert werden.

### Outcome-driven Research Change
Eine Änderung entsteht, weil zukünftige Performance/Outcomes betrachtet und daraufhin Regel, Schwelle, Feature oder Policy angepasst wurden.

Die betrachtete Evidenz wird für die geänderte Hypothese `spent_for_design` und darf die neue Regel nicht mehr als unspent confirmation bestätigen.

## QM-Arbeitsblöcke

### QM-A – Research Governance & Evidence Consumption
- append-only Inspection-/Decision-Log,
- spent/unspent-Status,
- Preventive-QA vs Outcome-driven Change,
- Code-/Config-Fingerprint pro Entscheidung,
- Verbot der stillen Wiederverwendung inspizierter Evidenz.

### QM-B – As-of Universe / Coverage / Survivorship
- historisches Universe-Ledger,
- Listing/Delisting/Suspension,
- Inclusion/Exclusion Reason,
- Exchange/Currency,
- Scanner-/Price-/Provider-Coverage,
- historische Sector-/Domain-Zuordnung mit `available_from`,
- Missing Feature getrennt von Missing Outcome.

### QM-C – Hypothesis Registry & Global Multiplicity
- eindeutige Hypothesen-ID,
- Evidence Family,
- Discovery vs Confirmatory,
- Freeze-Zeitpunkt,
- Horizon/Target/Universe,
- Multiplicity Family,
- vollständige Null-/Negativresultate aufbewahren,
- keine Auswahl einzelner Treffer aus einem größeren unsichtbaren Suchraum.

### QM-D – Dependence / Effective N / Robustness
Die bestehende horizonabhängige Moving-Block-Methodik wird nicht rückwirkend ersetzt. Ergänzt werden Diagnoseebenen:
- Date concentration,
- Symbol concentration,
- Sector/Domain concentration,
- Effective-N bzw. explizite Abhängigkeitsdiagnostik,
- Leave-one-sector/domain-out soweit Stichprobe reicht,
- alternative Blocklängen/Bootstrap-Verfahren nur als Sensitivität und niemals zur Auswahl des günstigsten Ergebnisses.

### QM-E – Probability Calibration Audit
Phase 2 unterscheidet robusten Probability Advantage bereits von Richtung. QM ergänzt echte Calibration-Diagnostik, soweit die Stichprobe reicht:
- Reliability Curve,
- Brier Score,
- Log Loss,
- Calibration Intercept/Slope,
- zeitliche Calibration nach Epoche.

Diese Diagnostik darf historische Claims nicht umdefinieren.

### QM-F – Decision-Layer Incremental Ablation
Nach Phase 8 kann zusätzlich ein Shadow-Vergleich aufgebaut werden. Er ersetzt Phase 7I nicht und darf dessen Evidenzstatus nicht umetikettieren.

Kandidaten:
- B0: bestehende Position unverändert / kein neuer Review,
- B1: Selection only,
- B2: Selection + eligible Timing ohne Decision-Hysterese,
- B3: Raw Universal Stance,
- B4: Universal Stance + Hysterese,
- B5: Portfolio Action Core ohne Elliott-Swing-Adjustment,
- B6: identischer Core mit Elliott-Swing-Adjustment.

Besonders wichtig: B5 vs B6 muss paarweise auf denselben Claims/Datumsständen verglichen werden.

### QM-G – Elliott Challenger Registry
Neue Elliott-Ideen verändern zunächst nicht den eingefrorenen Phase-6-Core.

Research-only Challenger:
- Wave Personality,
- W3 Momentum-/Volumenexpansion,
- W4 Volumen-/Seitwärtscharakter,
- W5 Momentum-/Volumen-Divergenz,
- Alternation Fit,
- Channel Fit / kausaler Channel Break,
- Deep-Correction Reclaim,
- Scenario Stability / Recount History,
- Ending-Diagonal-Reversal.

Grundsatz: Challenger starten als Soft-Evidence-Sidecar. Sie dürfen den Count nicht nachträglich so auswählen, dass sie sich selbst bestätigen.

## Nicht Teil des unmittelbaren QM-Core

- Dynamische Portfolio-Korrelation wird als späterer Portfolio-Risk-Overlay geführt.
- Elliott × External Evidence bleibt Phase 8H und setzt die Einzelpromotion der externen Familie voraus.
- Feste CRV-, Stop- oder Positionsgrößenregeln werden nicht aus fremden Quellen übernommen.

## Reihenfolge nach Abschluss von Phase 8

1. QM-Branch auf den finalen Phase-8-`main` synchronisieren.
2. Audit: Welche QM-Anforderungen wurden in Phase 8 bereits umgesetzt?
3. QM-A bis QM-C zuerst umsetzen, weil sie alle späteren Forschungsschritte kontrollieren.
4. QM-D und QM-E als statistische Diagnoseerweiterung.
5. QM-F als prospektiver Shadow-/Ablation-Track.
6. QM-G als registrierter Challenger-Track.
7. Erst danach entscheiden, welche Befunde echte Code-/Policy-Änderungen rechtfertigen.

## Abnahmekriterium

QM gilt nicht als erfolgreich, weil mehr Metriken existieren. Es gilt als erfolgreich, wenn für jede relevante zukünftige Forschungsentscheidung nachvollziehbar ist:
- welche Hypothese vorlag,
- welche Daten damals verfügbar waren,
- ob Evidenz bereits verbraucht war,
- welches Universe tatsächlich galt,
- welche Abhängigkeiten bestanden,
- wie viele Hypothesen tatsächlich geprüft wurden,
- und ob eine spätere Promotion auf genuinely later evidence beruht.
