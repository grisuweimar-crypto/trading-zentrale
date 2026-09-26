# QM / Research Governance Foundations

Status: Foundation-only. Keine produktive Integration. Start der eigentlichen QM-Umsetzung erst nach Abschluss von Phase 8 und erneuter Synchronisierung mit dem dann aktuellen `main`.

## Zweck

Dieser QM-Track ist keine neue Signalphase und keine nachträgliche Ergebnisoptimierung. Er dient als methodisches Qualitätsmanagement für die bereits aufgebaute Scanner-vNext-Architektur.

Die Grundlage sind:
- interne Architektur- und Codeprüfung,
- externe Zweitprüfungen durch Perplexity und DeepSeek,
- erkannte Unsicherheiten zu Multiplikität, Abhängigkeiten, Survivorship, Probability-Kalibrierung, menschlichem Evidenzverbrauch und Elliott-Zusatznutzen,
- Fehler/Beinahefehler/Inkonsistenzen aus der bisherigen Entwicklung,
- die neue Gefahr verdeckter informationeller Doppelzählung in einer mehrstufigen Architektur,
- Falsifikationsbedarf durch Negative Controls.

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

Besonders wichtig: B5 vs B6 muss paarweise auf denselben Claims/Datumsständen verglichen werden und ein Lineage-Check muss bestätigen, dass beide Varianten außerhalb des registrierten Elliott-Adjustments identisch sind.

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

### QM-H – Defect / Near-Miss / CAPA Management
QM erfasst nicht nur Methodenrisiken, sondern auch tatsächliche Fehler und Beinahefehler.

Klassen:
- DEFECT,
- NEAR_MISS,
- INCONSISTENCY,
- DEVIATION,
- DATA_QUALITY_EVENT,
- METHODOLOGY_RISK,
- OBSERVATION.

Lifecycle:
`DETECT -> CONTAIN -> ANALYZE -> CORRECT -> PREVENT -> VERIFY -> CLOSE`

Ein Fall darf erst geschlossen werden, wenn die Korrektur oder Präventionsmaßnahme überprüft wurde. Wiederkehrende Fehler erzwingen eine systemische Root-Cause-Prüfung statt nur weiterer Einzelpatches.

### QM-I – Evidence Lineage & Double-Counting Audit
Dies ist eine eigene Abstammungs- und Informationsabhängigkeitsprüfung, keine weitere Performance-Statistik.

Maschinenlesbar wird ein gerichteter Graph aufgebaut:
`raw feature -> derived metric -> research claim -> calibration/context/reliability -> decision usage`

Beispielhafte Fragen:
- steckt RS3M bereits im Scanner Score und wird später scheinbar erneut als unabhängige Evidenz gezählt?
- steckt Risk bereits in der Score-/Selection-Logik und erscheint zusätzlich als separater Risk Context?
- steckt Regime indirekt bereits in Selection?
- verwendet Confidence/Agreement Selection erneut und erzeugt dadurch scheinbare zweite Bestätigung?
- wird Probability fälschlich als zusätzlicher directional vote interpretiert?
- sind B5 und B6 tatsächlich identisch außer Elliott?

Wichtig: Ein gemeinsamer Rohdaten-Vorfahre ist ein Double-Counting-Review-Trigger, aber nicht automatisch der Beweis, dass zwei abgeleitete Signale identisch oder wertlos sind. Entscheidend ist, ob sie als unabhängige Evidenz gezählt werden dürfen.

### QM-J – Negative Controls / Falsification
Die Pipeline wird gezielt mit Kontrollen konfrontiert, die keinen echten Vorhersageeffekt tragen sollten.

Mögliche Control-Familien:
- zeitlich verschobene Signale,
- block-erhaltende Zeitpermutation,
- Symbolpermutation innerhalb desselben Datums,
- Feature-Permutation bei erhaltener Missingness,
- zufällige Pseudo-Events,
- bewusst irrelevante Features.

Regeln:
- ausschließlich isolierte Research-Kopie; niemals kanonische Artefakte verändern,
- erwartetes Nullverhalten vorab definieren,
- Negative Controls müssen nicht exakt Null ergeben,
- relevante Zeit-/Querschnittsstruktur möglichst erhalten,
- auffällig starke Placebo-Effekte blockieren eine confirmatory Promotion bis Leakage/Abhängigkeit/Pipeline geprüft ist,
- Controls dürfen nicht so lange verändert werden, bis sie endlich „bestehen“.

Bewusst falsche Symbolzuordnung ist nur als isolierter Integritäts-/Fehlertest zulässig und kein regulärer statistischer Negative Control, weil sie Datenverträge absichtlich verletzt.

## Nicht Teil des unmittelbaren QM-Core

- Dynamische Portfolio-Korrelation wird als späterer Portfolio-Risk-Overlay geführt.
- Elliott × External Evidence bleibt Phase 8H und setzt die Einzelpromotion der externen Familie voraus.
- Feste CRV-, Stop- oder Positionsgrößenregeln werden nicht aus fremden Quellen übernommen.

## Reihenfolge nach Abschluss von Phase 8

1. QM-Branch auf den finalen Phase-8-`main` synchronisieren.
2. Audit: Welche QM-Anforderungen wurden in Phase 8 bereits umgesetzt?
3. QM-A als Governance-Basis umsetzen.
4. QM-H als dauerhaftes Fehler-/CAPA-System aktivieren.
5. QM-B und QM-C aufbauen: historisches Universe + Hypothesen-/Multiplicity-Registry.
6. QM-I Evidence Lineage erstellen, bevor weitere Layer als vermeintlich unabhängige Evidenz bewertet werden.
7. QM-D Dependence/Effective-N ergänzen.
8. QM-J Negative Controls/Falsification gegen die Research-Pipeline laufen lassen.
9. QM-E Probability Calibration prüfen.
10. QM-F als prospektiven Shadow-/Ablation-Track aufbauen.
11. QM-G als registrierten Elliott-Challenger-Track durchführen.
12. Erst danach entscheiden, welche Befunde echte Code-/Policy-Änderungen rechtfertigen.

QM-H läuft ab Aktivierung querschnittlich weiter und endet nicht mit einem einzelnen Paket.

## Abnahmekriterium

QM gilt nicht als erfolgreich, weil mehr Metriken existieren. Es gilt als erfolgreich, wenn für jede relevante zukünftige Forschungsentscheidung nachvollziehbar ist:
- welche Hypothese vorlag,
- welche Daten damals verfügbar waren,
- ob Evidenz bereits verbraucht war,
- welches Universe tatsächlich galt,
- welche Abhängigkeiten bestanden,
- wie viele Hypothesen tatsächlich geprüft wurden,
- woher jede verwendete Information abstammt,
- ob vermeintlich unabhängige Evidenz gemeinsame Informationsquellen hat,
- ob die Pipeline kontrollierte Nullsignale korrekt als Null/unsicher behandelt,
- welche Fehler/Beinahefehler erkannt und verifiziert geschlossen wurden,
- und ob eine spätere Promotion auf genuinely later evidence beruht.
