# QM / Research Governance Foundations

Status: Foundation-only. Keine produktive Integration. Start der eigentlichen QM-Umsetzung erst nach Abschluss von Phase 8 und erneuter Synchronisierung mit dem dann aktuellen `main`.

## Zweck

Dieser QM-Track ist keine neue Signalphase und keine nachträgliche Ergebnisoptimierung. Er dient als methodisches Qualitätsmanagement für die bereits aufgebaute Scanner-vNext-Architektur.

Die Grundlage sind:
- interne Architektur- und Codeprüfung,
- externe Zweitprüfungen durch Perplexity und DeepSeek,
- erkannte Unsicherheiten zu Multiplikität, Abhängigkeiten, Survivorship, Probability-Kalibrierung, menschlichem Evidenzverbrauch und Elliott-Zusatznutzen,
- Fehler/Beinahefehler/Inkonsistenzen aus der bisherigen Entwicklung,
- die Gefahr verdeckter informationeller Doppelzählung in einer mehrstufigen Architektur,
- Falsifikationsbedarf durch Negative Controls.

Die wichtigste gemeinsame Konsequenz der externen Zweitprüfung lautet: QM darf nicht nur dokumentieren. Wo möglich, müssen unzulässige Forschungswege durch maschinenprüfbare Zustände, unveränderliche Identitäten und Fail-closed-Gates technisch erschwert oder verhindert werden.

## Kontrollkern

### Evidence State Machine

Confirmatory Forschung erhält einen expliziten Lebenszyklus:

`DRAFT -> EXPLORATORY -> FROZEN_FOR_CONFIRMATION -> CONFIRMATORY_EVALUATED -> CONFIRMATORY_SPENT -> REPLICATION_PENDING -> PROSPECTIVE_SHADOW -> PROMOTION_ELIGIBLE -> PROMOTED / REJECTED / RETIRED / INVALIDATED`

Verbotene Rücksprünge, insbesondere von ausgewerteter Evidenz zurück zu `unspent`, müssen fail-closed behandelt werden.

### Immutable Analysis Identity

Confirmatory Evaluation und Promotion werden an unveränderliche Identitäten gebunden, u. a.:
- Hypothesen-/Analysis-Plan-Hash,
- Code-/Commit-/Config-Hash,
- Dataset-Snapshot,
- Universe-/Instrument-Master-Version,
- Label- und Benchmarkdefinition,
- Environment-/Dependency-Fingerprint,
- Evaluation Cohort und Zeitgrenze.

Mutable Alias-Namen ersetzen keine echte Versionsbindung.

### Evidence Consumption

Die bisherige Zweiteilung wird verschärft:

1. `MECHANICALLY_EQUIVALENT_REPAIR`
   - nachweislich keine Änderung an Estimand, Eligibility, Labels, Evidence Population oder Decision Rule.

2. `PREVENTIVE_QA_NEW_VERSION`
   - QA-/Architekturverbesserung ohne Performance-Motivation, aber nicht nachweislich mechanisch äquivalent.
   - neue Version und neue Evidence Boundary.

3. `OUTCOME_DRIVEN_RESEARCH_CHANGE`
   - Änderung nach sichtbaren Outcomes oder performance-revealing Artefakten.
   - betroffene Evidenz wird `spent_for_design`.

Im Zweifel gilt nicht "preventive", sondern neue Version.

Auch Aggregate, Charts, Dashboard-Ansichten, Reports oder Failure Summaries können Evidenz verbrauchen, wenn sie Performance verraten.

## QM-Arbeitsblöcke

### QM-A – Research Governance & Evidence Consumption
- Evidence State Machine,
- append-only Inspection-/Decision-Log,
- immutable Analysis Identity,
- Actor-/Role-/Access-Mode,
- Outcome Visibility Level,
- Evidence Effect,
- superseding versions statt In-place-Änderung,
- unabhängige Review-Rollen soweit praktikabel; gleiche Person muss ausdrücklich als nicht unabhängig markiert werden.

### QM-B – As-of Universe / Coverage / Survivorship / Investability
Zusätzlich zum historischen Universe-Ledger:
- stabiler Instrument Master,
- Symbolwechsel/Ticker-Reuse,
- Listing/Delisting/Suspension/Halt,
- tatsächliche Tradability,
- Exchange/Venue, Währung/Pair,
- Session Calendar,
- Corporate-Action-Knowledge-Time,
- Provider-Coverage-Historie,
- effektive historische Sector-/Domain-Provenance,
- Outcome-Availability-Ledger,
- bekannte Delistings/Censoring nie als generisches Missing behandeln.

Historische Universen müssen ohne heutige Metadaten rekonstruierbar sein.

### QM-C – Hypothesis Registry / Global Multiplicity / Analysis Plan
Eine confirmatory Hypothese friert nicht nur einen Satz ein, sondern den vollständigen Analysepfad:
- Universe-Version,
- Target/Label,
- Event Generation,
- Include/Exclude,
- Missingness Policy,
- Feature Transformation,
- Benchmark,
- Estimand,
- Primärmethode,
- Block-/Resampling-Parameter,
- Decision Metric,
- Promotion Rule,
- Subgroups/Sensitivities,
- Sequential Monitoring Plan,
- Multiplicity Family/Control,
- Code-/Config-/Data-Hash.

Zusätzlich wird ein Research Derivation Graph geführt:
`Research Question -> Family -> Hypothesis -> Version -> Analysis Plan -> Evaluation Run -> Evidence Artifact -> Decision/Promotion Outcome`

Near-duplicate Nachfolger müssen ihren Parent behalten; schwache/negative Hypothesen bleiben sichtbar. Wiederholte confirmatory Looks brauchen einen vorab festgelegten Zeitplan, Alpha-Spending oder eine andere vorab definierte Sequential-Control-Logik.

### QM-D – Dependence / Effective N / Robustness
Die bestehende horizonabhängige Moving-Block-Methodik bleibt Primary. Ergänzt werden getrennte Diagnosen für:
- temporal dependence,
- cross-sectional/date dependence,
- repeated-symbol dependence,
- sector/domain/market-factor dependence.

Kein einzelner "effective N"-Wert soll diese Struktur vortäuschen.

Zu berichten sind u. a. N events/dates/symbols/sectors, Konzentrationsanteile/-indizes sowie Leave-one-period/sector/large-symbol-out soweit möglich. Alternative Cluster-/Bootstrap-Verfahren bleiben vorregistrierte Sensitivität, nicht Model Selection.

### QM-E – Probability Calibration Audit
Weiterhin diagnostic-only vor jeder Retraining-Entscheidung.

Pflichtpunkte:
- Calibration Population explizit definieren,
- alle eligible Claims und selected Claims sind getrennte Estimands,
- horizon-spezifisch,
- block-aware uncertainty,
- Reliability Curve,
- Brier,
- Log Loss,
- Calibration-in-the-large/Intercept,
- Slope,
- Sharpness,
- Epoch-/Prequential-Sicht soweit Daten reichen.

Ein Recalibrator ist ein neues Modell und benötigt eigenen Train/Validation/Promotion-Vertrag.

### QM-F – Decision-Layer Incremental Ablation
B0 wird in Flat/Long getrennt.

Wichtig: Portfolio Actions sind zustands- und pfadabhängig. Ein Claim-Level-Pairing ist nach divergierenden Aktionen nicht mehr automatisch ein valider Paarvergleich.

B5 vs B6 darf erst interpretiert werden, wenn:
- gleiche Ausgangslage,
- gleiche Eligibility,
- gleiche Costs/Execution/Tradeability,
- gleiche Action Availability,
- QM-I Lineage Equality außer registriertem Elliott-Adjustment,
- und Policy Path Divergence explizit modelliert ist.

Zusätzliche externe/simple Benchmarks sind nur dann verpflichtend, wenn das vorab definierte Estimand tatsächlichen Policy Value/Return betrifft.

### QM-G – Elliott Challenger Registry
Neue Elliott-Ideen verändern zunächst nicht den eingefrorenen Phase-6-Core.

Count/Scenario muss vor Challenger-Outcome-Evaluation eingefroren sein. Challenger-Familien unterliegen eigener Multiplicity und müssen inkrementellen Nutzen conditional on frozen core zeigen.

### QM-H – Defect / Near-Miss / CAPA
Incident-Kategorien:
- DEFECT,
- NEAR_MISS,
- INCONSISTENCY,
- DEVIATION,
- DATA_QUALITY_EVENT.

`METHODOLOGY_RISK` und `OBSERVATION` werden als Findings getrennt geführt, nicht als Incidents.

Lifecycle:
`DETECT -> CONTAIN -> ANALYZE -> CORRECT -> PREVENT -> VERIFY -> CLOSE`

Canonical Data, Labels, PIT State oder Frozen Hypothesis korrumpiert => Evidence Impact Assessment plus Invalidierung/Reklassifikation.

Near Miss ohne Canonical Contamination muss Evidenz nicht automatisch reklassifizieren.

QM-H bleibt nach Start dauerhaft aktiv.

### QM-I – Evidence Lineage & Double-Counting Audit
Typed Provenance Graph statt bloßem Textdiagramm.

Node-Klassen u. a.:
- RAW_SOURCE,
- RAW_FEATURE,
- DERIVED_METRIC,
- COMPOSITE_SCORE,
- RESEARCH_CLAIM,
- CALIBRATION,
- RISK_CONTEXT,
- RELIABILITY_ANNOTATION,
- STRUCTURAL_CONTEXT,
- EXTERNAL_EVIDENCE,
- DECISION_USAGE.

Shared ancestry löst Review aus, ist aber keine automatische Invalidierung.

Zusätzlich gibt es ein Independence-Claims-Registry: Wer zwei gemeinsame Informationsquellen als unabhängig behandelt, muss diese Behauptung explizit registrieren.

Phase-8 External Evidence muss in denselben Lineage Graph aufgenommen werden.

B5/B6: nach Entfernung des registrierten Elliott-Subgraphs müssen die kanonisierten Restgraphen sowie Snapshot/Eligibility/Costs/Start-State identisch sein.

### QM-J – Negative Controls / Falsification
Pre-registered, isoliert, reproduzierbar und ohne Mutation kanonischer Daten.

Mindestkandidaten:
- deterministic temporal shift,
- within-symbol block permutation,
- within-date symbol permutation,
- feature permutation bei erhaltener Missingness/relevanter Struktur,
- pseudo-events mit passender Frequenz/Konzentration.

Wrong-entity mapping bleibt Pipeline-Integrity-Test, kein primärer statistischer Placebo-Test.

Ein einzelner zufälliger Placebo-Treffer ist nicht automatisch ein Systemfehler. Ein vorab definierter `SYSTEMATIC_PLACEBO_SIGNAL` blockiert Confirmatory Promotion bis zur Untersuchung.

Die konkrete statistische Failure Rule wird nicht heute erfunden, sondern muss im späteren Analysis Plan vor Sicht auf Placebo-Ergebnisse eingefroren werden.

## QM erzeugt selbst Degrees of Freedom

Auch Governance kann nach Ergebnis selektiert werden. Deshalb müssen bei confirmatory Arbeit auch folgende Entscheidungen eingefroren oder als exploratory gekennzeichnet werden:
- Multiplicity Family,
- Block-/Cluster-Sensitivitätsmenü,
- Calibration Population,
- Negative-Control-Battery und Failure Rule,
- Lineage-Grenzen/Materiality,
- Promotion Estimand und Threshold,
- Sequential Look Schedule.

## Reihenfolge / Dependencies nach Phase 8

QM ist kein rein linearer Fahrplan.

Dauerhaft ab QM-Start:
- QM-A,
- QM-H.

Parallel foundational:
- QM-B,
- QM-C.

Sobald B/C-Schemas stehen:
- QM-I beginnen; vor QM-F/QM-G-Interpretation muss QM-I blocking sein.

Danach:
- QM-D aus B/C,
- QM-E aus D,
- QM-J aus C/D/I,
- QM-F aus D/I,
- QM-G aus C/I.

Vor jedem Start zuerst:
1. QM-Branch auf finalen Phase-8-main synchronisieren.
2. Delta Audit gegen durch Phase 8 bereits gelöste Punkte.
3. Doppelmaßnahmen entfernen.

## Nicht Teil des unmittelbaren QM-Core

- Dynamische Portfolio-Korrelation bleibt späterer Portfolio-Risk-Overlay.
- Elliott × External Evidence bleibt Phase 8H und setzt die Einzelpromotion der externen Familie voraus.
- Feste CRV-, Stop- oder Positionsgrößenregeln werden nicht aus fremden Quellen übernommen.
- Conditional Mutual Information ist optionale Forschung, kein automatischer Lineage-Gate.
- Kein einzelner scalar Effective N als Promotion Shortcut.

## Abnahmekriterium

QM gilt nicht als erfolgreich, weil mehr Metriken oder Dokumente existieren. Es gilt als erfolgreich, wenn für jede relevante zukünftige Forschungsentscheidung maschinenprüfbar nachvollziehbar ist:
- welche Hypothese/Version vorlag,
- welcher Analysis Plan eingefroren war,
- welche Daten/Universe/Labels verfügbar waren,
- welche Artefakte angesehen wurden,
- ob Evidenz bereits verbraucht war,
- welche Informationsabstammung und Independence Claims vorlagen,
- welche Multiplicity-/Sequential-Look-Regeln galten,
- welche Abhängigkeiten bestanden,
- ob Incidents Evidenz beeinflusst haben,
- ob Negative Controls die Pipeline ausreichend herausgefordert haben,
- und ob eine Promotion auf genuinely later evidence und gültigen State Transitions beruht.
