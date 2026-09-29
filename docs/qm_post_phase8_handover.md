# Projektübergabe – QM nach Phase 8

Repository: `grisuweimar-crypto/trading-zentrale`

Foundation-Branch: `qm-research-governance-foundations`

Status: vorbereitet, nicht produktiv, nicht vor Abschluss von Phase 8 mergen.

## Startbedingung

QM beginnt erst, wenn Phase 8 technisch abgeschlossen ist. Dann zuerst:
1. aktuellen `main` bestimmen,
2. diesen Branch darauf synchronisieren,
3. Phase-8-Ergebnisse gegen die QM-Anforderungen abgleichen,
4. doppelte oder inzwischen erledigte Maßnahmen entfernen,
5. erst danach QM-A/H aktivieren.

## Audit-Konsens aus Perplexity + DeepSeek

Beide unabhängigen Zweitprüfungen sehen denselben Kernpunkt: Der QM-Entwurf ist methodisch schlüssig, aber zentrale Regeln dürfen nicht nur dokumentiert oder selbst berichtet werden. Confirmatory Forschung benötigt maschinenprüfbare Zustände, unveränderliche Analysis Identity, nachvollziehbaren Evidenzzugriff und Fail-closed-Gates.

Daraus wurden folgende Foundation-Änderungen übernommen:
- Evidence State Machine,
- immutable Analysis/Data/Code/Label/Universe Identity,
- feinere Evidence-Consumption-Klassen und Access Modes,
- neue Version bei nicht nachweislich mechanisch äquivalenten QA-Änderungen,
- Investability + Outcome-Availability zusätzlich zu Universe/Survivorship,
- vollständiger Analysis-Plan-Freeze in QM-C,
- Research Derivation Graph,
- Sequential-Look-/Alpha-Spending- oder gleichwertige vorregistrierte Kontrolle,
- getrennte Abhängigkeitsachsen statt eines einzigen Effective-N-Werts,
- Calibration Population + block-aware Calibration Audit,
- stateful/path-dependent Policy Evaluation in QM-F,
- Incident/Findings-Trennung in QM-H,
- verpflichtender typed Provenance Graph + Independence Claims in QM-I,
- pre-registered Negative-Control Failure Rule in QM-J.

Nicht ungeprüft übernommen wurde eine konkrete DeepSeek-Placebo-Schwelle (z. B. bestimmter KS-/Bonferroni-Test). Der Foundation-Vertrag verlangt stattdessen, dass die konkrete Failure Rule vor Sicht auf die Placebo-Ergebnisse im späteren Analysis Plan eingefroren wird.

## Arbeitsstruktur

QM ist kein rein linearer Ablauf.

Dauerhaft ab QM-Start:
- QM-A Research Governance / Evidence Consumption / State Machine
- QM-H Defect / Near-Miss / CAPA

Parallel foundational:
- QM-B As-of Universe / Investability / Outcome Availability
- QM-C Hypothesis Registry / Multiplicity / Analysis Plan / Sequential Monitoring

Sobald B/C-Schemas stehen:
- QM-I Evidence Lineage / Double Counting beginnen

Danach / abhängig:
- QM-D benötigt B/C
- QM-E benötigt D
- QM-J benötigt C/D/I
- QM-F benötigt D/I
- QM-G benötigt C/I

QM-I muss vor Interpretation von B5/B6 oder neuen Elliott-Challengern als Blocking Gate wirksam sein.

## Wichtige Schutzgrenzen

- Keine rückwirkende Änderung historischer PIT-Daten.
- Keine Reparatur fehlender Provenance durch Erfindung.
- Keine Rückstufung ausgewerteter Evidenz auf unspent.
- Kein In-place-Umschreiben eingefrorener Registry-Einträge; nur superseding versions.
- Nicht nachweislich mechanisch äquivalente QA-Änderung => neue Version / neue Evidence Boundary.
- Aggregate/Charts/Dashboards/Reports können Evidenz verbrauchen.
- Keine heutige Universe-/Sector-/Provider-Metadaten-Rückprojektion.
- Delisting/Suspension/Censoring darf nicht als generisches Missing verschwinden.
- Keine Auswahl von Bootstrap/Cluster/Block-/Calibration-/Placebo-Verfahren anhand des schönsten Ergebnisses.
- Keine automatische Recalibration aus QM-E.
- Keine automatische Invalidierung nur wegen gemeinsamer Lineage-Ancestry.
- Keine Interpretation B6-B5 ohne Lineage Equality und stateful policy evaluation.
- Keine Placebo-Retuning-Schleife nach Sicht auf Resultate.

## QM-B Mindestziel

Historisch muss rekonstruierbar sein:
- welches stabile Instrument gemeint war,
- unter welchem Symbol / Venue / Currency-Pair,
- ob es existierte,
- ob es handelbar war,
- ob der Provider es abdeckte,
- ob Scanner-/Preis-/Outcome-Daten verfügbar waren,
- ob es suspendiert/delistet/mergered/migriert war,
- und warum ein Outcome vorhanden, censored oder fehlend ist.

## QM-C Mindestziel

Ein confirmatory Claim ist nur dann eingefroren, wenn nicht nur die verbale Hypothese, sondern der komplette Analysis Plan gebunden ist: Universe, Target, Event Rule, Filter, Missingness, Benchmark, Estimand, Inference, Resampling, Decision Metric, Promotion Rule, Planned Sensitivities, Sequential Looks, Multiplicity Scope und Hash Bundle.

## QM-I Mindestziel

Für jede relevante Information muss maschinenlesbar nachvollziehbar werden:
`raw source -> raw feature -> derived metric -> composite/claim -> calibration/context/reliability -> decision usage`.

Gemeinsame Abstammung ist ein Review-Trigger, kein automatischer Fehler. Unabhängigkeit muss bei gemeinsamem Ursprung explizit behauptet und begründet werden.

B5/B6: nach Entfernung des registrierten Elliott-Adjustments müssen Rest-Lineage, Snapshot, Eligibility, Costs und Starting State identisch sein, bevor der Unterschied als Elliott-Zusatznutzen interpretiert wird.

## QM-J Mindestziel

Kann die Pipeline struktur-erhaltende Placebos korrekt als Null/unsicher behandeln?

Mindestkandidaten:
- temporal shift,
- within-symbol block permutation,
- within-date symbol permutation,
- missingness-/struktur-erhaltende feature permutation,
- frequency/concentration-matched pseudo-events.

Wrong-entity mapping bleibt isolierter Pipeline-Integrity-Test.

Ein einzelner Placebo-Treffer ist kein automatischer Systemfehler. Ein vorab definierter systematischer Placebo-Fail blockiert Promotion bis zur Untersuchung.

## Nicht ungeprüft übernehmen

Folgende Kritik bleibt zu pauschal oder wurde bereits widerlegt/qualifiziert:
- Phase-5-Leak allein durch Maturity Cutoff,
- horizon-langer Event-Cooldown als notwendige Voraussetzung für Unabhängigkeit,
- zwei 5T-Timing-Treffer seien allein wegen 96 Mustern automatisch Zufall,
- Regime müsse zwingend als neuer directional vote in den Scanner,
- ein einzelner scalar Effective N löse Panel-Abhängigkeit,
- gemeinsame Rohdaten-Ancestry bedeute automatisch Double Counting.

Diese Punkte dürfen als Sensitivitäts-/Auditfrage untersucht werden, aber nicht als bereits feststehender Fehler behandelt werden.
