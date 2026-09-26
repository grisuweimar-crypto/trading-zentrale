# Phase 7 – Interpretation / Decision Layer: Research- und Bauplan

Status: vorbereitet. Start der eigentlichen Implementierung erst nach vollständigem Abschluss von Phase 6 Elliott vNext.

## Ziel

Phase 7 übersetzt die getrennt validierten Upstream-Sensoren in eine reproduzierbare, erklärbare Entscheidungsschicht. Sie baut keinen neuen Opportunity-Score und optimiert keine früheren Phasen nachträglich.

Vor dem ersten produktiven Decision-Policy-Code müssen die Semantiken von `HOLD`, `NO_ACTION` und `INSUFFICIENT_EVIDENCE` vollständig testbar sein.

## 7A – Input Contract, Coverage & State Semantics

Ziel:
- finale Output-Verträge der Phasen 1–6 erfassen,
- Point-in-Time-Verfügbarkeit und Versionen pro Block dokumentieren,
- ein unverändertes Evidence Bundle pro Titel erzeugen,
- fehlende Evidenz als `insufficient` statt neutral behandeln,
- Universal-HOLD semantisch differenzieren,
- Portfolio-Constraints strikt aus Universal Stance heraushalten.

Verbindliche HOLD-Untertypen:
- `HOLD_CONSTRUCTIVE`
- `HOLD_NEUTRAL`
- `HOLD_UNRESOLVED`

Wichtig:
- `HOLD_PORTFOLIO_CONSTRAINED` ist als Universal-Stance-Untertyp verboten.
- Ein universelles `BUY` bei zu hoher Depotkonzentration bleibt `BUY`; das Portfolio Overlay erzeugt `NO_ACTION`/`HOLD` mit passendem Portfolio-Reason-Code.

`INSUFFICIENT_EVIDENCE` erhält Ursachen-Codes:
- `INSUFFICIENT_DATA`
- `INSUFFICIENT_MODEL_COVERAGE`
- `INSUFFICIENT_CONSENSUS`
- `INSUFFICIENT_VALIDATION`
- `OUTSIDE_VALIDATED_DOMAIN`
- `STALE_OR_INCOMPATIBLE_INPUT`
- `INPUT_CONTRACT_VIOLATION`

Vorläufige Mindest-Trigger werden nicht als Performance-Schwellen erfunden. Vertragliche Fälle wie inkompatible Versionen können deterministisch fail-closed sein; empirische Mindesthistorie/Coverage wird erst anhand der Daten eingefroren.

Abnahme:
- ein Titel kann vollständig auf seine Input-Snapshots/Versionen zurückgeführt werden,
- Portfolioinformationen fehlen vollständig im Universal-Stance-Input,
- jedes Universal-HOLD besitzt eine zulässige Detailsemantik,
- jedes `INSUFFICIENT_EVIDENCE` besitzt mindestens einen Ursache-Code,
- Portfolio-Constraints verändern den Universal Stance nicht.

## 7B – Frozen Baseline & Decision Research Dataset

Ziel:
- den letzten pre-Phase-7 Zustand einfrieren,
- historische Decision-Research-Tabelle aus damals verfügbaren Upstream-Outputs erzeugen,
- aktuelle manuelle/regelbasierte Depot-Watch-Entscheidung soweit reproduzierbar als Vergleichsbasis dokumentieren.

Zu erfassen:
- Selection-/Timing-Zustände,
- Probability-Kalibrierung,
- Risk,
- Confidence,
- Phase-5 Adaptive State,
- Phase-6 Elliott/Structure State,
- Markt-/Sektorkontext nur PIT/quality-gated,
- spätere Outcomes getrennt vom Input.

Abnahme:
- keine Zukunftsinformation in Features,
- Baseline vor Policy-Suche eingefroren,
- Outcomes lassen sich nicht versehentlich als Input joinen.

## 7C – Conflict / Confirmation Research

Ziel:
- Konfluenz und Konflikte zwischen Modulen explizit untersuchen,
- Redundanz und Doppelzählung vermeiden,
- Probability, Risk und Confidence gemäß ihrer Semantik behandeln,
- Konfliktarten vor der eigentlichen Stance Policy definieren.

Mindestens zu unterscheiden:
- `SELECTION_TIMING_CONFLICT`
- `DIRECTION_STRUCTURE_CONFLICT`
- `PRIMARY_ALTERNATIVE_STRUCTURE_CONFLICT`
- `MARKET_CONTEXT_DIVERGENCE`
- `RELATIVE_STRENGTH_DIVERGENCE`
- `RISK_CONSTRAINT_NOT_DIRECTIONAL_CONFLICT`
- `RELIABILITY_OR_COVERAGE_CONFLICT`
- `DATA_OR_VERSION_CONFLICT`

Research-Fragen:
- Wann verbessert Timing eine starke Selection tatsächlich?
- Wann ist starke Selection bei schlechtem Timing nur HOLD statt BUY?
- Wann ist Elliott-Rescue nach schwacher Scannerlage nützlich oder gefährlich?
- Wann ist W5-/W3-Exhaustion trotz starker Selection ein valider Reduce-Kontext?
- Welche Primary/Alternative-Count-Konflikte sind handlungsrelevant?
- Wie stark verschlechtert geringe Decision Reliability die Trennschärfe?
- Welche Konflikte sind nur Rauschen, welche systematisch?

Harte Semantik:
- Risk darf Richtung nicht allein invertieren.
- Niedrige upstream Confidence ist keine bearish Evidenz.
- Probability bleibt Kalibrierung und wird nicht als dritter Richtungs-Vote gezählt.

Abnahme:
- jeder Konfliktfall ist typisiert,
- Risk-Constraint und Richtungs-Konflikt werden nicht vermischt,
- Conflict Taxonomy ist vor 7D eingefroren.

## 7D – Universal Stance Policy

Ziel:
- pro Universe-Titel `BUY`, `HOLD`, `SELL` oder `INSUFFICIENT_EVIDENCE` erzeugen.

Wichtig:
- kein Portfolioinput,
- keine festen Modulgewichte ohne Evidenz,
- kein neuer 0–100-Super-Score als Startpunkt,
- Policy muss Gründe und Gegenevidenz ausgeben,
- `HOLD` muss einen Detailzustand tragen,
- `INSUFFICIENT_EVIDENCE` muss Ursache(n) tragen.

Kandidaten für interpretable Policy-Familien:
- hierarchische Gate-/Rule-Policy,
- monotone/constraint-basierte Entscheidungsbäume,
- andere interpretable Modelle nur wenn sie klaren Out-of-sample-Mehrwert zeigen.

Nicht erlaubt:
- Blackbox-Modell ohne nachvollziehbare Gründe,
- upstream Module in Phase 7 neu trainieren.

Abnahme:
- vollständige Coverage des validierten Universums oder explizites `INSUFFICIENT_EVIDENCE`,
- gleiche Inputs/Versionen erzeugen reproduzierbar denselben Stance,
- Policy schlägt Baseline nicht nur in Return, sondern auch unter Kosten/Stabilität oder wird nicht promotet.

## 7E – Transition / Hysteresis

Ziel:
- tägliche Zustandswechsel kontrollieren,
- Signalflattern von echten Zustandswechseln unterscheiden.

Drei Mechanismen werden getrennt getestet:
1. `confirmation_window`: neuer Zustand benötigt Bestätigung über N Beobachtungen.
2. `evidence_margin`: neue Evidenz muss die bisherige Entscheidung ausreichend übertreffen.
3. `exception_override`: harte Ereignisse dürfen Hysterese sofort übersteuern.

N und Margin werden nicht vorab eingefroren.

Override-Kandidaten:
- Input Contract Violation
- stale/incompatible critical input
- harte strukturelle Invalidation
- harte Risk-/Domain-Invalidation

Research:
- Persistenz von BUY/HOLD/SELL,
- False-Flip-Rate,
- verpasster Ertrag durch zu starke Hysterese,
- Turnover und Kosten,
- Reaktionslatenz bei echten Zustandswechseln.

Abnahme:
- Hysterese schlägt eine ungeglättete Baseline out-of-sample oder bleibt deaktiviert,
- harte Invalidierungen werden niemals durch Hysterese fortgeschrieben.

## 7F – Portfolio Action Overlay & Swing Management

Ziel:
- Universal Stance nachgelagert auf die reale Position abbilden.

Mögliche Aktionen:
- OPEN
- ADD
- HOLD
- PARTIAL_REDUCE
- EXIT
- NO_ACTION

Portfolio-Inputs dürfen u. a. enthalten:
- Position vorhanden/nicht vorhanden,
- Positionsgewicht,
- Konzentration,
- Sektor-/Themen-/Währungs-/Länderexposure,
- Einstand/ungewinn/-verlust nur als Portfolio-Kontext,
- Haltedauer,
- verfügbare Liquidität sofern später sinnvoll dokumentiert.

Diese Informationen dürfen die Universal-Stance-Policy nicht trainieren.

Swing Research:
- W2/W4 + positive Bestätigung: Open/Add/Re-Add,
- W3 exhaustion: Partial Reduce,
- W5 completion risk: größere Reduce/Exit-Prüfung,
- Elliott Stage allein entscheidet nie,
- immer gegen No-Swing/Hold-Baseline vergleichen.

Kostenmodell mindestens:
- Gebühren,
- Spread soweit material,
- Slippage soweit material,
- Re-Entry-Kosten,
- verpasste Rebounds,
- Opportunity Cost geringeren Exposures in starken Trendphasen,
- Steuer-/Realisierungseffekte nur soweit belastbar modellierbar.

Abnahme:
- aktive Swing-Policy zeigt belastbaren Netto-Mehrwert gegen No-Swing oder wird nicht promotet.

## 7G – Decision Reliability & Explainability

Ziel:
- Decision Reliability als eigene strukturierte Aussage etablieren, getrennt von Phase-4-Confidence.

Verbindliche Komponenten:
- `level`: HIGH / MEDIUM / LOW / INSUFFICIENT
- `coverage`
- `data_quality`
- `module_agreement`
- `conflict_severity`
- `walk_forward_support`
- `reason_codes`

Ein interner numerischer Research-Wert 0–1 darf später existieren, muss aber empirisch kalibriert sein und darf nicht als objektive Wahrheit oder Richtungsaussage präsentiert werden.

Decision Reliability ist keine Richtungsaussage.

Abnahme:
- jeder Reliability-Level ist erklärbar,
- Richtungsentscheidung und Zuverlässigkeit können unabhängig voneinander variieren,
- LOW/INSUFFICIENT wird nicht automatisch zu SELL.

## 7H – Depot-Watch Integration

Ziel:
- `artifacts/research/decision_snapshot.json` als reproduzierbaren Core erzeugen,
- tägliche Wertpapierdepot-Watch liest diesen Core statt die Entscheidung jedes Mal komplett neu zu improvisieren.

Die Watch darf weiterhin erklären, priorisieren und nach Portfolio filtern.

Nicht sauber historisierte Live-Information bleibt außerhalb des Phase-7-Core. Zusätzliche externe Faktoren sind Phase 8.

Abnahme:
- Watch kann Universal Stance, Portfolio Action, Reliability, Gründe und Gegenevidenz direkt aus dem Snapshot erklären.

## 7I – Final Validation & Promotion

Vergleiche mindestens:
- Frozen pre-Phase-7 Baseline,
- Universal-Stance-Policy,
- Policy mit/ohne Hysterese,
- Portfolio Overlay,
- Swing vs. No-Swing.

Metriken:
- Forward Return / Alpha nach relevanten Horizonten,
- Drawdown/Downside,
- Treffer- und Fehlerraten nach Decision State,
- Kalibrierung,
- Turnover,
- Transaktionskosten,
- Stabilität,
- Coverage / Insufficient-Evidence-Rate,
- Konzentration nach Symbol/Sektor/Regime,
- Verhalten in Conflict-Zuständen.

Promotion nur bei reproduzierbarem Out-of-sample-Mehrwert und unverändertem Holdout.

## Kanonischer Decision-State-Katalog

Vor produktivem Policy-Code wird `configs/decision_state_catalog_v1.json` als ausführbarer Semantik-Katalog verwendet.

Foundation-Ziel:
- 20–40 Fälle,
- deterministische Contract-Fälle von Research-Pending-Policy-Fällen unterscheiden,
- parametrische Tests,
- bei neuen Stance-/Reason-/Conflict-Zuständen Katalog und Tests gemeinsam aktualisieren.

Die Foundation enthält zunächst 24 Fälle. Fälle mit `research_pending_policy` legen keine endgültige BUY/HOLD/SELL-Regel fest, sondern markieren die erwartete Forschungsfrage bzw. Kandidatenaktion.

## Empfohlene Reihenfolge

`7A -> 7B -> 7C -> 7D -> 7E -> 7F -> 7G -> 7H -> 7I`

7A/7B dürfen erst finalisiert werden, wenn Phase 6 ihre endgültigen Output-Verträge eingefroren hat. Die Foundations können vorher vorbereitet werden.
