# Phase 7 – Interpretation / Decision Layer: Research- und Bauplan

Status: vorbereitet. Start der eigentlichen Implementierung erst nach vollständigem Abschluss von Phase 6 Elliott vNext.

## Ziel

Phase 7 übersetzt die getrennt validierten Upstream-Sensoren in eine reproduzierbare, erklärbare Entscheidungsschicht. Sie baut keinen neuen Opportunity-Score und optimiert keine früheren Phasen nachträglich.

## 7A – Input Contract & Coverage

Ziel:
- finale Output-Verträge der Phasen 1–6 erfassen,
- Point-in-Time-Verfügbarkeit und Versionen pro Block dokumentieren,
- ein unverändertes Evidence Bundle pro Titel erzeugen,
- fehlende Evidenz als `insufficient` statt neutral behandeln.

Abnahme:
- ein Titel kann vollständig auf seine Input-Snapshots/Versionen zurückgeführt werden,
- Portfolioinformationen fehlen vollständig im Universal-Stance-Input.

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
- Baseline vor Policy-Suche eingefroren.

## 7C – Conflict / Confirmation Research

Ziel:
- Konfluenz und Konflikte zwischen Modulen explizit untersuchen,
- Redundanz und Doppelzählung vermeiden,
- Probability, Risk und Confidence gemäß ihrer Semantik behandeln.

Research-Fragen:
- Wann verbessert Timing eine starke Selection tatsächlich?
- Wann ist starke Selection bei schlechtem Timing nur HOLD statt BUY?
- Wann ist Elliott-Rescue nach schwacher Scannerlage nützlich oder gefährlich?
- Wann ist W5-/W3-Exhaustion trotz starker Selection ein valider Reduce-Kontext?
- Wie stark verschlechtert geringe Decision-Reliability die Trennschärfe?
- Welche Konflikte sind nur Rauschen, welche systematisch?

Methodik:
- interpretable Zustandskombinationen,
- ausreichend große Zellen bzw. hierarchische Zusammenfassung,
- keine post-hoc Holdout-Auswahl.

## 7D – Universal Stance Policy

Ziel:
- pro Universe-Titel `BUY`, `HOLD`, `SELL` oder `INSUFFICIENT_EVIDENCE` erzeugen.

Wichtig:
- kein Portfolioinput,
- keine festen Modulgewichte ohne Evidenz,
- kein neuer 0–100-Super-Score als Startpunkt,
- Policy muss Gründe und Gegenevidenz ausgeben.

Kandidaten für interpretable Policy-Familien:
- hierarchische Gate-/Rule-Policy,
- monotone/constraint-basierte Entscheidungsbäume,
- andere interpretable Modelle nur wenn sie klaren Out-of-sample-Mehrwert zeigen.

Nicht erlaubt:
- Blackbox-Modell ohne nachvollziehbare Gründe,
- upstream Module in Phase 7 neu trainieren.

## 7E – Transition / Hysteresis

Ziel:
- tägliche Zustandswechsel kontrollieren,
- Signalflattern von echten Zustandswechseln unterscheiden.

Research:
- Persistenz von BUY/HOLD/SELL,
- False-Flip-Rate,
- verpasster Ertrag durch zu starke Hysterese,
- Turnover und Kosten,
- zustandsabhängige Mindestbestätigung nur wenn empirisch belegt.

Abnahme:
- Hysterese schlägt eine ungeglättete Baseline out-of-sample oder bleibt deaktiviert.

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
- Kosten/Slippage/Re-Entry zwingend,
- immer gegen No-Swing/Hold-Baseline vergleichen.

## 7G – Decision Reliability & Explainability

Ziel:
- Decision Reliability als eigene Aussage etablieren, getrennt von Phase-4-Confidence.

Decision Reliability darf u. a. berücksichtigen:
- Upstream Confidence,
- Data Quality,
- Evidenzabdeckung,
- Konfliktgrad,
- Sample Size / historische Kalibrierung der aktuellen Policy-Zelle,
- Stabilität über Walk-forward-Epochen.

Sie ist keine Richtungsaussage.

Output muss enthalten:
- Hauptgründe,
- Gegenevidenz,
- fehlende Evidenz,
- relevante Konflikte,
- Versions-/PIT-Provenance.

## 7H – Depot-Watch Integration

Ziel:
- `artifacts/research/decision_snapshot.json` als reproduzierbaren Core erzeugen,
- tägliche Wertpapierdepot-Watch liest diesen Core statt die Entscheidung jedes Mal komplett neu zu improvisieren.

Die Watch darf weiterhin erklären, priorisieren und nach Portfolio filtern.

Nicht sauber historisierte Live-Information bleibt außerhalb des Phase-7-Core. Zusätzliche externe Faktoren sind Phase 8.

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

## Empfohlene Reihenfolge

`7A -> 7B -> 7C -> 7D -> 7E -> 7F -> 7G -> 7H -> 7I`

7A/7B dürfen erst finalisiert werden, wenn Phase 6 ihre endgültigen Output-Verträge eingefroren hat. Die Foundations können vorher vorbereitet werden.
