# Phase 7 – Interpretation / Decision Layer: Foundations

Status: Vorbereitung / Research-Grundlage. Noch keine produktive Entscheidungslogik.

Phase 7 ist der ursprünglich als Phase 6 geplante Interpretation-/Decision-Layer. Durch das Einschieben von Elliott vNext wurde er um eine Nummer nach hinten verschoben. Ziel ist nicht ein weiterer Sensor, sondern die nachvollziehbare Zusammenführung der bereits getrennt entwickelten Evidenzquellen zu einer reproduzierbaren Handlungsaussage.

## 1. Grundprinzip

Die upstream Module bleiben semantisch getrennt:
- Selection beantwortet, wie attraktiv ein Titel relativ zum Universe ist.
- Timing beantwortet, ob der aktuelle Zeitpunkt historisch günstig oder ungünstig ist.
- Probability kalibriert Selection-/Timing-Zustände und ist kein zusätzlicher Richtungs-Vote.
- Risk beschreibt Downside-/Fehlerrisiko und ist kein Return-Vote.
- Confidence beschreibt Zuverlässigkeit/Belastbarkeit der vorhandenen Aussagen und ist kein Opportunity-Score.
- Adaptive Learning darf nur bereits definierte Beziehungen, Zuverlässigkeiten, Konfliktstrafen und Horizon-Mappings lernen; es darf upstream Definitionen nicht stillschweigend verändern.
- Elliott vNext liefert Struktur, Wellengrad, Szenarien, Ziel-/Korrekturzonen und Swing-/Review-Kontext. Elliott erzeugt keinen autonomen BUY/HOLD/SELL-Befehl.
- Markt-/Sektorkontext und relative Stärke dürfen nur verwendet werden, wenn ihre Datenqualität und Point-in-Time-Eignung dokumentiert sind.

Phase 7 interpretiert diese Evidenz gemeinsam, ohne die ursprüngliche Bedeutung der Module zu verwischen.

## 2. Zwei strikt getrennte Entscheidungsstufen

### 2.1 Universal Stance

Für jeden Titel des Scanneruniversums wird ein universeller Zustand erzeugt, unabhängig davon, ob der Titel im Portfolio liegt.

Zielzustände:
- `BUY`
- `HOLD`
- `SELL`
- `INSUFFICIENT_EVIDENCE`

`INSUFFICIENT_EVIDENCE` ist kein neutrales HOLD. Es ist ein fail-closed Zustand für fehlende, widersprüchliche oder nicht ausreichend belastbare Evidenz.

Der Universal Stance darf keine Portfoliogröße, Einstandskurse, persönliche Depotkonzentration oder bestehende Position verwenden.

### 2.2 Portfolio Action Overlay

Erst nach dem Universal Stance wird die reale Depotposition berücksichtigt.

Mögliche Empfehlungen:
- `OPEN`
- `ADD`
- `HOLD`
- `PARTIAL_REDUCE`
- `EXIT`
- `NO_ACTION`
- `INSUFFICIENT_EVIDENCE`

Das Portfolio Overlay darf eine universell positive Bewertung aus Risiko-/Konzentrationsgründen zu `HOLD` oder `NO_ACTION` begrenzen, aber niemals rückwirkend den Universal Stance umetikettieren.

Portfolioinformationen bleiben vollständig außerhalb des Trainings/der Kalibrierung des Universal-Stance-Modells.

## 3. Evidence Bundle

Phase 7 beginnt nicht mit einem Super-Score. Für jeden Titel wird zunächst ein PIT-sauberes Evidence Bundle erzeugt.

Pflichtblöcke soweit verfügbar:
- Selection
- Timing
- Probability
- Risk
- Confidence
- Adaptive Learning State
- Elliott vNext
- Markt-/Sektorkontext
- Relative Strength
- Data Quality / Provenance

Jeder Block behält:
- eigenen Wert/Zustand,
- eigenen Zeitstempel `as_of`,
- eigene Version,
- eigene Verfügbarkeit,
- eigene Unsicherheit bzw. Evidenzstärke.

Kein Modul darf durch bloßes Umkopieren in Phase 7 doppelt gezählt werden.

## 4. Semantische Rollen in der Fusion

### 4.1 Directional Evidence

Richtungsaussagen dürfen primär aus Selection und Timing sowie später empirisch validierten strukturellen Elliott-Zuständen abgeleitet werden.

Probability ist Kalibrierung dieser Richtung, nicht ein zweiter Richtungsbeweis.

### 4.2 Risk

Risk begrenzt bzw. verschärft Entscheidungen. Risk darf ein attraktives Setup als zu asymmetrisch/gefährlich kennzeichnen, aber nicht als eigenständiges Kaufsignal wirken.

### 4.3 Confidence

Confidence steuert, wie stark vorhandene Aussagen vertraut werden dürfen. Niedrige Confidence darf nicht automatisch bearish interpretiert werden.

### 4.4 Elliott vNext

Elliott ist Stage-/Structure-Evidence:
- W2-/W4-Komplettierung kann Entry/Add-Kontext liefern.
- W3-Exhaustion kann Partial-Reduce-Kontext liefern.
- W5-Completion-Risk kann stärkere Profit-Protection/Exit-Prüfung auslösen.
- Zielzonen allein sind keine Trade-Entscheidung.
- Primary/Alternative Szenarien und Invalidation müssen erhalten bleiben.

Phase 7 konsumiert die endgültige, nach Phase 6 eingefrorene Elliott-Ausgabe. Bis Phase 6 abgeschlossen ist, werden keine konkreten Feldnamen außer stabilen Vertragsgrenzen fest verdrahtet.

## 5. Conflict / Confirmation Engine

Phase 7 muss Konfluenz und Widerspruch explizit modellieren.

Vorgesehene Zustände:
- `CONFIRMED`
- `MIXED`
- `CONFLICT`
- `STRUCTURAL_WARNING`
- `TIMING_WARNING`
- `INSUFFICIENT_EVIDENCE`

Beispiele:
- starke Selection + positives Timing + unterstützender Elliott-Zustand = mögliche Confirmation.
- starke Selection + W5-Completion-Risk = Selection/Structure-Conflict, nicht automatisch SELL.
- schwache Selection + günstige W2-Geometrie = Elliott-Rescue-Kandidat, nicht automatisch BUY.

Konfliktregeln werden empirisch kalibriert; keine willkürlichen Gewichte vorab.

## 6. Keine erfundene Einheitsmetrik

Phase 7 startet ausdrücklich ohne `decision_score_0_100`.

Verboten als Foundation-Shortcut:
- feste Prozentgewichte ohne empirische Kalibrierung,
- Score + Risk + Confidence + Elliott zu einer beliebigen Summenformel addieren,
- Probability als unabhängigen Vote doppelt zählen,
- niedrige Confidence als bearish behandeln,
- Portfolioinformationen in Universal-Stance-Training verwenden.

Falls später eine verdichtete Kennzahl nachweislich Mehrwert besitzt, muss sie aus Discovery/Validation hervorgehen und interpretiert bleiben.

## 7. Zustandsübergänge und Hysterese

Nicht nur der heutige Zustand ist relevant. Phase 7 führt pro Titel mindestens:
- `previous_stance`
- `current_stance`
- `transition`
- `transition_reasons`
- `days_in_current_stance`

Zu untersuchen:
- `HOLD -> BUY`
- `BUY -> HOLD`
- `HOLD -> SELL`
- `SELL -> HOLD`
- Persistenz vs. kurzlebige Flips

Hysterese-/Cooldown-Regeln dürfen nicht manuell erfunden werden; sie werden auf Wechselkosten, Fehlflip-Rate und Opportunitätsverlust geprüft.

## 8. Swing-/Positionsmanagement

Phase 7 ist der Ort, an dem Elliott-Swing-Kontext mit den übrigen Modulen zu einer Depotaktion werden kann.

Beispielhafte, noch zu validierende Pfade:
- W2 completion + positives Timing + ausreichende Reliability -> `OPEN`/`ADD` prüfen.
- W3 exhaustion + Overextension/Momentumverschlechterung -> `PARTIAL_REDUCE` prüfen.
- W4 completion + erneute Timing-Bestätigung -> `ADD`/Re-Add prüfen.
- W5 completion risk + negative Bestätigung -> `PARTIAL_REDUCE` oder `EXIT` prüfen.

Transaktionskosten, Slippage und Re-Entry-Kosten müssen in der späteren Swing-Validierung enthalten sein. Ein Swing-Pfad muss gegen einfaches Halten verglichen werden.

## 9. Forschung und Validierung

Phase 7 darf keine upstream Module neu optimieren.

Für alle Kombinationen gilt:
- Point-in-Time strikt,
- nur damals verfügbare Outputs,
- Discovery / Validation / Holdout getrennt,
- überlappende Forward-Windows nicht als unabhängig behandeln,
- purged/walk-forward wo erforderlich,
- frozen Baseline vor Policy-Lernen,
- Effektstärke, Unsicherheit, Stichprobengröße und Konzentration berichten.

Die zu optimierende Policy muss explizit zwischen mindestens folgenden Zielgrößen unterscheiden:
- Forward Return / Alpha,
- Downside / Drawdown,
- Fehlentscheidungskosten,
- Turnover,
- Transaktionskosten,
- Stabilität der Entscheidung.

## 10. Baseline

Vor dem eigentlichen Phase-7-Research wird der letzte Zustand vor Phase 7 eingefroren:
- produktive Scanner-/Watch-Logik,
- finale Outputs der Phasen 1–6,
- aktuelle manuelle/regelbasierte Decision-Routine der Depot-Watch soweit reproduzierbar.

Diese Baseline dient dem Vergleich. Phase 7 darf ihre Schwellen nicht nachträglich anhand des Holdouts verändern.

## 11. Phase 8 bleibt getrennt

Externe Faktoren bleiben die nachgelagerte Phase 8, z. B. zusätzliche PIT-fähige Quellen wie Sentiment, EPS-Revisionen, Short Interest oder weitere externe Daten.

Phase 7 Core muss ohne Phase-8-Daten reproduzierbar funktionieren.

Live-News oder nicht sauber historisierte Informationen dürfen höchstens als separater Watch-Overlay erscheinen und nicht heimlich das historische Core-Modell beeinflussen.

## 12. Geplanter Output

Zielartefakt zunächst:
`artifacts/research/decision_snapshot.json`

Pro Titel mindestens:
- Evidence Coverage
- Conflict/Confirmation State
- Universal Stance
- Decision Confidence / Evidence Sufficiency (getrennt von upstream Confidence)
- Reasons
- Counter Evidence
- Previous/Current State + Transition
- Portfolio Action, falls Portfolio-Kontext vorhanden
- Provenance/Versions

## 13. Abnahmekriterien

Phase 7 ist erst produktionsreif, wenn:
- alle Upstream-Semantiken unverändert bleiben,
- Probability nicht doppelt zählt,
- Risk und Confidence nicht als Directional Votes missbraucht werden,
- Portfolio-Overlay nach Universal Stance erfolgt,
- fehlende Evidenz fail-closed bleibt,
- Entscheidungshistorie PIT-reproduzierbar ist,
- Konflikt-/Konfluenzlogik empirisch validiert ist,
- Swing-Management gegen Buy-and-Hold/No-Swing mit Kosten verglichen wurde,
- Transition/Hysterese auf Flips und Opportunitätskosten geprüft wurde,
- Holdout unangetastet bleibt,
- Output erklärbar ist und Reasons/Counter-Evidence enthält,
- Phase-8-Daten nicht versehentlich in den Core gelangen.
