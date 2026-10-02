# Phase 7 – Interpretation / Decision Layer: Foundations

Status: Vorbereitung / Research-Grundlage. Noch keine produktive Entscheidungslogik.

Phase 7 ist der ursprünglich als Phase 6 geplante Interpretation-/Decision-Layer. Durch das Einschieben von Elliott vNext wurde er um eine Nummer nach hinten verschoben. Ziel ist nicht ein weiterer Sensor, sondern die nachvollziehbare Zusammenführung der bereits getrennt entwickelten Evidenzquellen zu einer reproduzierbaren Handlungsaussage.

Die Foundation enthält ausdrücklich die vor dem ersten Policy-Code einzufrierende Semantik für `HOLD`, `NO_ACTION`, `INSUFFICIENT_EVIDENCE`, Conflict-Typen, Decision Reliability und Hysterese-Grenzen. Ein kanonischer State-Katalog dient als ausführbare Referenz.

## 1. Grundprinzip

Die Upstream-Module bleiben semantisch getrennt:
- Selection: relative Attraktivität / Richtung im Universe.
- Timing: Zeitpunkt / kurzfristigere Richtung innerhalb eines Titels.
- Probability: Kalibrierung von Selection-/Timing-Zuständen, kein zusätzlicher Richtungs-Vote.
- Risk: Downside-/Fehlerrisiko, kein Return-Vote.
- Confidence: Zuverlässigkeit der vorhandenen Aussagen, kein Opportunity-Score und keine Richtung.
- Adaptive Learning: darf nur bereits definierte Beziehungen, Reliability, Konfliktstrafen und Horizon-Mappings nach Phase-5-Regeln anpassen.
- Elliott vNext: Struktur, Wellengrad, Primär-/Alternativszenarien, Invalidation, Ziel-/Korrekturzonen und Swing-/Review-Kontext; kein autonomer Trade-Befehl.
- Markt-/Sektorkontext und Relative Strength: nur bei dokumentierter Datenqualität und Point-in-Time-Eignung.

Phase 7 interpretiert diese Evidenz gemeinsam, ohne die ursprüngliche Bedeutung der Module zu verwischen oder dieselbe Evidenz mehrfach zu zählen.

## 2. Zwei strikt getrennte Entscheidungsstufen

### 2.1 Universal Stance

Für jeden Titel des Scanneruniversums, unabhängig vom realen Depot:
- `BUY`
- `HOLD`
- `SELL`
- `INSUFFICIENT_EVIDENCE`

Portfolioinformationen dürfen weder die Kalibrierung noch das Training des Universal Stance beeinflussen.

### 2.2 Portfolio Action Overlay

Erst nach Universal Stance wird die reale Position berücksichtigt:
- `OPEN`
- `ADD`
- `HOLD`
- `PARTIAL_REDUCE`
- `EXIT`
- `NO_ACTION`
- `INSUFFICIENT_EVIDENCE`

Ein Portfolio-Constraint darf eine Aktion begrenzen, aber den Universal Stance nicht rückwirkend umetikettieren.

Beispiel:
- Universal Stance: `BUY`
- Portfolio Action: `NO_ACTION`
- Portfolio Reason: `MAX_POSITION_CONCENTRATION`

Nicht erlaubt:
- Universal Stance: `HOLD`, nur weil die Position bereits groß ist.

## 3. HOLD-Semantik

`HOLD` darf nicht mehrere ununterscheidbare Bedeutungen tragen. Intern ist bei Universal `HOLD` ein Detailzustand Pflicht:

- `HOLD_CONSTRUCTIVE`: Titel bleibt grundsätzlich konstruktiv, aber aktuell kein neuer Einstieg bzw. keine neue Richtungsaktion.
- `HOLD_NEUTRAL`: keine klare positive oder negative Handlungsevidenz.
- `HOLD_UNRESOLVED`: relevante Sensoren widersprechen sich; Beobachtung statt Aktion.

`HOLD_PORTFOLIO_CONSTRAINED` ist als Universal-Stance-Untertyp verboten. Portfolioeinschränkungen gehören ausschließlich ins nachgelagerte Portfolio Overlay.

## 4. INSUFFICIENT_EVIDENCE

`INSUFFICIENT_EVIDENCE` ist ein aktiver fail-closed Zustand und niemals ein neutrales HOLD.

Ursachen-Codes:
- `INSUFFICIENT_DATA`
- `INSUFFICIENT_MODEL_COVERAGE`
- `INSUFFICIENT_CONSENSUS`
- `INSUFFICIENT_VALIDATION`
- `OUTSIDE_VALIDATED_DOMAIN`
- `STALE_OR_INCOMPATIBLE_INPUT`
- `INPUT_CONTRACT_VIOLATION`

Mögliche Auslöser:
- erforderliche Historientiefe fehlt,
- Pflichtmodul liefert keinen gültigen Output,
- Datenqualität liegt unter Mindestanforderung,
- Modell/Teilmodell ist nicht validiert oder nicht promotet,
- entscheidungsrelevanter Konflikt ist nicht auflösbar,
- Asset/Markt/Benchmark liegt außerhalb der validierten Domäne,
- kritischer Input ist veraltet oder versionsinkompatibel.

Vertragliche Fälle wie inkompatible Versionen dürfen deterministisch fail-closed sein. Empirische Mindestschwellen für Historientiefe, Coverage oder Consensus werden erst in Phase 7 validiert und nicht vorab erfunden.

## 5. Evidence Bundle

Phase 7 beginnt nicht mit einem Super-Score. Pro Titel wird zunächst ein PIT-sauberes Evidence Bundle erzeugt.

Blöcke soweit verfügbar:
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
- Wert/Zustand,
- `as_of`,
- Version,
- Verfügbarkeit,
- Unsicherheit/Evidenzstärke.

Kein Modul darf durch Umkopieren oder semantische Überlappung doppelt gezählt werden.

## 6. Semantische Rollen in der Fusion

### 6.1 Directional Evidence

Richtungsaussagen dürfen primär aus Selection und Timing sowie später empirisch validierter struktureller Elliott-Evidenz entstehen.

Probability kalibriert diese Richtung und ist kein zweiter oder dritter Richtungsbeweis.

### 6.2 Risk

Risk darf Aktionen begrenzen, Positionsgrößen reduzieren oder eine Lage als asymmetrisch/gefährlich kennzeichnen. Risk darf die Richtung nicht allein invertieren.

Insbesondere gilt:
- hohe Downside bei ansonsten positiver Evidenz kann `OPEN`/`ADD` verhindern,
- daraus folgt nicht automatisch Universal `SELL`.

### 6.3 Confidence

Upstream Confidence steuert, wie stark einer bereits vorhandenen Aussage vertraut werden darf. Niedrige Confidence ist nicht bearish.

### 6.4 Elliott vNext

Elliott ist Stage-/Structure-Evidence:
- mögliche W2-/W4-Komplettierung kann Entry/Add-Kontext liefern,
- W3-Exhaustion kann Partial-Reduce-Kontext liefern,
- W5-Completion-Risk kann stärkere Profit-Protection/Exit-Prüfung auslösen,
- Zielzonen allein sind keine Trade-Entscheidung,
- Primary/Alternative Szenarien und Invalidation bleiben sichtbar.

Phase 7 konsumiert den final eingefrorenen Phase-6-Vertrag. Vor Abschluss von Phase 6 werden keine instabilen Elliott-Feldnamen hart verdrahtet.

## 7. Conflict / Confirmation Engine

Conflict/Confirmation wird vor der Stance Policy definiert und untersucht.

Top-Level-Zustände:
- `CONFIRMED`
- `MIXED`
- `CONFLICT`
- `STRUCTURAL_WARNING`
- `TIMING_WARNING`
- `INSUFFICIENT_EVIDENCE`

Konfliktarten mindestens:
- `SELECTION_TIMING_CONFLICT`
- `DIRECTION_STRUCTURE_CONFLICT`
- `PRIMARY_ALTERNATIVE_STRUCTURE_CONFLICT`
- `MARKET_CONTEXT_DIVERGENCE`
- `RELATIVE_STRENGTH_DIVERGENCE`
- `RISK_CONSTRAINT_NOT_DIRECTIONAL_CONFLICT`
- `RELIABILITY_OR_COVERAGE_CONFLICT`
- `DATA_OR_VERSION_CONFLICT`

Beispiele:
- starke Selection + positives Timing + unterstützende Struktur: mögliche Confirmation.
- starke Selection + W5-Completion-Risk: Direction/Structure Conflict, nicht automatisch SELL.
- schwache Selection + günstige W2-Geometrie: Rescue-Kandidat, nicht automatisch BUY.
- hohe Downside: Risk Constraint, nicht automatisch Directional Conflict.

Konfliktgewichte werden nicht manuell vorgegeben, sondern empirisch kalibriert.

## 8. Keine erfundene Einheitsmetrik

Phase 7 startet ausdrücklich ohne `decision_score_0_100`.

Verboten:
- feste Prozentgewichte ohne empirische Herleitung,
- Probability als unabhängigen Vote doppelt zählen,
- Risk als Kaufsignal oder alleinige Richtungsinversion,
- niedrige Confidence als bearish,
- Elliott-Zielzone allein als Trade-Signal,
- Portfolioinformationen im Universal-Stance-Training.

Falls später eine verdichtete Kennzahl nachweislich Mehrwert besitzt, muss sie aus Discovery/Validation hervorgehen und interpretierbar bleiben.

## 9. Decision Reliability

Decision Reliability ist ein eigener Vertrag und ausdrücklich nicht Phase-4-Confidence.

Pflichtkomponenten:
- `level`: HIGH / MEDIUM / LOW / INSUFFICIENT
- `coverage`
- `data_quality`
- `module_agreement`
- `conflict_severity`
- `walk_forward_support`
- `reason_codes`

Ein interner numerischer Research-Wert 0–1 darf später verwendet werden, wenn er empirisch kalibriert ist. Er darf weder als objektive Wahrheit noch als Richtungsaussage präsentiert werden.

BUY + LOW Reliability und SELL + LOW Reliability sind beide zulässige Zustände. Reliability beschreibt Sicherheit/Belastbarkeit, nicht Richtung.

## 10. Zustandsübergänge und Hysterese

Pro Titel mindestens:
- `previous_stance`
- `current_stance`
- `transition`
- `transition_reasons`
- `days_in_current_stance`

Drei Mechanismen getrennt testen:
1. `confirmation_window`: neuer Zustand benötigt Bestätigung über mehrere Beobachtungen.
2. `evidence_margin`: neue Evidenz muss die bisherige Entscheidung ausreichend übertreffen.
3. `exception_override`: harte Ereignisse dürfen Hysterese sofort übersteuern.

N und Evidence Margin werden nicht vorab eingefroren.

Override-Kandidaten:
- Input Contract Violation,
- stale/incompatible critical input,
- harte strukturelle Invalidation,
- harte Risk-/Domain-Invalidation.

Hysterese darf niemals einen klar invalidierten Zustand künstlich fortschreiben.

## 11. Swing-/Positionsmanagement

Phase 7 ist der Ort, an dem Elliott-Swing-Kontext mit den übrigen Modulen zu einer Depotaktion werden kann.

Zu validierende Pfade:
- W2/W4 + positive Bestätigung -> `OPEN` / `ADD` / Re-Add prüfen.
- W3 exhaustion + weitere Ermüdung -> `PARTIAL_REDUCE` prüfen.
- W5 completion risk + negative Bestätigung -> stärkere Reduce-/Exit-Prüfung.

Nicht erlaubt:
- W2 = automatisch kaufen,
- W3 = automatisch reduzieren,
- W4 = automatisch nachkaufen,
- W5 = automatisch verkaufen.

Jeder aktive Swingpfad muss gegen No-Swing/Hold verglichen werden.

Kosten mindestens:
- Gebühren,
- Spread soweit material,
- Slippage soweit material,
- Re-Entry-Kosten,
- verpasste Rebounds,
- Opportunity Cost geringeren Exposures in starken Trendphasen,
- Steuer-/Realisierungseffekte nur soweit belastbar modellierbar.

## 12. Kanonischer Decision-State-Katalog

Vor produktivem Policy-Code wird ein ausführbarer State-Katalog geführt:

`configs/decision_state_catalog_v1.json`

Foundation: 24 kanonische Fälle.

Falltypen:
- `contract_deterministic`: semantische Regeln, die unabhängig von der späteren Policy gelten.
- `research_pending_policy`: Forschungsfälle/Kandidatenaktionen, deren finales Urteil noch nicht eingefroren wird.

Der Katalog wird parametrisch getestet. Neue Stance-/Reason-/Conflict-Zustände müssen Katalog und Tests gemeinsam aktualisieren.

## 13. Forschung und Validierung

Phase 7 darf keine Upstream-Module neu optimieren.

Für alle Kombinationen gilt:
- Point-in-Time strikt,
- nur damals verfügbare Outputs,
- Discovery / Validation / Holdout getrennt,
- überlappende Forward-Windows nicht als unabhängig behandeln,
- purged/walk-forward wo erforderlich,
- Frozen Baseline vor Policy-Lernen,
- Effektstärke, Unsicherheit, Stichprobengröße und Konzentration berichten.

Zu bewerten:
- Forward Return / Alpha,
- Downside / Drawdown,
- Fehlentscheidungskosten,
- Turnover,
- Transaktionskosten,
- Stabilität der Entscheidung.

## 14. Baseline

Vor dem eigentlichen Phase-7-Research wird eingefroren:
- produktive Scanner-/Watch-Logik,
- finale Outputs der Phasen 1–6,
- aktuelle manuelle/regelbasierte Decision-Routine der Depot-Watch soweit reproduzierbar.

Diese Baseline ist der Vergleichspunkt. Phase 7 darf Schwellen nicht nachträglich am Holdout anpassen.

## 15. Phase 8 bleibt getrennt

Externe Faktoren bleiben die nachgelagerte Phase 8, z. B. Sentiment, EPS-Revisionen, Short Interest oder weitere PIT-fähige Daten.

Phase 7 Core muss ohne Phase-8-Daten reproduzierbar funktionieren.

Nicht historisierte Live-Information darf höchstens als separater Watch-Overlay erscheinen und nicht heimlich das historische Core-Modell beeinflussen.

## 16. Geplanter Output

Zielartefakt:
`artifacts/research/decision_snapshot.json`

Pro Titel mindestens:
- Evidence Coverage
- Conflict/Confirmation State + Conflict Types
- Universal Stance
- Stance Detail bei HOLD
- Insufficient-Evidence-Reasons bei fehlender Entscheidbarkeit
- strukturierte Decision Reliability
- Reasons
- Counter Evidence
- Previous/Current State + Transition
- Portfolio Action + Portfolio Reason Codes, falls Portfolio-Kontext vorhanden
- Provenance/Versions

## 17. Abnahmekriterien

Phase 7 ist erst produktionsreif, wenn:
- Upstream-Semantiken unverändert bleiben,
- HOLD/NO_ACTION/INSUFFICIENT semantisch eindeutig und testbar sind,
- Probability nicht doppelt zählt,
- Risk und Confidence nicht als Directional Votes missbraucht werden,
- Portfolio Overlay nach Universal Stance erfolgt,
- Portfolio-Constraints den Universal Stance nicht umetikettieren,
- fehlende Evidenz fail-closed bleibt,
- Entscheidungshistorie PIT-reproduzierbar ist,
- Konflikt-/Konfluenzlogik empirisch validiert ist,
- Decision Reliability strukturiert und nicht-directional bleibt,
- Swing-Management gegen No-Swing/Hold inklusive Kosten verglichen wurde,
- Hysterese auf Flips, Latenz und Opportunitätskosten geprüft wurde,
- harte Invalidierungen Hysterese übersteuern,
- Holdout unangetastet bleibt,
- Output erklärbar ist und Reasons/Counter-Evidence enthält,
- State Catalog und Contract-Tests bestanden sind,
- Phase-8-Daten nicht versehentlich in den Core gelangen.
