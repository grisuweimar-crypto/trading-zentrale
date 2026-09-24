# Projektübergabe – Trading-Zentrale / Scanner-vNext

## Modul 6: Elliott vNext

Stand der Vorbereitung: 24.09.2026

Repository:
`grisuweimar-crypto/trading-zentrale`

WICHTIG: Diese Übergabe ist für einen neuen, sauberen Chat gedacht. Modul 6 soll erst begonnen werden, wenn Modul 5 vollständig abgeschlossen und auf `main` integriert ist. Vor Arbeitsbeginn deshalb zuerst den aktuellen `main`-Stand und den Abschluss von Modul 5 verifizieren. Frühere Phasen nicht neu aufrollen, sofern sie für Modul 6 nicht zwingend relevant sind.

## 1. Ziel von Modul 6

Elliott vNext wird als unabhängiger Struktur-/Kontextsensor gebaut. Elliott und Fibonacci treffen keine autonome Kauf-/Verkaufsentscheidung. Das finale Gesamturteil entsteht später aus dem gesamten System: Selection, Timing, Risk, Confidence, Probability, Fundamentals, Markt-/Sektorkontext, relative Stärke, Elliott/Fibonacci und weiteren validierten Modulen.

Elliott/Fibonacci darf Deep-Scans auslösen und selbst durch statistische Scannerereignisse zu tieferen Analysen getriggert werden.

## 2. Verbindliche Grundlagen

Vor Implementierung lesen:
- `docs/elliott_vnext_6_foundations.md`
- `docs/elliott_vnext_6_research_plan.md`
- `configs/elliott_vnext_contract_v1.json`
- `configs/elliott_vnext_output_schema_v1.json`
- `configs/market_context_history_schema_v1.json`
- `data/inputs/market_context_registry.csv`
- `tests/test_elliott_vnext_foundations.py`

Regelquelle für Elliott/Fibonacci v1:
`https://elliottwaver.live/kompendium/`

Die Lehrregeln sind Hypothesen-/Strukturregeln. Ihre prognostische Relevanz muss empirisch validiert werden.

## 3. Unverhandelbare Methodik

- strikt Point-in-Time arbeiten,
- keine erfundenen Kurse, Scannerwerte, Branchenbewegungen oder Quellen,
- `pivot_time` und `confirmed_time` getrennt speichern,
- historische Pivots erst ab `confirmed_time` verwenden,
- keine zukünftigen Daten zur rückwirkenden Verbesserung eines Counts,
- Fibonacci erst nach regelkonformer Strukturerkennung ansetzen,
- Fibonacci nicht zur Auswahl einer gewünschten Wellenzählung benutzen,
- mehrere plausible Szenarien statt eines angeblich eindeutigen Counts,
- harte Regeln von weichen Richtlinien trennen,
- Diagonalen als eigenen Regelzweig behandeln,
- Discovery, Validation und Holdout strikt trennen,
- Holdout nie zur Musterauswahl benutzen,
- überlappende Forward-Windows nicht als unabhängige Beobachtungen behandeln,
- Originalwährungen beibehalten.

## 4. Harte Elliott-Regeln für den klassischen Impuls

1. Welle 2 darf den Ursprung von Welle 1 nicht überschreiten.
2. Welle 3 darf nicht die kürzeste von Welle 1, 3 und 5 sein.
3. Welle 4 darf beim klassischen Impuls nicht in das Preisgebiet von Welle 1 eindringen.

Ausnahme: Bei Leading/Ending Diagonals ist 4/1-Überlappung zulässig und typisch. Deshalb Diagonalen separat prüfen.

Nicht jede Bewegung in fünf Wellen zwingen. Korrekturklassen umfassen mindestens Zigzag, Flat, Triangle, Double Three und Triple Three.

## 5. Fibonacci

Elliott bestimmt die zulässigen Ankerpunkte. Fibonacci beschreibt danach die Geometrie.

W2-Zonen aus dem Kompendium:
- 50 %,
- 61,8 %,
- 78,6 %,
- 88,7 %.

88,7 % ist eine tiefe Warn-/Grenzzone, nicht die harte Elliott-Invalidierung. Die harte W2-Invalidierung erfolgt beim Bruch des Ursprungs von W1.

W4 typischerweise:
- 14,6 %,
- 23,6 %,
- 38,2 %,
- ungefähr 50 % als obere Richtlinie.

Fib-Level als ATR-/volatilitätsabhängige Zonen behandeln, nicht als exakte Wendepunkte.

## 6. Fraktale Architektur

Normalfall top-down:
`Weekly/grob → Daily mittel → Daily fein`.

Zusätzlich bottom-up:
Ein statistisch auffälliges Scannerereignis darf `EW_BOTTOM_UP_SCAN` auslösen. Dann lokal Elliott-Struktur prüfen und anschließend herauszoomen, um die Parent-Struktur zu bestimmen.

Ein kleiner 1-2-3-4-5-Impuls kann auf höherer Ebene nur Teil einer Welle 1, A, C usw. sein.

## 7. Vorläufige Routing-Trigger

- `EW_CONTEXT`
- `EW_PREWATCH_382`
- `EW_DEEP_SCAN_500`
- `EW_W2_CORE`
- `EW_W2_DEEP`
- `EW_W2_DANGER`
- `EW_INVALIDATED`
- `EW_BOTTOM_UP_SCAN`

Diese Trigger steuern Rechenaufwand und Analysepriorität. Sie sind keine autonomen Handelssignale.

## 8. Evidenz sauber trennen

Nicht eine subjektive Elliott-Confidence erzeugen. Mindestens getrennt führen:
- `structural_fit`: formale Regelpassung,
- `confirmation_strength`: Reaktion/Kontext/Momentum/relative Stärke,
- `historical_expectancy`: empirisch beobachtete historische Erwartung.

Fibonacci selbst bleibt Geometrie. Scanner-Momentum oder Marktkontext nicht in einen Fib-Score mischen und später doppelt zählen.

## 9. Scanner ↔ Elliott erforschen

Mindestens untersuchen:
- Redundancy,
- Confirmation,
- Elliott Rescue,
- Scanner Rescue,
- Conflict,
- Lead/Lag.

Lead/Lag nicht nur am selben Tag messen, sondern in Ereignisfenstern, z. B. -20 bis +20 Handelstage.

Besonders wichtig: Prüfen, ob vorhandene statistische Timing-Muster frühere Wendepunkte anzeigen und ob Elliott/Fibonacci Situationen erkennt, die der Scanner sonst nicht priorisiert hätte.

## 10. Markt-/Branchen-/Kohortenkontext

Keine reale Branche aus wenigen Scannerwerten konstruieren. Scanner-Peers sind nur `scanner_peer_context`.

Für echten Kontext bevorzugen:
1. offizieller/breiter Index,
2. breiter liquider Sektor-/Themen-ETF,
3. extern definierter und Point-in-Time-versionierter Peer-Korb,
4. Scanner-Peers nur zusätzlich.

Kontextdaten separat in `artifacts/research/market_context_history.csv` dokumentieren, nicht in `history_analysis.csv` mischen.

Jeder Kontext erhält:
`sufficient | limited | unreliable | unavailable`.

`unreliable` und `unavailable` dürfen keine Markt-/Branchenwelle erzeugen.

Relative Stärke später mindestens:
- Aktie vs. Benchmark,
- Aktie vs. Sektor,
- Sektor vs. Markt.

Zustände: Leader, Follower, Divergence, Synchronized.

## 11. Empfohlene Modul-6-Reihenfolge

`6A Daten/Pivots`
→ `6B Szenario-Generator/Regelprüfer`
→ `6C Fibonacci-Geometrie`
→ `6D Scanner↔Elliott Cross-System-Research`
→ `6E externer Marktkontext`
→ `6F historische Walk-forward-/OOS-Validierung`
→ `6G stabiler Moduloutput und spätere Decision-Layer-Integration`.

6E darf parallel vorbereitet werden, solange keine produktive Scannerlogik verändert wird.

## 12. Aktueller vorbereitender Branch

Die Foundations wurden isoliert auf folgendem Branch angelegt:
`module6-elliott-vnext-foundations`

Dieser Branch basiert auf dem `main`-Stand vom 24.09.2026 unmittelbar nach Abschluss/Merge von Phase 4. Vor Modul 6 muss er gegen den dann aktuellen `main` nach Abschluss von Modul 5 abgeglichen werden. Nicht ungeprüft mergen, falls Modul 5 zwischenzeitlich dieselben Bereiche verändert hat.

## 13. Auftrag für den neuen Chat

1. Aktuellen `main` und Abschluss von Modul 5 verifizieren.
2. Foundations-Branch gegen aktuellen `main` vergleichen und nötigenfalls aktualisieren/rebasen.
3. Die Foundation-Dokumente und Verträge lesen und auf Konsistenz prüfen.
4. Modul 6A starten: PIT-sichere Pivot-/Swing-Erkennung auf Daily/Weekly.
5. Erst nach validierter Pivotkausalität zu Wellenzählung/Fibonacci übergehen.
6. Keine produktive Integration und keine Decision-Layer-Gewichtung vor erfolgreicher historischer Validation/Holdout-Prüfung.

## 14. Erfolgskriterium

Am Ende von Modul 6 soll das System für jedes Instrument strukturierte Elliott/Fibonacci-Evidenz liefern können, die reproduzierbar, Point-in-Time-sauber und empirisch bewertet ist. Das Modul selbst entscheidet niemals Kaufen/Halten/Verkaufen. Erst das gesamte Scanner-/Research-/Depot-Watch-System nimmt die finale Position ein.
