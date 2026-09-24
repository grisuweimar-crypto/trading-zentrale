# Modul 6 – Elliott vNext: Research- und Bauplan

Status: vorbereitet, Start erst nach Abschluss von Modul 5.

## Grundsatz

Modul 6 wird als unabhängiger Struktur-/Kontextsensor gebaut. Es darf keine autonome Handelsentscheidung treffen. Das finale Kaufen/Halten/Reduzieren/Verkaufen bleibt Aufgabe des späteren globalen Decision-Layers bzw. der orchestrierenden Depot-Watch.

## 6A – Daten- und Pivot-Layer

Ziel:
- Daily-OHLCV als Source of Truth nutzen,
- Weekly deterministisch daraus aggregieren,
- kausale Swing-/Pivot-Erkennung implementieren,
- für jeden Pivot `pivot_time` und `confirmed_time` speichern,
- verschiedene sinnvolle Wellengrade über robuste Swing-/ATR-Parameter erzeugen.

Research:
- Stabilität der Pivots gegen moderate Parameteränderungen,
- Bestätigungslatenz,
- Einfluss von Gaps und illiquiden Reihen,
- keine zukünftigen Bars für historische Zustände.

Abnahme:
- Point-in-Time-Test beweist, dass historische Pivots vor `confirmed_time` unsichtbar sind.

## 6B – Szenario-Generator und Regelprüfer

Ziel:
- mehrere plausible Counts erzeugen statt einen vermeintlich eindeutigen Count,
- klassischen Impuls strikt nach den drei harten Regeln prüfen,
- Diagonalen als eigenen Musterzweig behandeln,
- Zigzag und Flat als erste vollständige Korrekturklassen implementieren,
- Triangle/WXY/WXYXZ mindestens als Alternativ-/Unsicherheitszustände repräsentieren.

Research:
- strukturelle Trefferhäufigkeit nach Wellengrad,
- Parameterstabilität,
- Häufigkeit konkurrierender Szenarien,
- historische Invalidierungsraten ohne Nachoptimierung.

Abnahme:
- keine Regelverletzung in als gültig ausgegebenen klassischen Impulsen,
- keine pauschale Invalidierung von Diagonalen wegen 4/1-Überlappung,
- Primär- und Alternativszenarien reproduzierbar.

## 6C – Fibonacci-Geometrie

Ziel:
- Fibonacci ausschließlich an bestätigten, strukturell zulässigen Ankern ansetzen,
- Retracements/Extensions als Zonen statt exakter Linien berechnen,
- Zonendicke mindestens ATR-/volatilitäts- und wellengradabhängig machen,
- 88,7 % als tiefe W2-Warnzone und W1-Ursprung als harte Invalidation getrennt halten.

Routing-Trigger zunächst:
- `EW_PREWATCH_382`,
- `EW_DEEP_SCAN_500`,
- `EW_W2_CORE`,
- `EW_W2_DEEP`,
- `EW_W2_DANGER`,
- `EW_INVALIDATED`.

Research:
- Triggerhäufigkeit,
- Zeit von 38,2 → 50 → 61,8 → 78,6 → 88,7,
- nachfolgende Struktur und Forward-Returns,
- robuste Zonenbreite statt nachträglicher Fib-Optimierung.

Abnahme:
- Fibonacci beeinflusst niemals die Auswahl des zugrunde liegenden Counts.

## 6D – Scanner ↔ Elliott Cross-System Research

Ziel:
- Elliott-Ereignisse mit vorhandenen Scannerzuständen und eingefrorenen Timing-/Probability-/Confidence-Outputs verbinden,
- keine bestehende Phase neu optimieren.

Untersuchungen:
- Redundancy,
- Confirmation,
- Elliott Rescue,
- Scanner Rescue,
- Conflict,
- Lead/Lag.

Lead/Lag:
- Ereignisfenster mindestens -20 bis +20 Handelstage,
- nicht nur Gleichzeitigkeit messen,
- getrennte Analyse nach Richtung und Wellengrad.

Statistik:
- Discovery/Validation/Holdout strikt trennen,
- Holdout nicht zur Musterauswahl verwenden,
- überlappende Forward-Windows nicht als unabhängige Beobachtungen behandeln,
- Effektstärke, Unsicherheit und Stichprobengröße berichten.

Abnahme:
- quantifiziert, ob Elliott inkrementellen Informationswert gegenüber vorhandenen Scannerlogiken besitzt.

## 6E – Externer Markt-/Branchenkontext

Ziel:
- `market_context_history.csv` separat von `history_analysis.csv` aufbauen,
- historische OHLC(V)-Reihen für belastbare Markt-/Sektor-/Themen-/Commodity-Proxies dokumentieren,
- Registry mit `valid_from`/`valid_to`, Quelle und Qualitätsstufe führen.

Priorität der Proxies:
1. offizieller/breiter Index,
2. breiter liquider Branchen-/Themen-ETF,
3. extern definierter und Point-in-Time-versionierter Peer-Korb,
4. Scanner-Peers nur als separater interner Kontext.

Keine manuell erfundenen Branchenkörbe aus wenigen Scannerwerten.

Research:
- Elliott-/Strukturzustand des Benchmarks,
- relative Stärke Aktie↔Sektor↔Markt,
- Leader/Follower/Divergence/Synchronized,
- Lead/Lag zwischen Aktie und Kontext.

Abnahme:
- jeder externe Kontext trägt `context_quality`,
- `unreliable`/`unavailable` kann keine reale Branchenwelle erzeugen.

## 6F – Historische Validierung

Ziel:
- vollständiger Walk-forward-/Out-of-sample-Test der eingefrorenen Regeln,
- keine rückwirkende Nutzung später bestätigter Pivots,
- keine heutige Branchenklassifikation rückwirkend ohne gültige PIT-Zuordnung.

Zu messen:
- Structural fit getrennt von Performance,
- Confirmation strength getrennt von Performance,
- Historical expectancy auf 5/10/20/40/60T sofern Daten reichen,
- Alpha gegen passende Benchmark, wenn verfügbar,
- Invalidierungsraten,
- Parameterstabilität,
- Cross-System-Mehrwert.

Abnahme:
- Ergebnisse reproduzierbar und ohne Holdout-Leakage.

## 6G – Moduloutput und spätere Integration

Moduloutput enthält mindestens:
- Szenarien,
- Wellengrad,
- bestätigte Pivots,
- Fibonacci-Geometrie,
- harte Invalidierungen,
- Structural fit,
- Confirmation strength,
- Historical expectancy,
- Routing-Trigger,
- Marktkontextqualität,
- Warnungen.

Nicht enthalten:
- autonomes Kaufen/Halten/Verkaufen.

Die spätere Decision-Schicht entscheidet aus dem Gesamtbild des Systems.

## Reihenfolge

Empfohlene Bearbeitung nach Modul 5:
`6A → 6B → 6C → 6D → 6E → 6F → 6G`.

6E kann parallel zu 6A–6C vorbereitet werden, sofern die Kontextdatenbeschaffung keinerlei produktive Scannerlogik verändert.

## Vor dem Start von Modul 6

- Modul 5 vollständig abschließen und dessen produktiven/frozen Zustand dokumentieren.
- `module6-elliott-vnext-foundations-v2` gegen dann aktuelles `main` aktualisieren oder die Foundations gezielt auf einen frischen Modul-6-Branch übernehmen.
- Keine Foundation-Regel stillschweigend ändern: Änderungen müssen als bewusste Research-Entscheidung dokumentiert werden.
