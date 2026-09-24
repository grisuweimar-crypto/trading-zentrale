# Modul 6 – Elliott vNext: Foundations

Status: Vorbereitung / Research-Grundlage, noch keine produktive Integration.

Diese Datei friert die methodischen Grundlagen für das spätere Modul 6 ein. Modul 5 bleibt unangetastet. Elliott/Fibonacci ist kein autonomes Handelssystem und erzeugt keine Kauf-/Verkaufsentscheidung. Das Modul liefert strukturierte Evidenz an den späteren Gesamt-Decision-Layer.

## 1. Rolle im Gesamtsystem

Das Gesamturteil entsteht erst aus dem Zusammenspiel aller verfügbaren, empirisch geprüften Quellen, insbesondere Selection, Timing, Risk, Confidence, Probability, Fundamentals, Markt-/Sektorkontext, relative Stärke, Elliott/Fibonacci und weiteren später validierten Modulen.

Elliott/Fibonacci darf:
- Strukturen, Szenarien und Invalidierungen beschreiben,
- einen Deep-Scan des Scanners auslösen,
- durch Scanner-Signale selbst zu einer tieferen Analyse getriggert werden,
- als zusätzliche Evidenz in die spätere Entscheidung eingehen.

Elliott/Fibonacci darf nicht:
- autonom `buy`, `hold` oder `sell` ausgeben,
- die bestehende Selection-/Timing-/Risk-/Confidence-Logik ersetzen,
- Fibonacci-Level benutzen, um rückwirkend eine gewünschte Wellenzählung passend zu machen.

## 2. Datenbasis und Point-in-Time-Regel

Primäre Preisbasis ist die dokumentierte tägliche OHLCV-Historie. Daily-Daten bleiben Source of Truth; Weekly-Daten werden daraus deterministisch aggregiert. Minutencharts sind für v1 nicht erforderlich.

Jeder Pivot besitzt mindestens zwei Zeitpunkte:
- `pivot_time`: Zeitpunkt des tatsächlichen Hochs/Tiefs,
- `confirmed_time`: erster Zeitpunkt, an dem der Pivot mit den damals verfügbaren Daten erkannt werden durfte.

Im Backtest darf ein Pivot ausschließlich ab `confirmed_time` verwendet werden. Ein später sichtbarer Swing darf niemals rückwirkend einen historischen Elliott-Zustand verbessern. Dasselbe gilt für daraus abgeleitete Fibonacci-Level.

## 3. Fraktale Architektur

Normalweg: top-down.

1. Weekly / grober Tagesgrad: übergeordneten Kontext bestimmen.
2. Mittlerer Tagesgrad: mögliche Parent-Struktur und relevante Korrekturzone identifizieren.
3. Feiner Tagesgrad: Unterstruktur, Reaktion und Bestätigung prüfen.

Zusätzlich ist bottom-up erlaubt:
- Ein statistisch auffälliges Scannerereignis kann `EW_BOTTOM_UP_SCAN` auslösen.
- Eine auffällige lokale Elliott-Struktur kann einen Scanner-Deep-Scan auslösen.

Der Algorithmus darf mehrere Wellengrade gleichzeitig führen. Ein kleiner 1-2-3-4-5-Impuls kann Teil einer größeren Welle 1, A, C oder einer anderen Parent-Struktur sein.

## 4. Elliott-Regelwerk

Quelle für die v1-Regeln: `https://elliottwaver.live/kompendium/`.

### 4.1 Harte Regeln für klassischen Impuls

Für einen klassischen bullischen Impuls gelten strikt:
1. Welle 2 darf den Ursprung von Welle 1 nicht unterschreiten.
2. Welle 3 darf nicht die kürzeste der Impulswellen 1, 3 und 5 sein.
3. Welle 4 darf nicht in das Preisgebiet von Welle 1 eindringen.

Für bärische Strukturen gelten die Regeln spiegelbildlich.

### 4.2 Diagonalen

Leading und Ending Diagonals sind ein eigener Musterzweig. Dort ist eine 4/1-Überlappung zulässig bzw. typisch. Eine solche Überlappung darf daher einen klassischen Impuls invalidieren, aber nicht pauschal jede Elliott-Struktur.

### 4.3 Korrekturklassen

Mindestens zu berücksichtigen:
- Zigzag 5-3-5,
- Flat 3-3-5,
- Triangle 3-3-3-3-3,
- Double Three WXY,
- Triple Three WXYXZ.

V1 darf zunächst Impuls, Zigzag und Flat vollständig implementieren und komplexere Strukturen als alternative/unsichere Szenarien führen, sofern dies ausdrücklich markiert wird. Es darf niemals jede Bewegung zwanghaft in fünf Wellen gepresst werden.

## 5. Primär- und Alternativszenarien

Das Modul behauptet nie, den einzig wahren Count erkannt zu haben.

Mindestens auszugeben:
- `primary_scenario`,
- `alternative_scenarios`,
- `hard_invalidations`,
- `rule_violations`,
- `structural_fit`,
- `confirmation_strength`,
- `historical_expectancy`,
- `warnings`.

`structural_fit` beschreibt ausschließlich die formale Passung zum Elliott-Regelwerk.
`confirmation_strength` beschreibt die externe/reaktive Bestätigung, z. B. Kursreaktion, Momentum, relative Stärke oder Marktkontext.
`historical_expectancy` stammt ausschließlich aus Point-in-Time-sauberer historischer Validierung.

Diese Größen dürfen nicht zu einer scheinpräzisen Elliott-`confidence` vermischt werden, bevor ihre Kalibrierung empirisch belegt ist.

## 6. Fibonacci-Layer

Fibonacci wird erst nach einer regelkonform identifizierten Wellenstrecke angesetzt.

Grundprinzip:
- Elliott bestimmt die strukturell zulässigen Anker.
- Fibonacci beschreibt die Geometrie zwischen diesen Ankern.

V1-Retracement-Zonen aus dem Kompendium:
- Welle 2: 50 %, 61,8 %, 78,6 %, 88,7 % von Welle 1.
- Welle 4: 14,6 %, 23,6 %, 38,2 %, typischerweise nicht mehr als 50 % von Welle 3.

88,7 % ist für Welle 2 eine tiefe Warn-/Grenzzone, aber nicht die harte Elliott-Invalidierung. Die harte strukturelle Invalidierung erfolgt erst beim Bruch des Ursprungs von Welle 1.

Fib-Level werden als Zonen behandelt, nicht als exakte Wendepunkte. Die Breite einer Zone soll mindestens Volatilität/ATR, Preisniveau und Wellengrad berücksichtigen. Liquidität, Spread und Gaps können später ergänzt werden.

Fibonacci selbst bleibt geometrische Information. Momentum, Scannerzustand oder Marktkontext dürfen nicht in einen Fib-Score eingerechnet und anschließend im Decision-Layer ein zweites Mal gezählt werden.

## 7. Triggerkaskade

Vorläufige, historisch zu validierende Trigger:

- `EW_CONTEXT`: regelkonforme übergeordnete Struktur vorhanden.
- `EW_PREWATCH_382`: Korrektur erreicht ungefähr 38,2 %; günstige Vorwarnung.
- `EW_DEEP_SCAN_500`: Korrektur erreicht ungefähr 50 %; tiefe Elliott- und Scanneranalyse auslösen.
- `EW_W2_CORE`: W2 liegt in der Kernzone 50–78,6 %.
- `EW_W2_DEEP`: 78,6–88,7 %; strukturell möglich, aber erhöhte Vorsicht.
- `EW_W2_DANGER`: tiefer als 88,7 %, Ursprung W1 noch intakt; Alternativszenario hochstufen.
- `EW_INVALIDATED`: W1-Ursprung gebrochen; konkreten Count verwerfen.
- `EW_BOTTOM_UP_SCAN`: statistisches Scannerereignis löst lokale Elliott-Analyse mit anschließendem Herauszoomen aus.

Diese Schwellen sind Forschungs- und Routing-Trigger, keine Kauf-/Verkaufssignale.

## 8. Scanner ↔ Elliott: Cross-System-Research

Zu testen sind mindestens:
- Redundancy: beschreibt Elliott nur bereits vorhandene Scannerinformation?
- Confirmation: verbessert gemeinsame Evidenz die historische Trennschärfe?
- Rescue: findet Elliott relevante Situationen ohne Scannertrigger?
- Scanner Rescue: erkennt der Scanner Bewegungen früher als Elliott?
- Conflict: sind widersprüchliche Zustände selbst prognostisch nützlich?
- Lead/Lag: welches System reagiert typischerweise zuerst?

Lead/Lag wird als Ereignisfenster gemessen, nicht nur als Gleichzeitigkeit am selben Tag. Typische Fenster: -20T bis +20T um das jeweilige Ereignis.

Discovery, Validation und Holdout bleiben getrennt. Der Holdout darf niemals zur Musterauswahl oder Parameteranpassung verwendet werden. Überlappende Forward-Windows sind nicht als unabhängige Beobachtungen zu behandeln.

## 9. Markt-, Branchen- und Kohortenkontext

Der Scannerbestand ist keine automatisch repräsentative Branche. Zwei oder drei beobachtete Uran-, Cannabis- oder Robotics-Werte dürfen nicht als Branchenindex interpretiert werden.

Externer Kontext wird separat gespeichert und erhält einen Qualitätsstatus:
- `sufficient`: ausreichend breiter, sauberer und historisch belastbarer Benchmark,
- `limited`: brauchbarer Proxy mit dokumentierten Einschränkungen,
- `unreliable`: zu enge/instabile Abdeckung,
- `unavailable`: kein belastbarer Kontext.

Nur `sufficient` und nach expliziter Kennzeichnung `limited` dürfen als echte Markt-/Sektorevidenz in Analysen eingehen. `unreliable` und `unavailable` erzeugen keine Branchenwellen.

Bevorzugte Datenhierarchie:
1. geeigneter offizieller/breiter Index,
2. geeigneter liquider Branchen-/Themen-ETF,
3. extern definierter, ausreichend breiter und Point-in-Time-versionierter Peer-Korb,
4. Scanner-Peers nur als separater `scanner_peer_context`, niemals als Marktdefinition.

Historische Kontextkurse werden getrennt von `history_analysis.csv` geführt. Zielartefakt: `artifacts/research/market_context_history.csv` gemäß eigenem Schema.

## 10. Relative Stärke und Hierarchie

Nach Verfügbarkeit valider Kontextdaten sollen mindestens berechnet werden:
- `stock_vs_benchmark_rs`,
- `stock_vs_sector_rs`,
- `sector_vs_market_rs`.

Ableitbare Zustände:
- `leader`,
- `follower`,
- `divergence`,
- `synchronized`.

Diese Zustände werden separat von Elliott berechnet und später als Confirmation/Conflict-Evidenz zusammengeführt.

## 11. V1-Umfang von Modul 6

V1 soll bewusst begrenzt bleiben:
- Daily + daraus abgeleitet Weekly,
- kausal bestätigte Pivots,
- klassische Impulse,
- Zigzag und Flat vollständig,
- Diagonalen als eigener, konservativ behandelter Zweig,
- komplexe Korrekturen zunächst mindestens als Alternativ-/Unsicherheitszustände,
- Fibonacci-Zonen statt exakter Linien,
- Primär- und Alternativszenario,
- harte Invalidation getrennt von weicher Evidenz,
- Cross-System-Research mit Scannermerkmalen,
- externer Kontext nur bei ausreichender Datenqualität,
- kein autonomes Handelsurteil.

## 12. Abnahmekriterien vor produktiver Nutzung

Modul 6 ist erst produktionsreif, wenn mindestens folgende Punkte erfüllt sind:
- PIT-sichere Pivotbestätigung nachweisbar,
- keine zukünftigen Daten in historischen Counts/Fib-Leveln,
- harte Regeln und Diagonal-Ausnahmen getestet,
- Szenario-Output reproduzierbar,
- Parameterstabilität über sinnvolle Swing-/ATR-Bereiche geprüft,
- Discovery/Validation/Holdout sauber getrennt,
- Cross-System-Mehrwert gegenüber bestehenden Scannerlogiken gemessen,
- Markt-/Sektorkontext separat qualitätsgesichert,
- Decision-Layer konsumiert Elliott nur als Evidenz und behält das Gesamturteil.

## 13. Noch ausdrücklich offen

Nicht vor Modul 6 vorentscheiden:
- optimale Pivot-/ATR-Parameter pro Wellengrad,
- genaue Zonendicke der Fib-Level,
- Gewichtung von Elliott im späteren Decision-Layer,
- konkrete externe Benchmarks und deren Quellen,
- empirische Relevanz einzelner Korrekturklassen,
- finale Triggerprioritäten und Rechenbudgets.

Diese Punkte werden in Modul 6 empirisch bestimmt und nicht aus Lehrbuchregeln abgeleitet.