# Modul 6 – Elliott vNext: Research- und Bauplan

Status: vorbereitet, Start erst nach Abschluss von Modul 5.

Revision: Full-Cycle / Swing- und Positionsmanagement.

## Grundsatz

Modul 6 wird als unabhängiger Struktur-/Kontextsensor gebaut. Es darf keine autonome Handelsentscheidung treffen. Das finale Kaufen/Halten/Reduzieren/Verkaufen bleibt Aufgabe des späteren globalen Decision-Layers bzw. der orchestrierenden Depot-Watch.

Neu gegenüber der ersten Foundation: Modul 6 untersucht nicht nur W1→W2 als Einstiegslogik, sondern den vollständigen Zyklus `W1→W2→W3→W4→W5→höhergradige Korrektur` inklusive prospektiver Zielzonen und Swing-Routing.

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
- truncated fifth zulassen,
- Diagonalen als eigenen Musterzweig behandeln,
- Zigzag und Flat als erste vollständige Korrekturklassen implementieren,
- Triangle/WXY/WXYXZ mindestens als Alternativ-/Unsicherheitszustände repräsentieren,
- Szenario-ID über alle späteren Projektionen durchreichen.

Research:
- strukturelle Trefferhäufigkeit nach Wellengrad,
- Parameterstabilität,
- Häufigkeit konkurrierender Szenarien,
- historische Invalidierungsraten ohne Nachoptimierung.

Abnahme:
- keine Regelverletzung in als gültig ausgegebenen klassischen Impulsen,
- keine pauschale Invalidierung von Diagonalen wegen 4/1-Überlappung,
- kein hartes W5-Neuextrem-Erfordernis,
- Primär- und Alternativszenarien reproduzierbar.

## 6C – Fibonacci-Geometrie und prospektive Wellenkarte

Ziel:
- Fibonacci ausschließlich an bestätigten, strukturell zulässigen Ankern ansetzen,
- Retracements/Extensions als Zonen statt exakter Linien berechnen,
- Zonendicke mindestens ATR-/volatilitäts- und wellengradabhängig machen,
- 88,7 % als tiefe W2-Warnzone und W1-Ursprung als harte Invalidation getrennt halten,
- für plausible Szenarien zukünftige W3-/W4-/W5-Zonen kartieren,
- jede Projektion mit `scenario_id`, `basis` und `available_from` dokumentieren.

W2-Routing zunächst:
- `EW_PREWATCH_382`,
- `EW_DEEP_SCAN_500`,
- `EW_W2_CORE`,
- `EW_W2_DEEP`,
- `EW_W2_DANGER`,
- `EW_INVALIDATED`.

W3-Routing:
- `EW_W3_TARGET_APPROACH`,
- `EW_W3_EXHAUSTION`.

W4-Routing:
- `EW_W4_TARGET_ZONE`,
- `EW_W4_COMPLETION`.

W5-Routing:
- `EW_W5_TARGET_APPROACH`,
- `EW_W5_COMPLETION_RISK`.

Research W2:
- Triggerhäufigkeit,
- Zeit 38,2→50→61,8→78,6→88,7,
- nachfolgende Struktur und Forward-Returns.

Research W3:
- mehrere W1-basierte Extension-Kandidaten prospektiv erzeugen,
- Treffer-/Durchlaufhäufigkeit,
- Zeit bis Zielzone,
- Max-Favorable/Max-Adverse-Excursion vor und nach Zielberührung,
- Wahrscheinlichkeit und Tiefe einer anschließenden W4-Korrektur,
- Nutzen von Scanner-/RS-/Marktkontext zur Erkennung echter Ermüdung statt bloßer Zielberührung.

Research W4:
- Verteilung der W3-Retracements,
- Zeitdauer der Korrektur,
- Qualität möglicher Rückkauf-/Aufstockungszonen,
- Unterschiede nach Wellengrad und W3-Extension.

Research W5:
- mehrere Projektionsbasen getrennt testen,
- numerische W5-Level nicht vorab einfrieren,
- truncated fifth ausdrücklich zulassen,
- Korrekturtiefe/-dauer nach möglichem W5-Abschluss,
- Zusatznutzen von Scanner-/RS-/Marktkontext bei Gewinnsicherung.

Abnahme:
- Fibonacci beeinflusst niemals die Auswahl des zugrunde liegenden Counts,
- keine Zielzone wird als sichere Kursprognose ausgegeben,
- jede historische Projektion existiert erst ab ihrem kausalen `available_from`.

## 6D – Swing-Routing und Positionsmanagement-Forschung

Ziel:
- strukturell interessante Ebenen in Review-Prioritäten übersetzen, ohne autonome Handelsentscheidung.

Erlaubte Review-Kontexte:
- `entry_or_add_review`,
- `hold_review`,
- `partial_reduce_review`,
- `reentry_or_add_review`,
- `profit_protection_review`,
- `larger_reduce_or_exit_review`.

Zu testen:
- W2: wann verbessert eine Aufstockungsprüfung gegenüber einfachem Halten?
- W3: wann ist Teilreduktion historisch vorteilhaft gegenüber vollständigem Halten?
- W4: wann schafft Rückkauf nach vorheriger Teilreduktion realen Mehrwert?
- W5: wann ist größere Gewinnsicherung sinnvoller als weiteres Halten?
- Round-trip-Ergebnis von Teilverkauf + Rückkauf inklusive Spread/Kosten,
- Sensitivität gegen zu frühe/zu späte Wendebestätigung.

Abnahme:
- Routing ist reproduzierbar,
- Routing wird getrennt von finaler Entscheidung ausgegeben,
- keine theoretische Swing-Überrendite ohne Transaktionskosten-/Umsetzbarkeitsprüfung als nutzbar dargestellt.

## 6E – Scanner ↔ Elliott Cross-System Research

Ziel:
- Elliott-Ereignisse mit vorhandenen Scannerzuständen und eingefrorenen Timing-/Probability-/Confidence-Outputs verbinden,
- keine bestehende Phase neu optimieren.

Untersuchungen:
- Redundancy,
- Confirmation,
- Elliott Rescue,
- Scanner Rescue,
- Conflict,
- Lead/Lag,
- stage-specific incremental value,
- swing-routing value.

Lead/Lag:
- Ereignisfenster mindestens -20 bis +20 Handelstage,
- nicht nur Gleichzeitigkeit messen,
- getrennte Analyse nach Richtung, Wellengrad und Wellenphase.

Besonders prüfen:
- Momentum-/Score-Turn um mögliches W2-Ende,
- Overextension/Momentumverlust um W3-Zielnähe,
- Reversal- und Relative-Strength-Signale in W4-Zonen,
- Momentum-/Selection-Verschlechterung um mögliche W5-Abschlüsse.

Statistik:
- Discovery/Validation/Holdout strikt trennen,
- Holdout nicht zur Musterauswahl verwenden,
- überlappende Forward-Windows nicht als unabhängige Beobachtungen behandeln,
- Effektstärke, Unsicherheit und Stichprobengröße berichten.

Abnahme:
- quantifiziert, ob Elliott je Wellenphase inkrementellen Informationswert gegenüber vorhandenen Scannerlogiken besitzt.

## 6F – Externer Markt-/Branchenkontext

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
- Lead/Lag zwischen Aktie und Kontext,
- W3/W4/W5-Zustände der Aktie relativ zum übergeordneten Kontext.

Abnahme:
- jeder externe Kontext trägt `context_quality`,
- `unreliable`/`unavailable` kann keine reale Branchenwelle erzeugen.

## 6G – Historische Validierung

Ziel:
- vollständiger Walk-forward-/Out-of-sample-Test der eingefrorenen Regeln,
- keine rückwirkende Nutzung später bestätigter Pivots oder Projektionen,
- keine heutige Branchenklassifikation rückwirkend ohne gültige PIT-Zuordnung.

Zu messen:
- Structural fit getrennt von Performance,
- Confirmation strength getrennt von Performance,
- Historical expectancy auf 5/10/20/40/60T sofern Daten reichen,
- Alpha gegen passende Benchmark, wenn verfügbar,
- Invalidierungsraten,
- Parameterstabilität,
- W2/W3/W4/W5-Zielzonen-Treffer und Fehlsignale,
- Swing-Routing gegen Buy-and-Hold-/Hold-Baselines,
- Cross-System-Mehrwert.

Abnahme:
- Ergebnisse reproduzierbar und ohne Holdout-Leakage.

## 6H – Moduloutput und spätere Integration

Moduloutput enthält mindestens:
- Szenarien,
- Wellengrad,
- aktuellen Wellenzustand,
- bestätigte Pivots,
- Fibonacci-Geometrie,
- szenariospezifische prospektive Zielzonen,
- Wellenzyklus-Karte,
- harte Invalidierungen,
- Structural fit,
- Confirmation strength,
- Historical expectancy,
- Routing-Trigger,
- Swing-Review-Kontexte,
- Marktkontextqualität,
- Warnungen.

Nicht enthalten:
- autonomes Kaufen/Halten/Reduzieren/Verkaufen,
- konkrete Orderanweisung,
- scheinbar sicheres Kursziel.

Die spätere Decision-Schicht entscheidet aus dem Gesamtbild des Systems.

## Reihenfolge

Empfohlene Bearbeitung nach Modul 5:
`6A → 6B → 6C → 6D → 6E → 6F → 6G → 6H`.

6F kann parallel zu 6A–6C vorbereitet werden, sofern die Kontextdatenbeschaffung keinerlei produktive Scannerlogik verändert.

## Vor dem Start von Modul 6

- Modul 5 vollständig abschließen und dessen produktiven/frozen Zustand dokumentieren.
- `module6-elliott-vnext-foundations-v2` gegen dann aktuelles `main` aktualisieren oder die Foundations gezielt auf einen frischen Modul-6-Branch übernehmen.
- Für den Modulstart die v2-Verträge verwenden: `configs/elliott_vnext_contract_v2.json` und `configs/elliott_vnext_output_schema_v2.json`.
- Keine Foundation-Regel stillschweigend ändern: Änderungen müssen als bewusste Research-Entscheidung dokumentiert werden.
