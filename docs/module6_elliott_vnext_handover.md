# Projektübergabe – Trading-Zentrale / Scanner-vNext

## Modul 6: Elliott vNext

Stand: 24.09.2026

Repository: `grisuweimar-crypto/trading-zentrale`

Wichtig: Modul 6 wird erst nach vollständigem Abschluss von Modul 5 gebaut. Die Grundlagen wurden auf einem separaten Branch vorbereitet und dürfen Modul 5 nicht verändern.

Vorbereitungs-Branch: `module6-elliott-vnext-foundations-v2`

## 1. Grundprinzipien des Gesamtprojekts

Weiterhin verbindlich:

- strikt empirisch arbeiten,
- keine erfundenen Kurse, Scannerwerte, Ergebnisse oder Quellen,
- fehlende Daten ausdrücklich als fehlend behandeln,
- Point-in-Time-Regel strikt einhalten,
- keine heutigen Fundamentals, Risiken oder sonstigen Daten rückwirkend in historische Scannerzustände einbauen,
- Originalwährungen verwenden,
- Selection, Timing, Risk, Probability und Confidence konzeptionell getrennt halten,
- Discovery und Validation sauber trennen,
- Holdout niemals zur nachträglichen Musterauswahl benutzen,
- überlappende Forward-Windows statistisch nicht als unabhängige Beobachtungen behandeln,
- historische Punktwerte und statistische Sicherheit klar unterscheiden.

## 2. Rolle von Modul 6

Elliott vNext ist kein autonomes Handelssystem. Es darf niemals allein `buy`, `hold` oder `sell` ausgeben.

Das spätere Gesamturteil entsteht aus dem gesamten System, u. a.:

- Selection,
- Timing,
- Risk,
- Probability,
- Confidence,
- Fundamentals,
- Markt-/Sektor-/Themenkontext,
- relativer Stärke,
- Elliott/Fibonacci,
- weiteren später validierten Modulen,
- orchestriert durch den späteren Decision-Layer bzw. die tägliche Wertpapierdepot-Watch.

Elliott/Fibonacci liefert nur strukturierte Evidenz und Trigger für tiefere Analysen.

## 3. Verbindliche Regelquelle

Für die Elliott-v1-Regeln ist das Kompendium von ElliottWaver.live die Arbeitsreferenz:

`https://elliottwaver.live/kompendium/`

Wichtige Regeln:

- Welle 2 darf im klassischen Impuls den Ursprung von Welle 1 nicht überschreiten.
- Welle 3 darf nicht die kürzeste der Impulswellen 1, 3 und 5 sein.
- Welle 4 darf im klassischen Impuls nicht in das Preisgebiet von Welle 1 eindringen.
- Leading/Ending Diagonals sind ein eigener Zweig; 4/1-Überlappung darf dort nicht pauschal invalidieren.
- Eine truncated fifth muss möglich bleiben; Welle 5 muss nicht zwingend ein neues Extrem ausbilden.
- Korrekturen können Zigzag, Flat, Triangle, Double Three und Triple Three sein; nicht jede Bewegung darf in einen 5er-Impuls gezwungen werden.
- Primär- und Alternativszenarien sind ausdrücklich erwünscht.

## 4. Kausale Pivot-Erkennung

Dies ist eine harte technische Anforderung.

Jeder Pivot benötigt mindestens:

- `pivot_time`: tatsächlicher Zeitpunkt des Hochs/Tiefs,
- `confirmed_time`: erster Zeitpunkt, an dem der Pivot mit den damals bekannten Daten erkannt werden durfte.

Historische Counts und Fibonacci-Level dürfen einen Pivot erst ab `confirmed_time` benutzen.

Kein rückwirkendes Verschönern historischer Counts mit später bekannten Swingpunkten.

## 5. Fraktale Architektur

Normalweg: top-down.

1. Weekly / grober Tagesgrad: übergeordnete Struktur bestimmen.
2. Mittlerer Tagesgrad: Parent-Struktur und relevante Korrektur identifizieren.
3. Feiner Tagesgrad: Unterwellen und Reaktion prüfen.

Zusätzlich bottom-up:

- Ein starkes statistisches Scannerereignis darf `EW_BOTTOM_UP_SCAN` auslösen.
- Dann lokal Elliott-Struktur prüfen und anschließend herauszoomen, um Parent-Kontext zu bestimmen.
- Umgekehrt darf eine Elliott-Struktur einen Scanner-Deep-Scan auslösen.

Mehrere Wellengrade dürfen gleichzeitig gültige Szenarien führen.

## 6. Fibonacci

Fibonacci wird niemals verwendet, um eine Wellenzählung passend zu machen.

Reihenfolge:

1. strukturell zulässige Welle erkennen,
2. kausal bestätigte Anker bestimmen,
3. Fibonacci daran ansetzen,
4. Retracement/Extension als Geometrie ausgeben,
5. Bestätigung durch Scanner, Momentum, relative Stärke oder Marktkontext getrennt bewerten.

Fibonacci-Zonen statt exakter Linien.

Zonendicke soll mindestens ATR/Volatilität, Preisniveau und Wellengrad berücksichtigen.

W2-Zonen zunächst:

- 38,2 % = PREWATCH / früher Routing-Trigger,
- 50–78,6 % = Kernbereich,
- 78,6–88,7 % = tiefe Zone,
- >88,7 % bei intaktem W1-Ursprung = Danger / Alternativcount hochstufen,
- Bruch des W1-Ursprungs = harte Invalidierung des konkreten Counts.

88,7 % ist ausdrücklich keine harte Elliott-Invalidierung.

## 7. Routing-Trigger

Vorläufige, später historisch zu validierende Zustände:

- `EW_CONTEXT`
- `EW_PREWATCH_382`
- `EW_DEEP_SCAN_500`
- `EW_W2_CORE`
- `EW_W2_DEEP`
- `EW_W2_DANGER`
- `EW_INVALIDATED`
- `EW_BOTTOM_UP_SCAN`

Diese Zustände sind keine Handelssignale.

## 8. Cross-System-Research

Untersuchen:

- Redundancy: liefert Elliott nur bereits bekannte Scannerinformation?
- Confirmation: verstärken sich unabhängig erzeugte Signale?
- Elliott Rescue: findet Elliott gute Situationen ohne Scannertrigger?
- Scanner Rescue: erkennt der Scanner Bewegungen früher als Elliott?
- Conflict: sind widersprüchliche Zustände selbst informativ?
- Lead/Lag: welches System reagiert typischerweise zuerst?

Lead/Lag nicht nur am selben Tag messen, sondern in Ereignisfenstern, zunächst etwa -20T bis +20T.

Bestehende Scannerphasen dürfen dafür nicht rückwirkend neu optimiert werden.

## 9. Evidenzdimensionen getrennt halten

Nicht eine scheinpräzise Elliott-Confidence erzeugen.

Mindestens getrennt:

- `structural_fit`: formale Passung zum Elliott-Regelwerk,
- `confirmation_strength`: externe/reaktive Bestätigung,
- `historical_expectancy`: empirischer Verlauf vergleichbarer historischer Setups.

Diese Größen dürfen erst später kalibriert zusammengeführt werden, wenn der empirische Mehrwert belegt ist.

## 10. Markt-, Branchen- und Themenkontext

Keine Branche aus wenigen Scannerwerten erfinden.

Scanner-Peers sind nur `scanner_peer_context`, niemals automatisch reale Branchenrepräsentation.

Externe Kontextreihen getrennt von `history_analysis.csv` führen.

Zielartefakt:

`artifacts/research/market_context_history.csv`

Registry:

`data/inputs/market_context_registry.csv`

Qualitätszustände:

- `sufficient`
- `limited`
- `unreliable`
- `unavailable`

Nur `sufficient` und nach dokumentierter Einschränkung `limited` dürfen als reale Markt-/Sektorevidenz verwendet werden.

Bevorzugte Proxy-Hierarchie:

1. offizieller/breiter Index,
2. breiter liquider Branchen-/Themen-ETF,
3. extern definierter, Point-in-Time-versionierter Peer-Korb,
4. Scanner-Peers nur als interner Zusatzkontext.

Die Registry ist absichtlich noch leer. Keine Benchmarks ohne Prüfung eintragen.

## 11. Relative Stärke

Nach Verfügbarkeit valider Kontextdaten untersuchen:

- `stock_vs_benchmark_rs`
- `stock_vs_sector_rs`
- `sector_vs_market_rs`

Mögliche Zustände:

- `leader`
- `follower`
- `divergence`
- `synchronized`

Diese Zustände separat berechnen und erst danach mit Elliott-/Scannerzuständen kombinieren, um Doppelzählung zu vermeiden.

## 12. Bereits gewonnene Research-Erkenntnisse

Explorative History- und Depottests haben folgende Hypothesen gestützt, aber noch nicht produktiv validiert:

- grobe/übergeordnete Elliott-Strukturen sind stabiler als sehr feine Counts,
- ein top-down Ansatz reduziert scheinbare Fehlstrukturen,
- 38,2 % eignet sich als günstiger Vorwarn-/Routing-Punkt,
- 50 % ist ein plausibler Startpunkt für tiefe Analyse,
- erst bei 61,8 % zu starten wäre häufig zu spät,
- Elliott und Scanner scheinen nicht vollständig redundant,
- Lead/Lag zwischen Scanner-Timing und Elliott/Fib ist wahrscheinlich wichtiger als reine Gleichzeitigkeit,
- Fib-Schönheit darf die Auswahl des Counts nicht beeinflussen.

Diese Punkte sind Forschungsstand, keine eingefrorenen Performancebehauptungen.

## 13. V1-Umfang

V1 zunächst bewusst begrenzen:

- Daily + daraus abgeleitet Weekly,
- kausal bestätigte Pivots,
- klassische Impulse,
- Zigzag und Flat vollständig,
- Diagonalen als eigener konservativer Zweig,
- komplexere Korrekturen mindestens als Alternativ-/Unsicherheitszustände,
- Fibonacci-Zonen,
- Primär- und Alternativszenarien,
- harte Invalidierung getrennt von weicher Evidenz,
- Scanner↔Elliott Cross-System-Research,
- externer Kontext nur bei ausreichender Qualität,
- kein autonomes Handelsurteil.

## 14. Vorbereitete Dateien

Auf dem Foundation-Branch liegen:

- `docs/elliott_vnext_6_foundations.md`
- `docs/elliott_vnext_6_research_plan.md`
- `docs/module6_elliott_vnext_handover.md`
- `configs/elliott_vnext_contract_v1.json`
- `configs/elliott_vnext_output_schema_v1.json`
- `configs/market_context_history_schema_v1.json`
- `data/inputs/market_context_registry.csv`
- `tests/test_elliott_vnext_foundations.py`

## 15. Startauftrag für den neuen Chat

1. Zuerst prüfen, dass Modul 5 vollständig abgeschlossen und `main` sauber ist.
2. Foundation-Branch mit aktuellem `main` vergleichen; Foundations ggf. auf frischen Modul-6-Arbeitsbranch übernehmen.
3. Keine früheren Phasen erneut aufrollen, sofern für Modul 6 nicht zwingend nötig.
4. Mit 6A beginnen: kausaler Pivot-/Wellengrad-Layer.
5. Danach 6B Szenario-Generator/Regelprüfer, 6C Fibonacci-Geometrie, 6D Cross-System-Research, 6E Marktkontext, 6F Validierung, 6G Output/Integration.
6. Jede Phase mit Tests, reproduzierbaren Artefakten und sauberem PR abschließen.

Ziel: Elliott/Fibonacci als erklärbaren, empirisch geprüften Sensor in das Gesamtsystem integrieren, ohne die finale Entscheidungsgewalt aus dem globalen Decision-Layer herauszulösen.
