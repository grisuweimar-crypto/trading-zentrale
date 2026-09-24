# Projektübergabe – Trading-Zentrale / Scanner-vNext

## Modul 6: Elliott vNext – Full Cycle / Swing & Positionsmanagement

Stand: 24.09.2026

Repository: `grisuweimar-crypto/trading-zentrale`

Vorbereitungs-Branch: `module6-elliott-vnext-foundations-v2`

Wichtig: Modul 6 wird erst nach vollständigem Abschluss von Modul 5 gebaut. Die Grundlagen wurden auf einem separaten Branch vorbereitet und dürfen Modul 5 nicht verändern.

## 1. Grundprinzipien des Gesamtprojekts

Weiterhin verbindlich:
- strikt empirisch arbeiten,
- keine erfundenen Kurse, Scannerwerte, Ergebnisse oder Quellen,
- fehlende Daten ausdrücklich als fehlend behandeln,
- Point-in-Time-Regel strikt einhalten,
- keine heutigen Informationen rückwirkend in historische Zustände einbauen,
- Originalwährungen verwenden,
- Selection, Timing, Risk, Probability und Confidence getrennt halten,
- Discovery, Validation und Holdout sauber trennen,
- Holdout niemals zur nachträglichen Musterauswahl benutzen,
- überlappende Forward-Windows nicht als unabhängige Beobachtungen behandeln,
- statistische Sicherheit und eigentliche Signal-/Punktwerte unterscheiden.

## 2. Rolle von Modul 6

Elliott vNext ist kein autonomes Handelssystem.

Das Modul darf niemals allein `BUY`, `HOLD`, `REDUCE` oder `SELL` entscheiden und keine konkrete Orderanweisung erzeugen.

Das finale Urteil entsteht erst aus dem gesamten System: Selection, Timing, Risk, Probability, Confidence, Fundamentals, Markt-/Sektor-/Themenkontext, relative Stärke, Elliott/Fibonacci und weiteren validierten Modulen; orchestriert durch den späteren globalen Decision-Layer bzw. die tägliche Wertpapierdepot-Watch.

Neu: Elliott vNext soll nicht nur W1→W2-Einstiege erkennen, sondern den vollständigen Zyklus als prospektive Handlungskarte führen:

`W1 → W2 → W3 → W4 → W5 → Korrektur höheren Grades`

## 3. Verbindliche Regelquelle

Arbeitsreferenz: `https://elliottwaver.live/kompendium/`

Harte Kernregeln für klassischen Impuls:
- W2 darf den Ursprung von W1 nicht überschreiten.
- W3 darf nicht die kürzeste der Impulswellen W1/W3/W5 sein.
- W4 darf beim klassischen Impuls nicht in W1-Preisgebiet eindringen.

Zusätzlich:
- Leading/Ending Diagonal als eigener Regelzweig; 4/1-Überlappung dort nicht pauschal invalidieren.
- Truncated fifth muss möglich bleiben; W5 muss nicht zwingend ein neues Extrem erzeugen.
- Korrekturklassen: Zigzag, Flat, Triangle, Double Three, Triple Three.
- Nicht jede Bewegung in fünf Wellen zwängen.
- Primär- und Alternativszenarien führen.

## 4. Kausale Pivot- und Projektionslogik

Jeder Pivot benötigt:
- `pivot_time`,
- `confirmed_time`.

Ein historischer Count darf einen Pivot erst ab `confirmed_time` verwenden.

Jede prospektive Zielzone benötigt zusätzlich `available_from`: den frühesten Zeitpunkt, an dem alle nötigen Anker kausal verfügbar waren.

Keine historischen Zielkarten rückwirkend mit später bekannten Ankern verbessern.

## 5. Fraktale Architektur

Normalweg top-down:
1. Weekly / grober Tagesgrad,
2. mittlerer Tagesgrad,
3. feiner Tagesgrad.

Zusätzlich bottom-up:
- statistisch auffälliges Scannerereignis → `EW_BOTTOM_UP_SCAN`,
- lokale Elliott-Struktur → Parent-Kontext herauszoomen,
- Elliott-Struktur darf umgekehrt Scanner-Deep-Scan auslösen.

Mehrere Wellengrade und mehrere Szenarien dürfen gleichzeitig bestehen.

## 6. Fibonacci-Grundsatz

Reihenfolge zwingend:
1. strukturell zulässige Welle erkennen,
2. kausal bestätigte Anker bestimmen,
3. Fibonacci daran ansetzen,
4. Retracement/Extension als Zone ausgeben,
5. Bestätigung durch Scanner/Momentum/RS/Marktkontext getrennt bewerten.

Fib darf niemals den Count auswählen.

Zonendicke mindestens abhängig von ATR/Volatilität, Preisniveau und Wellengrad.

## 7. W2 – Einstieg/Aufstockung

Vorläufige Routing-Zonen:
- 38,2 % → `EW_PREWATCH_382`,
- 50 % → `EW_DEEP_SCAN_500`,
- 50–78,6 % → `EW_W2_CORE`,
- 78,6–88,7 % → `EW_W2_DEEP`,
- >88,7 % bei intaktem W1-Ursprung → `EW_W2_DANGER`,
- Bruch W1-Ursprung → `EW_INVALIDATED`.

88,7 % ist keine harte Elliott-Invalidierung.

## 8. W3 – Swing-Teilgewinn / Überdehnung

Nach kausal ausreichend bestimmtem W2-Ende mehrere W3-Projektionszonen erzeugen.

Erste Research-Kandidaten für W1-basierte Extensions: 1,0 / 1,618 / 2,0 / 2,618 / 3,236. Diese sind Research-Hypothesen, keine eingefrorenen Produktionsregeln.

Zustände:
- `wave_3_in_progress`,
- `EW_W3_TARGET_APPROACH`,
- `EW_W3_EXHAUSTION`.

W3-Zielnähe darf maximal eine Prüfung `hold vs partial reduce` priorisieren.

Eine W3-Zielzone allein ist niemals Verkaufsgrund.

## 9. W4 – Rückkauf/Aufstockung

Nach ausreichend bestätigtem W3-Ende mögliche W4-Retracementzonen der W3-Strecke kartieren.

Zustände:
- `wave_4_in_progress`,
- `EW_W4_TARGET_ZONE`,
- `EW_W4_COMPLETION`.

W4 kann nach einem taktischen W3-Teilverkauf als möglicher Rückkauf-/Aufstockungskontext dienen.

## 10. W5 – Gewinnsicherung / größere Reduktion

Nach plausiblem W4-Ende mehrere W5-Projektionsmethoden empirisch vergleichen.

Die numerischen W5-Level sind ausdrücklich noch nicht eingefroren.

Mindestens untersuchen:
- W1-Länge relativ zum W4-Ende,
- W1→W3-Struktur relativ zum W4-Ende.

Zustände:
- `wave_5_in_progress`,
- `EW_W5_TARGET_APPROACH`,
- `EW_W5_COMPLETION_RISK`,
- `post_wave_5_higher_degree_correction_risk`.

W5-Ende ist positionsstrategisch anders als W3-Ende: Eine mögliche W5-Vollendung kann eine stärkere Gewinnsicherungs-/Reduktionsprüfung rechtfertigen, weil eine Korrektur höheren Grades folgen kann.

Truncated fifth weiterhin zulassen.

## 11. Prospektive Preiszonen

Jede Zielzone mindestens mit:
- `scenario_id`,
- `wave_role`,
- `projection_type`,
- `price_low`,
- `price_high`,
- `basis`,
- `available_from`,
- `status`,
- optional `distance_to_zone_pct`.

Verboten:
- ein scheinexaktes Einzelkursziel,
- Zielzone als sichere Prognose darstellen,
- Projektion mit damals unbekannten Ankern,
- Zielzonen verschiedener Szenarien unmarkiert vermischen.

## 12. Swing-Routing statt Handelsentscheidung

Erlaubte Review-Kontexte:
- `entry_or_add_review`,
- `hold_review`,
- `partial_reduce_review`,
- `reentry_or_add_review`,
- `profit_protection_review`,
- `larger_reduce_or_exit_review`.

Diese sind ausdrücklich keine finalen Entscheidungen.

Beispiel:
- W3-Zone + Scanner weiter stark → eher Hold-Review.
- W3-Zone + Overextension + Momentumbruch → Partial-Reduce-Review.
- W4-Zone + Scanner-Turn positiv → Reentry/Add-Review.
- W5-Zone + Unterwellenabschluss + Scanner kippt → Larger-Reduce/Exit-Review.

## 13. Cross-System-Research

Mindestens untersuchen:
- Redundancy,
- Confirmation,
- Elliott Rescue,
- Scanner Rescue,
- Conflict,
- Lead/Lag,
- stage-specific incremental value,
- swing-routing value.

Lead/Lag nicht nur am selben Tag messen, sondern zunächst ungefähr -20T bis +20T um Ereignisse.

Scanner-/Elliott-Interaktionen getrennt nach W2/W3/W4/W5 analysieren.

## 14. Markt-/Branchen-/Themenkontext

Keine reale Branche aus wenigen Scannerwerten konstruieren.

Externe Kontextdaten separat in `artifacts/research/market_context_history.csv` führen.

Registry: `data/inputs/market_context_registry.csv`.

Qualität: `sufficient`, `limited`, `unreliable`, `unavailable`.

Nur `sufficient` und dokumentiert `limited` dürfen echte Markt-/Sektorevidenz liefern.

Proxy-Hierarchie:
1. offizieller/breiter Index,
2. breiter liquider Branchen-/Themen-ETF,
3. extern definierter PIT-versionierter Peer-Korb,
4. Scanner-Peers nur interner Zusatzkontext.

## 15. Relative Stärke

Nach verfügbaren validen Kontextdaten untersuchen:
- `stock_vs_benchmark_rs`,
- `stock_vs_sector_rs`,
- `sector_vs_market_rs`.

Zustände: `leader`, `follower`, `divergence`, `synchronized`.

## 16. Historische Forschungsfragen für Swing/Exit

Mindestens:
- Wie oft erreicht ein valides W2-Szenario die W3-Zonen?
- Wie oft folgt auf W3-Zielnähe eine relevante W4-Korrektur?
- Welche W4-Tiefen sind je Wellengrad typisch?
- Bringt Teilverkauf an W3 + Rückkauf in W4 gegenüber einfachem Halten Mehrwert?
- Welche W5-Projektionsmethoden besitzen echte Trennschärfe?
- Wie tief/oft korrigiert der Kurs nach möglichem W5-Ende?
- Welche Scanner-/RS-/Marktkontextsignale unterscheiden Zielberührung von tatsächlicher Wende?
- Wie stark beeinflussen Spread, Gebühren und Bestätigungslatenz den theoretischen Swing-Vorteil?

## 17. Evidenzdimensionen getrennt halten

Mindestens:
- `structural_fit`,
- `confirmation_strength`,
- `historical_expectancy`.

Keine unkalibrierte Gesamt-Confidence daraus konstruieren.

## 18. Aktuelle Research-Hypothesen

Explorativ gestützt, noch nicht produktiv validiert:
- grobe Strukturen stabiler als sehr feine,
- top-down als Normalweg sinnvoll,
- 38,2 % als Prewatch,
- 50 % als Deep-Scan-Schwelle,
- Elliott und Scanner nicht vollständig redundant,
- Lead/Lag wichtiger als reine Gleichzeitigkeit,
- W3/W4/W5 machen Elliott erst zu einem echten Positionsmanagement-Sensor.

## 19. Verbindliche v2-Dateien

- `docs/elliott_vnext_6_foundations.md`
- `docs/elliott_vnext_6_research_plan.md`
- `docs/module6_elliott_vnext_handover.md`
- `configs/elliott_vnext_contract_v2.json`
- `configs/elliott_vnext_output_schema_v2.json`
- `configs/market_context_history_schema_v1.json`
- `data/inputs/market_context_registry.csv`
- `tests/test_elliott_vnext_foundations.py`

## 20. Arbeitsplan nach Modul 5

6A Daten-/Pivot-Layer
→ 6B Szenario-/Regelparser
→ 6C Fibonacci + prospektive Wellenkarte
→ 6D Swing-Routing
→ 6E Scanner↔Elliott
→ 6F Markt-/Sektorkontext
→ 6G historische Validation
→ 6H Output/Integration.

## 21. Startauftrag im neuen Chat

1. Tatsächlichen Repo-Zustand prüfen.
2. Sicherstellen, dass Modul 5 vollständig abgeschlossen ist.
3. Foundation-Branch gegen dann aktuelles `main` vergleichen.
4. Foundations auf sauberen Modul-6-Arbeitsbranch übernehmen/aktualisieren.
5. v2-Verträge als verbindliche Basis benutzen.
6. Mit 6A beginnen und jede Teilphase reproduzierbar testen.
7. Frühere Phasen nicht unnötig neu aufrollen.
8. Keine Regeln, Counts, Kursziele oder Ergebnisse erfinden.

Ziel: Elliott/Fibonacci als empirisch geprüften Full-Cycle-Struktur-, Timing- und Positionsmanagement-Sensor integrieren, ohne die finale Entscheidungsgewalt aus dem globalen Decision-Layer herauszulösen.
