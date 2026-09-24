# Modul 6 – Elliott vNext: Foundations

Status: Vorbereitung / Research-Grundlage, noch keine produktive Integration.

Revision: Full-Cycle / Swing- und Positionsmanagement.

Diese Datei friert die methodischen Grundlagen für das spätere Modul 6 ein. Modul 5 bleibt unangetastet. Elliott/Fibonacci ist kein autonomes Handelssystem und erzeugt keine finale Kauf-/Halte-/Verkaufsentscheidung. Das Modul liefert strukturierte Evidenz, prospektive Preiszonen und Review-Trigger an den späteren globalen Decision-Layer.

## 1. Rolle im Gesamtsystem

Das Gesamturteil entsteht erst aus dem Zusammenspiel aller verfügbaren, empirisch geprüften Quellen, insbesondere Selection, Timing, Risk, Confidence, Probability, Fundamentals, Markt-/Sektorkontext, relative Stärke, Elliott/Fibonacci und weiteren später validierten Modulen.

Elliott/Fibonacci darf:
- Strukturen, Szenarien und Invalidierungen beschreiben,
- zukünftige, szenariospezifische Preiszonen für W2/W3/W4/W5 kartieren,
- einen Deep-Scan des Scanners auslösen,
- durch Scanner-Signale selbst zu einer tieferen Analyse getriggert werden,
- Review-Prioritäten für Einstieg, Teilreduktion, Rückkauf oder Gewinnsicherung erzeugen,
- als zusätzliche Evidenz in die spätere Entscheidung eingehen.

Elliott/Fibonacci darf nicht:
- autonom `buy`, `hold`, `reduce` oder `sell` ausgeben,
- konkrete Orderanweisungen erzeugen,
- eine projizierte Zielzone als sicheren zukünftigen Kurs darstellen,
- die bestehende Selection-/Timing-/Risk-/Confidence-Logik ersetzen,
- Fibonacci-Level benutzen, um rückwirkend eine gewünschte Wellenzählung passend zu machen.

## 2. Datenbasis und Point-in-Time-Regel

Primäre Preisbasis ist die dokumentierte tägliche OHLCV-Historie. Daily-Daten bleiben Source of Truth; Weekly-Daten werden daraus deterministisch aggregiert. Minutencharts sind für v1 nicht erforderlich.

Jeder Pivot besitzt mindestens zwei Zeitpunkte:
- `pivot_time`: Zeitpunkt des tatsächlichen Hochs/Tiefs,
- `confirmed_time`: erster Zeitpunkt, an dem der Pivot mit den damals verfügbaren Daten erkannt werden durfte.

Im Backtest darf ein Pivot ausschließlich ab `confirmed_time` verwendet werden. Ein später sichtbarer Swing darf niemals rückwirkend einen historischen Elliott-Zustand verbessern. Dasselbe gilt für daraus abgeleitete Fibonacci-Level und Zielprojektionen.

Jede prospektive Zielzone benötigt zusätzlich `available_from`: den ersten Zeitpunkt, an dem sämtliche für diese Projektion nötigen Anker kausal verfügbar waren.

## 3. Fraktale Architektur

Normalweg: top-down.

1. Weekly / grober Tagesgrad: übergeordneten Kontext bestimmen.
2. Mittlerer Tagesgrad: mögliche Parent-Struktur und relevante Wellenphase identifizieren.
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

### 4.2 Diagonalen und Truncation

Leading und Ending Diagonals sind ein eigener Musterzweig. Dort ist eine 4/1-Überlappung zulässig bzw. typisch. Eine solche Überlappung darf daher einen klassischen Impuls invalidieren, aber nicht pauschal jede Elliott-Struktur.

Eine truncated fifth muss möglich bleiben. Welle 5 darf daher nicht hart dazu gezwungen werden, ein neues Extrem über Welle 3 hinaus auszubilden.

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

Jede Projektion gehört eindeutig zu einem `scenario_id`. Zielzonen verschiedener Counts dürfen nicht zu einem scheinpräzisen gemeinsamen Kursziel vermischt werden.

`structural_fit` beschreibt ausschließlich die formale Passung zum Elliott-Regelwerk.
`confirmation_strength` beschreibt externe/reaktive Bestätigung, z. B. Kursreaktion, Momentum, relative Stärke oder Marktkontext.
`historical_expectancy` stammt ausschließlich aus Point-in-Time-sauberer historischer Validierung.

Diese Größen dürfen nicht zu einer scheinpräzisen Elliott-`confidence` vermischt werden, bevor ihre Kalibrierung empirisch belegt ist.

## 6. Fibonacci-Layer

Fibonacci wird erst nach einer regelkonform identifizierten Wellenstrecke angesetzt.

Grundprinzip:
- Elliott bestimmt die strukturell zulässigen Anker.
- Fibonacci beschreibt die Geometrie zwischen diesen Ankern.

V1-Retracement-Zonen:
- Welle 2: 50 %, 61,8 %, 78,6 %, 88,7 % von Welle 1.
- Welle 4: 14,6 %, 23,6 %, 38,2 %, typischerweise nicht mehr als 50 % von Welle 3.

88,7 % ist für Welle 2 eine tiefe Warn-/Grenzzone, aber nicht die harte Elliott-Invalidierung. Die harte strukturelle Invalidierung erfolgt erst beim Bruch des Ursprungs von Welle 1.

Fib-Level werden als Zonen behandelt, nicht als exakte Wendepunkte. Die Breite einer Zone soll mindestens Volatilität/ATR, Preisniveau und Wellengrad berücksichtigen. Liquidität, Spread und Gaps können später ergänzt werden.

Fibonacci selbst bleibt geometrische Information. Momentum, Scannerzustand oder Marktkontext dürfen nicht in einen Fib-Score eingerechnet und anschließend im Decision-Layer ein zweites Mal gezählt werden.

## 7. Vollständige Wellenzyklus-Karte

Modul 6 soll nicht nur W1→W2-Einstiegssituationen erfassen, sondern den vollständigen impulsiven Zyklus als prospektive Handlungskarte modellieren:

`W1 → W2 → W3 → W4 → W5 → Korrektur höheren Grades`

Für jedes plausible Szenario wird geführt:
- aktueller Wellenzustand,
- nächste strukturell plausible Phase,
- szenariospezifische Ziel-/Retracementzonen,
- Abstand des aktuellen Kurses zur nächsten Zone,
- harte Invalidierung,
- alternative Szenarien,
- unabhängige Bestätigung oder Konflikt durch Scanner/Marktkontext.

Die Zielkarte ist keine Kursprognose. Sie beschreibt: "Falls dieses Szenario gültig bleibt, liegen hier die nächsten strukturell relevanten Preisbereiche."

## 8. W2 – Einstieg/Aufstockungskontext

Vorläufige Routing-Zustände:
- `EW_PREWATCH_382`: Korrektur erreicht ungefähr 38,2 %; günstige Vorwarnung.
- `EW_DEEP_SCAN_500`: Korrektur erreicht ungefähr 50 %; tiefe Elliott- und Scanneranalyse auslösen.
- `EW_W2_CORE`: 50–78,6 %; mögliche Einstiegs-/Aufstockungsprüfung.
- `EW_W2_DEEP`: 78,6–88,7 %; erhöhte Vorsicht und Alternativcount priorisieren.
- `EW_W2_DANGER`: tiefer als 88,7 %, W1-Ursprung noch intakt; Zählung intensiv neu prüfen.
- `EW_INVALIDATED`: W1-Ursprung gebrochen; konkreten Count verwerfen.

Diese Zustände erzeugen nur Review-Kontext, keine Kaufentscheidung.

## 9. W3 – Swing-Teilgewinn und Überdehnung

Sobald W1 und ein kausal ausreichend bestimmtes W2-Ende vorliegen, darf das Modul mehrere W3-Projektionszonen erzeugen.

Für die Forschung werden zunächst Extension-Kandidaten von W1 relativ zum W2-Ende geprüft. Die verwendeten Zahlen bleiben Research-Hypothesen, bis ihre historische Relevanz validiert ist.

Zustände:
- `wave_3_in_progress`: W3-Szenario aktiv, Zielzonen noch entfernt.
- `EW_W3_TARGET_APPROACH`: Kurs nähert sich einer W3-Projektionszone.
- `EW_W3_EXHAUSTION`: Projektionszone plus unabhängige Ermüdungs-/Konfliktevidenz.

`EW_W3_TARGET_APPROACH` darf maximal "Hold vs. Teilreduktion prüfen" routen.
`EW_W3_EXHAUSTION` darf eine Teilgewinn-/Swing-Reduktionsprüfung priorisieren.

Eine W3-Zielzone allein ist niemals Verkaufsgrund.

## 10. W4 – Rückkauf/Aufstockung nach W3

Nach einem ausreichend bestätigten W3-Ende werden mögliche W4-Retracementzonen der W3-Strecke kartiert.

Zustände:
- `wave_4_in_progress`,
- `EW_W4_TARGET_ZONE`,
- `EW_W4_COMPLETION`.

W4 dient damit nicht nur als Risiko nach einem W3-Hoch, sondern als mögliche Rückkauf-/Aufstockungsphase nach einem vorherigen Swing-Teilverkauf.

Auch hier gilt: Zone allein ist kein Kaufgrund. Scanner, Unterstruktur, relative Stärke und Marktkontext bleiben getrennte Evidenzquellen.

## 11. W5 – Gewinnsicherung und mögliche größere Reduktion

Nach einem plausiblen W4-Ende sollen mehrere W5-Projektionsmethoden empirisch verglichen werden. Die exakten numerischen W5-Projektionslevel werden vor Modul 6 nicht eingefroren.

Mindestens als Projektionsbasen zu untersuchen:
- Länge/Relation von Welle 1 relativ zum W4-Ende,
- Struktur bzw. Strecke W1→W3 relativ zum W4-Ende.

Zustände:
- `wave_5_in_progress`,
- `EW_W5_TARGET_APPROACH`,
- `EW_W5_COMPLETION_RISK`,
- `post_wave_5_higher_degree_correction_risk`.

W5 ist für das Positionsmanagement anders zu behandeln als W3:
- W3-Zielnähe kann eine taktische Teilreduktion interessant machen, weil W5 noch folgen kann.
- W5-Abschlussrisiko kann eine deutlich stärkere Gewinnsicherungs-/Reduktionsprüfung auslösen, weil danach eine Korrektur höheren Grades möglich wird.

Eine truncated fifth muss dabei weiterhin möglich bleiben.

## 12. Prospektive Preiszonen und PIT

Jede ausgegebene Projektionszone benötigt mindestens:
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
- rückwirkende Projektionen mit damals noch unbekannten Ankern,
- ein einzelnes scheinexaktes Kursziel,
- Vermischen mehrerer Szenarien zu einem künstlichen Ziel,
- Darstellung einer Zone als sichere zukünftige Kursbewegung.

## 13. Swing-Routing statt Handelsentscheidung

Erlaubte Review-Kontexte:
- `entry_or_add_review`,
- `hold_review`,
- `partial_reduce_review`,
- `reentry_or_add_review`,
- `profit_protection_review`,
- `larger_reduce_or_exit_review`.

Diese sind ausdrücklich keine Entscheidungen. Der globale Decision-Layer bzw. die Depot-Watch entscheidet erst aus dem Gesamtbild, ob tatsächlich Kaufen, Halten, Reduzieren oder Verkaufen angemessen ist.

Beispiel:
- W3-Projektionszone erreicht + Scanner weiterhin stark → möglicherweise halten.
- W3-Projektionszone + Overextension + fallendes Momentum → Teilreduktion prüfen.
- W4-Zielzone + Scanner-Turn positiv → Rückkauf/Aufstockung prüfen.
- W5-Zielcluster + Unterwellenabschluss + Scanner kippt → stärkere Reduktion/Verkauf prüfen.

## 14. Scanner ↔ Elliott: Cross-System-Research

Zu testen sind mindestens:
- Redundancy,
- Confirmation,
- Elliott Rescue,
- Scanner Rescue,
- Conflict,
- Lead/Lag,
- stage-specific incremental value,
- swing-routing value.

Lead/Lag wird als Ereignisfenster gemessen, nicht nur als Gleichzeitigkeit am selben Tag. Typische Fenster: -20T bis +20T um das jeweilige Ereignis.

Zusätzlich müssen die Interaktionen getrennt nach Wellenphase untersucht werden. Ein Scanner-Momentumbruch nahe W3/W5 kann andere Bedeutung haben als derselbe Momentumbruch innerhalb einer W2.

Discovery, Validation und Holdout bleiben getrennt. Der Holdout darf niemals zur Musterauswahl oder Parameteranpassung verwendet werden. Überlappende Forward-Windows sind nicht als unabhängige Beobachtungen zu behandeln.

## 15. Historische Forschungsfragen für das Positionsmanagement

Mindestens untersuchen:
- Wie häufig erreichen gültige W2-Szenarien die jeweiligen W3-Projektionszonen?
- Welche Projektionen zeigen stabile Trefferbereiche ohne nachträgliche Optimierung?
- Wie häufig folgt auf W3-Zielnähe eine relevante W4-Korrektur?
- Welche W4-Retracementbereiche sind auf den verschiedenen Wellengraden empirisch typisch?
- Wie oft lohnt ein hypothetischer Teilverkauf nahe W3 gegenüber einfachem Halten bis W5?
- Wie häufig bietet W4 einen günstigeren Rückkaufbereich?
- Welche W5-Projektionsmethoden besitzen inkrementellen Wert?
- Wie groß sind typische Korrekturen nach möglichem W5-Abschluss?
- Verbessern Scanner-/RS-/Marktkontextsignale die Unterscheidung zwischen bloßer Zielberührung und tatsächlicher Wende?
- Wie unterscheiden sich Leader/Follower/Divergence-Zustände an W3/W4/W5?

Auswertungen müssen Transaktionskosten/Spreads später berücksichtigen, bevor aus einem theoretischen Swing-Vorteil ein real nutzbarer Positionsmanagement-Vorteil abgeleitet wird.

## 16. Markt-, Branchen- und Kohortenkontext

Der Scannerbestand ist keine automatisch repräsentative Branche. Zwei oder drei beobachtete Uran-, Cannabis- oder Robotics-Werte dürfen nicht als Branchenindex interpretiert werden.

Externer Kontext wird separat gespeichert und erhält einen Qualitätsstatus:
- `sufficient`,
- `limited`,
- `unreliable`,
- `unavailable`.

Nur `sufficient` und nach expliziter Kennzeichnung `limited` dürfen als echte Markt-/Sektorevidenz in Analysen eingehen. `unreliable` und `unavailable` erzeugen keine Branchenwellen.

Bevorzugte Datenhierarchie:
1. geeigneter offizieller/breiter Index,
2. geeigneter liquider Branchen-/Themen-ETF,
3. extern definierter, ausreichend breiter und Point-in-Time-versionierter Peer-Korb,
4. Scanner-Peers nur als separater `scanner_peer_context`.

Historische Kontextkurse werden getrennt von `history_analysis.csv` geführt. Zielartefakt: `artifacts/research/market_context_history.csv`.

## 17. Relative Stärke und Hierarchie

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

## 18. V1-Umfang von Modul 6

V1 soll bewusst begrenzt bleiben, aber den vollständigen Impulszyklus abbilden:
- Daily + daraus abgeleitet Weekly,
- kausal bestätigte Pivots,
- klassische Impulse,
- Zigzag und Flat vollständig,
- Diagonalen als eigener konservativ behandelter Zweig,
- komplexe Korrekturen mindestens als Alternativ-/Unsicherheitszustände,
- Fibonacci-Zonen statt exakter Linien,
- W2-, W3-, W4- und W5-Zustände,
- prospektive szenariospezifische Zielkarten,
- Swing-Routing ohne autonome Handelsentscheidung,
- Primär- und Alternativszenario,
- harte Invalidation getrennt von weicher Evidenz,
- Cross-System-Research mit Scannermerkmalen,
- externer Kontext nur bei ausreichender Datenqualität.

## 19. Abnahmekriterien vor produktiver Nutzung

Modul 6 ist erst produktionsreif, wenn mindestens folgende Punkte erfüllt sind:
- PIT-sichere Pivotbestätigung nachweisbar,
- keine zukünftigen Daten in historischen Counts/Fib-Leveln/Projektionen,
- harte Regeln, Diagonal-Ausnahmen und Truncation getestet,
- Szenario-Output reproduzierbar,
- Projektionen sind szenariospezifisch und speichern `available_from`,
- Parameterstabilität über sinnvolle Swing-/ATR-Bereiche geprüft,
- W3/W4/W5-Projektionsmethoden historisch bewertet,
- Swing-Routing gegen einfaches Halten und andere Baselines getestet,
- Discovery/Validation/Holdout sauber getrennt,
- Cross-System-Mehrwert gegenüber bestehenden Scannerlogiken gemessen,
- Markt-/Sektorkontext separat qualitätsgesichert,
- Decision-Layer konsumiert Elliott nur als Evidenz und behält das Gesamturteil.

## 20. Noch ausdrücklich offen

Nicht vor Modul 6 vorentscheiden:
- optimale Pivot-/ATR-Parameter pro Wellengrad,
- genaue Zonendicke der Fib-Level,
- empirisch beste W3-Projektionsbereiche,
- empirisch beste W5-Projektionsmethoden/-bereiche,
- Schwelle für `TARGET_APPROACH`,
- welche zusätzliche Evidenz `EXHAUSTION` bzw. `COMPLETION_RISK` benötigt,
- Größe einer möglichen Swing-Teilreduktion,
- Regeln für Rückkauf nach W4,
- Gewichtung von Elliott im späteren Decision-Layer,
- konkrete externe Benchmarks und deren Quellen,
- finale Triggerprioritäten und Rechenbudgets.

Diese Punkte werden in Modul 6 empirisch bestimmt und nicht aus Lehrbuchregeln oder dem aktuellen Depotbeispiel abgeleitet.
