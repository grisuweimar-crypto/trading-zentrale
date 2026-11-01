# ARGUS Forschungsdurchlauf – Geometrische Zonentref­fer versus gerichtete Rendite (2026-11-01)

## Tatsächlich ausgeführter Test
Quelle: `artifacts/research/elliott_vnext_stage4_historical_validation.json`, `projection_summary`. Filter: `partition=legacy_development_descriptive_only`, `horizon_sessions=10`, beide `N>0`. Genau **16** aggregierte Gruppen (4 Projektionsfamilien × 4 Wellengrade). Für diese Gruppen wurde die Korrelation zwischen `zone_hit.mean` und `signed_return.mean` neu berechnet, ohne einzelne Outcomes zu erfinden.

**Ergebnis:** Pearson **−0,5999**, Spearman **−0,8265** (je 16 Gruppen). Höchste Zonentrefferquote: Wave-4/W3-Retracement, Grad minor, **72,19 %**, mittlere signierte Rendite **−1,52 %** (Intervall −4,15 % bis +0,14 %). Höchste mittlere signierte Rendite: Wave-5/W1–W3-Struktur ab W4, Grad primary, **+1,55 %** (Intervall +0,15 % bis +3,33 %), Zonentreffer **30,93 %**.

**Wissenschaftliche Grenze:** Diese Korrelation ist *ökologisch*: nur 16 aggregierte, abhängige, nachträglich betrachtete Gruppen; die Treffer- und Rendite-Nenner sind teilweise verschieden. Keine Einzelbeobachtungs-Korrelation, kein kausaler Zusammenhang, kein p-Wert, keine Aussage über eine Strategie oder Out-of-Sample-Performance. Die negative Beziehung kann durch Projektionsfamilien, Auswahl und unterschiedliche Bedingungen entstehen. Kein Trading-Signal.

## Kombinierter Cycle × Elliott/Fibonacci-Test: explizites Ausführungsergebnis
**NOT_EXECUTABLE_FROM_VERIFIED_JOINED_ROWS**. Es wurde kein gemeinsamer `(asset, as_of, available_from, price_basis, currency, forecast_id, outcome)`-Datensatz belegt. Das CY-03-Archiv liegt als `observations.csv` mit 4.168.215 Bytes vor, das Repo hat bisher nur v1 mit `research_eligible=0`; ein großer Dateiabruf über den verfügbaren GitHub-Connector lieferte keine Zeilen. Deshalb wurden **keine** Cycle-Kombinationseffekte oder künstlichen Trefferquoten berechnet. Für den nächsten ausführbaren Vergleich sind die vollständigen archivierten Einzelfälle und ein PIT-korrekter Join erforderlich. Primärvergleich gleicher Fälle: Zone-only vs Zone+Cycle; daneben Elliott+Fib und Elliott+Fib+Cycle.

## Reproduktion
Für die 16 Gruppen die zwei Mittelwertreihen direkt aus `projection_summary` extrahieren. Pearson = Kovarianz / Produkt der Standardabweichungen; Spearman = Pearson der aufsteigend vergebenen Ränge (keine Gleichstände). Datenquelle ist ein bereits berechnetes Stage-4-Forschungsartefakt; diese Arbeit führt einen **neuen sekundären Zusammenhangstest**, aber **keinen neuen Markt-Replay** durch.

## Ergebnisentscheidung
Die Hypothese „häufige Zonentreffer sind automatisch gute gerichtete Prognosen“ ist in dieser aggregierten historischen Stichprobe **nicht gestützt**. Nächster Test: gepaarte Einzelereignisse und geeignete Kontrollen, dann as-of-verifizierte Cycle-Ablation; kein Promotionsgate überspringen.
