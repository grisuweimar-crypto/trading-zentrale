# CYCLE-DIR CY-02 — aktuelle Zyklusberechnung und Frischebeweis

**Plan-ID:** CYCLE-DIR-2026-10-09-v1 · **Paket:** ausschließlich CY-02  
**Basis:** main e77eb4082d04e9269a0fb5120259ecc9bb76109d, 10.10.2026  
**Branch:** feat/cycle-dir-cy02-pit-oscillator-20261010  
**Status:** IN_ARBEIT — Implementierung ist bis zum realen CI- und Veröffentlichungsnachweis ein Kandidat; keine fachliche bzw. empirische Freigabe vorausgesetzt.

## 1. Bestandsaufnahme und Abhängigkeiten

CY-00 (PR #259) hat anhand der Rohdaten vom 08.10.2026 den Pfad von \`Zyklus %\` über \`build_watchlist.py\`, \`cycle\`, die aktuellen Research-Views, History und Pattern-L2/L3 dokumentiert. CY-01 (PR #260; main HEAD e77eb408) beseitigte die Imputation von 80 Nullen und korrigierte Quality/Source und Anzeige. Die 80 imputierten Nullen und drei unbewiesenen expliziten Nullen vom 08.10.2026 liegen in einer separaten Quarantänemaske. CY-03 muss die historische Research-Eignung weiter absichern.

**Quellenbeweis bisher:** \`src/scanner/data/enrich/yahoo_prices.py\` führte im produktiven \`run_daily\`-Pfad Yahoo-\`1d\`/\`auto_adjust=True\` für Preise, Trend200, RS3M und Risiko aus. \`Zyklus %\` wurde damit **nicht** neu berechnet. Die alte \`legacy/market/cycle.py\`-Funktion (SMA20/detrended/40er Fenster, historische 50-Fallbacks) ist **nicht** als aktiver Quellcode für bestehende Werte nachgewiesen. Frühere numerisch gültige Quellenwerte bleiben unbestätigte **LEGACY_BEOBACHTUNGEN** und werden nicht nachträglich als neue Formel etikettiert.

## 2. Neue, eigenständige Spezifikation

| Eigenschaft | Deterministischer CY-02-Vertrag |
|---|---|
| Formelversion | \`cycle_detrended_sma20_range40_v1\` |
| Quellenkennung | \`YAHOO_PIT_CYCLE_V1\` |
| Preisquelle | \`yfinance.download\`, YahooSymbol/handelbare Original-Notierung |
| Frequenz/Basis | 1 Tagesbar pro Session, \`auto_adjust=True\`, \`Close\`, **keine Währungsumrechnung** |
| Formel | \`d[i] = Close[i] - SMA20[i]\`; letzter Wert innerhalb Min/Max der **letzten 40 d[i]** auf 0–100 skaliert |
| Mindesthistorie | **60** abgeschlossene Tagesbars (letzte 60, ohne Imputation); damit alle letzten 40 d[i] definiert |
| Konstantenfall | \`INVALID_VALUE\` / kein numerischer Wert (nicht 50) |
| Ausfälle | \`STALE\`, \`MISSING_SOURCE\`, \`INSUFFICIENT_HISTORY\`, \`INVALID_VALUE\`, numerisch nur bei \`VALID\` |
| Clamping/Rundung | [0,100], auf vier Nachkommastellen; echte 0/50/100 möglich |
| Bar-Cutoff | Bar-**Sessiondatum strikt kleiner als UTC-Kalendertag des Scans**; heutige/in Zukunft datierte Bars werden verworfen |
| Frische Aktien | letzter verwendeter Bar höchstens drei zurückliegende Werktage (Mo–Fr), Wochenende berücksichtigt; unbekannter Feiertagskalender bleibt Einschränkung |
| Frische Kryptos | letzte vollständig abgelaufene UTC-Tagesbar **gestern**; keine Wochenendpause |
| Datenlücken | Gap zwischen zwei verwendeten Bars größer als fünf Kalendertage (Aktien) oder ein Tag (Krypto) → blockiert |
| Split-/Diskontinuitätskontrolle | \`auto_adjust=True\`, zusätzlich 4x-/0,25x-Sprung als Sperre mit Reviewgrund; kein vollständiger Corporate-Actions-PIT-Beweis |

Die neue Berechnung kann aus numerischer Sicht ähnlich wie die Legacy-Formel sein. **Sie ist nicht historisch oder semantisch identisch nachgewiesen.** Für CY-03 dürfen alte und neue Werte nicht ohne Formel-/Quellenprüfung zu Deltas verbunden werden.

## 3. Herkunft pro Wert und Ausgabe

Die Berechnung liefert \`Zyklus %\` und Qualitätsfelder \`cycle_quality\`, \`cycle_source\`, \`cycle_quality_reason\`, dazu:

- \`cycle_formula_version\`, \`cycle_price_source\`, \`cycle_price_basis\`, \`cycle_price_symbol\`, \`cycle_currency\`
- \`cycle_last_bar\` (Yahoo-Tages-**Sessiondatum**), \`cycle_as_of\` (UTC-Scanzeit), \`cycle_computed_at\` (UTC)
- \`cycle_price_sha256\` (SHA256 von Formel, Basis, YahooSymbol, angegebener Originalwährung und **genau 60 verwendeten datierten Schlusskursen**), \`cycle_eligible_bars\`.

Neue Daten werden im aktuellen Watchlist-Datenfluss weitergereicht und im neu erweiterten **Current-/Research-View-Vertrag** berücksichtigt; der aktuelle \`artifacts/reports/cycle_quality.csv\` bleibt ein Prüfartefakt, dessen Spalten ebenfalls erweitert werden. \`normalize_cycle_source\` erhält die **neue** Quellkennung; aus einer numerisch gültigen Legacy-Spalte wird dadurch nicht ohne Berechnung ein angeblich aktueller Wert.

**Ausfallverhalten:** Bei ausgeschaltetem Datenabruf, fehlenden Provider-Daten oder Ausnahme wird der alte Cycle **nicht** als frisch weitergereicht. Die restlichen alten Preis-Features bleiben gemäß bestehendem Enrichment-Verhalten unberührt; die Zyklus-Evidence wird ausdrücklich blockiert.

## 4. Grenzen und verbleibende Prüfungen

1. Yahoo-Tagesindizes bezeichnen Sessions, keine signierten tatsächlichen Börsenschluss- oder Publikationszeitpunkte. Der UTC-Vortags-Cutoff ist **bewusst konservativ**, kann jüngste bereits abgeschlossene europäische Sessions bis zum nächsten UTC-Tag auslassen. Keine heutige Bar wird bloß wegen Download-Verfügbarkeit als vollständig angenommen.
2. Der Yahoo-Batch liefert keine durchgängig unabhängig bestätigte Handelsplatz-/Quote-Währung im verwendeten Frame. Die originale \`Currency\`/Symbol-Angabe wird mitgeführt, nicht extern verifiziert. Alias-Duplikate werden durch bestehende Canonical-/Dedup-Logik verwaltet; eine unabhängige Listing-/Currency-Provider-Verifikation ist **UNVERIFIZIERT** und darf nicht als Qualitätsbeweis ausgegeben werden.
3. Split-/Dividend-adjustierte aktuelle Yahoo-Preise sind nicht zwangsläufig historische PIT-Preise, die damals schon in dieser Adjustment-Version verfügbar waren. **Keine rückwirkende Rekonstruktion, keine alten Snapshot-Rewrites**.
4. Ein grün getesteter Algorithmus ist keine profitable Prognose; wissenschaftliche Muster- und Forward-Validierung finden erst in CY-05 bis CY-08 statt. Score, Opportunity, Risk, Confidence, Elliott, Selection, Timing, Decision, Portfolio-Action und Execution werden inhaltlich nicht geändert.
5. Vor fachlichem Abschluss erforderlich: erfolgreiche CY-02-PR-CI, Review der Feld-/Quelle-Semantik, ein **echter vollständig publizierter Scannerlauf nach Integration** mit numerischen Qualitäts-Counts, SHA-/Bar-/as_of-Nachweis, Regression auf Score/Decision und Nachweis, dass kein Live-Wert als historisch gültige Beobachtung fehlklassifiziert wird. Baseline-Bug #264 bleibt eigenständig und darf nicht als mit CY-02 erledigt bezeichnet werden.

## 5. Tests und Freigabeprotokoll

Neue Testdatei: \`tests/test_cycle_oscillator_cy02.py\`. Feste Goldenkursreihe (Erwartung 7,2258), doppelte Eingaben, konstante Reihe, zu wenige Bars, NaN/negativ/unendlich, Gap und Duplicate, Splitverdacht, Provider-Ausfall, UTC-Bargrenze, Aktien/7-Tage-Krypto, Ausfall- und Währungskennzeichnung, Yahoo-Pipeline-Integration, Research-Feldfortpflanzung.

GitHub-CI: \`.github/workflows/cycle_dir_cy02.yml\`, inklusive CY-01 und bestehender History-/Research-/L2-/L3-Regressionssuiten.

**Befunde nach PR-CI und Live-Prüfung ergänzen:** konkrete Run-ID, Commit-SHA, Testanzahlen, Probeasset, Cyclequalität/Lastbar/Hash, Ausnahmen, Vergleich der unveränderten Score-/Decision-Outputs.

## 6. Status / nächstes Paket

Bis die Nachweise unter Abschnitt 5 vollständig vorliegen, **kein \`FERTIG_FACHLICH\`** und kein Merge allein wegen Code oder plausibler Formula. CY-03 darf erst auf der nachgewiesenen Version aufbauen. Das Repository-Paket enthält bewusst weder Historiereparatur noch Delta/UI, L2/L3-Muster-Promotion oder Scoring-Neubewertung.
