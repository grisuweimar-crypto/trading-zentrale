# ARGUS-Einzelwertprüfung Fiserv (NASDAQ: FISV) – 26.10.2026

**Fragestellung:** Kann ARGUS auf Zuruf ein Wertpapier untersuchen, das nicht Teil der feststehenden fünf M5-Forschungsinstrumente ist? **Ergebnis:** Ja, für kausale M1–M3-Zonenrechnung bei verifizierbarer Börsenidentität und hinreichender OHLCV-Historie. **Nicht automatisch:** neues M5-First-seen-Archiv, M6-Depotanzeige oder M7-Prospektivstudie.

## Instrument und Datenherkunft

- **Gesellschaft:** Fiserv Inc., US3377381088, Handel seit 11.11.2025 unter `FISV` an der Nasdaq (vorher NYSE: `FI`). Offizielle Emittentenmitteilung: https://investors.fiserv.com/news-releases/news-release-details/fiserv-announces-transfer-stock-exchange-listing-nasdaq
- **Historischer Auswertungsstichtag:** 2026-10-23, letzter abgeschlossener US-Tageskurs 49,860001 USD; **nicht** Intraday-Forecast vom 26.10.
- **Originaldaten:** Read-only `_download`-Yahoo-Adapter mit `open/high/low/close/adj_close/volume`, 639 datierte Tageskerzen. Daten-Checksumme über die für die Berechnung verwendeten Zeilen: `cee2d1cd90bfe5b694794e18e8c5b183f413925390f87dc9a580068d8f28b350`.
- **Berechnung:** identische `scanner.research.zones_v01.build_zones` und M3-`zone_clusters` wie im getrennten ARGUS-Zonenprojekt. `research_only=true`, `data_quality=degraded`, `first_seen_prospective=false`. Keine echte historische PIT-Zertifizierung, kein unabhängiges Dividend-/Split-Ledger und keine Ausführung von Depot-/Orderfunktionen.
- **Nachweis:** [vollständig erfolgreicher GitHub-Run 40394218029](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/40394218029); [Rohartefakt zum Prüflauf, maximal sieben Tage verfügbar](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/40394218029/artifacts/12685737357).

## Tatsächlich berechnete Geometrie (alle Angaben USD)

| Kennzahl | Wert |
|---|---:|
| Quelle: analysierte Daily-Bars | 639 |
| Originale Daily-Zonenkandidaten | 75 |
| abgeschlossene Wochenzeitrahmen-Zonen | 25 |
| M3-Cluster | 49 |
| bedingte, **noch nicht** ausgelöste M3-Szenarien | 7 |

### Nähe zum Referenzschluss 49,86 USD

| Rolle | Band | Methodenfamilien und Verfügbarkeit |
|---|---:|---|
| Unterstützungs-Konfluenz | 48,93067–51,57829 | `horizontal` + `fib_retracement`, vier dokumentierte Ankergruppen, Zone als `touched` |
| Fib-Unterstützung | 47,61998–51,41874 | `fib_retracement`, `touched` |
| Horizontal-Unterstützung | 47,96125–48,77875 | `horizontal`, `watch` |
| weitere Horizontal-Unterstützung | 45,83950–48,24050 | `horizontal`, `watch` |
| Fib-Widerstandsband | 49,77032–52,17132 | `fib_retracement`, `touched` |
| weiteres Fib-Widerstandsband | 49,04179–50,33375 | `fib_retracement`, `touched` |
| oberes Fib-Widerstandsband | 50,55883–51,76273 | `fib_retracement`, `watch` |

**Interpretationsgrenze:** Einige Support- und Resistance-Bänder enthalten den Schlusskurs und überlappen sich gegenseitig. Eine Rollenbezeichnung für ein Modellcluster ist deshalb noch keine technisch saubere, empirisch überprüfte Unterstützung bzw. ein eigener unabhängiger Erfolg. Die 75 Rohzonen sind gerade **nicht** 75 Handelsmöglichkeiten.

### Zwei vollständige bedingte Beispiel-Szenarien

| Richtung | möglicher Einstiegsbereich | Invalidierungsband | erstes Ziel |
|---|---:|---:|---:|
| bullisch | 48,93067–51,57829 | 47,96125–48,77875 | 51,62749–52,44499 |
| bärisch | 50,55883–51,76273 | 52,49125–53,30875 | 48,50125–49,31875 |

Beide Szenarien sind `untriggered` und werden **nicht** als Einstiegsanweisungen interpretiert. Mögliche Folgezonen bullisch: 52,49125–54,39875 und 54,95125–57,03875. Mögliche Folgezonen bärisch: 45,83950–48,24050 und 42,66950–45,07050. Kursziele oder Stop-Bänder wurden nicht mit einer gemessenen Trefferwahrscheinlichkeit kalibriert.

## Scanner-Kontext – kein Ersatz für Zonenrechnung

Fiserv war im regulären 220-Werte-Scanner vom 25.10.2026 **bereits vorhanden** (Referenzkurs 23.10.: 49,86000061 USD, Score 6,32/100, Rang 161/220, R-Code R2, RS3M und Trend200 negativ). Weder Score noch Rang flossen in die obige unabhängige M1–M3-Zonengeometrie ein. Ob Fiserv im privaten Depot gehalten wird, geht daraus nicht hervor.

## Tatsächliche Grenzen

1. **Ad-hoc ist möglich:** Die bestehende Einzeltitel-CLI ist mit der börsenidentifizierten Fiserv-Ausnahme ergänzt; sie rechnet abgeschlossene Tageskerzen selbstständig durch die Forschungspipeline.
2. **Nicht beliebiges Kürzel ohne Prüfung:** Ticker-, Börsenplatz- und Währungsidentität sowie ein OHLCV-Zeitfenster sind nötig. Der Aktienname allein ist keine Asset-ID; das alte Fiserv-Kürzel `FI` wird hier bewusst nicht als Fiserv nach November 2025 akzeptiert.
3. **Nicht gleiche Betriebsstufe wie die fünf Pilotwerte:** Die automatische tägliche, immutable M5-Forecast-Kohorte und die M7-Prospektivstudien bleiben bislang bei AMD, INTC, ARM, SMH und NOW. Ein einmaliger rückwirkender Test darf nicht nachträglich zur frühzeitigen M5-Prognose umgedeutet werden.
4. **Kein statistischer Vorsprung belegt:** Auch ein qualitativ interessanter Konfluenzbereich ist erst einmal ein geometrischer Kandidat, keine Trefferwahrscheinlichkeit. Kapitalmaßnahmen/Yahoo-Backadjustment bleiben `degraded`.

Die Prüfung ist vollständig **read-only** und ändert weder Wertpapierdepot, Scanner-Score, W10, private Watch noch Orders.
