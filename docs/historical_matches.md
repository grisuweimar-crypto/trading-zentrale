# Historische Vergleichsfälle in daily_research.json

Implementierung und zentrale Konstanten: `src/scanner/reports/historical_matches.py`.
Die Auswertung bleibt eine abgeleitete Schicht. Sie verändert keine historischen
Scannerfelder und legt keine zweite Scannerhistorie an.

## Quellen und Ereignisse

- `history_recent.csv` liefert ausschließlich tatsächlich gespeicherte
  Scannerzustände aus dem gesamten verfügbaren Scanneruniversum, nicht nur aus
  der Historie des aktuellen Symbols. Preiszeilen werden nicht als Scannerfälle
  zugelassen. Nur Ereignisse **vor** dem Datum des aktuellen Snapshots zählen.
- Die erste gespeicherte Scannerzeile je Symbol/Datum wird unverändert verwendet.
  Weitere taggleiche Läufe sind keine unabhängigen Tagesereignisse. Felder aus
  verschiedenen Läufen werden nicht zusammengeführt oder aufgefüllt.
- Ein vorhandenes, endliches `rank_percentile` im Intervall [0,1] wird verwendet.
  Bei Legacy-Snapshots ohne dieses Feld wird der Rang deterministisch aus den
  gespeicherten Scanner-Scores derselben Snapshot-Gruppe abgeleitet. Preise,
  `rank/universe_size` und das heutige Universum fließen dabei nicht ein. Fehlendes
  historisches RS3M/Trend200 wird nicht rekonstruiert. R-Code wird für historische
  Zeilen niemals neu abgeleitet.
- `price_backfill.csv` liefert die getrennten Marktbeobachtungen für Sitzungen und
  Returns. Scanner-Datumswerte sind im aktuellen Projekt **Laufdaten**, nicht
  verlässlich Börsensitzungsdaten (`yahoo_prices.py` setzt MarketDate auf UTC-heute).
  Daher erzeugen auch Scannerzeilen mit einem Close keine Handelssitzungen.

## Feste Buckets

Gespeicherte Kennzahlen sind Bruchteile: 0,15 entspricht 15 %. Es gibt keine
automatische Einheitenkorrektur und keine pro Titel angepassten Grenzen.

| Feld | Exakte Intervalle in Prozent | Bucket-Bezeichnungen |
| --- | --- | --- |
| Rank-Perzentil, L1/L2 | [0,10], (10,20], (20,40], (40,60], (60,80], (80,90], (90,100] | top10; 10..20; 20..40; 40..60; 60..80; 80..90; bottom10 |
| Rank-Perzentil, L3 | [0,20], (20,60], (60,100] | top20; 20..60; bottom40 |
| RS3M | (-∞,-20), [-20,0), [0,15), [15,30), [30,+∞) | siehe zentrale `BUCKETS` |
| Trend200 | (-∞,-20), [-20,0), [0,20), [20,40), [40,+∞) | siehe zentrale `BUCKETS` |

L3 fasst ausschließlich ganze feine Rank-Buckets zusammen. Grenzwerte sind damit
eindeutig: z. B. 10 % Rank gehört zu top10; RS3M = 15 % gehört zu [15,30).

## Filterhierarchie und Mindestfallzahl

1. **L1:** feiner Rank-Bucket + gleicher gespeicherter R-Code (R0–R5) + RS3M-Bucket
   + Trend200-Bucket.
2. **L2:** feiner Rank-Bucket + RS3M-Bucket + Trend200-Bucket. R-Code entfällt.
3. **L3:** grober Rank-Bucket + RS3M-Bucket + Trend200-Bucket.

Standard: `MIN_MATCHES = 20`. Die engste Stufe mit mindestens so vielen
unabhängigen Ereignissen **nach Cooldown und vor Outcome-Verfügbarkeit** gewinnt.
Fehlende spätere Kurse oder ihre Renditen können die Filterstufe nicht bestimmen.
Reicht auch L3 nicht aus, wird die vorhandene L3-Stichprobe mit tatsächlichem N und
`sufficient_matches=false` ausgegeben. Ohne gültigen Vergleichsfall gilt
`filter_id="none"`, `N=0`. Die Mindestfallzahl ist eine transparente Schwelle,
kein statistisches Konfidenzniveau.

Jeder Titel enthält `filter_id`, konkrete `filter_description`, maschinenlesbare
`conditions`, `N`, `min_matches`, `sufficient_matches`, `cooldown_trading_days`
und die Fallzahlen der tatsächlich geprüften Stufen (`evaluated_levels`). Die
globalen Definitionen stehen einmal in `historical_match_method`.

## Cooldown und tatsächliche Sitzungen

Standard: **5 beobachtete Handelssitzungen pro Symbol**, für jede Filterstufe
erneut chronologisch angewendet. Gezählt werden gültige Preisbeobachtungen im
Intervall (letztes behaltenes Ereignis, Kandidat], nicht Kalendertage und nicht
Montag–Freitag. Genau nach fünf solchen Sitzungen ist ein neuer Fall erlaubt.
Andere Symbole liefern am selben Tag unabhängige Fälle. `cooldown=0` erlaubt
aufeinanderfolgende Tage, aber weiterhin keine taggleichen Duplikate.

Eine Sitzung braucht Symbol, gültiges Datum und einen endlichen positiven Close.
Doppelte identische Datum/Symbol-Kurse zählen nur einmal. Widersprüchliche Closes
für denselben Schlüssel werden ausgeschlossen. Es gibt keine Interpolation,
keinen Forward-Fill und keinen Rückgriff auf einen globalen Börsenkalender.
Feiertage/fehlende Sitzungen zählen nicht. Bei einem tatsächlich an Wochenenden
gehandelten Instrument zählen dessen echte Wochenendbeobachtungen.

**Fehlende Kalender:** Ohne ausreichende Preisbeobachtungen lässt sich ein
abgelaufener Cooldown nicht beweisen. Der erste passende Scannerfall eines
Symbols bleibt als Ereignis erfasst; weitere Fälle werden erst mit ausreichend
belegten Sitzungen zugelassen. N ist dadurch konservativ und kann geringer als
bei vollständigen Kursdaten sein. Auch ein Ereignis ohne Startkurs bleibt in
der Ereigniszahl N, liefert aber keinen Forward-Return.

## Outcomes und Lookahead

Startkurs ist ausschließlich der Markt-Close am exakten Ereignisdatum desselben
Symbols. `forward_5t` verwendet die fünfte **spätere** gültige Kursbeobachtung;
analog gelten 10, 20 und 40. Der Endpunkt muss spätestens am Snapshot-Stichtag
liegen. Fehlender Start-/Endkurs bedeutet keinen Return und keinen Beitrag zum
jeweiligen Horizon-N. Ein naher Kurs oder eine Wochentagsrechnung ersetzt nichts.

Formel: `Endkurs / Startkurs - 1`. Die Ergebnisse sind Preisreturns aus der
gespeicherten Preisquelle, keine nachträglich berechneten Total Returns.
Pro Horizont werden separat ausgegeben:

- `N`: Anzahl verfügbarer Returns, höchstens die unabhängige Ereigniszahl.
- `median_return`: mathematischer Median, bei geradem N Mittelwert der mittleren
  beiden Werte; bei N=0 `null`.
- `positive_count`: Anzahl strikt positiver Returns (Null ist nicht positiv).
- `positive_rate`: `positive_count/N`; bei N=0 `null`.

Nur gespeicherte Scannerfelder des Ereignisses bestimmen die fachliche
Übereinstimmung. Sitzungen bis zum jeweiligen Kandidatentag belegen den Cooldown.
Spätere Preise dienen ausschließlich den Outcomes. Zukünftige Scannerzeilen und
Preise nach dem aktuellen Snapshot-Stichtag werden nicht für Matches bzw.
Outcomes herangezogen. Es wird nicht nach positiver Rendite oder reifen
Horizonten selektiert.

## Konfiguration und Prüfung

```sh
python scripts/generate_daily_research.py --min-matches 20 --cooldown-trading-days 5
python scripts/generate_daily_research.py --validate-only
python scripts/generate_research_views.py --validate-only
python -m unittest discover -s tests -v
```

Die Python-Schnittstelle akzeptiert `match_policy=MatchPolicy(...)`.
Die effektiven Einstellungen werden mitgeschrieben. Die Daily-Validierung prüft
Snapshot, Symbole und Hash und berechnet zusätzlich die Vergleichsstatistik aus
den Originalquellen nach. Ein manipuliertes N oder ein falscher Median wird auch
dann abgewiesen, wenn jemand den Ausgabedatei-Hash passend geändert hat.

Die [Price-Session-Pipeline](price_session_pipeline.md) versorgt inzwischen das
gesamte aktuelle Universum; die tatsächliche Abdeckung steht in Metadata und
`historical_outcome_coverage`. Gespeicherte historische Rank-Perzentile existieren
erst ab 16.09.2026. Am 18.09. sind deshalb auch bei vorhandenen Cross-Universe-
Ereignissen sämtliche 5T/10T/20T/40T-Outcomes noch unreif. N=0 wird nicht durch
Rekonstruktion umgangen.
Die übrigen Daily-Felder und die Scanner-/Snapshot-Architektur bleiben erhalten.
