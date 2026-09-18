# Produktive Price Sessions

Die einzige OHLCV-Quelle ist `artifacts/market_data/yahoo_ohlcv.csv`.
`artifacts/research/price_backfill.csv` ist deren validierte, preisreine
Forschungsansicht; sie enthält keine Scannerkennzahlen. `price_fetch_state.json`
neben dem Cache speichert ausschließlich Abrufstatus, Provider-Zuordnungen und
Backfill-Prüfzeitpunkte, keine zweite Kurshistorie.

## Verbindlicher Datenfluss

1. Scanner vorbereiten, ausführen und Vollständigkeit validieren.
2. Latest/History-Views aus dem vollständigen Lauf erzeugen.
3. `prefetch_market_history.py` liest **alle** eindeutigen Symbole aus
   `latest_scanner.csv`. Snapshot-ID, Datum, Hash und Vollständigkeitsmarker
   müssen zum Manifest passen; sonst wird vor einem Providerabruf abgebrochen.
4. Für dieses Universum OHLCV-Cache aktualisieren.
5. `generate_research_views.py --refresh-prices` importiert ausschließlich
   gültige Preise, aktualisiert Hash und Coverage und validiert das Bundle.
   Scanner-CSV-Dateien und Snapshot-ID bleiben dabei bytegleich erhalten.
6. `generate_daily_research.py` erzeugt und validiert die kompakte Auswertung
   mit den neuen Kursen; erst danach folgen Reports und Commit/Push.

Der Preis-Publisher verwendet dieselbe exklusive Lockdatei wie der bestehende
Research-Publisher. Er prüft Quellen auf zwischenzeitliche Änderungen, schreibt
Preise atomar und das Manifest zuletzt. Der bisherige Daily-Publikationsmarker
wird beim Preisupdate entfernt und erst vom anschließenden Daily-Generator
neu gesetzt. Damit kann ein unterbrochener Ablauf keine alte Daily-Datei als
zur neuen Kursbasis passend bestätigen.

## Historientiefe und Folgeupdates

Standardziel: **300 gültige beobachtete Sessions**, nicht Kalendertage.

- Fehlende oder zu kurze Reihen: initial zwei Jahre anfragen. Liefert das weniger
  als das Ziel, wird für verfügbare Teilreihen einmal die maximale Providerhistorie
  geprüft. Bei größer konfiguriertem Ziel beginnt der Abruf mit fünf Jahren bzw.
  maximaler Historie.
- Ausreichende Reihen: nur ab letzter gespeicherter Session minus fünf
  **Kalendertagen Sicherheits-Overlap** abrufen. Diese Abrufspanne definiert keine
  Handelstage; gezählt werden ausschließlich echte zurückgegebene Bars.
- Nach erfolgreicher Maximalhistorien-Prüfung bleiben kurze Reihen erhalten und
  werden ebenfalls inkrementell aktualisiert. Ein erneuter tiefer Backfill ist
  frühestens nach 30 Tagen oder nach Änderung des Historienziels vorgesehen.
- Vorübergehende Abruffehler werden wiederholt; sie gelten nicht als Nachweis
  einer ausgeschöpften Historie.

Batchgröße standardmäßig 20, maximal konfigurierbar 50; maximal vier
gleichzeitige yfinance-Anfragen pro Batch. Batches laufen nacheinander, mit
0,5 Sekunden Pause. Fehlgeschlagene temporäre Einzelabrufe werden zweimal nach
2 bzw. 4 Sekunden wiederholt. Erfolgreiche Symbole werden dabei nicht erneut
abgerufen. Es werden keine Proxies oder Umgehungen von Provider-Limits benutzt.
Die yfinance-0.2.x-Batchschnittstelle und ihr unmittelbar ausgelesener Fehlerstatus
sind im Adapter `_download` gekapselt.

`end` ist exklusiv und höchstens das aktuelle UTC-Datum. So werden neu geladene,
noch laufende Tageskerzen nicht unveränderlich eingefroren. Frühere bereits
gespeicherte gültige Beobachtungen bleiben erhalten; dazu gehören auch die
vor Einführung gespeicherten taggleichen AVAV/ROL-Werte. Es wird kein Kalender
mit Montag–Freitag, keine Feiertagsliste und kein Forward-Fill eingesetzt.

## Mapping und Fehlerklassen

Die bestehenden expliziten Felder `YahooSymbol`/`yahoo_symbol`, die Zuordnung von
`asset_id` aus der aktuellen Watchlist und `data/inputs/symbol_map.csv` werden
wiederverwendet. Ein bereits im Universum gespeicherter Provider-Ticker wird
unverändert angefragt, einschließlich seines Börsensuffixes. ISINs und
`CRYPTO:*` brauchen eine eindeutige vorhandene Zuordnung. Insbesondere werden
Krypto-Quote-Währungen nicht geraten (z. B. SOL-EUR bleibt SOL-EUR).
Widersprüchliche oder nachträglich geänderte Provideridentitäten werden nicht
mit einer bestehenden Kursreihe vermischt. Es gibt keine Namensähnlichkeits-
Suche, kein Raten von Suffixen und keine automatische Ersatzaktie.

| Status | Bedeutung |
| --- | --- |
| `price_data_ok` | Gespeicherte gültige Sessionzahl erreicht das Ziel. |
| `price_data_partial` | Es gibt gültige Kurse, aber weniger als das Ziel. |
| `price_data_unavailable` | Keine gültigen gespeicherten Kurse; Provider liefert keine Reihe. |
| `ticker_mapping_missing` | Keine eindeutige zulässige Zuordnung bzw. abweichende Provideridentität. |
| `provider_error` | Technischer Abruffehler auch nach Wiederholungen; vorhandene Kurse bleiben erhalten. |

Einzelprobleme werden dokumentiert und blockieren andere Symbole nicht.
Fehlende/inkohärente Universumsdateien, kaputte Cache-/Provider-Schemata,
Dateisystemfehler oder ein technischer Ausfall aller angefragten Provider-Ticker
führen dagegen zu einem Fehler. Bestehende Kurse werden beim vollständigen
Providerausfall nicht zerstört.

## Validierung und Konflikte

Gemeinsame Regeln in `src/scanner/data/price_history.py`, verwendet von Downloader,
Research-Publisher, Bundle-Validator und HistoricalMatcher:

- Symbol nicht leer, Datum gültig, Close endlich und strikt positiv.
- Vorhandene OHLC-Werte müssen endlich/positiv sein; High/Low müssen mit den
  anderen Kursen konsistent sein (relative Rundungstoleranz 1e-10).
- Vorhandenes Volumen muss endlich und nicht negativ sein.
- Identische doppelte `(symbol,date)`-Beobachtungen zählen einmal.
- Verschiedene gültige Werte innerhalb einer erstmalig gelieferten Reihe für
  denselben Schlüssel: widersprüchliche Session ausschließen und melden.
- Spätere Providerrevision gegenüber einer bereits gespeicherten gültigen
  Beobachtung: bestehende Werte behalten und `provider_revision_kept_existing`
  melden. Keine willkürliche Überschreibung. Ambige bestehende Duplikate werden
  nicht durch einen späteren Download stillschweigend aufgelöst.

Auch fehlende Börsensitzungen, die bei einem Multi-Ticker-Batch nur als NaN-Zeile
im gemeinsamen Index auftauchen, werden nicht als Sitzungen gespeichert.

## Coverage und Forschungsgrenzen

`history_metadata.json.price_coverage` enthält tatsächliche Sessionzahlen für
jedes aktuelle Symbol, Status, Provideridentität, erste/letzte Session,
Abrufmodus und Fehlergründe. Aggregiert: Ziel, Anzahl mit irgendeinen Kursen,
Anzahl mit Zielhistorie, Teilreihen, fehlende Reihen, >=40 und >=300 Sessions
sowie Median/Minimum/Maximum. Die Statistik umfasst alle benötigten Symbole,
also auch Nullwerte. Vollständigkeit bezeichnet hier **Historientiefe**; die
Zeitnähe ist separat am jeweiligen `last_session` ablesbar.

Der Bundle-Validator rechnet Coverage gegen die tatsächlich gespeicherte
Preisdatei nach. `daily_research.json.historical_outcome_coverage` zählt zusätzlich
die aktuellen Titel mit verfügbaren Forward-Stichproben pro Horizont und die
aktuellen Symbole mit gültigen Preisen bis zum Snapshot-Datum.

Preistiefe ersetzt keine historischen Scannerbeobachtungen. Alte Rank-, R-Code-,
RS3M-, Trend200-, Score-, Confidence- oder Cycle-Felder werden nie rekonstruiert.
Am 18.09.2026 liegen gespeicherte Rank-Ereignisse erst seit dem 16.09. vor.
Trotz breiter Preisabdeckung sind deshalb noch keine fünf späteren Sitzungen
für diese Ereignisse vorhanden; die Forward-N bleiben korrekt null.

## Ausführung

```sh
python scripts/prefetch_market_history.py --minimum-sessions 300
python scripts/generate_research_views.py --refresh-prices
python scripts/generate_daily_research.py
python scripts/generate_research_views.py --validate-only
python scripts/generate_daily_research.py --validate-only
python -m unittest discover -s tests -v
```

`--minimum-days` bleibt ein kompatibler Alias für `--minimum-sessions`.
Explizite Tickerargumente sind nur eine administrative Teilmengenoption; der
produktive Workflow verwendet immer das vollständige validierte Latest-Universum.
Die übrigen Parameter sind `--batch-size`, `--retries` und `--root`.
