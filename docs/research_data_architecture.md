# Research-Datenarchitektur

Der produktive Daily-Einstieg ist `scripts/generate_research_views.py`.
`scripts/publish_history_analysis.py` bleibt ein kompatibler Offline-Export aus der
bestehenden Score-History und wird im Daily-Workflow nicht mehr aufgerufen.
Die Forschungsdateien liegen unter `artifacts/research/`. Die Implementierung liegt in
`src/scanner/reports/research_views.py`; die zentrale Version lautet
`research_views_v1`. Die produktive Score-Berechnung bleibt unverändert.

| Datei | Verwendung |
| --- | --- |
| `history_analysis.csv` | Vollständiges, dauerhaft erhaltenes Forschungsarchiv; neue Beobachtungen nur aus validierten aktuellen Scannerläufen. |
| `history_recent.csv` | Echte Scannerbeobachtungen im inklusiven Intervall vom neuesten Archiv-`as_of` minus 120 Kalendertage bis zu diesem Datum. |
| `latest_scanner.csv` | Tagesanalyse: letzter validierter vollständiger Snapshot, eine Zeile je Symbol. |
| `price_backfill.csv` | Separate historische OHLCV-Kurse; keinerlei Scannerkennzahlen. |
| `history_metadata.json` | Veröffentlichungsmanifest mit Prüfergebnis, Schwellen, Quellenhash und Datei-Hashes. |
| `daily_research.json` | Kompakte Derived-Research-Schicht mit aktuellen Feldern und historischen Vergleichsfällen. |

Die verbindliche L1/L2/L3-Logik, feste Buckets, Cross-Universe-Suche, Cooldown und
Kurs-Outcomes sind in [Historische Vergleichsfälle](historical_matches.md)
dokumentiert. `history_recent.csv` bleibt deren Scanner-Source-of-Truth.

## Datenfluss und Erhaltung

1. Vor `python -m scanner.app.run_daily` mit `--begin-run --receipt ...` eine
   Laufquittung außerhalb der Artefakte anlegen. Sie enthält Startzeit, Run-ID und
   Dateifingerabdruck der vorherigen vollständigen Watchlist.
2. Nach dem Scanner `generate_research_views.py --receipt ... --scanner-status ...`
   aufrufen. Quelle ist jetzt `artifacts/watchlist/watchlist_full.csv`, nicht ein
   möglicherweise alter oder bereits mit Restzeilen vermischter History-Upsert.
3. Erfolgsstatus und Neuschreiben der Watchlist prüfen (mtime/Größe müssen sich
   geändert haben; mtime darf unter Berücksichtigung der Sekundenauflösung nicht
   vor dem Start liegen). Pflichtspalten, Struktur, einheitliches MarketDate,
   eindeutige Symbole, ScoreError und numerische Score-Abdeckung prüfen.
4. Nur den vollständigen aktuellen Lauf in beide Historien übernehmen. Bereits enthaltene
   Beobachtungen werden anhand ihrer Originalfelder mengengetreu erkannt; bestehende
   Duplikate bleiben erhalten. Geänderte Beobachtungen werden zusätzlich angehängt,
   niemals überschrieben. Das gilt im neuen Daily-Pfad auch für
   `artifacts/snapshots/score_history.csv`. Tagesgleiche Wiederholungen haben eine
   eigene Run-ID; historische Beobachtungen bleiben erhalten. Ältere Läufe werden
   nicht automatisch nachimportiert. Die bestehende R-Code-Funktion wird nur für
   den gerade validierten aktuellen Scan angewandt. Opportunity, Risk, Confidence,
   Label und weitere vorhandene Werte werden direkt aus der Watchlist übernommen.
5. Kurse aus `artifacts/market_data/yahoo_ohlcv.csv` separat übernehmen. Eine feste
   Spaltenliste verhindert die Übernahme von Scannerkennzahlen. Vorhandene
   Datum/Symbol-Schlüssel behalten ihre ursprünglichen Werte.
6. Recent ausschließlich aus dem Archiv ableiten; Latest aus dem validierten Lauf.
7. SHA-256 der tatsächlich vorgesehenen Bytes berechnen und Dateien atomar ersetzen.
8. Vor dem Schreiben die zusammengehörigen Bytes vollständig validieren; erst
   zuletzt das Manifest schreiben. Danach die tatsächlich geschriebenen Dateien
   erneut prüfen. Der Workflow wiederholt diese Prüfung vor `git add artifacts/`.
9. History Delta mit `--report-only` aus der validierten History berechnen. Dieser
   Modus schreibt keine Scannerbeobachtungen; für jeden Tag wird dessen zuletzt
   angehängter Lauf ausgewählt. Weitere Reports, UI und Quality Gates folgen.

Das alte Archiv enthält bereits `market_data`-Zeilen. Diese bleiben aus Gründen
der Datenbewahrung unangetastet, werden in Metadata gemeldet und gelangen niemals
in Recent oder Latest. Neue Kurszeilen gelangen ausschließlich in Price-Backfill.
Für Forschung mit dem bestehenden Archiv deshalb nach `observed_scanner` filtern;
unmarkierte Altbeobachtungen gelten bei fehlender/für Scanner passender Herkunft
als Scannerbeobachtungen. Unbekannte Herkunft wird aus den Views ausgeschlossen.

Originalfelder bleiben als Strings erhalten, einschließlich leerer Felder,
numerischer Schreibweisen und realer mehrfacher Beobachtungen. Neue Spalten werden
angehängt und in Metadata dokumentiert; alte Zeilen bekommen dort leere Werte.
Die CSV-Serialisierung kann sich bei einer Erweiterung ändern, historische
Zellwerte nicht. Historische Ränge werden weder ergänzt noch korrigiert. Neu
validierte Daily-Beobachtungen erhalten ihre aktuellen Ränge in beiden Historien.

## Zeit, Identität und Teilscans

`as_of` ist der fachliche Scantag (`YYYY-MM-DD`), bei Altzeilen aus `date` übernommen.
`generated_at` ist die UTC-Erzeugungszeit der View im ISO-8601-Format. Diese
Provenienzfelder sind keine rekonstruierten Scannerkennzahlen.

Ein erfolgreicher Erzeugungslauf bekommt genau eine UUID als `snapshot_id`, identisch
in Latest, Recent und Metadata. `schema_version` steht in diesen drei Ausgaben.
Recent ist anhand der Originalfelder und des Datumsfilters aus dem Archiv ableitbar;
die zusätzlichen View-Provenienzfelder werden beim Export gesetzt.

Bei einem Teilscan bleiben Archiv, Latest **und Recent bytegleich**. Metadata meldet
`latest_run_complete=false`, Fehlergründe und den letzten vollständigen Scantag.
Die veröffentlichte `snapshot_id` bleibt erhalten. Jeder Versuch hat zusätzlich eine
neue `attempt_id`; Metadata-`generated_at` bezeichnet den Versuch, `views_generated_at`
die erhaltene Veröffentlichung. Dies löst den Konflikt zwischen unveränderlichem
Latest bei Teilscans und übereinstimmenden IDs. Ohne erste gültige Veröffentlichung
bleiben Scanner-Views aus, ihre Hashes und `snapshot_id` sind `null`.

## Vollständigkeit und Ränge

Zentrale Konfiguration: `ValidationPolicy`. Der Offline-Export verlangt `date`,
`symbol`, `score`. Der produktive Daily-Pfad verlangt zusätzlich `name`,
`opportunity`, `risk`, `confidence`, `rs3m`, `trend200`, `cycle` (bekannte originale
Scanner-Spaltennamen werden auf diese Exportnamen abgebildet).
Standardmäßig müssen mindestens 90 % der Zeilen endliche numerische Scores haben,
analog zum bestehenden Pipeline-Coverage-Gate. Symbole müssen nicht leer und pro
Snapshot eindeutig sein. Fehler wie abgebrochene CSV-Zeilen, fehlender abschließender
Zeilenumbruch, expliziter Abbruchstatus, Zukunftsdatum oder Rückschritt hinter den
letzten vollständigen Scan verhindern die Veröffentlichung.

Als Symbolzahl-Basis dient das Maximum aus dem letzten validierten Latest und den
letzten 20 früheren Scannergruppen mit vollständigen numerischen Scores. Der
Standardfaktor ist 1,0. Preiszeilen zählen nie zur Basis. Ohne belastbare Basis wird
konservativ abgelehnt (`missing_completeness_baseline`). Für Erstbetrieb oder bewusst
geänderte Universen lässt sich eine bekannte Sollzahl explizit konfigurieren:

```sh
python scripts/publish_history_analysis.py --expected-symbol-count 197
```

Die Zahl ist ein Beispiel, keine projektweit festgelegte Sollzahl. Weitere Optionen:
`--min-symbol-ratio` und `--min-score-ratio`. Die effektiven Werte und gemessenen
Zahlen stehen immer in Metadata. Unbekannte/weggefallene Spalten werden gemeldet.
Die Zählprüfung ist eine konservative Plausibilitätsprüfung, kein Beweis für die
vollständige Mitgliedschaft eines veränderten Universums. Ein unbemerkt reduziertes,
aber immer noch ausreichend großes Universe kann damit nicht sicher erkannt werden.
Die Laufquittung und direkte Watchlist-Quelle verhindern dagegen, dass ein nicht
gelaufener Scanner oder Restzeilen eines taggleichen History-Upserts als neuer
vollständiger Lauf gelten.

Nur vollständig validierte aktuelle Beobachtungen bekommen neu berechnete Ränge: Score absteigend, Gleichstand mit bestem
gemeinsamen Rang (`min`), `universe_size` gleich Anzahl endlicher numerischer Scores,
`rank_percentile = rank / universe_size`. Zeilen ohne numerischen Score bleiben bei
ausreichender Gesamtabdeckung enthalten, aber ohne Rank und Perzentil.

## Kurse und Fehlerverhalten

Backfill-Kurse dürfen für Kursberechnungen verwendet werden. Score, Opportunity,
Risk, Confidence, R-Code, Cycle, Rank, Perzentil, historisches RS3M und Trend200
werden daraus niemals erzeugt. Neue Downloads erhalten `retrieved_at` direkt nach
dem Abruf. Alte Caches ohne Abrufzeit behalten ein leeres Feld plus Metadata-Warnung;
der Exportzeitpunkt wird nicht als historischer Abrufzeitpunkt ausgegeben.

Der Daily-Workflow ruft den Generator direkt nach dem Scanner und vor History Delta
auf. `continue-on-error` am Scanner erlaubt die Veröffentlichung einer expliziten
Fehlermeldung. Ein unvollständiger Scan ist für den Generator ein dokumentierter
Zustand (CLI-Exitcode 0, Step-Output `complete=false`). Reports und UI werden dann
übersprungen. Nach erfolgreicher Kohärenzprüfung veröffentlicht der bestehende
Commit-&-Push-Schritt die Fehlermetadaten; ein abschließender Schritt setzt den
Workflow auf Fehler. Ein Teilscan erscheint dadurch nicht als grüner Tageslauf.
I/O-Fehler oder ungültige archivierte Dateien brechen vorher mit Fehler ab.
`concurrency` verhindert überlappende Daily-Veröffentlichungen; Push-Konflikte mit
anderen Schreibern werden nicht durch Force-Push oder automatisches Merge verdeckt.

Alle fünf Research-Dateien liegen unter `artifacts/research/` und werden vom
bestehenden `git add artifacts/` erfasst. Die Git-Schritte verschlucken keine
Staging-/Commit-Fehler mehr. Der Zeitplan bleibt bei 17:07 Uhr Berliner Sommerzeit.

Read-only-Abschlussprüfung:

```sh
python scripts/generate_research_views.py --validate-only
```

Die Prüfung verifiziert Hashes, Dateiexistenz, Zeilenzahlen, gemeinsame Snapshot-ID,
UTC-Zeitstempel, Symbol-Eindeutigkeit, aktuelle Ränge und die mengengenaue Ableitung
der Recent-Zeilen aus dem Archiv einschließlich realer Duplikate. Bei der Erzeugung
wird zusätzlich überprüft, dass alle alten Archivzeilen mit unveränderten
Originalwerten vor den neuen Zeilen erhalten geblieben sind.

Eine exklusive Lockdatei verhindert parallele Publisher. Nach hartem Prozessabbruch
kann `.research_views.lock` stehen bleiben; nur nach Prüfung, dass kein Publisher
läuft, entfernen. Vor dem Schreiben werden Quellen und alte Ausgaben nochmals auf
unveränderte Bytes geprüft. Ein Mehrdatei-Update ist keine Dateisystemtransaktion:
Leser müssen die Manifest-Hashes prüfen und bei Abweichung erneut lesen. Ein
unterbrochener Schreibvorgang veröffentlicht kein neues Erfolgsmanifest; erneutes
Ausführen repariert die Views. Andere Prozesse dürfen die Forschungsdateien nicht
gleichzeitig schreiben.

Der bisherige monatliche `history_research.csv`-Export bleibt unabhängig und
unverändert. Tests: `python -m unittest discover -s tests -v`.
