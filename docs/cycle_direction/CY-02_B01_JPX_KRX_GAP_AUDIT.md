# CY02-B01 – JPX/KRX Gap-Diagnostik (kein Quality-Gate-Override)

**Basis:** produktives Autopilot-Snapshot `2026-10-10` mit 6 `INSUFFICIENT_HISTORY/GAP_IN_DAILY_BARS` (4 × .T, 2 × .KS), siehe [#269](https://github.com/grisuweimar-crypto/trading-zentrale/issues/269).

**Quelle der kalenderrechtlichen Einordnung:** [JPX – offizieller Handelskalender für Aktien 2026](https://www.jpx.co.jp/english/corporate/about-jpx/calendar/index.html) zeigt ausdrücklich **21., 22. und 23. September 2026** geschlossen (Stock-Trading). Es existiert am selben Zeitraum **Derivate-Feiertagshandel**; [JPX Derivatives](https://www.jpx.co.jp/english/derivatives/rules/holidaytrading/index.html) darf **nicht** als Öffnung des Aktienhandels fehlinterpretiert werden.

**KRX:** die offizielle interaktive [KRX holiday page](https://open.krx.co.kr/contents/MKD/01/0110/01100305/MKD01100305.jsp) wurde identifiziert, jedoch keine statisch signierte Ausgabetabelle mit konkreten Gap-Daten ausgelesen. Kalenderreferenzen [MarketHours](https://markethours.io/market-holidays/krx/2026) und [MarketHoliday](https://market-holiday.com/markets/krx/holidays/2026) nennen 24.–25.09., 05.10., 09.10.2026 geschlossen. Diese vier Tage sind **nur Referenz**, ausdrücklich nicht als independently provider-certified markiert.

**Prüfmechanismus:** `scripts/diagnose_cycle_exchange_gaps.py` verwendet separaten read-only Yahoo-Download im GitHub-Runner und dieselbe per-Symbol-Close-Extraktion sowie `calculate_cycle` wie CY-02. Für alle 6 Assets listet es die fehlenden Wochentage zwischen tatsächlichen **60 zuletzt verwendbaren** täglichen Yahoo-Bar-Sessions, amtlich bestätigte JPX-Schließtage, nur referenzierte KRX-Feiertage und **unerklärte Handelstagslücken**. Es schreibt nichts und ändert keine Formel, Frischeschranken, Universe, Währung oder Quality-Status.

**Harte Grenze:** Die neue Abrufserie ist nicht zwingend bitgleich mit dem am 10.10.2026 im ursprünglichen produktiven Scanner verwendeten Yahoo-Frame; sie ist **diagnostische neue Quellbeobachtung**, kein archivierter historischer Session-Publikationsbeweis. Ein Tageskalender legitimiert weder fehlende Barausgaben noch rückwirkend identische Adjustments automatisch. Eine tatsächliche Entsperrung der 6 Werte bedarf einer separat versionierten, nach konkreten Lückentagen validierten Kalender-Policy mit Negativtests und einem neuen echten Publish.

**Status:** Untersuchung; kein CY02-Quality-Override, keine CY-03-Freigabe, #269 weiterhin offen.


## Diagnoseergebnis: unabhängiger GitHub-Runner am 10.10.2026, 08:14:34 UTC

**Run:** [CY02-B01 Exchange Gap Diagnostic #38037172960](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/38037172960), `completed/success`. Neuer Yahoo-`1d`-Download mit `auto_adjust=True` und identischer per-Symbol-Zeilenextraktion; fixer historischer Entscheidungscutoff `2026-10-10T08:00:00Z` (keine heutigen Sessionlabels).

| Symbol | letztes Sessionlabel | Tagesbar-Status | exakt nachgewiesene fehlende Werktage zwischen den 60 jüngsten 1d-Bars | kalendarischer Befund |
|---|---|---|---|---|
| `6503.T` | 2026-10-09 | INSUFFICIENT_HISTORY/GAP_IN_DAILY_BARS | 2026-09-21, -22, -23 (zwischen 18. und 24.) | alle drei JPX **amtlich geschlossen** |
| `6861.T` | 2026-10-09 | gleich | dieselben drei Tage | JPX bestätigt |
| `8035.T` | 2026-10-09 | gleich | dieselben drei Tage | JPX bestätigt |
| `6506.T` | 2026-10-09 | gleich | dieselben drei Tage | JPX bestätigt |
| `000660.KS` | 2026-10-08 | gleich | 2026-09-24, -25 (zwischen 23. und 28.) | KRX Chuseok, **nur Kalenderreferenz**, nicht Primärarchiv |
| `005930.KS` | 2026-10-08 | gleich | dieselben zwei Tage | KRX Chuseok, gleiche Einschränkung |

**Bestandsbeweis:** Der gesamte überprüfte 60-Bar-Bereich jedes der sechs Ticker enthielt genau die oben beschriebenen verlängerten Pausen und **keine weiteren unerklärten Werktagslücken**. Die neue Downloadserie ist nicht identisch beglaubigt mit dem ursprünglichen Scan-Frame, deshalb nicht als rückwirkende damalige Price-Window-Publikation ausgeben.

**Schlussfolgerung:** Die bestehende globale `>2`-Werktage-Gap-Sperre ist für diese nachgewiesenen Marktruhezeiträume übervorsichtig; es besteht kein Anlass, den allgemeinen Datenlückenschutz abzuschalten. CY-02 Kalenderbehandlung soll als **separate, versionierte, börsenbezogene Ausnahme** mit source-bound Datumsnachweis implementiert werden, sobald für das KRX-Segment Primärnachweise vorliegen. Jede Nicht-Feiertagslücke bleibt sperrpflichtig. Keine heutige Änderung des produktiven Oscillators und keine rückwirkende Validierung alter Fälle.

**Freigabe:** `FERTIG_TECHNISCH` **für Read-only-Diagnose**, `NICHT_FREIGEGEBEN` für die künftige Kalender-Policy und CY-03-Research-Promotion.
