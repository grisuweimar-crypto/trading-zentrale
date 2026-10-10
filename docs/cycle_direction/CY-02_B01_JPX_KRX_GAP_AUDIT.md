# CY02-B01 – JPX/KRX Gap-Diagnostik (kein Quality-Gate-Override)

**Basis:** produktives Autopilot-Snapshot `2026-10-10` mit 6 `INSUFFICIENT_HISTORY/GAP_IN_DAILY_BARS` (4 × .T, 2 × .KS), siehe [#269](https://github.com/grisuweimar-crypto/trading-zentrale/issues/269).

**Quelle der kalenderrechtlichen Einordnung:** [JPX – offizieller Handelskalender für Aktien 2026](https://www.jpx.co.jp/english/corporate/about-jpx/calendar/index.html) zeigt ausdrücklich **21., 22. und 23. September 2026** geschlossen (Stock-Trading). Es existiert am selben Zeitraum **Derivate-Feiertagshandel**; [JPX Derivatives](https://www.jpx.co.jp/english/derivatives/rules/holidaytrading/index.html) darf **nicht** als Öffnung des Aktienhandels fehlinterpretiert werden.

**KRX:** die offizielle interaktive [KRX holiday page](https://open.krx.co.kr/contents/MKD/01/0110/01100305/MKD01100305.jsp) wurde identifiziert, jedoch keine statisch signierte Ausgabetabelle mit konkreten Gap-Daten ausgelesen. Kalenderreferenzen [MarketHours](https://markethours.io/market-holidays/krx/2026) und [MarketHoliday](https://market-holiday.com/markets/krx/holidays/2026) nennen 24.–25.09., 05.10., 09.10.2026 geschlossen. Diese vier Tage sind **nur Referenz**, ausdrücklich nicht als independently provider-certified markiert.

**Prüfmechanismus:** `scripts/diagnose_cycle_exchange_gaps.py` verwendet separaten read-only Yahoo-Download im GitHub-Runner und dieselbe per-Symbol-Close-Extraktion sowie `calculate_cycle` wie CY-02. Für alle 6 Assets listet es die fehlenden Wochentage zwischen tatsächlichen **60 zuletzt verwendbaren** täglichen Yahoo-Bar-Sessions, amtlich bestätigte JPX-Schließtage, nur referenzierte KRX-Feiertage und **unerklärte Handelstagslücken**. Es schreibt nichts und ändert keine Formel, Frischeschranken, Universe, Währung oder Quality-Status.

**Harte Grenze:** Die neue Abrufserie ist nicht zwingend bitgleich mit dem am 10.10.2026 im ursprünglichen produktiven Scanner verwendeten Yahoo-Frame; sie ist **diagnostische neue Quellbeobachtung**, kein archivierter historischer Session-Publikationsbeweis. Ein Tageskalender legitimiert weder fehlende Barausgaben noch rückwirkend identische Adjustments automatisch. Eine tatsächliche Entsperrung der 6 Werte bedarf einer separat versionierten, nach konkreten Lückentagen validierten Kalender-Policy mit Negativtests und einem neuen echten Publish.

**Status:** Untersuchung; kein CY02-Quality-Override, keine CY-03-Freigabe, #269 weiterhin offen.
