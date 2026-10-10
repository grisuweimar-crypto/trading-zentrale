# CY-02-B01 — versionierte JPX/KRX-Aktienmarkt-Kalenderregel (10.10.2026)

**Plan:** `CYCLE-DIR-2026-10-09-v1` · **PR:** [#285](https://github.com/grisuweimar-crypto/trading-zentrale/pull/285) · **Issue:** [#269](https://github.com/grisuweimar-crypto/trading-zentrale/issues/269) · **Ausgangs-main:** `0c56335e6f4e5db5d52111553a75c690efe4414a`.

## Exakt belegte Marktschließtage

| Equity-Markt und Quellidentität | Termine 2026 | Unabhängige Primärquellen |
| --- | --- | --- |
| JPX Cash Equities, Yahoo `.T` **nur JPY** | **21., 22., 23. September** | [JPX offizieller Aktienmarkt-Kalender](https://www.jpx.co.jp/english/corporate/about-jpx/calendar/index.html); [JPX separates Derivathandels-Statement](https://www.jpx.co.jp/english/news/2040/20260918-01.html) |
| KRX KOSPI Cash Equities, Yahoo `.KS` **nur KRW** | **24., 25. September** | [Bank of Korea Chuseok 2026 (24.–26.)](https://www.bok.or.kr/eng/main/contents.do?menuNo=400373) und [KRX KOSPI amtliche Börsenfeiertagsregel](https://global.krx.co.kr/contents/GLB/06/0602/0602010201/GLB0602010201T1.jsp) |

**Keine Kalender-Verallgemeinerung:** Die beiden JPX-Derivathandels-Tage und der reguläre Aktienmarkt dürfen nicht vermischt werden. `.T` ist hier cash equity, nicht Osaka Futures; `.KS` ist KOSPI, nicht jede koreanische Börsenklasse. Anderes Symbol, andere Originalwährung, weiteres fehlendes Werktagslabel: **keine** autorisierte Ausnahme.

## Umsetzung und Audit

- `src/scanner/data/enrich/cycle_exchange_calendar.py`: eigenständige datums-/exchange-/währungsgebundene Policy `cycle_equity_calendar_closures_2026_v1`.
- `cycle_oscillator.calculate_cycle`: ältere unveränderte `>2`-Werktag-Gap-Sperre gilt weiterhin. Sie lässt eine ausgedehnte Unterbrechung nur dann durch, wenn **sämtliche** fehlenden Werktage explizit in der versionierten JPX-/KRX-Evidenz stehen. Unbekannte Session bleibt ausgeschlossen. Keine Preise, Bars oder historische Snapshots werden erzeugt; 60 tatsächliche Inputbars sind weiterhin erforderlich.
- Nur bei tatsächlich genutzter geprüfter Kalenderausnahme wird `cycle_quality_reason=COMPLETED_DAILY_BARS_cycle_equity_calendar_closures_2026_v1` aufgezeichnet. Formel `cycle_detrended_sma20_range40_v1`, SHA des echten 60er-Preisfensters und ursprüngliche Market-Daily-Daten bleiben unverändert.
- `src/scanner/reports/cycle_history.py`: `reason` gehört ab jetzt zur `identity()` der technischen Lag-Kette. Damit ist der Wechsel von voriger Gap-Regel zu neuem, explizitem Quellenregime eine **harte Identitätsgrenze**, kein rückwirkend erschlichener `1/5/10obs`-Lag.
- `tests/test_cycle_exchange_calendar_cy02_b01.py` deckt alle **6 tatsächlichen JP/KR-Listingklassen** mit normalen/abweichenden Kalendern, unerklärtem Extratag, falscher Originalwährung und historischer Identitätsgrenze ab.

**CI-Beleg:** [Kalender-Job #114206645965](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/38049862948/job/114206645965) auf PR-Commit `a3de70de4c5239050ce6e5668d50afdcfda3d2a1` meldete **58 bestanden**; der read-only Replay-Audit gegen den bereits veröffentlichten alten Produktivstand meldete **215 Assets: 209 VALID, 6 INSUFFICIENT_HISTORY**. Dies ist absichtlich kein neuer Scannerstand und keine vorhergesagte `215 VALID`-Zahl. Der tatsächliche Lauf ist **#38049862948**; keine nachträgliche Erfindung von Testergebnissen.

## Weiter offen: externe Forschungszulassung

Der geschlossene Kalenderfall beweist weder historische End-of-Day-Yahoo-Bar-Publikation, tatsächlichen Handelsplatzpreis, Adjusted-Close-PIT-Verfügbarkeit noch Originalwährung aus signierten Providerbytes. Die sechs früher ausgeschlossenen Werte bleiben in deren **alten Snapshots ausgeschlossen**. Ob künftige neue produktive Scanneraufnahmen numerische Cycle-Werte für alle sechs liefern, ist anhand echten Autopilot-Quality-/60-Bar-Replays neu zu messen. Die CY-03 `research_status=BLOCKED_EXTERNAL_VERIFICATION_269` und die CY-04 Richtungsquarantäne bleiben unangetastet; keine Handels-, L1-, L3- oder Empirie-Freigabe.

**Ergebnis:** Enger Kalender-CAPA technisch abgeschlossen; #269 wegen separater PIT-/Session-/Adjusted-Original-Datenlinie offen.
