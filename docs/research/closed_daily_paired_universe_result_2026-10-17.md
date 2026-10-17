# Abgeschlossene Tageskerzen – gepaarter Volluniversumstest (17.10.2026)

Der Shadow-Test auf Branch `research/closed-bars-scanner-cutover-20261017` wurde erfolgreich ausgeführt: [Actions-Lauf 39050522978](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/39050522978).

## Methodik
- 228 Watchlist-Zeilen.
- **Eine einzige eingefrorene yfinance-Tagesdatenlieferung** und derselbe `as_of`-Zeitpunkt für alten und neuen Enrichment-/Scoring-Pfad.
- Forschungsergebnisse unter `artifacts/research/closed_bar_shadow/comparison.json`; kein Autopilot-Publish.
- Die vollständige OHLCV-Historienmigration und echtes zeitgenössisches PIT/Provider-Finalitätszertifikat sind **nicht** Gegenstand dieses Tests.

## Tatsächlich gemessene Unterschiede
| Faktor | Vergleichbar | geändert | größter absoluter Unterschied |
|---|---:|---:|---:|
| Score |228|9|0.60|
| Opportunity|228|8|0.92|
| Risk|228|7|0.10|
| Confidence|228|9|6.20|
| Preis|228|8|330.0625 in jeweiligen Preisnotierungen, nicht instrumentübergreifend vergleichbar|
| Trend200|227|8|0.0260025|
| RS3M|228|7|0.0308135|

- 228/228 gültig bei `SCANNER_CLOSED_DAILY_ONLY=1`.
- Kerzendatum 2026-10-16: 226 Zeilen; 2026-10-15: 2 Zeilen.
- Acht zuvor fehlende Börsenplatzzuordnungen ergänzt (Oslo, Mexiko, Brüssel, München, Kopenhagen und London IOB).
- Intermittierende LVMH/MC.PA-Sperre nicht reproduziert; bei erneutem Fehlen bleiben Daten gesperrt, keine stillschweigende Wiederverwendung.

## Noch ausdrücklich vor Produktivfreigabe
1. Einheitliche, validierte bestehende `price_history`-OHLCV-Datenbasis **vor** Scoring einsetzen und historische Sessions versioniert nachladen. Der aktuelle Versuch lädt noch einen Yahoo-1-Jahres-Frame separat.
2. Identität, Basiswährung, Listing und Frische der Benchmark im Vergleich zur Aktie vollständig prüfen; derzeit ist der statische Börsen-Zeitzonenadapter eine Research-Lösung und kein vollständig geprüfter Börsenkalender.
3. Fehlen/Fehler an der Quellenlinie auch nach Canonicalisierung, Publication und Watch fail-closed behandeln. Volle End-to-End-Prüfung der realen Scan- und Watch-Pipeline, inklusive Kontrakttests, durchführen.
4. Faktor-/Datenquellenänderungen von originalen PIT-Scores trennen; neue retrospektive Scores als Ex-post-Rekonstruktion, niemals als ursprüngliche Scannerbeobachtung kennzeichnen.
5. Keine Änderung am Autopilot-Zeitplan erforderlich. Die neue Flag ist im Produktivworkflow nicht aktiviert.

**Befund:** Die früheren größeren Unterschiede enthielten auch Effekte durch zwei nacheinander statt identisch abgefragte Kurslieferungen. Die gepaarte Untersuchung isoliert den Effekt besser und zeigt nur 9 von 228 veränderten Scores.
