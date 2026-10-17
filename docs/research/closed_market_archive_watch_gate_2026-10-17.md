# Kanonisches Kursarchiv und Watch-Vorprüfung – 17.10.2026

## Verifizierter Lauf
[GitHub Actions 39051140864](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/39051140864): **success**. Branch research/closed-bars-scanner-cutover-20261017; keine Änderung an main.

## Tatsächliche Datenabdeckung
- Aktueller `latest_scanner.csv`-Satz: **220** eindeutige Titel (nicht gleich der älteren 228-Zeilen-Shadow-Watchlist).
- Bestehende Quelle: `artifacts/market_data/yahoo_ohlcv.csv` mit raw OHLCV und gesondertem `adj_close`.
- Vollständiger Altarchiv-Gültigkeitstest: 33/220, weil ältere historische Daten bei 187 Titeln ein fehlendes Adjusted Close enthalten.
- **Letztes 300-Zeilen-Fenster:** 219/220 Titel mit mindestens 200 beobachteten Sitzungen und lückenlos `adj_close` im geprüften Fenster.
- **SPCX**: nur 87 beobachtete Sitzungen; Status `INSUFFICIENT_OBSERVED_SESSIONS`, nicht künstlich mit Kursen ergänzt.
- Fehler werden nicht mit Null, letztem gespeicherten Kurs oder rückwirkender Anpassung verschleiert.
- Details `artifacts/research/closed_bar_shadow/archive_gate.json`.

## Technisch umgesetzt
- `src/scanner/data/closed_market_cache.py` nutzt vorhandene `validated_rows` und den vorangegangenen Kurskerzen-Gate; kein zweites Kursarchiv; strikte Mindestzahl 200 und adjusted-coverage im aktuellen max. 300-Session-Fenster.
- `scripts/audit_closed_market_archive.py` dokumentiert universeweite Qualitäts-/Altershinweise. Es handelt sich um einen **prüfenden Adapter**, noch keine umgeschaltete produktive Scoring-Eingangsquelle.
- Bestehende Regressionstests für Watch Snapshot Pair, Daily Research und History Query wurden zusätzlich im Workflow ausgeführt und liefen erfolgreich.
- Das historische Scoring bleibt zeitlich original; keine ex-post Backfills als zeitgenössische Scores ausgeben.

## Verbleibende Produktivfreigabe-Gates
1. Pre-Scoring-Marktcache-Update implementieren, ohne vom bereits neu erzeugten Scanner-Snapshot abhängig zu sein; der heutige `prefetch_market_history` baut auf dem bereits publizierten `latest_scanner` auf und läuft derzeit nach dem Scoring.
2. Universumsidentitäten vereinheitlichen: Archiv-Snapshot (220) und Shadow-Watchlist (228) betreffen verschiedene gespeicherte Stände, keine automatische 228/228-Behauptung.
3. Benchmarks (SPY, BTC-USD) für dieselbe Quellen-/Stichtagsfamilie nachvollziehbar sicherstellen. Vollständige finale Währung-/Handelskalender-/Session-Finalität und Fail-Closed-Meldung durch die gesamte UI/Decision/Watch-Publikationskette absichern.
4. Echten nichtpublizierenden vollständigen End-to-End-Probelauf mit dem gemeinsamen Marktcache durchführen; bisherige grünen Watch-Tests beweisen nur Vertragstreue einzelner Komponenten.
5. `SCANNER_CLOSED_DAILY_ONLY` bleibt im Produktivlauf aus; Merge ausschließlich nach allen Freigabegates.

**Fazit:** Zentralarchiv für 219/220 Titel im 200–300-Session-Fenster bereit, History-Altlasten erkannt. Produktivumschaltung ist wegen Pipeline-Reihenfolge und Source-Identity noch nicht freigegeben.
