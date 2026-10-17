# Abgeschlossene Tageskerzen – echter isolierter Scanner- und Decision-Integrationslauf (17.10.2026)

## Beweismittel
[GitHub Actions 39052351500](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/39052351500): **success**, Entwicklungsbranch, keine Commits/Publikation der berechneten Scannerergebnisse.

## Ablauf auf demselben Runner
1. Das existierende `prefetch_market_history.prefetch_history(...)` zieht das aktuelle Watchlist-Universum sowie SPY/BTC-USD in den existierenden kanonischen OHLCV-Cache. Der Runner löscht/ersetzt die produktive `main`-Historie nicht.
2. `SCANNER_PRICE_SOURCE=archive`, `SCANNER_CLOSED_DAILY_ONLY=1`, `SCANNER_FETCH_YAHOO=1` benutzen den validierten, auf letzte abgeschlossene Börsenkerzen gefilterten Cache **vor** dem Scoring. Keine separate ad-hoc-1d-Providerreihe während der Score-Rechnung.
3. Scanner `run_daily`: 220/220 verarbeitet, `Score coverage: 219/220 score>0, 1 score==0, 0 NA, 0 error-rows`.
4. Archivdeckungsstatus beim Scan: 220 `CLOSED_ARCHIVED_SESSION` und ein zusätzlicher Benchmark/IPO-Kurzhistorienfall `CLOSED_ARCHIVED_SHORT_HISTORY` in der Enrichment-Abfrage (enthält zusätzlich SPY/BTC); keine 200-Tage-SMA für den jungen Titel erfunden, alte Preisfaktoren zuvor genullt.
5. `generate_research_views.py --receipt ... --scanner-status success`: `latest_run_complete=true`, `latest_run_error=null`; anschließend `--refresh-prices` und `generate_daily_research.py` erfolgreich.
6. `build_current_decision_evidence_7a.py`: erfolgreich, **220** Evidence-Pakete mit neuer Snapshot-Identität.
7. Neue Snapshot-ID: `70b14ab9-e2dc-4143-9aa1-2a45969d81c0`, as_of 2026-10-17. Metadata und daily_research enthalten dieselbe ID. Nicht als tatsächlich am 17.10. publizierter produktiver Scannerlauf interpretieren.
8. `check_watch_snapshot_pair.py`: korrekte Antwort `ready=false`, Grund `awaiting_w10_for_latest_scanner`; bestehender versiegelter W10 2026-10-16 gehört Snapshot `41aab262-2765-4404-ab38-06c122bbf6ef`. Kein falsch-positives Altsnapshot-Ready. **Eine neue W10-Versiegelung und private Depot-Watch wurden in diesem isolierten Lauf nicht ausgeführt.**

## Eingebaute Schutzvorkehrungen
- Nur expliziter Branch-Opt-in, kein Produktiv-Flag; fehlender Benchmark oder fehlende abgeschlossene Sitzung => kein Score.
- Per-Instrument lokales Vorbörsentag-Gate; uneinheitliche Feiertage/Session-Lifecycle müssen vor Merge endgültig mit Kalenderdaten geklärt werden.
- Für IPO/junge Titel reichen 60 echte abgeschlossene Kerzen für begrenzte Analyse; 200-Tage-SMA bleibt absent (niemals von altem Watchlist-Cache übernommen).
- `raw` OHLCV und `adj_close` bleiben in bestehendem Archiv; der Scoring-Frame verwendet `adj_close` als adjustiertes Close (wie zuvor `auto_adjust=True` von yfinance), Provenienz bleibt getrennt.

## Was ein grüner E2E-Test NICHT bedeutet
- Kein neuer W10/7H/W11-Depot-Watch-Snapshot; diese Kette hat eigene Readiness-Voraussetzungen und private Positionsdaten.
- Keine formale Markt-/Kalender-Quellenattestierung von Schlusskursen, keine ex-post-PIT-Nachbildung historischer Scores, keine Vollhistorien-Rohdatenrevision.
- Keine Promotion der Forschung zu `main`; produktive Scannerzeit unverändert.

**Freigabekriterium:** Der separate W10-Watch-Workflow muss für den neuen Tages-Snapshot auf `main` neu laufen/sealen. Vor dem Merge zusätzlich Regression für ausländische Feiertage und Short-History-Szenarien, sowie klare Erfassung von Quelle/Datum im publizierten Watch-Layer.
