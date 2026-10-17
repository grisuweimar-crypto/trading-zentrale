# Umstellung der Scanner-Kursbasis – Entscheidung und Migrationsplan (17.10.2026)

## Befund aus Produktivcode
- `src/scanner/data/enrich/yahoo_prices.py`: `yf.download(...period="1y",interval="1d",auto_adjust=True)` ohne allgemeines Closed-Session-Gate.
- `_compute_features` benutzt `c.iloc[-1]` direkt für `Akt. Kurs`, `SMA200/Trend200`, `RS3M`, `Volatility`, `DownsideDev`, `MaxDrawdown` sowie `AvgVolume/DollarVolume`; `calculate_cycle(close,...)` erhält dieselbe Download-Reihe. Scoring folgt als separater Schritt `app/score_step.py`.
- Fehlende Abrufe behalten laut Quellcode bisherige Kursfeatures in der Watchlist, ohne alle als nicht frisch auszuschließen.
- `.github/workflows/run_scanner.yml`: Termine **16:07, 17:07, 18:37 und 20:37 Europe/Berlin** (je nach Autorun-State nur ein Tageslauf). US-Regular-Session an diesen Zeiten regelmäßig noch geöffnet. Scannerzeit ist nicht gleich Handelstag.
- `data/price_history.py` kann bereits OHLCV und Adj Close validieren; `scripts/prefetch_market_history.py --minimum-sessions 300` läuft im Workflow bisher **nach** Scoring. Für Kursverlauf soll diese validierte Quelle nach vorn rücken; vollständige Preisreihen nicht anhand der Momentaufnahmen zusammenkleben.

## Implementierter, bewusst nicht aktivierter Schritt
Opt-in `SCANNER_CLOSED_DAILY_ONLY=1` in `yahoo_prices.py`: Begrenzung der Yahoo-Tagesreihe auf **vor dem aktuellen UTC-Kalendertag datierte** Preisbars, und zwar **vor** Feature- und Cycle-Berechnung, auch für beide Benchmarks. Konservative Zeitbarriere, keine volle Exchange-Kalender-Lösung. Weil sie für Asien/Europa abgeschlossene heutige Kurse unnötig zurückhält, ist diese Flag **nicht** als finale Lösung zu deployen. Mit Tests im PR.

## Verbindliches Ziel (noch nicht umgesetzt)
1. Ein Datensatz pro Instrument und abgeschlossener Session `symbol,exchange,timezone,session_date,open,high,low,close,adj_close,volume,provider,retrieved_at,bar_final,source_hash`. Verschiedene Quellversionen versionieren, nicht heimlich umschreiben.
2. `as_of` ist **Datenverfügbarkeit**, nicht bloß generierter Dateizeitstempel. Pro Symbol letztes verifiziert abgeschlossenes Börsenbar mit offizieller Session-Kalenderregel, Sommerzeit und Kurzhandel. Aktienkurs im Scanner ist der Preis **dieser** Kerze; zusätzlich eindeutiges `price_bar_date`.
3. Die Bewertung darf **nicht** rückwirkend alte Scores mit neu heruntergeladenen historischen Daten als historische Point-in-Time-Ergebnisse ersetzen. Nur Originalbeobachtungen bleiben Originalbeobachtungen. Nachträgliche Retro-Scores sind separat `recomputed_ex_post`.
4. Fail-closed für veraltete oder fehlende Quelle statt Altwert-Recycling; kein Cross-Symbol-Mix aus aktuellem und stehendem Preiszustand. Benchmark- und Titeldaten müssen zeitlich kompatibel sein, inklusive RS3M.
5. ETF, HK, Japan, Kanada, EU, Australien, US, Krypto separat berücksichtigen. Kryptotag 00:00–00:00 UTC; Handelswoche der Börsen gilt für Aktien. Datenlieferverzug wird explizit ausgewiesen.
6. Pipeline-Reihenfolge: Markt-Datenpflege/Bar-Finalitätsprüfung **vor** Enrichment/Scoring; Scanner erst nach Gate; danach History, Decision, Research; ein Run kann warten oder ausfallen, aber keinen unvollständigen Datenstand fälschlich publizieren.
7. Zeitentscheidung: US-Regular-Session endet 16:00 **America/New_York**, also häufig 22:00 Europe/Berlin, in den DST-Wechselwochen bereits 21:00. Finalität + Provider-Latenz erfordern Puffer. Falls ein Lauf vorher erwünscht ist, verwendet er die letzte bereits final bestätigte US-Tageskerze (nicht den laufenden US-Tag). Watch bleibt snapshotabhängig und wird nicht starr auf den Folgetag verlegt, sofern die Nutzeranforderung so bleibt.
8. Tests: offene US-Kerze ausgeschlossen; HK-Kerze nach Sitzungsende verfügbar; DST-/Feiertags-/Half-Day-Umstellungen; fehlende Kursquelle blockiert Score statt stale fallback; deterministische Backfills, Split/Dividend-Fälle; Watch-Snapshot-Kohärenz; Score-Regression.
9. Migration: Alt-History unverändert archivieren, `price_basis=legacy_intraday_mixed` nur wenn wirklich nachgewiesen; fehlende Zeitbelege = `price_basis_unknown`. Stichtag und Qualitätsflag pro Score. Ab Cutover neues Schema/versionierte Historie. Keine fiktiven Retrokorrekturen.

## GitHub-PR-Status
Diese Pull Request ist **Draft** und absichtlich keine Änderung an `main`. Vor der Produktiv-Aktivierung ist eine echte per-Börse-Session-Finalitätsprüfung und das Wegfallen des Legacy-Preis-Fallbacks nötig. Tests und Rückrechnung zuerst. Doppelte Preisarchive vermeiden.
