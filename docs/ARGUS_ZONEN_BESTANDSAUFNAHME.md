# ARGUS M0 – Bestandsaufnahme und Abhängigkeitsmatrix (26.10.2026)

**Verbindliche Quelle:** `ARGUS_ZONEN_MASTERPLAN_2026-10-23.md` (M0-Abnahme). **Status:** Inventur abgeschlossen; daraus folgt **keine** empirische M4/M5-Freigabe. Diese Datei gehört zur isolierten öffentlichen Forschungs-PR [#338](https://github.com/grisuweimar-crypto/trading-zentrale/pull/338); die private Live-Forschung befindet sich gesondert im Repository [ARGUS-Private](https://github.com/grisuweimar-crypto/ARGUS-Private).

## Nachweisbare Komponenten und Pfade

| Baustein | Fundstelle | Verifizierter Zweck | Offenes Qualitätsproblem |
|---|---|---|---|
| abgeschlossene Scanner-Kerzen | `artifacts/research/daily_research.json`, `artifacts/research/decision_snapshot_w10.json`, `artifacts/research/watch_runtime/manifest.json` | abgeschlossener Scanner-/W10-Snapshot und versiegelte Watch-Linie | Scanner-Zeitstempel und M2-Forschungsasof sind getrennt; keine Verbindung durch Rang |
| tägliche rückwirkende OHLCV-Bars | `artifacts/market_data/yahoo_ohlcv.csv` und `scripts/prefetch_market_history.py::_download` | OHLCV + separater `adj_close`, Kursbasis für M1/M2 | gemischter Umfang der 5 Pilotwerte; rückwirkend bezogene historische Vendoradjustierung, keine echte historische PIT-Archivierung |
| kausale Pivots | `src/scanner/research/elliott_vnext/pivots.py` | `prepare_daily_ohlcv`, `detect_confirmed_pivots`, `atr_series`; `pivot_time` ≠ `confirmed_time` | es existieren strukturell plausible, aber nicht eindeutige Swinggrade; Kursrevisionen und Splits separat prüfen |
| M1/M2-Prototyp | `src/scanner/research/zones_v01.py` | Quellformatprüfung, deterministisches `chronological`, Pivot-Z1, Fib-Z2, ABC-Z3; JSON-Ausgabe | standardmäßiger CLI-Aufruf ohne `asset_id` meldet Identität als nicht unabhängig geprüft; vollständiger Provider-Kursbeleg notwendig |
| M3-Tageskonfluenz | `src/scanner/research/zones_m3.py` | Zonencluster, Setup-Entstehung, Ziel- und Invalidierungslevel | doppelte Quellfamilien nicht fälschlich unabhängige Treffer; keine Erfolgschance |
| M3-Wochenstruktur | `src/scanner/research/zones_m3_weekly.py` | nur abgeschlossene Wochenkerzen, Weekly-Level und cross-timeframe overlaps | keine gleichzeitige Bestätigung unvervollständigter laufender Wochen |
| M4-Ereignis-/Kostenreplay | `src/scanner/research/zones_m4.py`, `scripts/replay_argus_zones_m4.py`, `docs/argus_zones_m4_research.md` | unveränderliches Hypothesenledger, spätere Outcomes, angenommene 30 bp Roundtripkosten | kein eigenständiger historischer PIT-Provider und keine empirisch nachgewiesenen Brokerkosten |
| M4.1 Zonendynamik | `src/scanner/research/zones_m41.py`, `scripts/replay_argus_zones_m41.py`, `docs/argus_zones_m41_research.md` | Zone-only Touch, Folgen, Nichtkontakt, Negativkontrollen, 1/3/5/10/20/40/60-Session-Fenster | korrelierte Zonen, kein unabhängiger Holdout/Cluster-Bootstrap, retrospektive Adjustierung |
| fünf Pilotwerte | `scripts/probe_argus_zone_symbols.py`, `scripts/m41_refetch_now_readonly.py` | read-only OHLCV-Probe, gesonderter NOW-Nachbezug, Forschungsartefakte | USD/Börsenidentität im eigentlichen CLI ausdrücklich belegen; Yahoo-Limits |
| Tests | `tests/test_argus_zones_v01.py`, `test_argus_zones_m3.py`, `test_argus_zones_m4.py`, `test_argus_zones_m41.py` | 47 Forschungstests (PR #338 CI `zones-research-only` erfolgreich) | Tests sind keine Realmarkt-Prognosevalidierung |
| produktiver abgeschlossener Kerzen-/Watch-Umbau | [PR #326](https://github.com/grisuweimar-crypto/trading-zentrale/pull/326) | am **18.10.2026** nach `main` gemergt | aufgelöste Tageskerzenbasis ist getrennt von Forschungs-Zielzonespezifik |
| private M5-Erstarchivierung | `ARGUS-Private/research/zones/m5_daily.py`, `shadow/{snapshots,sources,inputs,receipts}` | append-only archivierte fünf Erstprognosen, Quellhashes, First-seen, Outcome-Prozess | Anbieterhistorie bleibt `degraded`; parallele main-Pusher können Ref-Rennen haben (Betriebsgate in Bearbeitung) |
| private M6 | `ARGUS-Private/research/zones/m6_watch_view.py`, `m6_dashboard.py` | read-only Forschungsbänder; Source-/Frische-Fail-closed | alter archivierter Watch-Bericht enthält noch keinen Forschungsabschnitt; Nachtrag zum aktuellen Zeiger im separaten Betriebsfix |
| private M7 | `ARGUS-Private/research/zones/m7_challenger.py`, `m7_elliott_counts.py` | vorab gefrorene horizontal/Fib/Konfluenz- und plausible EW-Kandidaten | Prospektive 5/20/60-Horizonte noch nicht ausreichend ausgereift, 5 Pilotwerte keine statistische Freigabe |

## Verifizierter Datenpfad – vom Börsentag zum Nutzer

`Provider Yahoo (_download, raw-OHLCV + adj_close)` → **separate Datenskalenprüfung / vollständig abgeschlossene Handelssitzung** → `chronological(rows, asof)` → `prepare_daily_ohlcv(..., price_basis='adjusted')` → `raw-asof-Referenzskalierung` → `detect_confirmed_pivots` (erst ab `confirmed_time`) → `build_zones` Z1/Z2/Z3 → M3-Kandidatencluster und bedingte Szenarien → im privaten Repository `m5_import_from_m3` → versioniertes `m5_shadow`-Forecast + unveränderte Eingangskerzen → `m6_watch_view`/Dashboard + separat `m5_results` und M7-Vergleiche. Kein Eingang aus YouTube-/Newsletter-Kursmarken erforderlich.

Die tatsächliche Tageskerze kann älter sein als der Zeitpunkt ihrer **erstmaligen Publikation**; sie wird niemals dadurch zu einer historischen First-seen-Prognose umgedeutet. Aus vergangener Anbieterhistorie künstlich erzeugte frühere Backtests bleiben ausdrücklich **retrospektive PIT-Proxys**.

## Abhängigkeiten und Freigabegates

| Übergang | Benötigt | Reproduzierbarer Prüfpunkt | Entscheidung |
|---|---|---|---|
| M0 → M1 | fundierter Daten- und Pivotsnachweis | Source-Pfad, Input-Hash, `confirmed_time`, Tests No-lookahead | als Forschungsgrundlage ausreichend |
| M1 → M2 | abgeschlossene OHLCV + eindeutige Preisbasis + Herkunft | `build_zones` liefert JSON mit `last_completed_bar`, `asset_id`, Preisbändern | Forschung ja, Markt-PIT-Qualität `degraded` |
| M2 → M3 | belegte Zonen IDs/Anker, keine doppelte Konfluenz | Cluster- und Szenario-Referenzen und Status `untriggered` | Forschung ja, keine Statistik |
| M3 → M4 | unabhängiger as-of-Prognoseschnitt | Forecast-Ledger strikt ohne Zukunftskerzen und getrenntes Outcome | historischer Replay ja; Belastbarkeit nur mit OOS & Kosten |
| M4 → M5 | versionierte Regeln + abgeschlossene Börsenkerze | erster tatsächlicher Git-First-seen-Commit mit Originalinput-SHA | Shadow-Forschung ja, erwartete Zukunftsoutcomes fehlen naturgemäß |
| M5 → M6 | vollständiger Quellhash, Frische-/Anker-/Währungsidentität | fail-closed View und explizites `research_only` | private Ansicht ja, keine Decision-Wirkung |
| M5/M6 → M7 | genügend unabhängige spätere Beobachtungen | Nullkontrollen, Walk-forward Holdout, Bootstrap, Kostenannahme | **keine** evidenzbasierte Promotion |

## Präzise verbleibende Arbeiten (keine Wiederholung von M0)

1. M1: unabhängige Kapitalmaßnahmen-Receipts/PIT-Historie sowie Split-/Dividenden-Zeitpunkte dokumentieren; bestehende kausale Tests ergänzen. Mit Yahoo-Proxy nicht als erledigt deklarierbar.
2. M2: den isolierten direkten Aufruf für **ein explizit identifiziertes Wertpapier** mit realem OHLCV-Feed als handhabbaren CLI vervollständigen; zurückgegebene Sample-Kurse mit Ein-/Ausgangsdatenbeleg.
3. M3: Geometrie-Tests fortsetzen; keine künstliche historische Berührung zählen.
4. M4/M5/M7: echtes spätere Outcome, Holdout, Kosten- und Intervallprüfung erst bei reifen Daten; bis dahin Negativfälle erhalten und `empirical_probability=null`.
5. M5/M6-Betriebsfix ist ein getrennter Change im privaten Repository. Produktive Scanner-/Orderentscheidungen hier **nicht** verändern.

**M0-Abnahme:** Implementierungsorte, Datenabhängigkeiten, erkannte reale Lücken und PR-Status geprüft und dokumentiert. Dieses Dokument ist reine Inventur ohne pauschale Produktionsfreigabe.
