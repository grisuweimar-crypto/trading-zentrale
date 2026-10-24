# ARGUS M4.1 – abgeschlossene Tageskerzen, Zonentiefen und zeitverzögerte Kursreaktionen

**Stand: 23.10.2026 · isolierte Forschungsimplementierung · Draft-PR #338 · keine Integration in Watch/Decision/Orders.**

## Ziel und Schnittstellen

Das alte M4-Ledger simuliert Trades mit strenger Bestätigung direkt am Tag nach Kontakt. Es beantwortet **nicht** die Frage, ob eine Fibonacci-/Swing-Unterstützungszone als **mehrtägiger Kursbereich** später reagiert. M4.1 ist eine unabhängige **Zone-only**-Messung ohne Trade, Stop, Gebühren oder manuelle Optimierung.

- `src/scanner/research/zones_m41.py`: eingefrorene Zonen, geometrisch ähnliche Negativkontrollen, 60-Session-Kontakte, Episoden/Tiefe/Reaktionsfenster, transparente Gruppierung.
- `scripts/replay_argus_zones_m41.py`: 60-Sessions-fest-gegrideter historischer Replay, `read_rows` und `build_zones` aus bestehendem M1/M2/M3. Historische as-of-Modelle sehen ausschließlich den Schnitt bis Stichtags-Close.
- `scripts/m41_refetch_now_readonly.py`: neu bezogene historische NOW-Kerzen mit `adj_close` **nur in zusätzlichem temporärem Forschungsartefakt**, kein Überschreiben des alten OHLCV-Archivs.
- `tests/test_argus_zones_m41.py`: 14 zusätzliche adversariale Tests (Gesamtsuite M1–M4.1: 47 Tests).

## Preis-/Ereignisvertrag

- Festlegung der Prognose auf **vorab abgeschlossene** Tages- und bestätigte Wochenkerzen; `known_at <= asof`, eindeutiges Symbol, Währung, Zeitrahmen, Rolle, `[lower,upper]` und `source_snapshot_hash`.
- Bänder werden als Intervalle bewertet. Ein abgeschlossener Daily-Bar berührt die Zone, wenn `low <= upper AND high >= lower`. Bestätigung von Support-/Widerstandsreaktionen und strukturellen Brüchen erfolgt ausschließlich via **vollständigem Tages-Close** außerhalb des Bands.
- Für Support von oben: `max_penetration = clip((upper-low)/(upper-lower),0,1)`; für Widerstand von unten spiegelbildlich `clip((high-lower)/(upper-lower),0,1)`. Gesonderte erste ununterbrochene Kontaktepisode und maximale Eindringtiefe während gesamtem Nachlauf. Ein Ausstieg am zweiten Tag ohne Bestätigung ist **kein** unmittelbarer Zonenfehlschlag.
- `first_touch`, ursprüngliche Einfallsseite, Starttage wiederholter Berührungen, abgeschlossene Schlusskurse innerhalb der Zone, längster ununterbrochener Innenaufenthalt, Bruch per Close, Reclaim/Rebound auch nach mehreren Tagen, Docht-/Low-High-Information (keine erfundene intraday Reihenfolge).
- Reaktionsfenster **1/3/5/10/20/40/60 Handelstage nach der ersten Berührung**, Touch-Erreichbarkeit 20/40/60 Sitzungen nach Prognose. Alle nicht verfügbaren Horizonte ausdrücklich `right_censored`.
- Der `first_decisive_exit_after_touch` ist der erste vollständige Schlusskurs jenseits einer Seite **nach** dem Zonenkontakt, außer einem schon am Touchtag abgeschlossenen Bruch; ein vermeintlich günstiger Abschluss **am selben Berührungstag** ist kein *späterer* Rebound. Break-then-reclaim dokumentiert mögliche Mehrstufigkeit.
- **Nur Support/Resistance** darf eine directional Reaktionsaussage erhalten; `target` und `invalidation` zählen als `level_reached`, nicht als ausgedachte Support-/Resistance-Signale.
- `raw_daily`, `raw_weekly`, `cluster` sind ausdrücklich überlappende *Darstellungen derselben* Kursstruktur; keine unabhängigen 3 Stichproben.

## Negativkontrolle / Design vor Sichtung zukünftiger Tageskerzen

Für noch nicht gebrochene, zum As-of von erwarteter Seite kommende Support- und Resistance-Bänder wird eine Kontrollzone mit identischer **Breite**, Rolle, Seite, Zeitraum und deterministisch um Faktor **0,70/0,85/1,15/1,30** abgewandeltem Preisabstand zur abgeschlossenen As-of-Kerze eingefroren. Multiplikator-Auswahl basiert auf SHA256 der Quellforecast-ID; Outcomes werden erst danach gelesen. Kein Neufit auf vergangene Erfolgsfälle.

Diese Kontrolle ist **eine einfache Näherung**, kein vollständiges Volatilitäts-/Trend-/Pivot-Alter-Matching und kann mit anderen realen Bands überlappen. `sample_independent=false`, `calibrated_probability=null`; keine Testsignifikanz oder Profitabilität behaupten.

## Befehle

```sh
PYTHONPATH=src:. python -m pytest -q tests/test_argus_zones_v01.py tests/test_argus_zones_m3.py tests/test_argus_zones_m4.py tests/test_argus_zones_m41.py

PYTHONPATH=src:. python scripts/replay_argus_zones_m41.py \
  --input /tmp/argus-zones-provider/m41_complete_source_ohlcv.csv \
  --symbols AMD INTC ARM SMH NOW --cutoff 2026-10-21 \
  --first-index 150 --stride 60 --max-snapshots 12 \
  --output-dir /tmp/argus-zones-provider/m41
```

Ausgabedateien: `m41_frozen_zone_forecasts.jsonl`, `m41_observed_zone_outcomes.jsonl`, `m41_asof_coverage.jsonl`, `m41_summary.json`; SHA256 dokumentiert Eingang, Frozen- und Outcome-Dateien. Dateien kommen nur in isolierte Workflow-Artefakte, nie in produktive Scanner-Outputs.

## Tatsächliche Abnahme und Grenzen

- **CI-Test: [39963987457](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/39963987457) – SUCCESS, 47 passed.**
- **Fünf-Werte-History: [39963987361](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/39963987361) – SUCCESS, 35 as-of-Snapshots und 6.781 Prognose-/Outcome-Paare.**
- 1.735 eingefrorene Kontrollpaarungen, 973 mit beiderseitigem Kontakt. Erwarteter erster Schlusskurs-Ausgang: **596 echte Zonen**, **599 Kontrollzonen**. Daten: AMD/INTC/ARM/SMH/NOW jeweils 599 später abgefragte Kerzen, **keine** blockierten as-of-Snapshots.
- Kein allgemeiner empirischer Gewinn erkennbar. **Kein** authentisches historisches Markt-PIT, keine unabhängige Survivor-Free-Auswahl, keine vollständige Elliott-Wave-Zählung, keine robusten Signifikanzintervalle. Nachträgliche Parameteränderungen sind separate Hypothesen; M5-Prospektivarchiv wird eigenständig fortgesetzt.
- Forschungsbericht mit Datenhashes liegt privat in `ARGUS-Private/research/zones/M4_1_ERGEBNISSE_2026-10-23.md`.

**Abnahme:** M4.1 als reproduzierbare technische Forschungsstufe abgeschlossen; Schlussfolgerung *nicht* „Fibonacci gescheitert“, sondern „diese konkrete automatisch gesampelte Fib-/Swing-Zonenkonstruktion zeigte in diesem nicht unabhängigen historischen Proxy-Kontrollvergleich keinen klaren allgemeinen Vorteil“.
