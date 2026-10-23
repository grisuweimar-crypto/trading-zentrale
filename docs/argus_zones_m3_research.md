# ARGUS M3 – konvergente Preiszonen, bestätigte Wochenkerzen und bedingte Szenarien

Stand: 23.10.2026 · Version: `argus-zones-m3-v0.1-research`. **Forschung, keine Live-Handelsregel.**

## Abgrenzung / Schnittstelle

- Eingabe: M2 `build_zones` aus `src/scanner/research/zones_v01.py`, vollständig abgeschlossene Daily-OHLCV-Sessions; `asof` fix. Bestehende Z1/Z2/Z3-Rohkandidaten bleiben erhalten.
- Primäre Implementierung: `src/scanner/research/zones_m3.py` (`cluster_zones`, `build_scenario_plans`, `enrich_m3`, `write_readable_tables`).
- Wochen: `src/scanner/research/zones_m3_weekly.py` nutzt bestehende `aggregate_weekly`, `detect_confirmed_pivots`, `PivotSpec(primary, weekly, 2, 2, 10, 1.0)`. Eine Wochenbar wird erst beim **ersten tatsächlich beobachteten Sitzungstag der Folge-ISO-Woche** bekannt. Eine noch laufende Woche wird nicht als abgeschlossen rekonstruiert.
- Keine geänderten Scanner-Score-, Decision-, Watch- oder Produktiv-Datenpfade; ausschließlich isolierter PR #338.

## Deterministisches Clustering

1. Bänder nur bei gleicher Rolle (`support|resistance|target|invalidation`), Seite und Zeitrahmen kombinieren.
2. Reale Intervallüberlappung erforderlich. Gesamtband höchstens 8 % des vereinigten Mittelpunktpreises. Sortierung nach Rolle, Seite, Zeitrahmen, Unter-/Oberkante, ID; Ergebnis und IDs von Eingabereihenfolge unabhängig.
3. Original-`zone_id` bleiben als Quellen referenziert, `cluster_id` wird aus kanonisch sortierten Quellen-IDs plus Versionskennung abgeleitet.
4. Pro Gruppe Familien wie horizontal / Fib-Retracement / ABC-Projektion benennen. Zonen mit demselben zugrunde liegenden Pivot bilden **eine abhängige Ankergruppe**, unabhängig von Fib-Level oder Pivot-`degree`. Gemeldet werden `distinct_family_count`, `independent_anchor_groups` (genauer: nach gemeinsamen Ankern getrennte Komponenten) und `dependent_source_count`. Diese Werte belegen **keine statistische Unabhängigkeit**.
5. Daily und Weekly werden **nicht** zu einem angeblich unabhängigen Super-Band gemischt. `cross_timeframe_overlaps` verweist auf geometrisch überlappende Cluster gleicher Rolle/Seite; `independence_claim=false`.
6. Kein Trefferprozentsatz; `empirical_probability=null`, `evidence_status=not_validated`.

## Bedingte M3-Szenarien

- Long: nicht bereits gebrochener Supportcluster; Short-Analyse: nicht bereits gebrochener Widerstandscluster.
- Zugehörige Invalidierung nur jenseits des Einstiegsbereichs und mit mindestens einem geteilten physischen Pivotanker. Ohne passende Invalidierung **kein gültiger Szenarioplan**.
- Ziele: bullisch oberhalb von Einstieg und letztem Close, bärisch darunter. Bis zu drei voneinander getrennte Preisziele; überlappende Targets/Widerstände bzw. Targets/Supports zählen nicht doppelt.
- Trigger **für zukünftige Tageskerzen**: abgeschlossener Bar berührt Einstiegszone; auf einer **späteren** vollständig abgeschlossenen Tageskerze Schlusskurs jenseits Oberkante (bullisch) bzw. Unterkante (bärisch). Frühester Beobachtungsbeginn strikt **nach** `asof`.
- Szenario ist bis dahin `untriggered`. Ein vorheriger historischer Zonenkontakt wird nicht in ein Trade-Ereignis umgewandelt. `horizon_bars=20` ist eingefrorene Forschungsannahme, noch nicht empirisch bewertet.
- Keine Fill-, Spread-, Slippage-, Risiko-/Kostenrechnung: `fill_assumption=null`, `risk_reward_after_costs=null`. Kein Handlungsautomatismus.

## Output-Vertrag

Die CLI `python -m scanner.research.zones_v01 --input ... --symbols NOW --asof YYYY-MM-DD --output-dir ...` erzeugt zusätzlich zur alten JSON:
- `zone_clusters` / `weekly_zones` / `last_confirmed_week` / `cross_timeframe_overlaps` / `scenario_plans` im JSON;
- `*_clusters.csv` und `*_plans.csv`;
- `*_M3.md` mit lesbaren Kursbereichen und Abhängigkeitsangaben.

Fehlende oder inkonsistente Kursbasis: `data_quality=blocked`, leere Arrays, `m3_status=no_valid_setup`. Keine künstlichen Zonen. `degraded` für historische OHLC-Proxyreihe und nicht belegte as-of-Provider-Snapshots unverändert.

## Forschungsgrenzen und nächste Abnahme

- Die Zone ist nicht der Trade; der historische Ereignis-Ledger (touch, Bestätigung, Nichterreichen, Kosten, MAE/MFE) gehört zu M4.
- Die aktuellen ATR-Breiten, Pivotfenster, 8-%-Clusterschranke und der 20-Bar-Horizont sind **vorläufige Parameter**, noch nicht out-of-sample validiert.
- Correlated evidence wird konservativ gruppiert, nicht in eine Wahrscheinlichkeit übersetzt. Fehlende Börsen-/Corporate-Action-Identität bleibt ein Datenqualitätsrisiko.
- Keine automatische Ersetzung bestehender Watch- oder Decision-Signale.

## Tests

`PYTHONPATH=src python -m pytest -q tests/test_argus_zones_v01.py tests/test_argus_zones_m3.py`

Testet deterministische Reihenfolge, harte Bandbreite, Dependenz über Swinggrade, valide Gegenrichtung für Stops/Ziele, keine erzwungenen Setups, Not-validated-Gate, `asof`-Stabilität auch bei angehängter Zukunft, Wochenbar-Bestätigung sowie JSON-/CSV-/Markdown-Ausgabe. CI zusätzlich mit archiviertem NOW-Historien-Smoke. Erfolgsnachweise sind in GitHub Actions zu verifizieren, bevor M3 als fertig markiert wird.
