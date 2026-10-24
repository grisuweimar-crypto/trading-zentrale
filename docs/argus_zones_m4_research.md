# ARGUS M4 – Kausaler historischer Eignungsprüfstand, Forschungsfassung v0.1

**Stand:** 23.10.2026. **Status:** isolierter technischer Replay-Prototyp. **Keine** Freigabe, Erfolgsquote oder historische Point-in-Time-Güte behaupten.  
Implementierung: `src/scanner/research/zones_m4.py`, `scripts/replay_argus_zones_m4.py`; [Draft PR #338](https://github.com/grisuweimar-crypto/trading-zentrale/pull/338).

## Vorab festgelegte Forschungsregel / keine nachträgliche Optimierung

| Element | Eingefrorene Ausprägung |
|---|---|
| Beobachtungsfenster | 20 vollständige spätere tägliche Börsensitzungen ab `asof` |
| Stichtagsstichprobe | Session-Index 150, danach jeder 20. beobachtete Schluss, maximal 30 Stichtage/Symbol; nicht nach Rendite ausgewählt |
| Eingabe | ausschließlich M3-`scenario_plans` mit `state=untriggered`, bekannten Clusterankern und `data_quality=degraded\|ok` |
| Zonenkontakt | Nächste, **nach** `asof` abgeschlossene Tageskerze berührt Entry-Band |
| Bestätigung | **unmittelbar folgende** Tageskerze schließt oberhalb Entry-Oberkante (bullisch) bzw. unterhalb Unterkante (bärisch) |
| Eintrittsannahme | erst **übernächster** Börsentag nach Kontakt: Open nach Bestätigung; Stop/Target auf Bestätigungskerze wirken ausdrücklich nicht rückwirkend |
| Eintritts-Risikogate | initiales Abstand-/Preisrisiko höchstens 12 %; modelliertes R:R vor Kosten mindestens 1,0; Ziel und Stop müssen auf richtiger Seite des Open liegen |
| Ziel und Stop | nächstgelegener im M3-Plan dokumentierter Zielcluster; Long-Ziel an Ziel-Unterkante / Short-Ziel an Ziel-Oberkante; Invalidierung Long-Unterkante / Short-Oberkante |
| Tageskerze berührt Stop und Ziel | **Stop zuerst** (konservative Reihenfolge, da Intraday-Verlauf unbekannt) |
| Open über Stoplücke | schlechterer Preis aus Open oder Stop; bei günstigem Kurszielgap maximal Ziel-Limit, kein erfundener Zusatzgewinn |
| Ablauf | 20. Sitzung nach Stichtag: Schlusskurs, wenn Position noch offen. Fehlen vollständige spätere Kerzen, Ausgang `right_censored` |
| Kosten | **Annahme** 15 Basispunkte je Seite, zusammen 30 bp; keine brokerkonkrete Gebühren-, Steuer-, FX- oder Borrow-Modellierung |
| Vergleich | ein einfacher Long-/Short-Hold vom Open der ersten Folgesitzung bis Close der 20. Sitzung je **as-of-Snapshot**, unabhängig davon, ob M3 einen Plan erzeugt; NICHT risikomatched und NICHT als Alpha-Test |
| Ergebnis | Prognose-/Outcome-Ledger nach fixem Stichtag, alle Negativfälle einschließlich `unreached` und `touch_not_confirmed`, getrennte Risikogate-Gründe, keine nachträgliche Musterwahl |

**Methodischer Warnhinweis:** Die historischen Kurse stammen aus einer *später* abgefragten Yahoo-/adj-close-Historie. Deren kapitalmaßnahmenbereinigte OHLC-Proxyrekonstruktion und damaliger Symbolbestand sind nicht als unabhängig archivierte historische PIT-Snapshots belegt. Der Replay lässt die M3-Modellberechnung zwar keine späteren Preiszeilen sehen, beweist aber **keine echte historische Marktdatenverfügbarkeit, keine unverfälschte PIT-Handelbarkeit und kein Survivorship-freies Ergebnis**.

## Tracebarkeit und Ergebnisstruktur

`scripts/replay_argus_zones_m4.py` schreibt ausschließlich in `--output-dir`:
- `forecast_ledger.jsonl`: eingangs eingefrorene M3-Szenarien, referenzierte Entry-/Stop-/Zielcluster, Identität des historischen Slice (`snapshot_hash`), Inputdatei-Hash, `forecast_sha256`, Methode/Parameter; **ohne** spätere Outcomes;
- `outcome_ledger.jsonl`: `event_id`, `forecast_sha256`, as-of, Kontakt/Bestätigung/Fill/Exit, Negativfall oder Outcome, Preis-/Kostenannahme, MAE/MFE und `rejection_reason`;
- `asof_coverage.jsonl`: **jeder** ausgewählte as-of-Snapshot, auch ohne Kandidaten oder bei blockierter Datenbasis, samt planunabhängigen 20-Bar-Long/Short-Vergleichsverläufen;
- `m4_summary.json`: SHA256 für beide Ledger, Input-Hash, Counts je Symbol/Ausgang, separate Mittelwerte ausschließlich tatsächlich geschlossener Probesimulationen und Baselines.

Die verwendeten Tageshochs/-tiefs für MAE/MFE sind vollständige Bar-Extrema; bei Stop/Target an derselben Bar ist die intraday Reihenfolge unbekannt, daher **nicht** als exakt handelbare Exkursion deklarieren.

## Verbindliche methodische Trennung

1. **Kurszone:** Technikannahme; **nicht** bereits eine prognostische Trefferquote.
2. **Szenario:** bedingte M3-Regel; **nicht** automatisch ein realisierbarer Auftrag.
3. **Replay:** konservative mechanische Simulation; **nicht** historischer Tradebeleg.
4. **Auswertung:** sämtliche Fälle im Nenner dokumentieren, `closed_only`-Mittelwerte niemals als Gesamtmodell-Erwartungswert darstellen.
5. **Keine unabhängigen Trades:** überlappende Signale/Kursanker, Symbole und wiederkehrende Szenarien korrelieren; signifikante Tests und Unsicherheitsintervalle fehlen.
6. **Keine Produktivanpassung:** der Run kann weder Orders senden noch Scanner-Score, Watch oder historische Marktdaten ändern.

## Rückblickender erster Ergebnisbeleg (vor Diagnose-/Benchmark-Erweiterung)

[CI 39956011244](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/39956011244) und [fünf-Symbol-Anbieter-Run 39956011157](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/39956011157) liefen mit Commit `8e077e8d73613a6d682b8dccf71a46d96252b2ac` erfolgreich. Im zum damaligen Zeitpunkt protokollierten Ergebnis: **574** M3-Prognosen, `unreached=282`, `touch_not_confirmed=177`, `risk_rejected=103`, `invalidated_before_entry=5`, `touch_at_horizon_no_confirmation=5`, `stop=2`; keine als Zieltreffer geschlossenen Positionen. Die zwei geschlossenen (extrem dünnen) Simulationen hatten im arithmetischen Mittel `-9.782244 %` modellierte Nettorendite. Das ist keine statistische Prognose über zukünftige Chancen; die Stichprobe ist selektiv und winzig und es gibt keine unverzerrten historischen PIT-Daten. `NOW` wies 16 von 18 ausgewählten Snapshot-Stichtagen als `blocked` aus wegen unvollständiger früherer `adj_close`-Abdeckung.

Die darauffolgende Ergänzung um Ablehnungsgründe und planunabhängige Baselines ändert die vordefinierten Trigger-, Risk- und Exit-Regeln **nicht**. Ergebnisse der finalen Codefassung werden getrennt anhand der GitHub-Run-ID nachgetragen.

## Abnahmekriterien / Forschungsgrenzen

- Reproduzierbare Tests für Stichtag, Future-Slice, keine Lookahead-Trigger, Gap-Stop, pessimistischen Stop bei dualem Kontakt, sofortige Bestätigung nur am Folgetag, Risk-Rejection und Nicht-Einstieg.
- Für alle fünf Symbole ein tatsächlich ausgeführter Run auf isolierter Eingabe mit SHA256-Provenienz, vollständigen Negativfällen und Ergebnis-/Baseline-Dateien.
- Technisch `success` ist **keine empirische Validierung**. Für eine belastbare Prognosegüteaussage weiterhin nötig: unabhängige Corporate-Action-/Exchange-Prüfung, echte historische PIT-Abdeckung, delistete/ausgeschiedene Titel, blockweise OOS-Design, unabhängige Zeit-/Assetkohorten, echte Ausführungskosten sowie gegenprüfbare Baseline und Irrtumsintervalle.
