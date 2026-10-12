# ARGUS Execution Quality / Handelskostenforschung

Dieses Forschungsmodul ergänzt private Depottransaktionen um **zwei unterschiedliche Dokumentklassen**, ohne eine historische Geld-/Briefquote zu erfinden.

- `ex_ante`: vor Ordererteilung generierte Kostensimulation. Quelle, Dokumentzeit, Bid/Ask-Indikation, ggf. Broker-Spread-Schätzung sowie Kosten für ein **ausdrücklich genanntes standardisiertes** Ordervolumen.
- `execution_statement`: tatsächlich abgerechneter Kurs, Stückzahl, Ausführungszeit und Kontobelastung. Ohne Bid/Ask zum tatsächlichen Ausführungszeitpunkt ist ein realisierter Spread **unbekannt**.
- Ex-ante-Prozentsatz und aus Bid/Ask zum Mittelkurs berechneter Spread müssen nicht exakt übereinstimmen (Rundung/Zeitpunkt/Methodik). Dokumentierte und berechnete Werte separat halten.
- Beim 12.10.2026er TTD-Beispiel: Geld/Brief der Orderansicht 10,882/10,934 EUR, ungefähr 0,477 % relativ zum Mittelkurs. PDF rechnet als Szenario mit 300 EUR Ordervolumen: Spread 1,43 EUR, Einstiegskosten 2,43 EUR, simulierte Gesamtkosten 3,43 EUR. **Diese 300-EUR-Kosten nicht einer realen Ein-Stück-Order zurechnen.**
- Die TTD-Kaufabrechnung vom 11.08.2026 zeigt neun Stück zu 11,748 EUR, Cashbelastung 106,73 EUR, GETTEX, 16:07:52,556 MESZ, aber keine dazu synchrone Bid-/Ask-Quote.

## Forschungsregeln

1. Dokumente privat behalten; nur ein `sha256`-Dokumentfingerabdruck wird in einem privaten Evidence-Paket referenziert. Kein Upload von PDFs oder Depot-Identifikatoren in öffentliche GitHub Actions.
2. Kein vermuteter Spread aus Kurswert plus Gebühren; keine doppelte Kostenanrechnung.
3. Prospektive Aussagekraft nur bei zeitgerecht archivierter Quote; ansonsten retrospektiv oder unbekannt.
4. Noch **kein automatischer PDF-Parser**, kein gettex-Historienfeed und keine autonome Live-Brokerabfrage. Diese Bausteine erfordern eine eigene private Integration und gesonderte Tests.
5. Execution-Quality-Metriken bleiben von Selection, Timing, Probability, Risk, Confidence und Universal Stance getrennt. Kein freigegebenes Kaufsignal.

**Beispiel der Schemabenutzung:** `build_evidence(isin=..., document_kind="ex_ante", document_time=..., bid=..., ask=..., standard_order_eur=..., source_sha256=...)`. Aktuell werden Daten manuell geprüft eingegeben.
