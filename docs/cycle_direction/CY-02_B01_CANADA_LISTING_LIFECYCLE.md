# CY02-B01 – Kanada-Lifecycle: DV.V Delisting / SLVR.V TSX Graduation

**Datum:** 10.10.2026 · **Issue:** [#269](https://github.com/grisuweimar-crypto/trading-zentrale/issues/269) · **Scope:** nur aktive Masteridentitäten, keine alte History oder rückwirkenden Preise.

## Dolly Varden Silver `DV.V` – endgültiges Delisting

Primärquellen [Emittent Pressemitteilung, 26.03.2026](https://dollyvardensilver.com/contango-completes-merger-with-dolly-varden/) sowie [SEC Material Change Report](https://www.sec.gov/Archives/edgar/data/1542028/000106299326001692/exhibit99-1.htm): Contango übernahm Dolly Varden am **26.03.2026**. Die Aktie wurde mit Schluss 27.03.2026 von TSXV genommen. Verhältnis für die Umtauschemission `0.1652` Contango-Aktien pro Dolly-Varden-Aktie; dies ist eine Unternehmensmaßnahme, **keine** einfache Umbenennung von `DV.V` nach `CTGO` oder direkte Kursfortsetzung.

**Änderung:** Bestehende Masterzeile `DV.V / CA2568277834 / CAD` bleibt als inaktive Referenz (`active=0`) erhalten; keine neue Contango-Position angelegt, keine alten historischen Scanner-/Kurswerte gelöscht oder auf einen Nachfolger umgeschrieben. `STALE/PROVIDER_UNAVAILABLE` war daher als Fail-Closed sicher, die laufende aktive Beobachtung war aber fachlich nicht länger haltbar.

## Silver Tiger Metals `SLVR.V` – gleiche Aktie an neuem Börsenplatz

[Offizieller Börsen-Listing-Bulletin TSX vom 21.05.2026](https://www.tsx.com/en/news/new-company-listings?id=2402) und [Emittent vom 19.05.2026](https://silvertigermetals.com/news/2026/silver-tiger-metals-announces-graduation-to-the-toronto-stock-exchange/): Die **identischen** Aktien wanderten von TSX Venture Exchange zum Hauptmarkt TSX (Ende Venture 20.05., Start Toronto 21.05.2026). Emittent bestätigt **kein** Handlungsbedarf für bestehende Aktionäre. Ticker an der Börse bleibt `SLVR`, Yahoo-Marktplatzsuffix wird `.V` → `.TO`, CAD bleibt unverändert; ISIN `CA82831T1093` bleibt. [Yahoo SLVR.TO](https://ca.finance.yahoo.com/quote/SLVR.TO/history/) zeigt historische echte CAD-Preise.

**Änderung:** Originale aktive `SLVR.V` Masterzeile wird `SLVR.TO` (identische ISIN/Originalwährung, keine rückwirkende ID-Synchronisation früherer Scanner). **Eine** Wertpapieridentität, keine neue Aktie. CY-03 muss den geänderten Yahoo-/Listing-Symbol-Nachweis für die technische Lag-Eignung als Versions-/Listingwechsel behandeln; keine stillen Vorher-Nachher-Deltas.

## Gesamtwirkung / Verifikation

Gegen die zuvor korrigierten zwei Vale-Listings beträgt der kanonische erwartete Universe-Count **215** = nach Vale 216 minus deaktiviertes `DV.V` (Silver Tiger venuewechsel hält Anzahl konstant). Dies ist keine automatische Erfolgs-/Cycle-Coverage-Zusage. Testbare Anforderungen:
- Nur eine Masterzeile `DV.V` und **inaktiv**, ISIN im historischen Masterkontext erhalten.
- Genau eine aktive `SLVR.TO`, alte `SLVR.V` **nicht** im Active Master, Original-CAD und ISIN unverändert.
- Neue Master-Sync-Ausgabe enthält weder DV.V noch SLVR.V als aktives Listing, Yahoo-Feld `SLVR.TO` explizit.
- Canonical Universe 215 nach der gesamten Identitätskorrektur, ohne historische Rewrite.
- Nächster Autopilot nach Merge vollständig erfolgreich, alle 60-Bar-/Frische- und Research-/UI-Prüfungen grün; tatsächlichen `VALID` Count erst dann dokumentieren.

**Fachliche Grenze:** Weder ein Unternehmenskauf noch ein Börsenplatzwechsel beweist frühere Yahoo-Adjusted-Kursverfügbarkeit zu den damaligen Scannerzeiten. #269 bleibt wegen JPX/KRX-, unabhängiger PIT-/Currency-Provider- und Quellenprüfung offen. Keine Trading- oder Research-Promotion.
