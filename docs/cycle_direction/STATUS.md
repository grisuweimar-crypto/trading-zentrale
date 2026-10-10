# CYCLE-DIR – aktuelles Statusregister

Plan-ID: `CYCLE-DIR-2026-10-09-v1` · Datum: **10.10.2026, Europe/Berlin**

Verbindliche Vorgabe: vom Nutzer bereitgestellter CYCLE-DIR-Masterplan vom 09.10.2026. Diese Datei ist nur das **Statusregister**, keine stillschweigende Änderung der Phasen- und Qualitätsregeln.

| Paket | Status | Nachweis / Grenze |
|---|---|---|
| CY-00 | FERTIG_FACHLICH (Inventur) | [PR #259](https://github.com/grisuweimar-crypto/trading-zentrale/pull/259), [Bestandsaufnahme](CY-00_BESTANDSAUFNAHME.md); historische Herkunft teils UNVERIFIZIERT |
| CY-01 | FERTIG_TECHNISCH (gemergt) | [PR #260](https://github.com/grisuweimar-crypto/trading-zentrale/pull/260), [Qualitätsbericht](CY-01_DATENQUALITAET.md); Frische alter Quellenwerte und globale QM Issue #264 offen |
| CY-02 | **FERTIG_TECHNISCH / BETRIEBSNACHWEIS BESTANDEN / NICHT_FREIGEGEBEN (unabhängige Quell-PIT-Herkunft)** | [PR #266](https://github.com/grisuweimar-crypto/trading-zentrale/pull/266), [#268](https://github.com/grisuweimar-crypto/trading-zentrale/pull/268), Stammdaten [#271](https://github.com/grisuweimar-crypto/trading-zentrale/pull/271)/[#272](https://github.com/grisuweimar-crypto/trading-zentrale/pull/272)/[#274](https://github.com/grisuweimar-crypto/trading-zentrale/pull/274); [Produktiver Scanner #38037814673](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/38037814673): **209/215 intern VALID, 5 INSUFFICIENT_HISTORY, 1 STALE**. Quelle/Handelskalender und historische Bar-Verfügbarkeitszeitpunkte weiter [#269](https://github.com/grisuweimar-crypto/trading-zentrale/issues/269) offen. |
| CY-03 | **FERTIG_TECHNISCH_PRODUKTIV (erste Prospektiv-Publikation verifiziert) / NICHT_FERTIG_FACHLICH (Research gesperrt)** | [PR #275](https://github.com/grisuweimar-crypto/trading-zentrale/pull/275) gemergt, [erster echter Autopilot #38040088358](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/38040088358) **SUCCESS**, Snapshot `f409c312-0349-4b29-8dab-3ecdbd9463b3`: 215 archivierte aktuelle Beobachtungen, 209 intern replaybar, 6 ausgeschlossen, 1 echte Quelle/Manifest/Bar-Archiv, `lag_1/5/10=0`, `research_eligible=0`. [Produktive Hash-/Coverage-Abnahme](CY-03_HISTORISIERUNG_UND_EIGNUNG.md#9-erster-vollständiger-produktiver-cy-03-publikationsnachweis--10102026); [#269](https://github.com/grisuweimar-crypto/trading-zentrale/issues/269), Mehrtag-Ketten und unabhängige PIT-Herkunft weiter offen. |
| CY-04 | **BLOCKIERT / NICHT_FREIGEGEBEN (Bestandsaufnahme 10.10.2026)** | [CY-04 Istaufnahme + Stop-Gate](CY-04_ISTAUFNAHME_UND_STOP_GATE.md). 1 echter CY-03 Snapshot, 0/0/0 vergleichbare Lags, `research_eligible=0`; #269 und CY-03 Fachgate offen; keine UI-/Score-/Decision-Aenderung. |
| CY-05 | GEPLANT | setzt CY-03 voraus; L1-Präregistrierung vor Outcome-Lauf |
| CY-06 | GEPLANT | setzt CY-05 voraus |
| CY-07 | GEPLANT | setzt CY-06 und Forschungs-Gates voraus |
| CY-08 | GEPLANT | setzt CY-07 und L12/Reviewer-Freigabe voraus |

**Kein stiller Übergang:** CY-03 darf prospektive Rohbeobachtungen unter Quarantäne aufzeichnen, solange #269 offen ist, aber keinerlei daraus abgeleitete Research-, Decision- oder Handelssignale freigeben. Eine echte CY-03 Abnahme benötigt wenigstens mehrere zukünftig veröffentlichte Snapshots mit reproduzierbaren Lags, Coverage und nachgewiesener alter Unverändertheit; CY-04/CY-05 werden nicht stillschweigend aktiviert. Die offene Cross-QM [Issue #264](https://github.com/grisuweimar-crypto/trading-zentrale/issues/264) bleibt unabhängig nachzuverfolgen.

**Unverändert:** produktive Scores/Decision/Portfolio-Aktionen, alte Scanner-Snapshots, ursprüngliche Cycle-Imputationsmasken, Elliott-Logik. CY-02 ist keine empirische Handelsvalidierung.

**CY-04-Vorpruefung (10.10.2026):** Nur read-only Herkunfts-/Lag-/UI-Inventur und Dokumentation; ausdruecklich kein Feature-Abschluss. Wiederaufnahme erst nach fachlicher CY-03-Freigabe mit unabhaengiger PIT-Quellenverifikation (#269), echten 1/5/10obs-Ketten und separatem CY-04-Tests/PR.
