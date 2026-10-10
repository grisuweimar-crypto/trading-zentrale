# CY02-B01 — VALE ADR/B3-Identitätskorrektur, 10.10.2026

**PR:** eigener Quellen-/Identitäts-Patch für [#269](https://github.com/grisuweimar-crypto/trading-zentrale/issues/269). **Kein** rückwirkender Kursimport und keine historische Umschreibung.

## Unabhängig belegter Sachverhalt

Der Emittent [Vale – Börsenplätze/ISINs](https://www.vale.com/de/check-out-our-company) und [J.P. Morgan – Verwahrstellen-ADR-Profil](https://www.adr.com/drprofile/91912E105) dokumentieren zwei **verschiedene** Handelswertpapiere für denselben Emittenten:

| Primäridentität | B3 Brasilien | NYSE US-ADR |
|---|---|---|
| YahooSymbol | `VALE3.SA` | `VALE` |
| Handelswährung | BRL | USD |
| ISIN | `BRVALEACNOR0` | `US91912E1055` |
| Originale Einheit | brasilianische Stammaktie | USD-notiertes 1:1 ADR |
| Quellen | [Yahoo Brasilien](https://finance.yahoo.com/quote/VALE3.SA/) | [Yahoo NYSE](https://finance.yahoo.com/quote/VALE/) |

Das bisherige Master-Universum enthielt zwei aktive `VALE`-Zeilen mit identischer brasilianischer ISIN, jedoch widersprüchlicher Währung (BRL/USD). Yahoo-Download und Cycle-Quality markierten beide mit `CONFLICTING_DECLARED_CURRENCIES` zu Recht als unbrauchbar. Eine implizite FX-Konversion hätte zusätzlich eine falsche Kursidentität geschaffen.

**Geändert:** Nur `data/inputs/universe_master.csv`: die BRL-Zeile wird zu `VALE3.SA` (brasilianische ISIN bleibt), die USD-Zeile `VALE` erhält die korrekte US-ADR-ISIN. Alle sonstigen Stammdaten, Preise, Scores, History und Decision/Portfolio-Artefakte bleiben unberührt.

**Universe-Effekt:** Zwei bisher auf eine `asset_id` deduplizierte Master-Zeilen bilden jetzt zwei eindeutige Handelsidentitäten. Die Soll-Coverage steigt gezielt von **215 auf 216**; dies ist **kein** heimlicher zusätzlicher Kurs oder eine Verdopplung derselben Börsennotierung. Das deduplizierte Universe und die bestehende Eingabeaktualisierung müssen im CI zusammen geprüft werden. Rückwirkende Identitätspaarungen/`cycle-delta` über beide Listings sind weiterhin unzulässig.

**Nicht behauptet:** Der 1:1-ADR-Bezug macht BRL- und USD-Renditen nicht gleich. Provider-Session-As-of-Zeitpunkte, eigene FX-Effekte und Yahoo-Adjusted-Bar-PIT bleiben extern nicht signiert. Für ältere Scanner-Snapshots findet kein `VALE`-Rewrite statt. Die vorherige historische Quarantäne bleibt bestehen; ein neuer, erfolgreicher Scanner-/60-Bar-Replay-Lauf ist die notwendige Betriebsprüfung.

**Status:** Als getrennter technischer Kandidat unter CI. Issue #269 bleibt für Feiertagskalender, Provider-Ausfälle und historisches Close-Publikationsgate offen; CY-03 ohne empirische Freigabe.
