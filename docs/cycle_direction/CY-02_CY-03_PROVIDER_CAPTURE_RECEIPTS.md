# CY-02/CY-03 — Beleg der tatsächlichen zukünftigen Yahoo-Abfragezeit (Client-Receipt v1)

Plan: CYCLE-DIR-2026-10-09-v1 · 10.10.2026 · Scope **Prospektive Quell-Provenance**, kein neues Signal.

## 1. Welche offene Lücke geschlossen wird

Issue [#269](https://github.com/grisuweimar-crypto/trading-zentrale/issues/269) verlangt unabhängige Kursquellen-Herkunft und Bar-Verfügbarkeit *zum Scanner-Zeitpunkt*. Bisher lag nur ein Hash auf einem Yahoo-Datenframe und die getrennten 60 adjustierten Kursbars vor. Der `cycle_as_of`-Wert bezeichnete einen Scanner-Zeitanker, **nicht den tatsächlich abgeschlossenen Provider-Abruf**, und die älteren Datensätze besitzen keinen belegbaren Download-Abschlusszeitpunkt.

**Neu wird deshalb direkt um den wirklichen `yfinance.download`-Aufruf herum** der Client-UTC-Zeitpunkt vor dem Aufruf und nach dessen Rückkehr erfasst. Die deterministischen 60-Bar-`cycle_input_bars.csv.gz` werden wie bisher gespeichert und mit dem Frame-Fingerprint (`provider_frame_sha256`) in einem unverändert konservativen *Client-Receipt* verbunden.

## 2. Neue, auditierbare Belege

| Ort | Inhalt / Aussage |
| --- | --- |
| `artifacts/reports/cycle_provider_fetch_receipt_v1.json` | Nur zum aktuellen tatsächlichen Provider-Abruf (nicht wiederverwendet bei Offline/Fehler). `started_at_utc`, `finished_at_utc`, Hash des yfinance-Frames, Frame-Zeilenanzahl und SHA-256 der gzip-60-Bar-Datei; rekursiv kanonischer `receipt_sha256` |
| `artifacts/cycle_history/provider_receipts/{snapshot_id}.json` | Nach vollständigem CY-03-Archiveintrag an die reale `snapshot_id`, `run_id`, den archivierten 60-Bar-SHA und bereits publizierten `history_metadata.generated_at` gebunden; byteidentische Wiederholung ist zulässig, geänderte Daten für dieselbe ID verboten |
| `scripts/record_cycle_provider_receipt.py` | Erfolgt nach `scripts/record_cycle_history.py` im Scanner-Autopilot; prüft aktuelle Snapshot-Herkunft und Zeitreihenfolge. Bei validen Kurszyklen ohne zugehörigen Receipt scheitert der Publikationslauf **fail-closed**. |
| `scripts/watch_cycle_science.py` | Prüft die neuen archivierten Receipts gegen den tatsächlichen unveränderten Ledger, die archivierten Kursbarbytes, den damaligen Scan-Publikationszeitpunkt sowie die signierten `receipt_sha256`-Felder. Fehlende Belege älterer Snapshots bleiben als fehlend sichtbar. |

Die Abnahme wird über `.github/workflows/cycle_provider_receipt.yml` inklusive gesonderter Negativtests ausgeführt.

## 3. Harte wissenschaftliche Grenzen

- `CLIENT_RUNNER_UTC_UNATTESTED` ist **nur die UTC-Uhr des GitHub-Läufers**, kein vom Kursanbieter bestätigter Server-Zeitstempel und kein verlässliches Attest zur Publikationszeit einer einzelnen Yahoo-Session-Bar.
- Der Receipt **bescheinigt weder** die Originalwährung, Yahoo-Provider-Quote-Einheit (GBp/ZAc), unabhängige Handelskalender-/Venueidentität noch die historische As-of-Verfügbarkeit eines bestimmten Bar-Schlusskurses. Yahoo-Frames können später revidierte adjustierte Close-Werte enthalten.
- Die `provider_frame_sha256` kennzeichnet das lokal gesehene ganze Datenframe. Der Originalframe ist dadurch allein **nicht wiederherstellbar**; die 60 tatsächlich für den Zyklus verwendeten Kursbars werden gesondert archiviert. Dies beweist, welche Daten verarbeitet wurden, nicht, wann der Anbieter sie ursprünglich veröffentlichte.
- **Keine** rückwirkende Receipt-Erstellung für den Anfangs-Snapshot vom 10.10.2026 oder andere alte Beobachtungen ohne damals wirklich erfassten Zeitstempel.
- `research_eligible=0` und `BLOCKED_EXTERNAL_VERIFICATION_ISSUE_269` bleiben für alle CY-03-v1-Beobachtungen unverändert. **Issue #269 und [#290](https://github.com/grisuweimar-crypto/trading-zentrale/issues/290) bleiben offen**, bis ein unabhängiger Provider-PIT-/Listing-/Bar-Zeit-Beleg plus gesonderter wissenschaftlicher CY-03-v2-Release vorliegt.
- Weder Score, Timing, Selection, Decision Layer, Portfolio Action, Order/Execution noch tatsächliche Prospektiv-Research-Auswertung werden durch diese Beweisaufnahme verändert.

## 4. Was wissenschaftlich weiterhin nachgearbeitet werden muss

1. Für jede relevante Quellnotierung unabhängige Referenz zu Börse, *Quote-Währung und Einheit*, Listing-Lebenszyklus und korrekter Börsen-/Crypto-Session beschaffen, Sonderfälle aus #269 separat schließen.
2. **Für echte Bar-PIT:** unabhängige, zum Downloadzeitpunkt verfügbare Provider-/Börsen-Metadaten sowie gegebenenfalls deren dokumentierte Aktualisierungszeiten/Witness-Hashes pro Asset/Bar sichern. Lokale Empfangszeiten sind nur ein Teil dieses Beweises.
3. Ein **neues, versioniertes CY-03-v2-Freigabeschema** ausschließlich für extern belegte Snapshot×Asset×Lag-Ketten mit unabhängiger Prüfung, Ausschlüssen und Research-only-Adapter entwickeln. Keine Manipulation der existierenden Ledger-Bytes oder automatische Freigabe durch wachsendes N.
4. Vor einem tatsächlichen L3-Discovery-Lauf echten CY-05-`FROZEN_PRE_RUN` mit Cutoff/holdout/Evidenzhashes und QM-C-Plan erstellen. Spätere CY-07-Claims dürfen erst nach Pattern- und Prüfplan-Freeze entstehen.

**Status dieses Pakets:** technische Zukunfts-Receipt-Implementierung; für die tatsächliche erste neue Live-Aufzeichnung nach Merge ist ein erfolgreicher zukünftiger Scannerlauf und sein veröffentlichtes Receipt zu kontrollieren.
