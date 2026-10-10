# ARGUS PANOPTES — Verständlichkeit ist eine Anforderung der gesamten Website

Stand: 10.10.2026 · Arbeitsfassung zur gestalterischen Abnahme

Grundlage: ARGUS-Übergabe vom 10.10.2026, verbindlicher Dashboard-Masterplan vom
09.10.2026 und freigegebene ARGUS-Designrichtlinie. Diese Datei ist **nur**
ein UI-/Abnahmeplan und stellt keinerlei neue Research- oder QM-Entscheidung dar.

## Verbindliche Sprachregeln

1. **Zuerst beantworten: Was ist das, was bedeutet es, was ist nicht belegt?**
   Originalcodes, Hashes und Fachbegriffe stehen danach in aufklappbaren
   Nachweisen — niemals anstelle einer Erklärung.
2. **Status und Beweis trennen.** `PASS` bei einer technischen Kontrolle beweist
   keine Profitabilität. `CLOSED` im QM-H-Ledger bedeutet Abschluss des
   dokumentierten Fehlerfalls, nicht empirische Handelsmusterfreigabe.
3. **Kandidaten nicht vermischen.** Im Bereich Beobachtungen sind
   **Aktienkandidaten** gemeint. In L5–L10 sind **Musterkandidaten** gemeint.
   Die Aufnahmeentscheidung erfolgt nicht in ARGUS.
4. **Jede Zahl stammt aus einem benannten Snapshot bzw. Originalregister.**
   Unterschiedliche Datenstände werden explizit angezeigt; keine eigenen
   Scores, Quoten oder impliziten Fortschrittswerte.
5. **Persönliche Notizen bleiben privat.** Anonymisierte öffentliche
   Referenzmarken dürfen weder vertrauliche Quelldaten noch Preisgrenzen verraten.
6. **Ein Leser muss die wesentliche Aussage ohne Anklicken erkennen.**
   Vertiefende Quellen und technische Codes sind sichtbar zugänglich, aber
   nicht Hauptinhalt der ersten Ansicht.
7. Dunkelblau/Gold, die vorhandenen Originalsymbole, ruhige Typografie und
   ein einheitlicher Titelblock bleiben unverändert.

## Fünf verbindliche Bereiche und ihre Leserfrage

| Bereich | Leserfrage | Abnahme |
|---|---|---|
| Start | Was hat sich gegenüber einem wirklich gespeicherten früheren Stand verändert? | Herkunft und Frische transparent; kein erfundener Vergleich |
| Scanner | Was wird hier gemessen und worin unterscheiden sich Score, Timing, Risiko und Confidence? | Legende direkt am Wert; keine Interpretation als Wahrscheinlichkeit |
| Beobachtungen | Warum beobachte ich einen Titel, und gehört er schon zum Scanner? | Aktienideen vs. Forschung getrennt; frühere Quelle/Status nicht als aktuelle Freigabe |
| Forschungsstand | Welche Muster liegen vor, was wird noch geprüft und was bedeutet ein Test? | Phasen und Originaldaten getrennt; fehlende Evidenz nicht ersetzt |
| Über ARGUS | Was kann die Seite und was ausdrücklich nicht? | Darstellungsfunktion / produktive Scannerautorität eindeutig |

## Umsetzungsetappen

**Etappe 1 – vorbereitet in PR #294**

- Verständlicher L14-Einstieg, übersetzte Forschungsstationen und Originalcode-Details.
- QM-H/QM-J: Bearbeitungsstand in Alltagssprache.
- Kandidatenradar und Elliott-Ausgaben: Bedeutungen, Beispiele, Grenzen.
- Seitenübergreifendes Lexikon »Begriffe verstehen«.
- Keine Änderung an Scanner, Research, Entscheidung und QM-Ausführung.

**Etappe 2 – nach visueller Abnahme von Etappe 1**

- Startseite: Verständlichkeit aller Kennzahlkarten, Änderungen und Aktualisierungen.
- Scanner: Legenden für Score, Risk, Confidence, Timing, Kurs/Originalwährung.
- Eigene Beobachtungen: Gespeicherte Ausgangslage versus aktueller Snapshot.
- Über ARGUS: klarer Überblick ohne Architekturjargon.
- Konsistenz der Begriffe in Navigation, Tabellen, Detailansichten und Lade-/Fehlerzuständen.

**Etappe 3 – gestalterische Qualitätskontrolle**

- Breiter Desktop, 1366 px, 1092 px und ca. 390 px Smartphone.
- Tastaturbedienung, Screenreader-Beschriftungen, Kontrast und Zoom.
- Kein Overflow, keine gequetschten Tabellen, keine verdeckten Schaltflächen.
- Navigation, Filter, Suche, CSV-Export, Wertpapierdetails, privater Import,
  lokale Speicherung und Datumsabgleich bleiben erhalten.
- Fehlende Daten, Offline-Zustand und widersprüchliche Snapshot-IDs
  dürfen nicht als gültige aktuelle Ergebnisse dargestellt werden.

## Freigabegrenze

PR #294 verändert zunächst nur einen Entwicklungszweig.
Ein grüner Syntax-/Vertragscheck ersetzt keine Sichtprüfung im echten Browser.
Die bestehende produktive Scanner-Seite darf erst nach ausdrücklicher Freigabe
ersetzt oder umgeleitet werden.
