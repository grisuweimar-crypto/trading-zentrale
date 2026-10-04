# Elliott vNext – Stage 4 Prospective Observation

## Einordnung

Die technische Aktivierung von Elliott vNext endet mit Stage 3.

Stage 4 ist daher **keine weitere Aktivierungsstufe**, sondern die anschließende
prospektive Beobachtungs- und Datensammlungsphase. Sie nutzt ausschließlich die
bereits real erzeugten Stage-1-Captures und verändert weder den eingefrorenen
Elliott-Core noch Decision-Layer-Semantik.

## Ziel

Stage 4 soll aus den im Shadow-Archiv wachsenden echten prospektiven 6H-Zuständen
reproduzierbare Beobachtungsdaten ableiten, ohne bereits einen Nutzen zu
behaupten.

Der erste automatisierte Beobachtungsbaustein ist die bereits in QM-G definierte
**Scenario Stability**.

Verglichen werden nur aufeinanderfolgende tatsächlich verfügbare 6H-Ausgaben für
dieselbe Dimension:

- Symbol
- Timeframe
- Wave Degree

Status:

- `STABLE`
- `CHANGED`
- `INSUFFICIENT_EVIDENCE`

Die Definition wird nicht neu erfunden. Stage 4 verwendet die vorhandene
`qm_g_scenario_stability_v1`-Logik.

## Datenfluss

```text
Scanner Snapshot
  -> Stage 1 Prospective Capture
  -> elliott_vnext_prospective_history_6h.jsonl
  -> Stage 4 Prospective Observation
  -> elliott_vnext_prospective_observation.json
```

Der Beobachtungsreport bleibt wie die großen Elliott-Captures im isolierten
Branch:

`elliott-vnext-shadow-data`

Er wird nicht als produktive Evidenz nach `main` transportiert.

## Harte Grenzen

Stage 4 bestätigt explizit:

- research-only
- nur prospektive Inputs
- keine Markt-Outcomes zur Feature-Bildung
- keine rückwirkende Reklassifikation
- keine Änderung des Elliott-Core
- kein Degree-Reducer
- keine Auswahl eines Szenarios nach späterem Erfolg
- keine neuen Review-Kontexte
- keine Änderung der Universal Stance
- keine Änderung der Portfolio Action
- keine Order-/Execution-Wirkung
- keine produktive Promotion
- keine empirische Schlussfolgerung allein aus dem Beobachtungsreport

## Umgang mit unzureichender Historie

Mit weniger als zwei effektiven prospektiven Captures lautet der Status:

`INSUFFICIENT_PROSPECTIVE_HISTORY`

Existieren mehrere Captures, aber keine Dimension mit mindestens zwei
tatsächlich verfügbaren 6H-Ausgaben:

`INSUFFICIENT_COMPARABLE_OUTPUTS`

Erst wenn echte Vergleiche möglich sind:

`OBSERVATION_ACTIVE`

Diese Statuswerte sind keine Qualitätsurteile über Elliott.

## Reparierte Legacy-Captures

Der bekannte leere Pre-v2-Capture bleibt auditierbar im Archiv.

Wenn ein reparierter Capture ihn explizit über
`repair.supersedes_capture_ids` ersetzt, wird der alte Datensatz für die
Stage-4-Beobachtung nicht als zweite echte Beobachtung gezählt.

Es findet kein historisches Umschreiben statt.

## Aktueller Umfang

Stage 4 automatisiert zunächst ausschließlich Scenario Stability.

Noch **nicht** Gegenstand dieser Stufe:

- Treffer-/Fehlerraten nach späterem Kursverlauf
- Kosten-Nutzen-Auswertung
- Regimeanalyse
- B5-vs-B6-Inkrementalwert
- Degree-Ranking
- automatische Primary-Scenario-Auswahl
- Promotion einzelner Elliott-Komponenten

Diese Auswertungen benötigen zunächst ausreichend viele echte prospektive
Beobachtungen.

## Relevante Dateien

- `src/scanner/research/elliott_vnext/prospective_observation.py`
- `scripts/run_elliott_prospective_observation.py`
- `tests/test_elliott_vnext_prospective_observation.py`
- `.github/workflows/elliott_vnext_prospective_capture.yml`
- `src/scanner/research/governance/qm_g_scenario_stability.py`

## Abschlusskriterium Stage 4 – Beobachtungsfundament

Das Beobachtungsfundament ist technisch fertig, wenn:

- jeder neue echte Elliott-Capture den Report deterministisch aktualisiert,
- nur `prospective_unspent`-Captures zugelassen werden,
- superseded Legacy-Leercaptures nicht doppelt zählen,
- Scenario Stability ausschließlich für identische
  `(symbol, timeframe, degree)`-Dimensionen berechnet wird,
- fehlende Historie explizit unzureichend bleibt,
- der Report im Shadow-Branch isoliert bleibt,
- keinerlei Decision-/Order-/Promotion-Wirkung entsteht.

Danach lautet der Betriebsmodus:

**laufen lassen, prospektive Beobachtungen sammeln und erst bei ausreichender
Datenmenge die vorab definierten empirischen Auswertungen starten.**
