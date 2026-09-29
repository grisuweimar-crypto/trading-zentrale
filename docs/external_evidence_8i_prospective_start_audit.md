# Phase 8I-E – Prospective Start Audit

## Zweck

Dieser Governance-Block dokumentiert eine eng begrenzte, outcome-blinde Ausnahme für den Start der prospektiven 8I-E-Evidenz. Er verschiebt **nicht** den allgemeinen Starttermin und verändert weder Phase 7 noch die eingefrorene 8I-E-Hypothesenfamilie.

## Unveränderter Standardstart

Der im 8I-E-Vertrag eingefrorene Standardstart bleibt:

`2026-09-29T00:00:00+00:00`

Es gibt **keine** pauschale Rückdatierung auf den 18.09. oder auf den 28.09. um 00:00 UTC.

## 18.–27.09. und sonstige Vorstart-Beobachtungen

Beobachtungen ab dem 18.09. vor dem Standardstart werden grundsätzlich als `AUDITED_PRESTART_SHADOW` behandelt. Sie dürfen für PIT-, Datenverfügbarkeits-, Latenz- und Operational-Audits verwendet werden, zählen aber nicht zur confirmatory 8I-E-Familie und nicht zu deren Mindeststichprobe.

Das verhindert, dass später eingefrorene Regeln künstlich zu bereits vergangenen Beobachtungen als vermeintlich prospektive Evidenz zurückdatiert werden.

## Exakte Ausnahme vom 28.09.2026

Nur folgender Snapshot ist als `ADMITTED_FIRST_PROSPECTIVE` zugelassen:

- Snapshot/Attempt: `e2444010-ad30-4c08-af92-037bb0cc28d6`
- Start: `2026-09-28T16:17:13.819811+00:00`
- Generiert: `2026-09-28T16:17:31.855174+00:00`
- 213/213 Ticker vollständig
- Scanner-Commit: `efd4d60fd9fa1ca57555f9d68f2515995bc4439b`
- `history_metadata.json` Blob: `78097bc67ab4d2d2d727d5ebc634aade912f99a6`
- `latest_scanner.csv` Blob: `92463ee9cb2ca570659a99f5afcbf648d00f96bb`
- Run-Quelle: `github-36449782706-1`

Der 8I-E-Vertrag war bereits am `2026-09-28T15:32:28+00:00` eingefroren; auch 8I-D war vorher abgeschlossen. Der Snapshot entstand damit nach dem relevanten Regel-Freeze.

Ein anderer Snapshot vom 28.09. wird **nicht** durch das Datum allein zugelassen. Die Ausnahme ist vollständig an die obige Identität und Provenienz gebunden.

## Source-Identity-Korrektur

Der bekannte Aliasfehler wird mit `external_evidence_upstream_source_identity_correction_v1` versioniert und outcome-blind aufgelöst:

- `FED_H15` → `federal_reserve_board_h15`
- `ECB_EXR` → `ecb_data_portal`

Die Korrektur ist ausschließlich eine Identitätsauflösung. Sie darf insbesondere keine beobachteten Werte, Faktorwerte, Prognosen, Modell-Hashes, Vorzeichenlogik, Schwellen, Horizonte, Mapping-Semantik, Outcomes, Evidence-Consumption-Zustände oder historische Promotion-Receipts ändern. Historische 8G-Artefakte werden nicht umgeschrieben.

## Was die Ausnahme erlaubt – und was nicht

Die Ausnahme macht den gepinnten Snapshot **grundsätzlich prospektiv zulässig**, sofern später zusätzlich alle normalen Binding-, Annotation-, PIT-, Maturity- und Coverage-Gates erfüllt sind.

Sie:

- erzeugt selbst keine gültige 8I-E-Annotation,
- erhöht die reale Stichprobe nicht automatisch,
- öffnet keine Outcomes,
- macht 8I-E nicht empirisch vollständig,
- aktiviert keine Extended Reliability,
- verändert keine Universal Stance,
- verändert keine Portfolio Action,
- erzeugt keine Orders oder Trades.

Ab dem 29.09. gilt wieder der normale `STANDARD_PROSPECTIVE`-Pfad.

## Technische Guardrails

`prospective_start_audit_8i.py` prüft:

1. den eingebetteten SHA-256 der Source-Identity-Korrektur,
2. die exakte Alias→Canonical-Abbildung gegen den eingefrorenen 8I-B-Vertrag,
3. den SHA-256 des Start-Audits,
4. die unveränderte Standardgrenze 29.09. 00:00 UTC,
5. die komplette Provenienz des einzigen zugelassenen 28.09.-Snapshots,
6. die zeitliche Reihenfolge Freeze → Snapshot,
7. outcome-blinde Eingaben,
8. die fortbestehende Trennung von Reliability, Stance, Portfolio Action und Trading.

Jede Abweichung beim exakten Ausnahmesnapshot schlägt fail-closed fehl. Andere Vorstart-Snapshots bleiben Shadow.
