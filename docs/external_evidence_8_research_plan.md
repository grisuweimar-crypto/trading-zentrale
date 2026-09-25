# Phase 8 – External Evidence Layer: Research- und Bauplan

Status: vorbereitet. Phase 7A–7I ist technisch abgeschlossen; der Phase-7I-Validierungsvertrag wurde am 2026-09-25 eingefroren. Die eigentliche Phase-8-Implementierung beginnt erst nach gesondertem Startauftrag.

## Ziel

Phase 8 prüft externe Informationsfamilien ausschließlich auf inkrementellen Point-in-Time-Zusatznutzen gegenüber dem eingefrorenen Phase-7-Core.

Verbindliche Baseline:
- `configs/decision_validation_promotion_v1.json`
- Freeze: `2026-09-25`
- `prospective_unspent` ab: `2026-09-26`

## 8A – External Source & PIT Contract

Ziel:
- Quelleninventar aufbauen,
- PIT-Tauglichkeit nach Datenfamilie prüfen,
- Lizenz- und Kostenstatus dokumentieren,
- Coverage/History/Publication/Vintage/Restatement erfassen,
- `external_source_registry_v1` befüllen,
- ein As-of-Universe-/Coverage-Ledger definieren, das historische Mitgliedschaft, Ein-/Ausschlussgründe, Listing/Delisting und damalige Datenverfügbarkeit nachvollziehbar macht.

Abnahme:
- Quelle ist entweder `SAFE`, `PARTIAL`, `UNSAFE` oder `UNKNOWN`,
- nur `SAFE` und ausdrücklich begründete `PARTIAL`-Quellen dürfen in Research-Datasets gelangen,
- aktuelle Werte ohne historische Vintages dürfen nicht rückprojiziert werden,
- aktuelle Universe-Mitgliedschaft darf historische Zugehörigkeit nicht ersetzen.

## 8B – Revisions Single-Family Pilot

Erste Priorität, sofern 8A eine brauchbare Quelle findet.

Kandidaten:
- EPS revision 7d / 30d,
- Revenue revision 30d,
- Target revision,
- Upgrade/Downgrade balance.

Pflicht:
- echte historische Konsens-Snapshots,
- `published_at`/`valid_from`,
- kein heutiger Konsens rückwirkend,
- Domain- und Coverage-Gating.

Vergleich:
`Frozen Phase7 Core` vs. `Phase7 Core + Revisions`.

## 8C – Fundamentals & Structured Corporate Events

Fundamental Change statt statischer Bewertung.

Kandidaten:
- Umsatzwachstum Beschleunigung/Verlangsamung,
- Margin Change,
- FCF Change,
- Leverage Change,
- Guidance Raise/Cut,
- Dividend Change,
- Buyback,
- Capital Raise,
- Earnings Beat/Miss soweit timestampbar.

Preliminary/Final/Restated getrennt behandeln.

## 8D – Positioning / Crowding

Kandidaten:
- Short Interest,
- Short Interest Change,
- Days to Cover,
- Borrow Rate sofern historisch sauber,
- Insider Activity,
- weitere Positionierungsreihen nur bei PIT-fähiger Historie.

Absolute hohe Werte sind keine vorab festgelegte Richtungsaussage. Änderungen und Interaktion mit Phase-7-Zuständen werden getestet.

## 8E – Structured Events & News

Zunächst strukturierte, timestampbare Ereignisse; generisches Sentiment nachrangig.

Event-Taxonomie z. B.:
- guidance_raise / guidance_cut,
- regulatory_approval / rejection,
- major_contract,
- acquisition / takeover_offer,
- capital_raise,
- litigation,
- management_change,
- product_launch,
- production_disruption.

First Public Release und Source Hierarchy sind Pflicht.

## 8F – Macro & Exposure Context

Nur mit versionierter Exposure Map.

Mögliche Faktoren:
- rates / yield curve,
- inflation,
- FX,
- oil / gas,
- gold / silver,
- uranium,
- copper,
- lithium,
- weitere branchenspezifische Reihen.

Kein theoretisches Exposure darf ohne dokumentierte Zuordnung als Evidenz gelten.

## 8G – Incremental Evidence Research

Jede Familie einzeln gegen Phase 7 testen.

Pflichtmetriken:
- inkrementelle Trennschärfe / Outcome-Verbesserung,
- OOS-Performance,
- Regime-Stabilität,
- Domain-Stabilität,
- Coverage-Sensitivität,
- Missingness-Robustheit,
- Turnover-/Cost-Effekt wenn handlungsrelevant,
- Unsicherheit und Stichprobengröße,
- Multiple-Testing-Kontrolle,
- Konzentration nach Datum, Symbol und soweit sinnvoll Sektor/Domain.

Promotion nur nach bestandenen Gates.

## Research Governance

Vor confirmatory Tests werden Hypothesen familienweise registriert und eingefroren. Exploratory und confirmatory Analysen bleiben getrennt.

Menschliche Outcome-Inspektionen, die zu einer Regel-, Schwellen- oder Featureänderung führen, werden protokolliert. Die dabei betrachtete Evidenz gilt anschließend als `spent_for_design` und darf nicht erneut als unabhängige Bestätigung verwendet werden.

Präventive QA-Änderungen ohne Betrachtung zukünftiger Outcomes dürfen dokumentiert werden, ohne automatisch die prospektive Evidenz zu verbrauchen.

## 8H – Cross-Factor Interaction

Erst nach Promotion einzelner Familien.

Beispiele:
- Timing × EPS Revision,
- Elliott × Guidance,
- Selection × Short-Interest-Change,
- Sector/Market × Commodity Exposure,
- Core Stance × External Conflict.

Interaktionen werden vorab registriert und dürfen keine erneute ungehemmte Feature-Suche eröffnen. Eine Interaktion darf weder Phase-7-Elliott noch eine andere unpromotete Komponente rückwirkend durch Proxy „validieren“.

## 8I – Decision Layer Extension

Erst nach 8G/8H.

Phase 7 wird nicht überschrieben, sondern um einen separaten `external_evidence`-Block erweitert.

Zu prüfen:
- wie `CONFIRMING`, `CONFLICTING`, `EXTERNAL_ONLY`, `MIXED_EXTERNAL`, `UNKNOWN`, `INSUFFICIENT_EXTERNAL` die Decision Reliability beeinflussen,
- ob und wann sie eine erneute Stance-Berechnung rechtfertigen,
- ob der externe Mehrwert prospektiv stabil bleibt.

Keine direkte Order-/Trade-Entscheidung aus externer Evidenz.

## Promotionskriterien pro Familie

Alle müssen erfüllt sein:
1. PIT sauber
2. ausreichende Coverage in definierter Domäne
3. inkrementeller OOS-Mehrwert vs. Phase 7
4. Regime-Stabilität
5. Robustheit gegen realistische Datenlücken
6. vertretbarer Daten-/Handelskosten-Nutzen
7. Multiple-Testing-Schutz
8. nachvollziehbare Provenance und Lizenzlage
9. As-of-Universe-/Survivorship-Nachweis für die verwendete Research-Domäne
10. dokumentierter Evidence-Consumption-Status der confirmatory Evidenz

## Empfohlene Reihenfolge

`8A -> 8B -> 8C -> 8D -> 8E -> 8F -> 8G -> 8H -> 8I`

Praktisch darf 8C vorgezogen werden, falls Revisionsdaten in 8A keine ausreichend saubere Historie oder keine vertretbare Lizenz-/Kostenstruktur bieten.
