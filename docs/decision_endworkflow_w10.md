# W10 – Decision Endworkflow / Same-Snapshot Orchestration

W10 verbindet die bereits gebauten Module zu einem zeitlich kontrollierten Endworkflow. Es führt **keine neue Investmentlogik** ein und ändert weder die Fachlogik der Phasen 2–6 noch 7D–7H.

## Zielkette

Für einen Scanner-Snapshot `X` gilt:

`Scanner X -> daily_research X -> Phase 2 X -> Phase 3 X -> Phase 4/5 -> Phase 6 boundary -> finaler 7A-Build X -> 7A-Archiv -> 7D -> 7E -> 7F -> 7G -> 7H`

Der öffentliche Repository-Workflow endet nach dem versiegelten 7A-Archiv. Die bereits vorhandene private Depot-Watch-Orchestrierung übernimmt danach 7D–7H mit privaten Positionsdaten. Positionsdaten werden nicht in das Repository oder in öffentliche Workflow-Artefakte geschrieben.

## W10-Manifest

`artifacts/research/decision_snapshot_w10.json` verwendet das Schema
`decision_snapshot_orchestration_w10_v1`.

Vor dem finalen 7A-Build müssen folgende Stufen aufgelöst sein:

- `scanner_daily_research`
- `phase2_probability`
- `phase3_risk`
- `phase4_confidence`
- `phase5_governance`
- `phase6_elliott`

Danach wird das Upstream-Set unveränderlich eingefroren. Erst **nach** diesem Freeze darf der finale 7A-Build stattfinden.

## Zeitregeln

W10 unterscheidet zwei Arten von Verfügbarkeit:

1. Bereits persistierte Artefakte erhalten `available_from` aus der realen Git-Commit-Zeit ihres Source-Commits.
2. Im aktuellen Workflow erzeugte Artefakte erhalten `available_from` erst unmittelbar nach ihrer tatsächlichen Erzeugung durch den W10-Recorder.

Ein Runtime-Caller kann keinen früheren Zeitstempel übergeben. Nach dem Pre-7A-Freeze können keine weiteren vorgelagerten Artefakte in denselben W10-Manifestzustand aufgenommen werden.

Der finale 7A-Build läuft mit `--force-revision`. Damit kann kein bereits vor dem W10-Freeze erzeugtes 7A-Paket als vermeintlich finales Paket wiederverwendet werden.

## Phase 5

Phase 5E bleibt Governance-/Shadow-Kontext und ist kein Markt-Snapshot-Artefakt. W10 bindet deshalb seine echte Git-Verfügbarkeit ein, behauptet aber **keine** Same-Snapshot-Identität für das Phase-5-Governance-Artefakt.

## Phase 6

W10 erzeugt keine Elliott-Evidence künstlich. Ein gültiger PIT-gestempelter
`decision_elliott_6h_source_v1` kann aufgelöst werden. Liegt kein solcher Source-Envelope vor, wird Phase 6 explizit als `not_supplied` dokumentiert.

`not_supplied` bedeutet:

- keine neutrale Evidence,
- keinen Direction-Vote,
- keine Universal-Stance-Änderung,
- keinen Portfolio-Action-Effekt.

Ein vorhandener gültiger Phase-6-Source bleibt gemäß dem vorhandenen W6-Vertrag Review-Kontext downstream von 7D/7E und wirkt nur am bereits definierten 7F-Review-Gate.

## Finaler 7A-Freeze und Seal

Nach dem Upstream-Freeze wird 7A neu gebaut und das 7A-Archiv geschrieben. Anschließend prüft W10:

- `snapshot_id` des finalen 7A-Pakets entspricht dem W10-Snapshot,
- `7A.as_of >= pre_7a_frozen_at`,
- `7A.as_of <= sealed_at`,
- keine vorgelagerte Evidence liegt zeitlich hinter dem Freeze,
- keine Evidence liegt hinter dem Seal,
- der No-Backdating-Guard ist aktiv,
- private Positionsdaten sind nicht persistiert.

Danach erhält der Manifestzustand `sealed`.

## Private 7D–7H-Kette

`scripts/run_depot_watch_orchestrated.py` akzeptiert die private Depot-Watch-Kette nur noch mit einem versiegelten W10-Manifest für denselben `snapshot_id` wie `daily_research`.

Die vorhandene Orchestrierung bleibt unverändert in ihrer Fachreihenfolge:

`7D Universal Stance -> 7E State Transition -> 7F Portfolio Action / Review -> 7G Reliability & Explainability -> 7H Watch`

W10 erzeugt keine Broker-Order und persistiert keine privaten Positionsdaten.

## Abgrenzung zu W11

W10 baut und prüft den Orchestrierungsvertrag. Der echte vollständige Lauf eines realen Snapshots durch die gesamte Kette einschließlich der End-to-End-Prüfungen ist Aufgabe von W11.
