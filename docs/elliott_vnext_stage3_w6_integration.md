# Elliott vNext – Stage 3: W6/W10 Review-Integration

## Ziel

Stage 3 aktiviert die bereits vorbereitete W6/W10-Integration von Elliott vNext
als **Research-only Review-Kontext** für 7F.

Sie ist keine produktive Promotion von Elliott und keine autonome
Handelsentscheidung. Die bestehende Entscheidungsreihenfolge bleibt:

`7A -> 7D Universal Stance -> 7E Hysterese -> W6 Elliott Review Context -> 7F Portfolio Action -> W8 -> 7G -> 7H`

Elliott darf in dieser Stufe nur bereits in 6D/6H vorhandene Review-Kontexte
transportieren.

## Verwendete Quelle

Grundlage ist ausschließlich der prospektive Stage-1-Capture:

`elliott_vnext_prospective_capture_v1`

aus dem isolierten Branch `elliott-vnext-shadow-data`.

Für W6 wird daraus zur Laufzeit das bereits vorhandene Envelope

`decision_elliott_6h_source_v1`

gebaut. Der Source wird **nicht** als große tägliche Duplikatdatei auf `main`
archiviert. W10 speichert nur die Provenienz und den Integrationsstatus.

## Point-in-Time- und Snapshot-Bindung

Ein Capture darf Stage 3 nur erreichen, wenn

- `capture.snapshot_id == daily_research.snapshot_id`,
- `capture.as_of == daily_research.as_of`,
- der Capture bereits verfügbar war,
- der daraus gebaute W6-Source vor dem W10-`pre_7a_frozen_at` verfügbar ist.

W10 speichert bei verfügbarer Phase 6 mindestens:

- `source_commit`,
- `available_from`,
- `source_capture_id`,
- `output_count`,
- `symbol_count`,
- `snapshot_identity_state = verified`.

Fehlt der Capture oder gehört er zu einem anderen Snapshot, bleibt
`phase6_elliott = not_supplied`. Fehlend bedeutet weiterhin **keine neutrale
Evidenz**.

## Mehrere Wellengrade

Der Modul-6-Bauplan erlaubt ausdrücklich mehrere Wellengrade gleichzeitig.
Stage 1 bewahrt daher alle verfügbaren `(symbol, timeframe, degree)`-Outputs.

Für Stage 3 wird **kein "bester" Wellengrad ausgewählt**.

W6 gruppiert alle validierten 6H-Ausgaben eines Symbols und bildet ausschließlich
die Vereinigung der bereits vorhandenen 6D-Review-Kontexte.

Dabei gilt:

- keine Gewichtung der Wellengrade,
- keine Scorebildung,
- keine Richtungsaggregation,
- keine Auswahl nach späterem Erfolg,
- kein Fib-basierter Count-Wechsel,
- kein Reducer.

Wenn unterschiedliche Grade Add- und Reduce-Kontexte liefern, bleibt der
Konflikt erhalten. Die bestehende 7F-Regel behandelt diesen Konflikt
fail-closed, statt willkürlich eine Seite auszuwählen.

Zur Auditierung enthält der W6-Kontext:

- alle `source_output_ids`,
- alle verwendeten Timeframe-/Degree-Paare,
- Gesamtzahl der 6H-Ausgaben und Routen,
- ausgelassene `hold_review`-Routen,
- `multi_degree_reducer_used = false`,
- `all_available_degrees_aggregated = true`,
- `direction_from_elliott_used = false`,
- `changes_universal_stance = false`.

## Transportierte Review-Kontexte

W6 transportiert weiterhin nur die bereits eingefrorenen action-relevanten
Review-Kontexte:

- `entry_or_add_review`
- `reentry_or_add_review`
- `partial_reduce_review`
- `profit_protection_review`
- `larger_reduce_or_exit_review`

`hold_review` bleibt gültige 6D-Forschungsevidenz, hat aber keinen
7F-Aktionseffekt und wird daher nur auditierbar gezählt.

## Wirkung in 7F

Elliott darf die Universal Stance nicht ändern und wird nicht als
Richtungs-Vote verwendet.

Die bereits vorhandene 7F-Logik bleibt maßgeblich:

- bestätigte positive Long-Stance + Reduce-Kontext -> `REDUCE_REVIEW`;
- bestätigte positive Long-Stance + Add-Kontext + explizite Kapazität ->
  `ADD_REVIEW`;
- gleichzeitiger Add- und Reduce-Kontext -> Konflikt bleibt erhalten, keine
  willkürliche Auswahl;
- negative Long-Stance bleibt unabhängig von Elliott beim bestehenden
  Exit-Review-Pfad;
- `hold_review` allein erzeugt keinen neuen Action-State.

Alle States bleiben Review-Zustände. Es wird keine Order erzeugt.

## Workflow-Orchestrierung

`Decision Watch Integration Pipeline` wird zusätzlich nach erfolgreichem

`Module 6 Elliott Prospective Shadow`

ausgelöst.

Damit kann ein früherer W10-Lauf desselben Scanner-Snapshots zunächst korrekt
`not_supplied` sein. Sobald der prospektive Elliott-Capture wirklich vorliegt,
wird derselbe aktuelle Scanner-Snapshot erneut kausal integriert und Phase 6
vor dem neuen 7A-Freeze als `available` gebunden.

Es findet keine rückwirkende Rekonstruktion älterer Scanner-Snapshots statt.

## Watch Runtime

`Decision Watch Runtime Publish` rekonstruiert bei
`phase6_elliott = available` exakt den von W10 eingefrorenen W6-Source anhand
von:

- `source_commit`,
- `available_from`,
- `source_capture_id`.

Ein späterer oder anderer Capture darf nicht still in einen bereits versiegelten
W10-Snapshot eingeschleust werden.

Der öffentliche `public_long_reference`-Pfad verwendet danach denselben
Research-only W6-Kontext. Damit kann die Depot-Watch die daraus resultierenden
Review-Zustände sehen, ohne private Positionen zu veröffentlichen.

## Private Depot-Watch

`run_depot_watch_orchestrated.py` akzeptiert weiterhin einen expliziten
`decision_elliott_6h_source_v1` und zusätzlich einen prospektiven Capture.

Wenn W10 Phase 6 als `available` eingefroren hat, muss der private Lauf den
exakt dazugehörigen Source/Capture verwenden. Ein späteres Elliott-Injizieren
in einen W10-Snapshot, der Phase 6 als `not_supplied` eingefroren hat, wird
abgelehnt.

## Unveränderte harte Grenzen

Stage 3 ändert nicht:

- 6A–6H Elliott-Regeln,
- Szenario-/Count-Auswahl,
- Fibonacci-Regeln,
- Universal Stance,
- 7E Hysterese,
- W8 Action Matrix,
- Broker-/Orderlogik,
- produktive Promotion.

Weiterhin gilt:

- `research_only = true`,
- `productive_integration_enabled = false`,
- `direct_ordering_allowed = false`,
- `elliott_direction_used_as_vote = false`,
- `changes_universal_stance = false`.

Stage 3 aktiviert damit den bereits vorgesehenen **Positionsmanagement-Sensor**
im Decision Layer, ohne Elliott zur eigenständigen Entscheidungsinstanz zu
machen.
